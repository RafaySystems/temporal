package mongodb

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
)

// TransactionManager manages MongoDB transactions
type TransactionManager struct {
	client *mongo.Client
	logger log.Logger
}

// Transaction represents a MongoDB transaction
type Transaction struct {
	session    mongo.Session
	ctx        context.Context
	logger     log.Logger
	operations []Operation
}

// Operation represents a database operation within a transaction
type Operation struct {
	Collection string
	Type       OperationType
	Filter     interface{}
	Document   interface{}
	Options    *options.UpdateOptions
}

// OperationType represents the type of database operation
type OperationType int

const (
	OperationInsert OperationType = iota
	OperationUpdate
	OperationDelete
	OperationUpsert
)

// NewTransactionManager creates a new transaction manager
func NewTransactionManager(client *mongo.Client, logger log.Logger) *TransactionManager {
	return &TransactionManager{
		client: client,
		logger: logger,
	}
}

// BeginTransaction starts a new transaction
func (tm *TransactionManager) BeginTransaction(ctx context.Context) (*Transaction, error) {
	session, err := tm.client.StartSession()
	if err != nil {
		return nil, fmt.Errorf("failed to start session: %w", err)
	}

	// Start transaction
	if err := session.StartTransaction(); err != nil {
		session.EndSession(ctx)
		return nil, fmt.Errorf("failed to start transaction: %w", err)
	}

	return &Transaction{
		session:    session,
		ctx:        ctx,
		logger:     tm.logger,
		operations: make([]Operation, 0),
	}, nil
}

// AddOperation adds an operation to the transaction
func (t *Transaction) AddOperation(collection string, opType OperationType, filter, document interface{}, opts *options.UpdateOptions) {
	t.operations = append(t.operations, Operation{
		Collection: collection,
		Type:       opType,
		Filter:     filter,
		Document:   document,
		Options:    opts,
	})
}

// AddInsertOperation adds an insert operation
func (t *Transaction) AddInsertOperation(collection string, document interface{}) {
	t.AddOperation(collection, OperationInsert, nil, document, nil)
}

// AddUpdateOperation adds an update operation
func (t *Transaction) AddUpdateOperation(collection string, filter, update interface{}, opts *options.UpdateOptions) {
	t.AddOperation(collection, OperationUpdate, filter, update, opts)
}

// AddUpsertOperation adds an upsert operation
func (t *Transaction) AddUpsertOperation(collection string, filter, update interface{}) {
	opts := options.Update().SetUpsert(true)
	t.AddOperation(collection, OperationUpsert, filter, update, opts)
}

// AddDeleteOperation adds a delete operation
func (t *Transaction) AddDeleteOperation(collection string, filter interface{}) {
	t.AddOperation(collection, OperationDelete, filter, nil, nil)
}

// Execute executes all operations in the transaction
func (t *Transaction) Execute() error {
	if len(t.operations) == 0 {
		return nil
	}

	// Execute operations within the transaction session
	err := mongo.WithSession(t.ctx, t.session, func(sessCtx mongo.SessionContext) error {
		for _, op := range t.operations {
			if err := t.executeOperation(sessCtx, op); err != nil {
				return fmt.Errorf("failed to execute operation on collection %s: %w", op.Collection, err)
			}
		}
		return nil
	})

	if err != nil {
		t.logger.Error("transaction execution failed", tag.Error(err))
		return err
	}

	return nil
}

// executeOperation executes a single operation within the transaction
func (t *Transaction) executeOperation(ctx context.Context, op Operation) error {
	collection := t.session.Client().Database("temporal").Collection(op.Collection)

	switch op.Type {
	case OperationInsert:
		_, err := collection.InsertOne(ctx, op.Document)
		return err

	case OperationUpdate:
		_, err := collection.UpdateOne(ctx, op.Filter, op.Document, op.Options)
		return err

	case OperationUpsert:
		_, err := collection.UpdateOne(ctx, op.Filter, op.Document, op.Options)
		return err

	case OperationDelete:
		_, err := collection.DeleteOne(ctx, op.Filter)
		return err

	default:
		return fmt.Errorf("unsupported operation type: %d", op.Type)
	}
}

// Commit commits the transaction
func (t *Transaction) Commit() error {
	if err := t.session.CommitTransaction(t.ctx); err != nil {
		t.logger.Error("failed to commit transaction", tag.Error(err))
		return fmt.Errorf("failed to commit transaction: %w", err)
	}
	return nil
}

// Abort aborts the transaction
func (t *Transaction) Abort() error {
	if err := t.session.AbortTransaction(t.ctx); err != nil {
		t.logger.Error("failed to abort transaction", tag.Error(err))
		return fmt.Errorf("failed to abort transaction: %w", err)
	}
	return nil
}

// Close closes the transaction session
func (t *Transaction) Close() {
	t.session.EndSession(t.ctx)
}

// ExecuteWithRetry executes a transaction with retry logic
func (tm *TransactionManager) ExecuteWithRetry(ctx context.Context, fn func(*Transaction) error) error {
	maxRetries := 3
	backoff := time.Millisecond * 100

	for attempt := 0; attempt < maxRetries; attempt++ {
		txn, err := tm.BeginTransaction(ctx)
		if err != nil {
			return fmt.Errorf("failed to begin transaction: %w", err)
		}

		// Execute the transaction function
		if err := fn(txn); err != nil {
			_ = txn.Abort()
			txn.Close()
			return err
		}

		// Execute operations
		if err := txn.Execute(); err != nil {
			_ = txn.Abort()
			txn.Close()

			// Check if it's a retryable error
			if isRetryableError(err) && attempt < maxRetries-1 {
				tm.logger.Warn("retryable transaction error, retrying",
					tag.Error(err),
					tag.NewInt("attempt", attempt+1))
				time.Sleep(backoff)
				backoff *= 2 // Exponential backoff
				continue
			}
			return err
		}

		// Commit the transaction
		if err := txn.Commit(); err != nil {
			txn.Close()

			// Check if it's a retryable error
			if isRetryableError(err) && attempt < maxRetries-1 {
				tm.logger.Warn("retryable commit error, retrying",
					tag.Error(err),
					tag.NewInt("attempt", attempt+1))
				time.Sleep(backoff)
				backoff *= 2 // Exponential backoff
				continue
			}
			return err
		}

		txn.Close()
		return nil
	}

	return fmt.Errorf("transaction failed after %d attempts", maxRetries)
}

// isRetryableError checks if an error is retryable
func isRetryableError(err error) bool {
	// Check for MongoDB retryable errors
	if mongo.IsDuplicateKeyError(err) {
		return false // Duplicate key errors are not retryable
	}

	// Check for network errors, timeouts, etc.
	// This is a simplified check - in a real implementation, you'd check for specific error codes
	return true
}
