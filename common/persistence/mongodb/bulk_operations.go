package mongodb

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
)

// BulkProcessor handles bulk operations with batching and optimization
type BulkProcessor struct {
	client     *mongo.Client
	logger     log.Logger
	batchSize  int
	batchDelay time.Duration
	workers    int
	queue      chan BulkOperation
	wg         sync.WaitGroup
	ctx        context.Context
	cancel     context.CancelFunc
}

// BulkOperation represents a bulk operation to be processed
type BulkOperation struct {
	Collection string
	Operations []Operation
	Callback   func(error)
}

// NewBulkProcessor creates a new bulk processor
func NewBulkProcessor(client *mongo.Client, logger log.Logger, batchSize int, batchDelay time.Duration, workers int) *BulkProcessor {
	ctx, cancel := context.WithCancel(context.Background())

	processor := &BulkProcessor{
		client:     client,
		logger:     logger,
		batchSize:  batchSize,
		batchDelay: batchDelay,
		workers:    workers,
		queue:      make(chan BulkOperation, 1000), // Buffer size
		ctx:        ctx,
		cancel:     cancel,
	}

	// Start worker goroutines
	for i := 0; i < workers; i++ {
		processor.wg.Add(1)
		go processor.worker(i)
	}

	return processor
}

// worker processes bulk operations
func (bp *BulkProcessor) worker(id int) {
	defer bp.wg.Done()

	var batch []BulkOperation
	ticker := time.NewTicker(bp.batchDelay)
	defer ticker.Stop()

	for {
		select {
		case op := <-bp.queue:
			batch = append(batch, op)

			// Process batch if it reaches the size limit
			if len(batch) >= bp.batchSize {
				bp.processBatch(batch)
				batch = batch[:0] // Reset slice
			}

		case <-ticker.C:
			// Process batch after delay if there are operations
			if len(batch) > 0 {
				bp.processBatch(batch)
				batch = batch[:0] // Reset slice
			}

		case <-bp.ctx.Done():
			// Process remaining operations before shutting down
			if len(batch) > 0 {
				bp.processBatch(batch)
			}
			return
		}
	}
}

// processBatch processes a batch of bulk operations
func (bp *BulkProcessor) processBatch(batch []BulkOperation) {
	// Group operations by collection
	collections := make(map[string][]Operation)
	for _, bulkOp := range batch {
		collections[bulkOp.Collection] = append(collections[bulkOp.Collection], bulkOp.Operations...)
	}

	// Process each collection
	for collectionName, operations := range collections {
		if err := bp.processCollectionOperations(collectionName, operations); err != nil {
			bp.logger.Error("failed to process bulk operations",
				tag.NewStringTag("collection", collectionName),
				tag.Error(err))

			// Notify callbacks of the error
			for _, bulkOp := range batch {
				if bulkOp.Collection == collectionName && bulkOp.Callback != nil {
					bulkOp.Callback(err)
				}
			}
		}
	}

	// Notify successful callbacks
	for _, bulkOp := range batch {
		if bulkOp.Callback != nil {
			bulkOp.Callback(nil)
		}
	}
}

// processCollectionOperations processes operations for a specific collection
func (bp *BulkProcessor) processCollectionOperations(collectionName string, operations []Operation) error {
	if len(operations) == 0 {
		return nil
	}

	collection := bp.client.Database("temporal").Collection(collectionName)

	// Convert operations to bulk write models
	models := make([]mongo.WriteModel, 0, len(operations))
	for _, op := range operations {
		switch op.Type {
		case OperationInsert:
			models = append(models, mongo.NewInsertOneModel().SetDocument(op.Document))
		case OperationUpdate:
			models = append(models, mongo.NewUpdateOneModel().SetFilter(op.Filter).SetUpdate(op.Document))
		case OperationUpsert:
			models = append(models, mongo.NewUpdateOneModel().SetFilter(op.Filter).SetUpdate(op.Document).SetUpsert(true))
		case OperationDelete:
			models = append(models, mongo.NewDeleteOneModel().SetFilter(op.Filter))
		}
	}

	// Execute bulk write with retry logic
	return bp.executeBulkWriteWithRetry(collection, models)
}

// executeBulkWriteWithRetry executes bulk write with retry logic
func (bp *BulkProcessor) executeBulkWriteWithRetry(collection *mongo.Collection, models []mongo.WriteModel) error {
	maxRetries := 3
	backoff := time.Millisecond * 100

	for attempt := 0; attempt < maxRetries; attempt++ {
		opts := options.BulkWrite().SetOrdered(false) // Unordered for better performance
		_, err := collection.BulkWrite(bp.ctx, models, opts)

		if err == nil {
			return nil
		}

		// Check if it's a retryable error
		if !isRetryableError(err) || attempt == maxRetries-1 {
			return fmt.Errorf("bulk write failed after %d attempts: %w", attempt+1, err)
		}

		bp.logger.Warn("retryable bulk write error, retrying",
			tag.Error(err),
			tag.NewInt("attempt", attempt+1))
		time.Sleep(backoff)
		backoff *= 2 // Exponential backoff
	}

	return fmt.Errorf("bulk write failed after %d attempts", maxRetries)
}

// AddOperation adds an operation to the bulk processor
func (bp *BulkProcessor) AddOperation(collection string, opType OperationType, filter, document interface{}, callback func(error)) {
	op := BulkOperation{
		Collection: collection,
		Operations: []Operation{{
			Collection: collection,
			Type:       opType,
			Filter:     filter,
			Document:   document,
		}},
		Callback: callback,
	}

	select {
	case bp.queue <- op:
		// Operation queued successfully
	case <-bp.ctx.Done():
		// Processor is shutting down
		if callback != nil {
			callback(errors.New("bulk processor is shutting down"))
		}
	}
}

// AddBulkOperation adds multiple operations to the bulk processor
func (bp *BulkProcessor) AddBulkOperation(collection string, operations []Operation, callback func(error)) {
	op := BulkOperation{
		Collection: collection,
		Operations: operations,
		Callback:   callback,
	}

	select {
	case bp.queue <- op:
		// Operations queued successfully
	case <-bp.ctx.Done():
		// Processor is shutting down
		if callback != nil {
			callback(errors.New("bulk processor is shutting down"))
		}
	}
}

// Close closes the bulk processor
func (bp *BulkProcessor) Close() {
	bp.cancel()
	bp.wg.Wait()
	close(bp.queue)
}

// OptimizedBulkWriter provides optimized bulk writing capabilities
type OptimizedBulkWriter struct {
	processor *BulkProcessor
	client    *mongo.Client
	logger    log.Logger
}

// NewOptimizedBulkWriter creates a new optimized bulk writer
func NewOptimizedBulkWriter(client *mongo.Client, logger log.Logger) *OptimizedBulkWriter {
	processor := NewBulkProcessor(client, logger, 1000, 100*time.Millisecond, 4)

	return &OptimizedBulkWriter{
		processor: processor,
		client:    client,
		logger:    logger,
	}
}

// BulkInsert performs optimized bulk insert operations
func (obw *OptimizedBulkWriter) BulkInsert(ctx context.Context, collection string, documents []interface{}) error {
	if len(documents) == 0 {
		return nil
	}

	// For large batches, split into smaller chunks
	const maxBatchSize = 1000
	if len(documents) <= maxBatchSize {
		return obw.insertBatch(ctx, collection, documents)
	}

	// Split into chunks and process in parallel
	chunks := obw.chunkDocuments(documents, maxBatchSize)
	errChan := make(chan error, len(chunks))

	for _, chunk := range chunks {
		go func(docs []interface{}) {
			errChan <- obw.insertBatch(ctx, collection, docs)
		}(chunk)
	}

	// Collect results
	for i := 0; i < len(chunks); i++ {
		if err := <-errChan; err != nil {
			return err
		}
	}

	return nil
}

// insertBatch inserts a batch of documents
func (obw *OptimizedBulkWriter) insertBatch(ctx context.Context, collection string, documents []interface{}) error {
	coll := obw.client.Database("temporal").Collection(collection)

	// Use ordered=false for better performance
	opts := options.InsertMany().SetOrdered(false)
	_, err := coll.InsertMany(ctx, documents, opts)
	return err
}

// BulkUpdate performs optimized bulk update operations
func (obw *OptimizedBulkWriter) BulkUpdate(ctx context.Context, collection string, updates []BulkUpdate) error {
	if len(updates) == 0 {
		return nil
	}

	// Convert to bulk write models
	models := make([]mongo.WriteModel, 0, len(updates))
	for _, update := range updates {
		model := mongo.NewUpdateOneModel().
			SetFilter(update.Filter).
			SetUpdate(update.Update)

		if update.Upsert {
			model.SetUpsert(true)
		}

		models = append(models, model)
	}

	return obw.executeBulkWrite(ctx, collection, models)
}

// BulkUpsert performs optimized bulk upsert operations
func (obw *OptimizedBulkWriter) BulkUpsert(ctx context.Context, collection string, upserts []BulkUpsert) error {
	if len(upserts) == 0 {
		return nil
	}

	// Convert to bulk write models
	models := make([]mongo.WriteModel, 0, len(upserts))
	for _, upsert := range upserts {
		model := mongo.NewUpdateOneModel().
			SetFilter(upsert.Filter).
			SetUpdate(upsert.Update).
			SetUpsert(true)

		models = append(models, model)
	}

	return obw.executeBulkWrite(ctx, collection, models)
}

// BulkDelete performs optimized bulk delete operations
func (obw *OptimizedBulkWriter) BulkDelete(ctx context.Context, collection string, filters []interface{}) error {
	if len(filters) == 0 {
		return nil
	}

	// Convert to bulk write models
	models := make([]mongo.WriteModel, 0, len(filters))
	for _, filter := range filters {
		model := mongo.NewDeleteOneModel().SetFilter(filter)
		models = append(models, model)
	}

	return obw.executeBulkWrite(ctx, collection, models)
}

// executeBulkWrite executes a bulk write operation
func (obw *OptimizedBulkWriter) executeBulkWrite(ctx context.Context, collection string, models []mongo.WriteModel) error {
	coll := obw.client.Database("temporal").Collection(collection)

	// Use ordered=false for better performance
	opts := options.BulkWrite().SetOrdered(false)
	_, err := coll.BulkWrite(ctx, models, opts)
	return err
}

// chunkDocuments splits documents into chunks
func (obw *OptimizedBulkWriter) chunkDocuments(documents []interface{}, chunkSize int) [][]interface{} {
	var chunks [][]interface{}
	for i := 0; i < len(documents); i += chunkSize {
		end := i + chunkSize
		if end > len(documents) {
			end = len(documents)
		}
		chunks = append(chunks, documents[i:end])
	}
	return chunks
}

// BulkUpdate represents a bulk update operation
type BulkUpdate struct {
	Filter interface{}
	Update interface{}
	Upsert bool
}

// BulkUpsert represents a bulk upsert operation
type BulkUpsert struct {
	Filter interface{}
	Update interface{}
}

// Close closes the optimized bulk writer
func (obw *OptimizedBulkWriter) Close() {
	obw.processor.Close()
}
