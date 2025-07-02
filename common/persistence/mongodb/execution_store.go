package mongodb

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/tasks"
)

type (
	// ExecutionStore implements the ExecutionStore interface for MongoDB
	ExecutionStore struct {
		client     *mongo.Client
		database   *mongo.Database
		collection *mongo.Collection
		logger     log.Logger
	}

	// WorkflowExecutionDocument represents a workflow execution document in MongoDB
	WorkflowExecutionDocument struct {
		ShardID                int32     `bson:"shard_id"`
		NamespaceID            string    `bson:"namespace_id"`
		WorkflowID             string    `bson:"workflow_id"`
		RunID                  string    `bson:"run_id"`
		ExecutionInfo          []byte    `bson:"execution_info,omitempty"`
		ExecutionInfoEncoding  string    `bson:"execution_info_encoding,omitempty"`
		ExecutionState         []byte    `bson:"execution_state,omitempty"`
		ExecutionStateEncoding string    `bson:"execution_state_encoding,omitempty"`
		NextEventID            int64     `bson:"next_event_id"`
		DBRecordVersion        int64     `bson:"db_record_version"`
		CreatedAt              time.Time `bson:"created_at"`
		UpdatedAt              time.Time `bson:"updated_at"`
	}

	// HistoryNodeDocument represents a history node document in MongoDB
	HistoryNodeDocument struct {
		ShardID           int32     `bson:"shard_id"`
		BranchToken       []byte    `bson:"branch_token"`
		NodeID            int64     `bson:"node_id"`
		TransactionID     int64     `bson:"transaction_id"`
		PrevTransactionID int64     `bson:"prev_transaction_id"`
		Events            []byte    `bson:"events,omitempty"`
		EventsEncoding    string    `bson:"events_encoding,omitempty"`
		CreatedAt         time.Time `bson:"created_at"`
	}

	// HistoryTaskDocument represents a history task document in MongoDB
	HistoryTaskDocument struct {
		ShardID      int32     `bson:"shard_id"`
		TaskCategory string    `bson:"task_category"`
		TaskID       int64     `bson:"task_id"`
		Task         []byte    `bson:"task,omitempty"`
		TaskEncoding string    `bson:"task_encoding,omitempty"`
		FireTime     time.Time `bson:"fire_time,omitempty"`
		CreatedAt    time.Time `bson:"created_at"`
	}

	// CurrentExecutionDocument represents current execution document in MongoDB
	CurrentExecutionDocument struct {
		ShardID          int32     `bson:"shard_id"`
		NamespaceID      string    `bson:"namespace_id"`
		WorkflowID       string    `bson:"workflow_id"`
		RunID            string    `bson:"run_id"`
		StartRequestID   string    `bson:"start_request_id"`
		State            int32     `bson:"state"`
		Status           int32     `bson:"status"`
		LastWriteVersion int64     `bson:"last_write_version"`
		StartTime        time.Time `bson:"start_time"`
		CreatedAt        time.Time `bson:"created_at"`
		UpdatedAt        time.Time `bson:"updated_at"`
	}
)

// NewExecutionStore creates a new MongoDB execution store
func NewExecutionStore(database *mongo.Database, logger log.Logger) *ExecutionStore {
	return &ExecutionStore{
		client:     database.Client(),
		database:   database,
		collection: database.Collection("workflow_executions"),
		logger:     logger,
	}
}

// Close closes the execution store
func (s *ExecutionStore) Close() {
	// MongoDB client is managed by the factory
}

// GetName returns the name of the execution store
func (s *ExecutionStore) GetName() string {
	return "mongodb-execution-store"
}

// GetHistoryBranchUtil returns the history branch utility
func (s *ExecutionStore) GetHistoryBranchUtil() p.HistoryBranchUtil {
	return &HistoryBranchUtil{
		store: s,
	}
}

// CreateWorkflowExecution creates a new workflow execution
func (s *ExecutionStore) CreateWorkflowExecution(ctx context.Context, request *p.InternalCreateWorkflowExecutionRequest) (*p.InternalCreateWorkflowExecutionResponse, error) {
	now := time.Now()
	doc := &WorkflowExecutionDocument{
		ShardID:                request.ShardID,
		NamespaceID:            request.NewWorkflowSnapshot.NamespaceID,
		WorkflowID:             request.NewWorkflowSnapshot.WorkflowID,
		RunID:                  request.NewWorkflowSnapshot.RunID,
		ExecutionInfo:          request.NewWorkflowSnapshot.ExecutionInfoBlob.Data,
		ExecutionInfoEncoding:  request.NewWorkflowSnapshot.ExecutionInfoBlob.EncodingType.String(),
		ExecutionState:         request.NewWorkflowSnapshot.ExecutionStateBlob.Data,
		ExecutionStateEncoding: request.NewWorkflowSnapshot.ExecutionStateBlob.EncodingType.String(),
		NextEventID:            request.NewWorkflowSnapshot.NextEventID,
		DBRecordVersion:        request.NewWorkflowSnapshot.DBRecordVersion,
		CreatedAt:              now,
		UpdatedAt:              now,
	}

	// Use upsert to handle both create and update cases
	filter := bson.M{
		"shard_id":     doc.ShardID,
		"namespace_id": doc.NamespaceID,
		"workflow_id":  doc.WorkflowID,
		"run_id":       doc.RunID,
	}

	update := bson.M{
		"$set": doc,
		"$setOnInsert": bson.M{
			"created_at": now,
		},
	}

	opts := options.Update().SetUpsert(true)
	_, err := s.collection.UpdateOne(ctx, filter, update, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to create workflow execution: %w", err)
	}

	return &p.InternalCreateWorkflowExecutionResponse{}, nil
}

// UpdateWorkflowExecution updates a workflow execution
func (s *ExecutionStore) UpdateWorkflowExecution(ctx context.Context, request *p.InternalUpdateWorkflowExecutionRequest) error {
	filter := bson.M{
		"shard_id":     request.ShardID,
		"namespace_id": request.UpdateWorkflowMutation.NamespaceID,
		"workflow_id":  request.UpdateWorkflowMutation.WorkflowID,
		"run_id":       request.UpdateWorkflowMutation.RunID,
	}

	update := bson.M{
		"$set": bson.M{
			"execution_info":           request.UpdateWorkflowMutation.ExecutionInfoBlob.Data,
			"execution_info_encoding":  request.UpdateWorkflowMutation.ExecutionInfoBlob.EncodingType.String(),
			"execution_state":          request.UpdateWorkflowMutation.ExecutionStateBlob.Data,
			"execution_state_encoding": request.UpdateWorkflowMutation.ExecutionStateBlob.EncodingType.String(),
			"next_event_id":            request.UpdateWorkflowMutation.NextEventID,
			"db_record_version":        request.UpdateWorkflowMutation.DBRecordVersion,
			"updated_at":               time.Now(),
		},
	}

	result, err := s.collection.UpdateOne(ctx, filter, update)
	if err != nil {
		return fmt.Errorf("failed to update workflow execution: %w", err)
	}

	if result.MatchedCount == 0 {
		return &p.ConditionFailedError{
			Msg: "workflow execution not found",
		}
	}

	return nil
}

// ConflictResolveWorkflowExecution resolves conflicts in workflow execution
func (s *ExecutionStore) ConflictResolveWorkflowExecution(ctx context.Context, request *p.InternalConflictResolveWorkflowExecutionRequest) error {
	// This is a simplified implementation
	// In a real implementation, you'd handle conflict resolution properly
	return s.UpdateWorkflowExecution(ctx, &p.InternalUpdateWorkflowExecutionRequest{
		ShardID:                 request.ShardID,
		UpdateWorkflowMutation:  p.InternalWorkflowMutation{}, // Convert from snapshot to mutation
		UpdateWorkflowNewEvents: request.ResetWorkflowEventsNewEvents,
	})
}

// DeleteWorkflowExecution deletes a workflow execution
func (s *ExecutionStore) DeleteWorkflowExecution(ctx context.Context, request *p.DeleteWorkflowExecutionRequest) error {
	filter := bson.M{
		"shard_id":     request.ShardID,
		"namespace_id": request.NamespaceID,
		"workflow_id":  request.WorkflowID,
		"run_id":       request.RunID,
	}

	_, err := s.collection.DeleteOne(ctx, filter)
	if err != nil {
		return fmt.Errorf("failed to delete workflow execution: %w", err)
	}

	return nil
}

// DeleteCurrentWorkflowExecution deletes the current workflow execution
func (s *ExecutionStore) DeleteCurrentWorkflowExecution(ctx context.Context, request *p.DeleteCurrentWorkflowExecutionRequest) error {
	// This is a simplified implementation
	// In a real implementation, you'd handle current workflow execution deletion
	return s.DeleteWorkflowExecution(ctx, &p.DeleteWorkflowExecutionRequest{
		ShardID:     request.ShardID,
		NamespaceID: request.NamespaceID,
		WorkflowID:  request.WorkflowID,
		RunID:       request.RunID,
	})
}

// GetCurrentExecution retrieves the current execution
func (s *ExecutionStore) GetCurrentExecution(ctx context.Context, request *p.GetCurrentExecutionRequest) (*p.InternalGetCurrentExecutionResponse, error) {
	filter := bson.M{
		"shard_id":     request.ShardID,
		"namespace_id": request.NamespaceID,
		"workflow_id":  request.WorkflowID,
	}

	var doc CurrentExecutionDocument
	err := s.collection.FindOne(ctx, filter).Decode(&doc)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, &p.ConditionFailedError{
				Msg: "current execution not found",
			}
		}
		return nil, fmt.Errorf("failed to get current execution: %w", err)
	}

	return &p.InternalGetCurrentExecutionResponse{
		RunID:          doc.RunID,
		ExecutionState: nil, // This would be populated in a real implementation
	}, nil
}

// GetWorkflowExecution retrieves a workflow execution
func (s *ExecutionStore) GetWorkflowExecution(ctx context.Context, request *p.GetWorkflowExecutionRequest) (*p.InternalGetWorkflowExecutionResponse, error) {
	filter := bson.M{
		"shard_id":     request.ShardID,
		"namespace_id": request.NamespaceID,
		"workflow_id":  request.WorkflowID,
		"run_id":       request.RunID,
	}

	var doc WorkflowExecutionDocument
	err := s.collection.FindOne(ctx, filter).Decode(&doc)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, &p.ConditionFailedError{
				Msg: "workflow execution not found",
			}
		}
		return nil, fmt.Errorf("failed to get workflow execution: %w", err)
	}

	executionInfoEncoding, err := enumspb.EncodingTypeFromString(doc.ExecutionInfoEncoding)
	if err != nil {
		executionInfoEncoding = enumspb.ENCODING_TYPE_UNSPECIFIED
	}

	executionStateEncoding, err := enumspb.EncodingTypeFromString(doc.ExecutionStateEncoding)
	if err != nil {
		executionStateEncoding = enumspb.ENCODING_TYPE_UNSPECIFIED
	}

	return &p.InternalGetWorkflowExecutionResponse{
		State: &p.InternalWorkflowMutableState{
			ExecutionInfo: &commonpb.DataBlob{
				Data:         doc.ExecutionInfo,
				EncodingType: executionInfoEncoding,
			},
			ExecutionState: &commonpb.DataBlob{
				Data:         doc.ExecutionState,
				EncodingType: executionStateEncoding,
			},
			NextEventID:     doc.NextEventID,
			DBRecordVersion: doc.DBRecordVersion,
		},
		DBRecordVersion: doc.DBRecordVersion,
	}, nil
}

// SetWorkflowExecution sets a workflow execution
func (s *ExecutionStore) SetWorkflowExecution(ctx context.Context, request *p.InternalSetWorkflowExecutionRequest) error {
	// This is a simplified implementation
	// In a real implementation, you'd handle setting workflow execution
	return s.UpdateWorkflowExecution(ctx, &p.InternalUpdateWorkflowExecutionRequest{
		ShardID:                request.ShardID,
		UpdateWorkflowMutation: p.InternalWorkflowMutation{}, // Convert from snapshot to mutation
	})
}

// ListConcreteExecutions lists concrete executions
func (s *ExecutionStore) ListConcreteExecutions(ctx context.Context, request *p.ListConcreteExecutionsRequest) (*p.InternalListConcreteExecutionsResponse, error) {
	filter := bson.M{
		"shard_id": request.ShardID,
	}

	opts := options.Find().
		SetSort(bson.D{{Key: "workflow_id", Value: 1}}).
		SetLimit(int64(request.PageSize))

	// Handle pagination if page token is provided
	if len(request.PageToken) > 0 {
		// For now, we'll skip the first N documents based on the page token
		// In a real implementation, you'd decode the page token to get the last seen ID
		skipCount := len(request.PageToken) // Simplified approach
		opts.SetSkip(int64(skipCount))
	}

	cursor, err := s.collection.Find(ctx, filter, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to list concrete executions: %w", err)
	}
	defer cursor.Close(ctx)

	var states []*p.InternalWorkflowMutableState
	for cursor.Next(ctx) {
		var doc WorkflowExecutionDocument
		if err := cursor.Decode(&doc); err != nil {
			s.logger.Error("failed to decode workflow execution document", tag.Error(err))
			continue
		}

		executionInfoEncoding, err := enumspb.EncodingTypeFromString(doc.ExecutionInfoEncoding)
		if err != nil {
			executionInfoEncoding = enumspb.ENCODING_TYPE_UNSPECIFIED
		}

		executionStateEncoding, err := enumspb.EncodingTypeFromString(doc.ExecutionStateEncoding)
		if err != nil {
			executionStateEncoding = enumspb.ENCODING_TYPE_UNSPECIFIED
		}

		states = append(states, &p.InternalWorkflowMutableState{
			ExecutionInfo: &commonpb.DataBlob{
				Data:         doc.ExecutionInfo,
				EncodingType: executionInfoEncoding,
			},
			ExecutionState: &commonpb.DataBlob{
				Data:         doc.ExecutionState,
				EncodingType: executionStateEncoding,
			},
			NextEventID:     doc.NextEventID,
			DBRecordVersion: doc.DBRecordVersion,
		})
	}

	return &p.InternalListConcreteExecutionsResponse{
		States: states,
	}, nil
}

// AddHistoryTasks adds history tasks
func (s *ExecutionStore) AddHistoryTasks(ctx context.Context, request *p.InternalAddHistoryTasksRequest) error {
	now := time.Now()
	var documents []interface{}

	for category, taskList := range request.Tasks {
		for _, task := range taskList {
			doc := &HistoryTaskDocument{
				ShardID:      request.ShardID,
				TaskCategory: category.Name(),
				TaskID:       task.Key.TaskID,
				Task:         task.Blob.Data,
				TaskEncoding: task.Blob.EncodingType.String(),
				FireTime:     task.Key.FireTime,
				CreatedAt:    now,
			}
			documents = append(documents, doc)
		}
	}

	if len(documents) > 0 {
		collection := s.database.Collection("history_tasks")
		_, err := collection.InsertMany(ctx, documents)
		if err != nil {
			return fmt.Errorf("failed to add history tasks: %w", err)
		}
	}

	return nil
}

// GetHistoryTasks retrieves history tasks
func (s *ExecutionStore) GetHistoryTasks(ctx context.Context, request *p.GetHistoryTasksRequest) (*p.InternalGetHistoryTasksResponse, error) {
	filter := bson.M{
		"shard_id":      request.ShardID,
		"task_category": request.TaskCategory.Name(),
		"task_id": bson.M{
			"$gte": request.InclusiveMinTaskKey.TaskID,
			"$lt":  request.ExclusiveMaxTaskKey.TaskID,
		},
	}

	opts := options.Find().
		SetSort(bson.D{{Key: "task_id", Value: 1}}).
		SetLimit(int64(request.BatchSize))

	cursor, err := s.database.Collection("history_tasks").Find(ctx, filter, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to get history tasks: %w", err)
	}
	defer cursor.Close(ctx)

	var historyTasks []p.InternalHistoryTask
	for cursor.Next(ctx) {
		var doc HistoryTaskDocument
		if err := cursor.Decode(&doc); err != nil {
			s.logger.Error("failed to decode history task document", tag.Error(err))
			continue
		}

		encodingType, err := enumspb.EncodingTypeFromString(doc.TaskEncoding)
		if err != nil {
			encodingType = enumspb.ENCODING_TYPE_UNSPECIFIED
		}

		historyTasks = append(historyTasks, p.InternalHistoryTask{
			Key: tasks.Key{
				TaskID:   doc.TaskID,
				FireTime: doc.FireTime,
			},
			Blob: &commonpb.DataBlob{
				Data:         doc.Task,
				EncodingType: encodingType,
			},
		})
	}

	return &p.InternalGetHistoryTasksResponse{
		Tasks: historyTasks,
	}, nil
}

// CompleteHistoryTask completes a history task
func (s *ExecutionStore) CompleteHistoryTask(ctx context.Context, request *p.CompleteHistoryTaskRequest) error {
	filter := bson.M{
		"shard_id":      request.ShardID,
		"task_category": request.TaskCategory.Name(),
		"task_id":       request.TaskKey.TaskID,
	}

	_, err := s.database.Collection("history_tasks").DeleteOne(ctx, filter)
	if err != nil {
		return fmt.Errorf("failed to complete history task: %w", err)
	}

	return nil
}

// RangeCompleteHistoryTasks completes a range of history tasks
func (s *ExecutionStore) RangeCompleteHistoryTasks(ctx context.Context, request *p.RangeCompleteHistoryTasksRequest) error {
	filter := bson.M{
		"shard_id":      request.ShardID,
		"task_category": request.TaskCategory.Name(),
		"task_id": bson.M{
			"$gte": request.InclusiveMinTaskKey.TaskID,
			"$lt":  request.ExclusiveMaxTaskKey.TaskID,
		},
	}

	_, err := s.database.Collection("history_tasks").DeleteMany(ctx, filter)
	if err != nil {
		return fmt.Errorf("failed to range complete history tasks: %w", err)
	}

	return nil
}

// PutReplicationTaskToDLQ puts a replication task to DLQ
func (s *ExecutionStore) PutReplicationTaskToDLQ(ctx context.Context, request *p.PutReplicationTaskToDLQRequest) error {
	// This is a simplified implementation
	// In a real implementation, you'd handle DLQ operations properly
	return nil
}

// GetReplicationTasksFromDLQ gets replication tasks from DLQ
func (s *ExecutionStore) GetReplicationTasksFromDLQ(ctx context.Context, request *p.GetReplicationTasksFromDLQRequest) (*p.InternalGetReplicationTasksFromDLQResponse, error) {
	// This is a simplified implementation
	// In a real implementation, you'd handle DLQ operations properly
	return &p.InternalGetReplicationTasksFromDLQResponse{}, nil
}

// DeleteReplicationTaskFromDLQ deletes a replication task from DLQ
func (s *ExecutionStore) DeleteReplicationTaskFromDLQ(ctx context.Context, request *p.DeleteReplicationTaskFromDLQRequest) error {
	// This is a simplified implementation
	// In a real implementation, you'd handle DLQ operations properly
	return nil
}

// RangeDeleteReplicationTaskFromDLQ deletes a range of replication tasks from DLQ
func (s *ExecutionStore) RangeDeleteReplicationTaskFromDLQ(ctx context.Context, request *p.RangeDeleteReplicationTaskFromDLQRequest) error {
	// This is a simplified implementation
	// In a real implementation, you'd handle DLQ operations properly
	return nil
}

// IsReplicationDLQEmpty checks if the replication DLQ is empty
func (s *ExecutionStore) IsReplicationDLQEmpty(ctx context.Context, request *p.GetReplicationTasksFromDLQRequest) (bool, error) {
	// This is a simplified implementation
	// In a real implementation, you'd handle DLQ operations properly
	return true, nil
}

// AppendHistoryNodes appends history nodes
func (s *ExecutionStore) AppendHistoryNodes(ctx context.Context, request *p.InternalAppendHistoryNodesRequest) error {
	doc := &HistoryNodeDocument{
		ShardID:           request.ShardID,
		BranchToken:       request.BranchToken,
		NodeID:            request.Node.NodeID,
		TransactionID:     request.Node.TransactionID,
		PrevTransactionID: request.Node.PrevTransactionID,
		Events:            request.Node.Events.Data,
		EventsEncoding:    request.Node.Events.EncodingType.String(),
		CreatedAt:         time.Now(),
	}

	collection := s.database.Collection("history_nodes")
	_, err := collection.InsertOne(ctx, doc)
	if err != nil {
		return fmt.Errorf("failed to append history nodes: %w", err)
	}

	return nil
}

// DeleteHistoryNodes deletes history nodes
func (s *ExecutionStore) DeleteHistoryNodes(ctx context.Context, request *p.InternalDeleteHistoryNodesRequest) error {
	filter := bson.M{
		"shard_id":     request.ShardID,
		"branch_token": request.BranchToken,
		"node_id":      request.NodeID,
	}

	_, err := s.database.Collection("history_nodes").DeleteOne(ctx, filter)
	if err != nil {
		return fmt.Errorf("failed to delete history nodes: %w", err)
	}

	return nil
}

// ReadHistoryBranch reads history branch
func (s *ExecutionStore) ReadHistoryBranch(ctx context.Context, request *p.InternalReadHistoryBranchRequest) (*p.InternalReadHistoryBranchResponse, error) {
	filter := bson.M{
		"shard_id":     request.ShardID,
		"branch_token": request.BranchToken,
		"node_id": bson.M{
			"$gte": request.MinNodeID,
			"$lte": request.MaxNodeID,
		},
	}

	opts := options.Find().SetSort(bson.D{{Key: "node_id", Value: 1}})

	cursor, err := s.database.Collection("history_nodes").Find(ctx, filter, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to read history branch: %w", err)
	}
	defer cursor.Close(ctx)

	var nodes []p.InternalHistoryNode
	for cursor.Next(ctx) {
		var doc HistoryNodeDocument
		if err := cursor.Decode(&doc); err != nil {
			s.logger.Error("failed to decode history node document", tag.Error(err))
			continue
		}

		encodingType, err := enumspb.EncodingTypeFromString(doc.EventsEncoding)
		if err != nil {
			encodingType = enumspb.ENCODING_TYPE_UNSPECIFIED
		}

		nodes = append(nodes, p.InternalHistoryNode{
			NodeID:            doc.NodeID,
			TransactionID:     doc.TransactionID,
			PrevTransactionID: doc.PrevTransactionID,
			Events: &commonpb.DataBlob{
				Data:         doc.Events,
				EncodingType: encodingType,
			},
		})
	}

	return &p.InternalReadHistoryBranchResponse{
		Nodes: nodes,
	}, nil
}

// ForkHistoryBranch forks a history branch
func (s *ExecutionStore) ForkHistoryBranch(ctx context.Context, request *p.InternalForkHistoryBranchRequest) error {
	// This is a simplified implementation
	// In a real implementation, you'd handle branch forking properly
	return nil
}

// DeleteHistoryBranch deletes a history branch
func (s *ExecutionStore) DeleteHistoryBranch(ctx context.Context, request *p.InternalDeleteHistoryBranchRequest) error {
	filter := bson.M{
		"shard_id":     request.ShardID,
		"branch_token": request.BranchToken,
	}

	_, err := s.database.Collection("history_nodes").DeleteMany(ctx, filter)
	if err != nil {
		return fmt.Errorf("failed to delete history branch: %w", err)
	}

	return nil
}

// GetHistoryTreeContainingBranch gets history tree containing branch
func (s *ExecutionStore) GetHistoryTreeContainingBranch(ctx context.Context, request *p.InternalGetHistoryTreeContainingBranchRequest) (*p.InternalGetHistoryTreeContainingBranchResponse, error) {
	// This is a simplified implementation
	// In a real implementation, you'd handle history tree operations properly
	return &p.InternalGetHistoryTreeContainingBranchResponse{}, nil
}

// GetAllHistoryTreeBranches gets all history tree branches
func (s *ExecutionStore) GetAllHistoryTreeBranches(ctx context.Context, request *p.GetAllHistoryTreeBranchesRequest) (*p.InternalGetAllHistoryTreeBranchesResponse, error) {
	// This is a simplified implementation
	// In a real implementation, you'd handle history tree operations properly
	return &p.InternalGetAllHistoryTreeBranchesResponse{}, nil
}

// MockTask implements tasks.Task interface for testing
type MockTask struct {
	taskID         int64
	payload        []byte
	visibilityTime time.Time
}

func (t *MockTask) GetTaskID() int64 {
	return t.taskID
}

func (t *MockTask) GetPayload() []byte {
	return t.payload
}

func (t *MockTask) GetVisibilityTime() time.Time {
	return t.visibilityTime
}

// HistoryBranchUtil implements p.HistoryBranchUtil interface
type HistoryBranchUtil struct {
	store *ExecutionStore
}

// NewHistoryBranch creates a new history branch
func (h *HistoryBranchUtil) NewHistoryBranch(
	namespaceID string,
	workflowID string,
	runID string,
	treeID string,
	branchID *string,
	ancestors []*persistencespb.HistoryBranchRange,
	runTimeout time.Duration,
	executionTimeout time.Duration,
	retentionDuration time.Duration,
) ([]byte, error) {
	// This is a simplified implementation
	// In a real implementation, you'd handle history branch creation properly
	return []byte("mock-branch-token"), nil
}

// ParseHistoryBranchInfo parses history branch info
func (h *HistoryBranchUtil) ParseHistoryBranchInfo(
	branchToken []byte,
) (*persistencespb.HistoryBranch, error) {
	// This is a simplified implementation
	// In a real implementation, you'd handle history branch parsing properly
	return &persistencespb.HistoryBranch{}, nil
}

// UpdateHistoryBranchInfo updates history branch info
func (h *HistoryBranchUtil) UpdateHistoryBranchInfo(
	branchToken []byte,
	branchInfo *persistencespb.HistoryBranch,
	runID string,
) ([]byte, error) {
	// This is a simplified implementation
	// In a real implementation, you'd handle history branch info updates properly
	return []byte("updated-branch-token"), nil
}
