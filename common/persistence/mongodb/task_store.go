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
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	p "go.temporal.io/server/common/persistence"
)

type (
	// TaskStore implements the TaskStore interface for MongoDB
	TaskStore struct {
		client     *mongo.Client
		database   *mongo.Database
		collection *mongo.Collection
		logger     log.Logger
	}

	// TaskDocument represents a task document in MongoDB
	TaskDocument struct {
		NamespaceID       []byte    `bson:"namespace_id"`
		TaskQueueName     string    `bson:"task_queue_name"`
		TaskQueueType     int32     `bson:"task_queue_type"`
		Type              int32     `bson:"type"`
		TaskID            int64     `bson:"task_id"`
		RangeID           int64     `bson:"range_id"`
		Task              []byte    `bson:"task,omitempty"`
		TaskEncoding      string    `bson:"task_encoding,omitempty"`
		TaskQueue         []byte    `bson:"task_queue,omitempty"`
		TaskQueueEncoding string    `bson:"task_queue_encoding,omitempty"`
		ExpiryTime        time.Time `bson:"expiry_time,omitempty"`
		CreatedAt         time.Time `bson:"created_at"`
		UpdatedAt         time.Time `bson:"updated_at"`
	}

	// TaskQueueDocument represents a task queue document in MongoDB
	TaskQueueDocument struct {
		NamespaceID       []byte    `bson:"namespace_id"`
		TaskQueueName     string    `bson:"task_queue_name"`
		TaskQueueType     int32     `bson:"task_queue_type"`
		RangeID           int64     `bson:"range_id"`
		TaskQueue         []byte    `bson:"task_queue,omitempty"`
		TaskQueueEncoding string    `bson:"task_queue_encoding,omitempty"`
		ExpiryTime        time.Time `bson:"expiry_time,omitempty"`
		CreatedAt         time.Time `bson:"created_at"`
		UpdatedAt         time.Time `bson:"updated_at"`
	}

	// TaskQueueUserDataDocument represents task queue user data in MongoDB
	TaskQueueUserDataDocument struct {
		NamespaceID   []byte    `bson:"namespace_id"`
		TaskQueueName string    `bson:"task_queue_name"`
		BuildID       string    `bson:"build_id,omitempty"`
		Data          []byte    `bson:"data,omitempty"`
		DataEncoding  string    `bson:"data_encoding,omitempty"`
		Version       int64     `bson:"version"`
		CreatedAt     time.Time `bson:"created_at"`
		UpdatedAt     time.Time `bson:"updated_at"`
	}
)

// NewTaskStore creates a new MongoDB task store
func NewTaskStore(database *mongo.Database, logger log.Logger) *TaskStore {
	return &TaskStore{
		client:     database.Client(),
		database:   database,
		collection: database.Collection("tasks"),
		logger:     logger,
	}
}

// Close closes the task store
func (s *TaskStore) Close() {
	// MongoDB client is managed by the factory
}

// GetName returns the name of the task store
func (s *TaskStore) GetName() string {
	return "mongodb-task-store"
}

// CreateTaskQueue creates a new task queue
func (s *TaskStore) CreateTaskQueue(ctx context.Context, request *p.InternalCreateTaskQueueRequest) error {
	now := time.Now()
	doc := &TaskQueueDocument{
		NamespaceID:       []byte(request.NamespaceID),
		TaskQueueName:     request.TaskQueue,
		TaskQueueType:     int32(request.TaskType),
		RangeID:           request.RangeID,
		TaskQueue:         request.TaskQueueInfo.Data,
		TaskQueueEncoding: request.TaskQueueInfo.EncodingType.String(),
		ExpiryTime:        request.ExpiryTime.AsTime(),
		CreatedAt:         now,
		UpdatedAt:         now,
	}

	// Use upsert to handle both create and update cases
	filter := bson.M{
		"namespace_id":    doc.NamespaceID,
		"task_queue_name": doc.TaskQueueName,
		"task_queue_type": doc.TaskQueueType,
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
		return fmt.Errorf("failed to create task queue: %w", err)
	}

	return nil
}

// GetTaskQueue retrieves a task queue
func (s *TaskStore) GetTaskQueue(ctx context.Context, request *p.InternalGetTaskQueueRequest) (*p.InternalGetTaskQueueResponse, error) {
	filter := bson.M{
		"namespace_id":    []byte(request.NamespaceID),
		"task_queue_name": request.TaskQueue,
		"task_queue_type": int32(request.TaskType),
	}

	var doc TaskQueueDocument
	err := s.collection.FindOne(ctx, filter).Decode(&doc)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, &p.ConditionFailedError{
				Msg: "task queue not found",
			}
		}
		return nil, fmt.Errorf("failed to get task queue: %w", err)
	}

	encodingType, err := enumspb.EncodingTypeFromString(doc.TaskQueueEncoding)
	if err != nil {
		encodingType = enumspb.ENCODING_TYPE_UNSPECIFIED
	}

	return &p.InternalGetTaskQueueResponse{
		RangeID: doc.RangeID,
		TaskQueueInfo: &commonpb.DataBlob{
			Data:         doc.TaskQueue,
			EncodingType: encodingType,
		},
	}, nil
}

// UpdateTaskQueue updates a task queue
func (s *TaskStore) UpdateTaskQueue(ctx context.Context, request *p.InternalUpdateTaskQueueRequest) (*p.UpdateTaskQueueResponse, error) {
	filter := bson.M{
		"namespace_id":    []byte(request.NamespaceID),
		"task_queue_name": request.TaskQueue,
		"task_queue_type": int32(request.TaskType),
		"range_id":        request.PrevRangeID, // Optimistic concurrency control
	}

	update := bson.M{
		"$set": bson.M{
			"range_id":            request.RangeID,
			"task_queue":          request.TaskQueueInfo.Data,
			"task_queue_encoding": request.TaskQueueInfo.EncodingType.String(),
			"expiry_time":         request.ExpiryTime.AsTime(),
			"updated_at":          time.Now(),
		},
	}

	result, err := s.collection.UpdateOne(ctx, filter, update)
	if err != nil {
		return nil, fmt.Errorf("failed to update task queue: %w", err)
	}

	if result.MatchedCount == 0 {
		return nil, &p.ConditionFailedError{
			Msg: "task queue not found or range ID mismatch",
		}
	}

	return &p.UpdateTaskQueueResponse{}, nil
}

// ListTaskQueue lists task queues
func (s *TaskStore) ListTaskQueue(ctx context.Context, request *p.ListTaskQueueRequest) (*p.InternalListTaskQueueResponse, error) {
	// This is a simplified implementation
	// In a real implementation, you'd want to handle pagination properly
	filter := bson.M{}

	cursor, err := s.collection.Find(ctx, filter)
	if err != nil {
		return nil, fmt.Errorf("failed to list task queues: %w", err)
	}
	defer cursor.Close(ctx)

	var items []*p.InternalListTaskQueueItem
	for cursor.Next(ctx) {
		var doc TaskQueueDocument
		if err := cursor.Decode(&doc); err != nil {
			s.logger.Error("failed to decode task queue document", tag.Error(err))
			continue
		}

		encodingType, err := enumspb.EncodingTypeFromString(doc.TaskQueueEncoding)
		if err != nil {
			encodingType = enumspb.ENCODING_TYPE_UNSPECIFIED
		}

		items = append(items, &p.InternalListTaskQueueItem{
			TaskQueue: &commonpb.DataBlob{
				Data:         doc.TaskQueue,
				EncodingType: encodingType,
			},
			RangeID: doc.RangeID,
		})
	}

	return &p.InternalListTaskQueueResponse{
		Items: items,
	}, nil
}

// DeleteTaskQueue deletes a task queue
func (s *TaskStore) DeleteTaskQueue(ctx context.Context, request *p.DeleteTaskQueueRequest) error {
	filter := bson.M{
		"namespace_id":    []byte(request.TaskQueue.NamespaceID),
		"task_queue_name": request.TaskQueue.TaskQueueName,
		"task_queue_type": int32(request.TaskQueue.TaskQueueType),
	}

	_, err := s.collection.DeleteOne(ctx, filter)
	if err != nil {
		return fmt.Errorf("failed to delete task queue: %w", err)
	}

	return nil
}

// CreateTasks creates new tasks
func (s *TaskStore) CreateTasks(ctx context.Context, request *p.InternalCreateTasksRequest) (*p.CreateTasksResponse, error) {
	now := time.Now()
	var documents []interface{}

	for _, task := range request.Tasks {
		doc := &TaskDocument{
			NamespaceID:   []byte(request.NamespaceID),
			TaskQueueName: request.TaskQueue,
			TaskQueueType: int32(request.TaskType),
			Type:          int32(task.Subqueue), // Using subqueue as type
			TaskID:        task.TaskId,
			RangeID:       request.RangeID,
			Task:          task.Task.Data,
			TaskEncoding:  task.Task.EncodingType.String(),
			ExpiryTime:    task.ExpiryTime.AsTime(),
			CreatedAt:     now,
			UpdatedAt:     now,
		}
		documents = append(documents, doc)
	}

	if len(documents) > 0 {
		_, err := s.collection.InsertMany(ctx, documents)
		if err != nil {
			return nil, fmt.Errorf("failed to create tasks: %w", err)
		}
	}

	return &p.CreateTasksResponse{}, nil
}

// GetTasks retrieves tasks
func (s *TaskStore) GetTasks(ctx context.Context, request *p.GetTasksRequest) (*p.InternalGetTasksResponse, error) {
	filter := bson.M{
		"namespace_id":    []byte(request.NamespaceID),
		"task_queue_name": request.TaskQueue,
		"task_queue_type": int32(request.TaskType),
		"type":            int32(request.TaskType),
		"task_id": bson.M{
			"$gt": request.InclusiveMinTaskID,
		},
	}

	if request.ExclusiveMaxTaskID > 0 {
		filter["task_id"].(bson.M)["$lt"] = request.ExclusiveMaxTaskID
	}

	opts := options.Find().
		SetSort(bson.D{{"task_id", 1}}).
		SetLimit(int64(request.PageSize))

	cursor, err := s.collection.Find(ctx, filter, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to get tasks: %w", err)
	}
	defer cursor.Close(ctx)

	var tasks []*commonpb.DataBlob
	for cursor.Next(ctx) {
		var doc TaskDocument
		if err := cursor.Decode(&doc); err != nil {
			s.logger.Error("failed to decode task document", tag.Error(err))
			continue
		}

		encodingType, err := enumspb.EncodingTypeFromString(doc.TaskEncoding)
		if err != nil {
			encodingType = enumspb.ENCODING_TYPE_UNSPECIFIED
		}

		tasks = append(tasks, &commonpb.DataBlob{
			Data:         doc.Task,
			EncodingType: encodingType,
		})
	}

	return &p.InternalGetTasksResponse{
		Tasks: tasks,
	}, nil
}

// CompleteTasksLessThan completes tasks less than the given task ID
func (s *TaskStore) CompleteTasksLessThan(ctx context.Context, request *p.CompleteTasksLessThanRequest) (int, error) {
	filter := bson.M{
		"namespace_id":    []byte(request.NamespaceID),
		"task_queue_name": request.TaskQueueName,
		"task_queue_type": int32(request.TaskType),
		"type":            int32(request.TaskType),
		"task_id": bson.M{
			"$lt": request.ExclusiveMaxTaskID,
		},
	}

	result, err := s.collection.DeleteMany(ctx, filter)
	if err != nil {
		return 0, fmt.Errorf("failed to complete tasks: %w", err)
	}

	return int(result.DeletedCount), nil
}

// GetTaskQueueUserData retrieves task queue user data
func (s *TaskStore) GetTaskQueueUserData(ctx context.Context, request *p.GetTaskQueueUserDataRequest) (*p.InternalGetTaskQueueUserDataResponse, error) {
	filter := bson.M{
		"namespace_id":    []byte(request.NamespaceID),
		"task_queue_name": request.TaskQueue,
	}

	var doc TaskQueueUserDataDocument
	err := s.collection.FindOne(ctx, filter).Decode(&doc)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return &p.InternalGetTaskQueueUserDataResponse{
				Version:  0,
				UserData: nil,
			}, nil
		}
		return nil, fmt.Errorf("failed to get task queue user data: %w", err)
	}

	encodingType, err := enumspb.EncodingTypeFromString(doc.DataEncoding)
	if err != nil {
		encodingType = enumspb.ENCODING_TYPE_UNSPECIFIED
	}

	return &p.InternalGetTaskQueueUserDataResponse{
		Version: doc.Version,
		UserData: &commonpb.DataBlob{
			Data:         doc.Data,
			EncodingType: encodingType,
		},
	}, nil
}

// UpdateTaskQueueUserData updates task queue user data
func (s *TaskStore) UpdateTaskQueueUserData(ctx context.Context, request *p.InternalUpdateTaskQueueUserDataRequest) error {
	now := time.Now()

	for taskQueue, update := range request.Updates {
		filter := bson.M{
			"namespace_id":    []byte(request.NamespaceID),
			"task_queue_name": taskQueue,
		}

		updateDoc := bson.M{
			"$set": bson.M{
				"data":          update.UserData.Data,
				"data_encoding": update.UserData.EncodingType.String(),
				"version":       update.Version,
				"updated_at":    now,
			},
		}

		if update.UserData == nil {
			updateDoc["$set"].(bson.M)["data"] = nil
			updateDoc["$set"].(bson.M)["data_encoding"] = ""
		}

		opts := options.Update().SetUpsert(true)
		_, err := s.collection.UpdateOne(ctx, filter, updateDoc, opts)
		if err != nil {
			return fmt.Errorf("failed to update task queue user data for %s: %w", taskQueue, err)
		}
	}

	return nil
}

// ListTaskQueueUserDataEntries lists task queue user data entries
func (s *TaskStore) ListTaskQueueUserDataEntries(ctx context.Context, request *p.ListTaskQueueUserDataEntriesRequest) (*p.InternalListTaskQueueUserDataEntriesResponse, error) {
	filter := bson.M{
		"namespace_id": []byte(request.NamespaceID),
	}

	opts := options.Find().
		SetSort(bson.D{{"task_queue_name", 1}}).
		SetLimit(int64(request.PageSize))

	if len(request.NextPageToken) > 0 {
		// In a real implementation, you'd decode the page token
		// and use it for pagination
	}

	cursor, err := s.collection.Find(ctx, filter, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to list task queue user data entries: %w", err)
	}
	defer cursor.Close(ctx)

	var entries []p.InternalTaskQueueUserDataEntry
	for cursor.Next(ctx) {
		var doc TaskQueueUserDataDocument
		if err := cursor.Decode(&doc); err != nil {
			s.logger.Error("failed to decode task queue user data document", tag.Error(err))
			continue
		}

		encodingType, err := enumspb.EncodingTypeFromString(doc.DataEncoding)
		if err != nil {
			encodingType = enumspb.ENCODING_TYPE_UNSPECIFIED
		}

		entries = append(entries, p.InternalTaskQueueUserDataEntry{
			TaskQueue: doc.TaskQueueName,
			Data: &commonpb.DataBlob{
				Data:         doc.Data,
				EncodingType: encodingType,
			},
			Version: doc.Version,
		})
	}

	return &p.InternalListTaskQueueUserDataEntriesResponse{
		Entries: entries,
	}, nil
}

// GetTaskQueuesByBuildId retrieves task queues by build ID
func (s *TaskStore) GetTaskQueuesByBuildId(ctx context.Context, request *p.GetTaskQueuesByBuildIdRequest) ([]string, error) {
	filter := bson.M{
		"namespace_id": []byte(request.NamespaceID),
		"build_id":     request.BuildID,
	}

	cursor, err := s.collection.Find(ctx, filter)
	if err != nil {
		return nil, fmt.Errorf("failed to get task queues by build ID: %w", err)
	}
	defer cursor.Close(ctx)

	var taskQueues []string
	for cursor.Next(ctx) {
		var doc TaskQueueUserDataDocument
		if err := cursor.Decode(&doc); err != nil {
			s.logger.Error("failed to decode task queue user data document", tag.Error(err))
			continue
		}
		taskQueues = append(taskQueues, doc.TaskQueueName)
	}

	return taskQueues, nil
}

// CountTaskQueuesByBuildId counts task queues by build ID
func (s *TaskStore) CountTaskQueuesByBuildId(ctx context.Context, request *p.CountTaskQueuesByBuildIdRequest) (int, error) {
	filter := bson.M{
		"namespace_id": []byte(request.NamespaceID),
		"build_id":     request.BuildID,
	}

	count, err := s.collection.CountDocuments(ctx, filter)
	if err != nil {
		return 0, fmt.Errorf("failed to count task queues by build ID: %w", err)
	}

	return int(count), nil
}
