package mongodb

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/log"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives/timestamp"
)

// TaskStoreTestSuite tests the MongoDB task store implementation
type TaskStoreTestSuite struct {
	suite.Suite
	client    *mongo.Client
	database  *mongo.Database
	taskStore p.TaskStore
	ctx       context.Context
	logger    log.Logger
}

func TestTaskStoreSuite(t *testing.T) {
	suite.Run(t, new(TaskStoreTestSuite))
}

func (s *TaskStoreTestSuite) SetupSuite() {
	s.ctx = context.Background()

	// Connect to MongoDB
	clientOptions := options.Client().ApplyURI("mongodb://localhost:27017")
	client, err := mongo.Connect(s.ctx, clientOptions)
	require.NoError(s.T(), err)

	s.client = client
	s.database = client.Database("test_temporal")

	// Create task store
	s.taskStore = NewTaskStore(s.database, s.logger)
}

func (s *TaskStoreTestSuite) TearDownSuite() {
	if s.client != nil {
		err := s.client.Disconnect(s.ctx)
		s.NoError(err)
	}
}

func (s *TaskStoreTestSuite) SetupTest() {
	// Clean up collections before each test
	collections := []string{"task_queues", "tasks", "user_data"}
	for _, collectionName := range collections {
		_, err := s.database.Collection(collectionName).DeleteMany(s.ctx, map[string]interface{}{})
		s.NoError(err)
	}
}

func (s *TaskStoreTestSuite) TestCreateTaskQueue() {
	request := &p.InternalCreateTaskQueueRequest{
		NamespaceID: "test-namespace",
		TaskQueue:   "test-queue",
		TaskType:    enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		RangeID:     1,
		TaskQueueInfo: &commonpb.DataBlob{
			Data:         []byte("test-data"),
			EncodingType: enumspb.ENCODING_TYPE_PROTO3,
		},
		ExpiryTime: timestamp.TimePtr(time.Now().Add(time.Hour)),
	}

	err := s.taskStore.CreateTaskQueue(s.ctx, request)
	s.NoError(err)

	// Verify task queue was created
	getRequest := &p.InternalGetTaskQueueRequest{
		NamespaceID: "test-namespace",
		TaskQueue:   "test-queue",
		TaskType:    enumspb.TASK_QUEUE_TYPE_WORKFLOW,
	}

	response, err := s.taskStore.GetTaskQueue(s.ctx, getRequest)
	s.NoError(err)
	s.Equal(int64(1), response.RangeID)
	s.Equal([]byte("test-data"), response.TaskQueueInfo.Data)
	s.Equal(enumspb.ENCODING_TYPE_PROTO3, response.TaskQueueInfo.EncodingType)
}

func (s *TaskStoreTestSuite) TestGetTaskQueue_NotFound() {
	request := &p.InternalGetTaskQueueRequest{
		NamespaceID: "non-existent",
		TaskQueue:   "non-existent",
		TaskType:    enumspb.TASK_QUEUE_TYPE_WORKFLOW,
	}

	_, err := s.taskStore.GetTaskQueue(s.ctx, request)
	s.Error(err)
	s.IsType(&p.ConditionFailedError{}, err)
}

func (s *TaskStoreTestSuite) TestUpdateTaskQueue() {
	// Create initial task queue
	createRequest := &p.InternalCreateTaskQueueRequest{
		NamespaceID: "test-namespace",
		TaskQueue:   "test-queue",
		TaskType:    enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		RangeID:     1,
		TaskQueueInfo: &commonpb.DataBlob{
			Data:         []byte("initial-data"),
			EncodingType: enumspb.ENCODING_TYPE_PROTO3,
		},
	}
	err := s.taskStore.CreateTaskQueue(s.ctx, createRequest)
	s.NoError(err)

	// Update task queue
	updateRequest := &p.InternalUpdateTaskQueueRequest{
		NamespaceID: "test-namespace",
		TaskQueue:   "test-queue",
		TaskType:    enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		RangeID:     2,
		PrevRangeID: 1,
		TaskQueueInfo: &commonpb.DataBlob{
			Data:         []byte("updated-data"),
			EncodingType: enumspb.ENCODING_TYPE_PROTO3,
		},
		ExpiryTime: timestamp.TimePtr(time.Now().Add(time.Hour)),
	}

	response, err := s.taskStore.UpdateTaskQueue(s.ctx, updateRequest)
	s.NoError(err)
	s.NotNil(response)

	// Verify update
	getRequest := &p.InternalGetTaskQueueRequest{
		NamespaceID: "test-namespace",
		TaskQueue:   "test-queue",
		TaskType:    enumspb.TASK_QUEUE_TYPE_WORKFLOW,
	}

	getResponse, err := s.taskStore.GetTaskQueue(s.ctx, getRequest)
	s.NoError(err)
	s.Equal(int64(2), getResponse.RangeID)
	s.Equal([]byte("updated-data"), getResponse.TaskQueueInfo.Data)
}

func (s *TaskStoreTestSuite) TestUpdateTaskQueue_ConditionFailed() {
	// Create initial task queue
	createRequest := &p.InternalCreateTaskQueueRequest{
		NamespaceID: "test-namespace",
		TaskQueue:   "test-queue",
		TaskType:    enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		RangeID:     1,
		TaskQueueInfo: &commonpb.DataBlob{
			Data:         []byte("initial-data"),
			EncodingType: enumspb.ENCODING_TYPE_PROTO3,
		},
	}
	err := s.taskStore.CreateTaskQueue(s.ctx, createRequest)
	s.NoError(err)

	// Try to update with wrong prev range ID
	updateRequest := &p.InternalUpdateTaskQueueRequest{
		NamespaceID: "test-namespace",
		TaskQueue:   "test-queue",
		TaskType:    enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		RangeID:     2,
		PrevRangeID: 999, // Wrong prev range ID
		TaskQueueInfo: &commonpb.DataBlob{
			Data:         []byte("updated-data"),
			EncodingType: enumspb.ENCODING_TYPE_PROTO3,
		},
	}

	_, err = s.taskStore.UpdateTaskQueue(s.ctx, updateRequest)
	s.Error(err)
	s.IsType(&p.ConditionFailedError{}, err)
}

func (s *TaskStoreTestSuite) TestCreateTasks() {
	// Create task queue first
	createQueueRequest := &p.InternalCreateTaskQueueRequest{
		NamespaceID: "test-namespace",
		TaskQueue:   "test-queue",
		TaskType:    enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		RangeID:     1,
		TaskQueueInfo: &commonpb.DataBlob{
			Data:         []byte("queue-data"),
			EncodingType: enumspb.ENCODING_TYPE_PROTO3,
		},
	}
	err := s.taskStore.CreateTaskQueue(s.ctx, createQueueRequest)
	s.NoError(err)

	// Create tasks
	createTasksRequest := &p.InternalCreateTasksRequest{
		NamespaceID: "test-namespace",
		TaskQueue:   "test-queue",
		TaskType:    enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		RangeID:     1,
		Tasks: []*p.InternalCreateTask{
			{
				TaskId: 1,
				Task: &commonpb.DataBlob{
					Data:         []byte("task-1-data"),
					EncodingType: enumspb.ENCODING_TYPE_PROTO3,
				},
				ExpiryTime: timestamp.TimePtr(time.Now().Add(time.Hour)),
			},
			{
				TaskId: 2,
				Task: &commonpb.DataBlob{
					Data:         []byte("task-2-data"),
					EncodingType: enumspb.ENCODING_TYPE_PROTO3,
				},
				ExpiryTime: timestamp.TimePtr(time.Now().Add(time.Hour)),
			},
		},
	}

	_, err = s.taskStore.CreateTasks(s.ctx, createTasksRequest)
	s.NoError(err)
}

func (s *TaskStoreTestSuite) TestGetTasks() {
	// Create task queue and tasks first
	createQueueRequest := &p.InternalCreateTaskQueueRequest{
		NamespaceID: "test-namespace",
		TaskQueue:   "test-queue",
		TaskType:    enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		RangeID:     1,
		TaskQueueInfo: &commonpb.DataBlob{
			Data:         []byte("queue-data"),
			EncodingType: enumspb.ENCODING_TYPE_PROTO3,
		},
	}
	err := s.taskStore.CreateTaskQueue(s.ctx, createQueueRequest)
	s.NoError(err)

	createTasksRequest := &p.InternalCreateTasksRequest{
		NamespaceID: "test-namespace",
		TaskQueue:   "test-queue",
		TaskType:    enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		RangeID:     1,
		Tasks: []*p.InternalCreateTask{
			{
				TaskId: 1,
				Task: &commonpb.DataBlob{
					Data:         []byte("task-1-data"),
					EncodingType: enumspb.ENCODING_TYPE_PROTO3,
				},
				ExpiryTime: timestamp.TimePtr(time.Now().Add(time.Hour)),
			},
			{
				TaskId: 2,
				Task: &commonpb.DataBlob{
					Data:         []byte("task-2-data"),
					EncodingType: enumspb.ENCODING_TYPE_PROTO3,
				},
				ExpiryTime: timestamp.TimePtr(time.Now().Add(time.Hour)),
			},
		},
	}
	_, err = s.taskStore.CreateTasks(s.ctx, createTasksRequest)
	s.NoError(err)

	// Get tasks
	getTasksRequest := &p.GetTasksRequest{
		NamespaceID:        "test-namespace",
		TaskQueue:          "test-queue",
		TaskType:           enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		InclusiveMinTaskID: 0,
		ExclusiveMaxTaskID: 10,
		PageSize:           10,
	}

	response, err := s.taskStore.GetTasks(s.ctx, getTasksRequest)
	s.NoError(err)
	s.Len(response.Tasks, 2)
	s.Equal([]byte("task-1-data"), response.Tasks[0].Data)
	s.Equal([]byte("task-2-data"), response.Tasks[1].Data)
}

func (s *TaskStoreTestSuite) TestCompleteTasksLessThan() {
	// Create task queue and tasks first
	createQueueRequest := &p.InternalCreateTaskQueueRequest{
		NamespaceID: "test-namespace",
		TaskQueue:   "test-queue",
		TaskType:    enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		RangeID:     1,
		TaskQueueInfo: &commonpb.DataBlob{
			Data:         []byte("queue-data"),
			EncodingType: enumspb.ENCODING_TYPE_PROTO3,
		},
	}
	err := s.taskStore.CreateTaskQueue(s.ctx, createQueueRequest)
	s.NoError(err)

	createTasksRequest := &p.InternalCreateTasksRequest{
		NamespaceID: "test-namespace",
		TaskQueue:   "test-queue",
		TaskType:    enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		RangeID:     1,
		Tasks: []*p.InternalCreateTask{
			{
				TaskId: 1,
				Task: &commonpb.DataBlob{
					Data:         []byte("task-1-data"),
					EncodingType: enumspb.ENCODING_TYPE_PROTO3,
				},
				ExpiryTime: timestamp.TimePtr(time.Now().Add(time.Hour)),
			},
			{
				TaskId: 2,
				Task: &commonpb.DataBlob{
					Data:         []byte("task-2-data"),
					EncodingType: enumspb.ENCODING_TYPE_PROTO3,
				},
				ExpiryTime: timestamp.TimePtr(time.Now().Add(time.Hour)),
			},
		},
	}
	_, err = s.taskStore.CreateTasks(s.ctx, createTasksRequest)
	s.NoError(err)

	// Complete tasks less than task ID 2
	completeRequest := &p.CompleteTasksLessThanRequest{
		NamespaceID:        "test-namespace",
		TaskQueueName:      "test-queue",
		TaskType:           enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		ExclusiveMaxTaskID: 2,
		Limit:              10,
	}

	deletedCount, err := s.taskStore.CompleteTasksLessThan(s.ctx, completeRequest)
	s.NoError(err)
	s.Equal(1, deletedCount)

	// Verify task 1 is deleted but task 2 remains
	getTasksRequest := &p.GetTasksRequest{
		NamespaceID:        "test-namespace",
		TaskQueue:          "test-queue",
		TaskType:           enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		InclusiveMinTaskID: 0,
		ExclusiveMaxTaskID: 10,
		PageSize:           10,
	}

	response, err := s.taskStore.GetTasks(s.ctx, getTasksRequest)
	s.NoError(err)
	s.Len(response.Tasks, 1)
	s.Equal([]byte("task-2-data"), response.Tasks[0].Data)
}

func (s *TaskStoreTestSuite) TestGetTaskQueueUserData() {
	// Create task queue user data
	updates := map[string]*p.InternalSingleTaskQueueUserDataUpdate{
		"test-queue": {
			UserData: &commonpb.DataBlob{
				Data:         []byte("user-data"),
				EncodingType: enumspb.ENCODING_TYPE_PROTO3,
			},
			BuildIdsAdded:   []string{"build-1"},
			BuildIdsRemoved: []string{"build-2"},
			Applied:         nil,
			Conflicting:     nil,
		},
	}
	err := s.taskStore.UpdateTaskQueueUserData(s.ctx, &p.InternalUpdateTaskQueueUserDataRequest{
		NamespaceID: "test-namespace",
		Updates:     updates,
	})
	s.NoError(err)

	// Get task queue user data
	getRequest := &p.GetTaskQueueUserDataRequest{
		NamespaceID: "test-namespace",
		TaskQueue:   "test-queue",
	}

	response, err := s.taskStore.GetTaskQueueUserData(s.ctx, getRequest)
	s.NoError(err)
	s.Equal([]byte("user-data"), response.UserData.Data)
	s.Equal(enumspb.ENCODING_TYPE_PROTO3, response.UserData.EncodingType)
}

func (s *TaskStoreTestSuite) TestGetTaskQueueUserData_NotFound() {
	getRequest := &p.GetTaskQueueUserDataRequest{
		NamespaceID: "test-namespace",
		TaskQueue:   "non-existent",
	}

	response, err := s.taskStore.GetTaskQueueUserData(s.ctx, getRequest)
	s.NoError(err)
	s.Nil(response.UserData)
}

func (s *TaskStoreTestSuite) TestListTaskQueueUserDataEntries() {
	// Create multiple task queue user data entries
	updates := map[string]*p.InternalSingleTaskQueueUserDataUpdate{
		"queue-1": {
			UserData: &commonpb.DataBlob{
				Data:         []byte("data-1"),
				EncodingType: enumspb.ENCODING_TYPE_PROTO3,
			},
		},
		"queue-2": {
			UserData: &commonpb.DataBlob{
				Data:         []byte("data-2"),
				EncodingType: enumspb.ENCODING_TYPE_PROTO3,
			},
		},
	}
	err := s.taskStore.UpdateTaskQueueUserData(s.ctx, &p.InternalUpdateTaskQueueUserDataRequest{
		NamespaceID: "test-namespace",
		Updates:     updates,
	})
	s.NoError(err)

	// List task queue user data entries
	listRequest := &p.ListTaskQueueUserDataEntriesRequest{
		NamespaceID: "test-namespace",
		PageSize:    10,
	}

	response, err := s.taskStore.ListTaskQueueUserDataEntries(s.ctx, listRequest)
	s.NoError(err)
	s.Len(response.Entries, 2)

	// Verify entries are sorted by task queue name
	s.Equal("queue-1", response.Entries[0].TaskQueue)
	s.Equal("queue-2", response.Entries[1].TaskQueue)
}

func (s *TaskStoreTestSuite) TestGetTaskQueuesByBuildId() {
	// Create task queue user data with build ID
	updates := map[string]*p.InternalSingleTaskQueueUserDataUpdate{
		"queue-1": {
			UserData: &commonpb.DataBlob{
				Data:         []byte("data-1"),
				EncodingType: enumspb.ENCODING_TYPE_PROTO3,
			},
		},
	}
	err := s.taskStore.UpdateTaskQueueUserData(s.ctx, &p.InternalUpdateTaskQueueUserDataRequest{
		NamespaceID: "test-namespace",
		Updates:     updates,
	})
	s.NoError(err)

	// Get task queues by build ID
	getRequest := &p.GetTaskQueuesByBuildIdRequest{
		NamespaceID: "test-namespace",
		BuildID:     "build-1",
	}

	taskQueues, err := s.taskStore.GetTaskQueuesByBuildId(s.ctx, getRequest)
	s.NoError(err)
	s.Len(taskQueues, 0) // No build ID in our test data
}

func (s *TaskStoreTestSuite) TestCountTaskQueuesByBuildId() {
	// Create task queue user data
	updates := map[string]*p.InternalSingleTaskQueueUserDataUpdate{
		"queue-1": {
			UserData: &commonpb.DataBlob{
				Data:         []byte("data-1"),
				EncodingType: enumspb.ENCODING_TYPE_PROTO3,
			},
		},
	}
	err := s.taskStore.UpdateTaskQueueUserData(s.ctx, &p.InternalUpdateTaskQueueUserDataRequest{
		NamespaceID: "test-namespace",
		Updates:     updates,
	})
	s.NoError(err)

	// Count task queues by build ID
	countRequest := &p.CountTaskQueuesByBuildIdRequest{
		NamespaceID: "test-namespace",
		BuildID:     "build-1",
	}

	count, err := s.taskStore.CountTaskQueuesByBuildId(s.ctx, countRequest)
	s.NoError(err)
	s.Equal(0, count) // No build ID in our test data
}
