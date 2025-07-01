package mongodb

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/tasks"
)

// ExecutionStoreTestSuite tests the MongoDB execution store implementation
type ExecutionStoreTestSuite struct {
	suite.Suite
	client         *mongo.Client
	database       *mongo.Database
	executionStore persistence.ExecutionStore
	ctx            context.Context
}

func TestExecutionStoreSuite(t *testing.T) {
	suite.Run(t, new(ExecutionStoreTestSuite))
}

func (s *ExecutionStoreTestSuite) SetupSuite() {
	s.ctx = context.Background()

	// Connect to MongoDB
	clientOptions := options.Client().ApplyURI("mongodb://localhost:27017")
	client, err := mongo.Connect(s.ctx, clientOptions)
	require.NoError(s.T(), err)

	s.client = client
	s.database = client.Database("test_temporal")

	// Create execution store
	s.executionStore = NewExecutionStore(s.database, log.NewTestLogger())
}

func (s *ExecutionStoreTestSuite) TearDownSuite() {
	if s.client != nil {
		s.client.Disconnect(s.ctx)
	}
}

func (s *ExecutionStoreTestSuite) SetupTest() {
	// Clean up collections before each test
	collections := []string{"workflow_executions", "history_tasks", "history_nodes", "history_branches"}
	for _, collectionName := range collections {
		s.database.Collection(collectionName).DeleteMany(s.ctx, map[string]interface{}{})
	}
}

func (s *ExecutionStoreTestSuite) TestCreateWorkflowExecution() {
	request := &persistence.InternalCreateWorkflowExecutionRequest{
		ShardID: 1,
		NewWorkflowSnapshot: persistence.InternalWorkflowSnapshot{
			NamespaceID: "test-namespace",
			WorkflowID:  "test-workflow",
			RunID:       "test-run-id",
			ExecutionInfoBlob: &commonpb.DataBlob{
				Data:         []byte("execution-info"),
				EncodingType: enumspb.ENCODING_TYPE_PROTO3,
			},
			ExecutionStateBlob: &commonpb.DataBlob{
				Data:         []byte("execution-state"),
				EncodingType: enumspb.ENCODING_TYPE_PROTO3,
			},
			NextEventID:     1,
			DBRecordVersion: 1,
		},
	}

	response, err := s.executionStore.CreateWorkflowExecution(s.ctx, request)
	s.NoError(err)
	s.NotNil(response)
}

func (s *ExecutionStoreTestSuite) TestGetWorkflowExecution() {
	// Create workflow execution first
	createRequest := &persistence.InternalCreateWorkflowExecutionRequest{
		ShardID: 1,
		NewWorkflowSnapshot: persistence.InternalWorkflowSnapshot{
			NamespaceID: "test-namespace",
			WorkflowID:  "test-workflow",
			RunID:       "test-run-id",
			ExecutionInfoBlob: &commonpb.DataBlob{
				Data:         []byte("execution-info"),
				EncodingType: enumspb.ENCODING_TYPE_PROTO3,
			},
			ExecutionStateBlob: &commonpb.DataBlob{
				Data:         []byte("execution-state"),
				EncodingType: enumspb.ENCODING_TYPE_PROTO3,
			},
			NextEventID:     1,
			DBRecordVersion: 1,
		},
	}
	_, err := s.executionStore.CreateWorkflowExecution(s.ctx, createRequest)
	s.NoError(err)

	// Get workflow execution
	getRequest := &persistence.GetWorkflowExecutionRequest{
		ShardID:     1,
		NamespaceID: "test-namespace",
		WorkflowID:  "test-workflow",
		RunID:       "test-run-id",
	}

	response, err := s.executionStore.GetWorkflowExecution(s.ctx, getRequest)
	s.NoError(err)
	s.NotNil(response)
	s.Equal(int64(1), response.State.NextEventID)
	s.Equal(int64(1), response.State.DBRecordVersion)
	s.Equal([]byte("execution-info"), response.State.ExecutionInfo.Data)
	s.Equal([]byte("execution-state"), response.State.ExecutionState.Data)
}

func (s *ExecutionStoreTestSuite) TestGetWorkflowExecution_NotFound() {
	getRequest := &persistence.GetWorkflowExecutionRequest{
		ShardID:     1,
		NamespaceID: "non-existent",
		WorkflowID:  "non-existent",
		RunID:       "non-existent",
	}

	_, err := s.executionStore.GetWorkflowExecution(s.ctx, getRequest)
	s.Error(err)
	s.IsType(&persistence.ConditionFailedError{}, err)
}

func (s *ExecutionStoreTestSuite) TestUpdateWorkflowExecution() {
	// Create workflow execution first
	createRequest := &persistence.InternalCreateWorkflowExecutionRequest{
		ShardID: 1,
		NewWorkflowSnapshot: persistence.InternalWorkflowSnapshot{
			NamespaceID: "test-namespace",
			WorkflowID:  "test-workflow",
			RunID:       "test-run-id",
			ExecutionInfoBlob: &commonpb.DataBlob{
				Data:         []byte("initial-execution-info"),
				EncodingType: enumspb.ENCODING_TYPE_PROTO3,
			},
			ExecutionStateBlob: &commonpb.DataBlob{
				Data:         []byte("initial-execution-state"),
				EncodingType: enumspb.ENCODING_TYPE_PROTO3,
			},
			NextEventID:     1,
			DBRecordVersion: 1,
		},
	}
	_, err := s.executionStore.CreateWorkflowExecution(s.ctx, createRequest)
	s.NoError(err)

	// Update workflow execution
	updateRequest := &persistence.InternalUpdateWorkflowExecutionRequest{
		ShardID: 1,
		UpdateWorkflowMutation: persistence.InternalWorkflowMutation{
			NamespaceID: "test-namespace",
			WorkflowID:  "test-workflow",
			RunID:       "test-run-id",
			ExecutionInfoBlob: &commonpb.DataBlob{
				Data:         []byte("updated-execution-info"),
				EncodingType: enumspb.ENCODING_TYPE_PROTO3,
			},
			ExecutionStateBlob: &commonpb.DataBlob{
				Data:         []byte("updated-execution-state"),
				EncodingType: enumspb.ENCODING_TYPE_PROTO3,
			},
			NextEventID:     2,
			DBRecordVersion: 2,
		},
	}

	err = s.executionStore.UpdateWorkflowExecution(s.ctx, updateRequest)
	s.NoError(err)

	// Verify update
	getRequest := &persistence.GetWorkflowExecutionRequest{
		ShardID:     1,
		NamespaceID: "test-namespace",
		WorkflowID:  "test-workflow",
		RunID:       "test-run-id",
	}

	response, err := s.executionStore.GetWorkflowExecution(s.ctx, getRequest)
	s.NoError(err)
	s.Equal(int64(2), response.State.NextEventID)
	s.Equal(int64(2), response.State.DBRecordVersion)
	s.Equal([]byte("updated-execution-info"), response.State.ExecutionInfo.Data)
	s.Equal([]byte("updated-execution-state"), response.State.ExecutionState.Data)
}

func (s *ExecutionStoreTestSuite) TestDeleteWorkflowExecution() {
	// Create workflow execution first
	createRequest := &persistence.InternalCreateWorkflowExecutionRequest{
		ShardID: 1,
		NewWorkflowSnapshot: persistence.InternalWorkflowSnapshot{
			NamespaceID: "test-namespace",
			WorkflowID:  "test-workflow",
			RunID:       "test-run-id",
			ExecutionInfoBlob: &commonpb.DataBlob{
				Data:         []byte("execution-info"),
				EncodingType: enumspb.ENCODING_TYPE_PROTO3,
			},
			ExecutionStateBlob: &commonpb.DataBlob{
				Data:         []byte("execution-state"),
				EncodingType: enumspb.ENCODING_TYPE_PROTO3,
			},
			NextEventID:     1,
			DBRecordVersion: 1,
		},
	}
	_, err := s.executionStore.CreateWorkflowExecution(s.ctx, createRequest)
	s.NoError(err)

	// Delete workflow execution
	deleteRequest := &persistence.DeleteWorkflowExecutionRequest{
		ShardID:     1,
		NamespaceID: "test-namespace",
		WorkflowID:  "test-workflow",
		RunID:       "test-run-id",
	}

	err = s.executionStore.DeleteWorkflowExecution(s.ctx, deleteRequest)
	s.NoError(err)

	// Verify deletion
	getRequest := &persistence.GetWorkflowExecutionRequest{
		ShardID:     1,
		NamespaceID: "test-namespace",
		WorkflowID:  "test-workflow",
		RunID:       "test-run-id",
	}

	_, err = s.executionStore.GetWorkflowExecution(s.ctx, getRequest)
	s.Error(err)
	s.IsType(&persistence.ConditionFailedError{}, err)
}

func (s *ExecutionStoreTestSuite) TestGetCurrentExecution() {
	// Create workflow execution first
	createRequest := &persistence.InternalCreateWorkflowExecutionRequest{
		ShardID: 1,
		NewWorkflowSnapshot: persistence.InternalWorkflowSnapshot{
			NamespaceID: "test-namespace",
			WorkflowID:  "test-workflow",
			RunID:       "test-run-id",
			ExecutionInfoBlob: &commonpb.DataBlob{
				Data:         []byte("execution-info"),
				EncodingType: enumspb.ENCODING_TYPE_PROTO3,
			},
			ExecutionStateBlob: &commonpb.DataBlob{
				Data:         []byte("execution-state"),
				EncodingType: enumspb.ENCODING_TYPE_PROTO3,
			},
			NextEventID:     1,
			DBRecordVersion: 1,
		},
	}
	_, err := s.executionStore.CreateWorkflowExecution(s.ctx, createRequest)
	s.NoError(err)

	// Get current execution
	getRequest := &persistence.GetCurrentExecutionRequest{
		ShardID:     1,
		NamespaceID: "test-namespace",
		WorkflowID:  "test-workflow",
	}

	response, err := s.executionStore.GetCurrentExecution(s.ctx, getRequest)
	s.NoError(err)
	s.NotNil(response)
	s.Equal("test-run-id", response.RunID)
}

func (s *ExecutionStoreTestSuite) TestGetCurrentExecution_NotFound() {
	getRequest := &persistence.GetCurrentExecutionRequest{
		ShardID:     1,
		NamespaceID: "non-existent",
		WorkflowID:  "non-existent",
	}

	_, err := s.executionStore.GetCurrentExecution(s.ctx, getRequest)
	s.Error(err)
	s.IsType(&persistence.ConditionFailedError{}, err)
}

func (s *ExecutionStoreTestSuite) TestListConcreteExecutions() {
	// Create multiple workflow executions
	for i := 0; i < 3; i++ {
		createRequest := &persistence.InternalCreateWorkflowExecutionRequest{
			ShardID: 1,
			NewWorkflowSnapshot: persistence.InternalWorkflowSnapshot{
				NamespaceID: "test-namespace",
				WorkflowID:  fmt.Sprintf("test-workflow-%d", i),
				RunID:       fmt.Sprintf("test-run-id-%d", i),
				ExecutionInfoBlob: &commonpb.DataBlob{
					Data:         []byte(fmt.Sprintf("execution-info-%d", i)),
					EncodingType: enumspb.ENCODING_TYPE_PROTO3,
				},
				ExecutionStateBlob: &commonpb.DataBlob{
					Data:         []byte(fmt.Sprintf("execution-state-%d", i)),
					EncodingType: enumspb.ENCODING_TYPE_PROTO3,
				},
				NextEventID:     int64(i + 1),
				DBRecordVersion: int64(i + 1),
			},
		}
		_, err := s.executionStore.CreateWorkflowExecution(s.ctx, createRequest)
		s.NoError(err)
	}

	// List concrete executions
	listRequest := &persistence.ListConcreteExecutionsRequest{
		ShardID:   1,
		PageSize:  10,
		PageToken: nil,
	}

	response, err := s.executionStore.ListConcreteExecutions(s.ctx, listRequest)
	s.NoError(err)
	s.NotNil(response)
	s.Len(response.States, 3)
}

func (s *ExecutionStoreTestSuite) TestAddHistoryTasks() {
	request := &persistence.InternalAddHistoryTasksRequest{
		ShardID: 1,
		Tasks: map[tasks.Category][]persistence.InternalHistoryTask{
			tasks.CategoryTransfer: {
				{
					Key: tasks.Key{
						TaskID:   1,
						FireTime: time.Now(),
					},
					Blob: &commonpb.DataBlob{
						Data:         []byte("task-data"),
						EncodingType: enumspb.ENCODING_TYPE_PROTO3,
					},
				},
			},
			tasks.CategoryTimer: {
				{
					Key: tasks.Key{
						TaskID:   2,
						FireTime: time.Now(),
					},
					Blob: &commonpb.DataBlob{
						Data:         []byte("timer-data"),
						EncodingType: enumspb.ENCODING_TYPE_PROTO3,
					},
				},
			},
		},
	}
	err := s.executionStore.AddHistoryTasks(s.ctx, request)
	s.NoError(err)
}

func (s *ExecutionStoreTestSuite) TestGetHistoryTasks() {
	// Add some tasks first
	historyTasks := map[tasks.Category][]persistence.InternalHistoryTask{
		tasks.CategoryTransfer: {
			{
				Key: tasks.Key{
					TaskID:   1,
					FireTime: time.Now(),
				},
				Blob: &commonpb.DataBlob{
					Data:         []byte("task-data"),
					EncodingType: enumspb.ENCODING_TYPE_PROTO3,
				},
			},
		},
	}
	addRequest := &persistence.InternalAddHistoryTasksRequest{
		ShardID: 1,
		Tasks:   historyTasks,
	}
	err := s.executionStore.AddHistoryTasks(s.ctx, addRequest)
	s.NoError(err)

	// Get tasks
	getRequest := &persistence.GetHistoryTasksRequest{
		ShardID:             1,
		TaskCategory:        tasks.CategoryTransfer,
		InclusiveMinTaskKey: tasks.NewImmediateKey(0),
		ExclusiveMaxTaskKey: tasks.NewImmediateKey(10),
		BatchSize:           10,
		NextPageToken:       nil,
	}

	response, err := s.executionStore.GetHistoryTasks(s.ctx, getRequest)
	s.NoError(err)
	s.NotNil(response)
	s.Len(response.Tasks, 1)
}

func (s *ExecutionStoreTestSuite) TestCompleteHistoryTask() {
	// Add a task first
	historyTasks := map[tasks.Category][]persistence.InternalHistoryTask{
		tasks.CategoryTransfer: {
			{
				Key: tasks.Key{
					TaskID:   1,
					FireTime: time.Now(),
				},
				Blob: &commonpb.DataBlob{
					Data:         []byte("task-data"),
					EncodingType: enumspb.ENCODING_TYPE_PROTO3,
				},
			},
		},
	}
	addRequest := &persistence.InternalAddHistoryTasksRequest{
		ShardID: 1,
		Tasks:   historyTasks,
	}
	err := s.executionStore.AddHistoryTasks(s.ctx, addRequest)
	s.NoError(err)

	// Complete the task
	completeRequest := &persistence.CompleteHistoryTaskRequest{
		ShardID:      1,
		TaskCategory: tasks.CategoryTransfer,
		TaskKey:      tasks.NewImmediateKey(1),
	}
	err = s.executionStore.CompleteHistoryTask(s.ctx, completeRequest)
	s.NoError(err)

	// Verify task is completed
	getRequest := &persistence.GetHistoryTasksRequest{
		ShardID:             1,
		TaskCategory:        tasks.CategoryTransfer,
		InclusiveMinTaskKey: tasks.NewImmediateKey(0),
		ExclusiveMaxTaskKey: tasks.NewImmediateKey(10),
		BatchSize:           10,
		NextPageToken:       nil,
	}

	response, err := s.executionStore.GetHistoryTasks(s.ctx, getRequest)
	s.NoError(err)
	s.Len(response.Tasks, 0) // Task should be deleted
}

func (s *ExecutionStoreTestSuite) TestRangeCompleteHistoryTasks() {
	// Add multiple tasks first
	historyTasks := map[tasks.Category][]persistence.InternalHistoryTask{
		tasks.CategoryTransfer: {
			{
				Key: tasks.Key{
					TaskID:   1,
					FireTime: time.Now(),
				},
				Blob: &commonpb.DataBlob{
					Data:         []byte("task-1"),
					EncodingType: enumspb.ENCODING_TYPE_PROTO3,
				},
			},
			{
				Key: tasks.Key{
					TaskID:   2,
					FireTime: time.Now(),
				},
				Blob: &commonpb.DataBlob{
					Data:         []byte("task-2"),
					EncodingType: enumspb.ENCODING_TYPE_PROTO3,
				},
			},
			{
				Key: tasks.Key{
					TaskID:   3,
					FireTime: time.Now(),
				},
				Blob: &commonpb.DataBlob{
					Data:         []byte("task-3"),
					EncodingType: enumspb.ENCODING_TYPE_PROTO3,
				},
			},
		},
	}
	addRequest := &persistence.InternalAddHistoryTasksRequest{
		ShardID: 1,
		Tasks:   historyTasks,
	}
	err := s.executionStore.AddHistoryTasks(s.ctx, addRequest)
	s.NoError(err)

	// Complete range of history tasks
	completeRequest := &persistence.RangeCompleteHistoryTasksRequest{
		ShardID:             1,
		TaskCategory:        tasks.CategoryTransfer,
		InclusiveMinTaskKey: tasks.NewImmediateKey(1),
		ExclusiveMaxTaskKey: tasks.NewImmediateKey(3),
	}
	err = s.executionStore.RangeCompleteHistoryTasks(s.ctx, completeRequest)
	s.NoError(err)

	// Verify tasks are completed
	getRequest := &persistence.GetHistoryTasksRequest{
		ShardID:             1,
		TaskCategory:        tasks.CategoryTransfer,
		InclusiveMinTaskKey: tasks.NewImmediateKey(0),
		ExclusiveMaxTaskKey: tasks.NewImmediateKey(10),
		BatchSize:           10,
		NextPageToken:       nil,
	}

	response, err := s.executionStore.GetHistoryTasks(s.ctx, getRequest)
	s.NoError(err)
	s.Len(response.Tasks, 0) // All tasks in range should be deleted
}

func (s *ExecutionStoreTestSuite) TestAppendHistoryNodes() {
	// Create a mock branch info
	branchInfo := &persistencespb.HistoryBranch{
		TreeId:   "tree-id",
		BranchId: "branch-id",
		Ancestors: []*persistencespb.HistoryBranchRange{
			{
				BranchId:    "ancestor-branch",
				BeginNodeId: 1,
				EndNodeId:   10,
			},
		},
	}

	request := &persistence.InternalAppendHistoryNodesRequest{
		ShardID:     1,
		BranchToken: []byte("branch-token"),
		IsNewBranch: true,
		Info:        "test-info",
		BranchInfo:  branchInfo,
		TreeInfo:    &commonpb.DataBlob{Data: []byte("tree-info"), EncodingType: enumspb.ENCODING_TYPE_PROTO3},
		Node: persistence.InternalHistoryNode{
			NodeID:            1,
			Events:            &commonpb.DataBlob{Data: []byte("events"), EncodingType: enumspb.ENCODING_TYPE_PROTO3},
			PrevTransactionID: 0,
			TransactionID:     1,
		},
	}

	err := s.executionStore.AppendHistoryNodes(s.ctx, request)
	s.NoError(err)
}

func (s *ExecutionStoreTestSuite) TestReadHistoryBranch() {
	// Create a mock branch info
	branchInfo := &persistencespb.HistoryBranch{
		TreeId:   "tree-id",
		BranchId: "branch-id",
		Ancestors: []*persistencespb.HistoryBranchRange{
			{
				BranchId:    "ancestor-branch",
				BeginNodeId: 1,
				EndNodeId:   10,
			},
		},
	}

	appendRequest := &persistence.InternalAppendHistoryNodesRequest{
		ShardID:     1,
		BranchToken: []byte("branch-token"),
		IsNewBranch: true,
		Info:        "test-info",
		BranchInfo:  branchInfo,
		TreeInfo:    &commonpb.DataBlob{Data: []byte("tree-info"), EncodingType: enumspb.ENCODING_TYPE_PROTO3},
		Node: persistence.InternalHistoryNode{
			NodeID:            1,
			Events:            &commonpb.DataBlob{Data: []byte("events"), EncodingType: enumspb.ENCODING_TYPE_PROTO3},
			PrevTransactionID: 0,
			TransactionID:     1,
		},
	}
	err := s.executionStore.AppendHistoryNodes(s.ctx, appendRequest)
	s.NoError(err)

	// Read history branch
	readRequest := &persistence.InternalReadHistoryBranchRequest{
		ShardID:       1,
		BranchToken:   []byte("branch-token"),
		BranchID:      "branch-id",
		MinNodeID:     0,
		MaxNodeID:     10,
		PageSize:      10,
		NextPageToken: nil,
	}

	response, err := s.executionStore.ReadHistoryBranch(s.ctx, readRequest)
	s.NoError(err)
	s.NotNil(response)
	s.Len(response.Nodes, 1) // Should have one node with the events
}

func (s *ExecutionStoreTestSuite) TestDeleteHistoryNodes() {
	// Create a mock branch info
	branchInfo := &persistencespb.HistoryBranch{
		TreeId:   "tree-id",
		BranchId: "branch-id",
		Ancestors: []*persistencespb.HistoryBranchRange{
			{
				BranchId:    "ancestor-branch",
				BeginNodeId: 1,
				EndNodeId:   10,
			},
		},
	}

	appendRequest := &persistence.InternalAppendHistoryNodesRequest{
		ShardID:     1,
		BranchToken: []byte("branch-token"),
		IsNewBranch: true,
		Info:        "test-info",
		BranchInfo:  branchInfo,
		TreeInfo:    &commonpb.DataBlob{Data: []byte("tree-info"), EncodingType: enumspb.ENCODING_TYPE_PROTO3},
		Node: persistence.InternalHistoryNode{
			NodeID:            1,
			Events:            &commonpb.DataBlob{Data: []byte("events"), EncodingType: enumspb.ENCODING_TYPE_PROTO3},
			PrevTransactionID: 0,
			TransactionID:     1,
		},
	}
	err := s.executionStore.AppendHistoryNodes(s.ctx, appendRequest)
	s.NoError(err)

	// Delete history nodes
	deleteRequest := &persistence.InternalDeleteHistoryNodesRequest{
		ShardID:       1,
		BranchToken:   []byte("branch-token"),
		BranchInfo:    branchInfo,
		NodeID:        1,
		TransactionID: 1,
	}

	err = s.executionStore.DeleteHistoryNodes(s.ctx, deleteRequest)
	s.NoError(err)
}

func (s *ExecutionStoreTestSuite) TestDeleteHistoryBranch() {
	// Create a mock branch info
	branchInfo := &persistencespb.HistoryBranch{
		TreeId:   "tree-id",
		BranchId: "branch-id",
		Ancestors: []*persistencespb.HistoryBranchRange{
			{
				BranchId:    "ancestor-branch",
				BeginNodeId: 1,
				EndNodeId:   10,
			},
		},
	}

	// Append history nodes first
	appendRequest := &persistence.InternalAppendHistoryNodesRequest{
		ShardID:     1,
		BranchToken: []byte("branch-token"),
		IsNewBranch: true,
		Info:        "test-info",
		BranchInfo:  branchInfo,
		TreeInfo:    &commonpb.DataBlob{Data: []byte("tree-info"), EncodingType: enumspb.ENCODING_TYPE_PROTO3},
		Node: persistence.InternalHistoryNode{
			NodeID:            1,
			Events:            &commonpb.DataBlob{Data: []byte("events"), EncodingType: enumspb.ENCODING_TYPE_PROTO3},
			PrevTransactionID: 0,
			TransactionID:     1,
		},
	}
	err := s.executionStore.AppendHistoryNodes(s.ctx, appendRequest)
	s.NoError(err)

	// Delete history branch
	deleteRequest := &persistence.InternalDeleteHistoryBranchRequest{
		ShardID:     1,
		BranchToken: []byte("branch-token"),
		BranchInfo:  branchInfo,
	}

	err = s.executionStore.DeleteHistoryBranch(s.ctx, deleteRequest)
	s.NoError(err)

	// Verify branch is deleted
	readRequest := &persistence.InternalReadHistoryBranchRequest{
		ShardID:       1,
		BranchToken:   []byte("branch-token"),
		BranchID:      "branch-id",
		MinNodeID:     0,
		MaxNodeID:     10,
		PageSize:      10,
		NextPageToken: nil,
	}

	response, err := s.executionStore.ReadHistoryBranch(s.ctx, readRequest)
	s.NoError(err)
	s.Len(response.Nodes, 0) // Branch should be deleted
}

func (s *ExecutionStoreTestSuite) TestGetHistoryBranchUtil() {
	util := s.executionStore.GetHistoryBranchUtil()
	s.NotNil(util)
}

func (s *ExecutionStoreTestSuite) TestGetName() {
	name := s.executionStore.GetName()
	s.Equal("mongodb-execution-store", name)
}

func (s *ExecutionStoreTestSuite) TestClose() {
	// This should not panic
	s.executionStore.Close()
}
