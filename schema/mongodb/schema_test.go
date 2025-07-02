package mongodb

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// TestSchemaCreation tests the creation of all collections and indexes
func TestSchemaCreation(t *testing.T) {
	// Skip if not running integration tests
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	// Connect to MongoDB
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI("mongodb://localhost:27017"))
	require.NoError(t, err)
	defer func() {
		err := client.Disconnect(ctx)
		require.NoError(t, err)
	}()

	// Test database name
	dbName := "temporal_test"

	// Create schema
	schema := NewSchema(dbName)
	err = schema.CreateSchema(ctx, client)
	require.NoError(t, err)

	// Verify collections exist
	db := client.Database(dbName)
	collections, err := db.ListCollectionNames(ctx, bson.M{})
	require.NoError(t, err)

	expectedCollections := []string{
		"executions",
		"history_node",
		"history_tree",
		"tasks",
		"task_queue_user_data",
		"namespaces_by_id",
		"namespaces",
		"queue_metadata",
		"queue",
		"cluster_metadata_info",
		"cluster_membership",
		"queues",
		"queue_messages",
		"nexus_endpoints",
	}

	for _, expected := range expectedCollections {
		assert.Contains(t, collections, expected, "Collection %s should exist", expected)
	}

	// Test sample data insertion
	t.Run("TestSampleDataInsertion", func(t *testing.T) {
		testSampleDataInsertion(ctx, t, db)
	})

	// Clean up
	err = db.Drop(ctx)
	require.NoError(t, err)
}

// testSampleDataInsertion tests inserting and querying sample data
func testSampleDataInsertion(ctx context.Context, t *testing.T, db *mongo.Database) {
	// Test executions collection
	t.Run("ExecutionsCollection", func(t *testing.T) {
		coll := db.Collection("executions")

		// Create sample execution document
		sampleExecution := bson.M{
			"shard_id":                      1,
			"type":                          1,
			"namespace_id":                  []byte("12345678901234567890123456789012"),
			"workflow_id":                   "test-workflow",
			"run_id":                        []byte("12345678901234567890123456789012"),
			"current_run_id":                []byte("12345678901234567890123456789012"),
			"visibility_ts":                 time.Now(),
			"task_id":                       123456789,
			"shard_encoding":                "proto3",
			"execution_encoding":            "proto3",
			"execution_state_encoding":      "proto3",
			"transfer_encoding":             "proto3",
			"replication_encoding":          "proto3",
			"timer_encoding":                "proto3",
			"visibility_task_encoding":      "proto3",
			"task_encoding":                 "proto3",
			"next_event_id":                 1,
			"range_id":                      1,
			"activity_map_encoding":         "proto3",
			"timer_map_encoding":            "proto3",
			"child_executions_map_encoding": "proto3",
			"request_cancel_map_encoding":   "proto3",
			"signal_map_encoding":           "proto3",
			"chasm_node_map_encoding":       "proto3",
			"workflow_last_write_version":   1,
			"workflow_state":                1,
			"checksum_encoding":             "proto3",
			"db_record_version":             1,
		}

		// Insert document
		result, err := coll.InsertOne(ctx, sampleExecution)
		require.NoError(t, err)
		assert.NotNil(t, result.InsertedID)

		// Query document
		var found bson.M
		err = coll.FindOne(ctx, bson.M{"_id": result.InsertedID}).Decode(&found)
		require.NoError(t, err)
		assert.Equal(t, "test-workflow", found["workflow_id"])

		// Clean up
		_, err = coll.DeleteOne(ctx, bson.M{"_id": result.InsertedID})
		require.NoError(t, err)
	})

	// Test namespaces collection
	t.Run("NamespacesCollection", func(t *testing.T) {
		coll := db.Collection("namespaces")

		sampleNamespace := bson.M{
			"namespaces_partition": 1,
			"name":                 "test-namespace",
			"id":                   []byte("12345678901234567890123456789012"),
			"detail_encoding":      "proto3",
			"is_global_namespace":  false,
			"notification_version": 1,
		}

		result, err := coll.InsertOne(ctx, sampleNamespace)
		require.NoError(t, err)
		assert.NotNil(t, result.InsertedID)

		var found bson.M
		err = coll.FindOne(ctx, bson.M{"_id": result.InsertedID}).Decode(&found)
		require.NoError(t, err)
		assert.Equal(t, "test-namespace", found["name"])

		// Clean up
		_, err = coll.DeleteOne(ctx, bson.M{"_id": result.InsertedID})
		require.NoError(t, err)
	})

	// Test tasks collection
	t.Run("TasksCollection", func(t *testing.T) {
		coll := db.Collection("tasks")

		sampleTask := bson.M{
			"namespace_id":        []byte("12345678901234567890123456789012"),
			"task_queue_name":     "test-queue",
			"task_queue_type":     1,
			"type":                1,
			"task_id":             123456789,
			"range_id":            1,
			"task_encoding":       "proto3",
			"task_queue_encoding": "proto3",
		}

		result, err := coll.InsertOne(ctx, sampleTask)
		require.NoError(t, err)
		assert.NotNil(t, result.InsertedID)

		var found bson.M
		err = coll.FindOne(ctx, bson.M{"_id": result.InsertedID}).Decode(&found)
		require.NoError(t, err)
		assert.Equal(t, "test-queue", found["task_queue_name"])

		// Clean up
		_, err = coll.DeleteOne(ctx, bson.M{"_id": result.InsertedID})
		require.NoError(t, err)
	})
}

// TestIndexValidation tests that all required indexes are created
func TestIndexValidation(t *testing.T) {
	// Skip if not running integration tests
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI("mongodb://localhost:27017"))
	require.NoError(t, err)
	defer func() {
		err := client.Disconnect(ctx)
		require.NoError(t, err)
	}()

	dbName := "temporal_test"
	schema := NewSchema(dbName)
	err = schema.CreateSchema(ctx, client)
	require.NoError(t, err)

	db := client.Database(dbName)

	// Test executions indexes
	t.Run("ExecutionsIndexes", func(t *testing.T) {
		coll := db.Collection("executions")
		indexes, err := coll.Indexes().List(ctx)
		require.NoError(t, err)

		indexNames := make([]string, 0)
		for indexes.Next(ctx) {
			var index bson.M
			err := indexes.Decode(&index)
			require.NoError(t, err)
			indexNames = append(indexNames, index["name"].(string))
		}

		expectedIndexes := []string{
			"_id_",
			"executions_primary_key",
			"executions_workflow_lookup",
			"executions_visibility",
		}

		for _, expected := range expectedIndexes {
			assert.Contains(t, indexNames, expected, "Index %s should exist", expected)
		}
	})

	// Test history_node indexes
	t.Run("HistoryNodeIndexes", func(t *testing.T) {
		coll := db.Collection("history_node")
		indexes, err := coll.Indexes().List(ctx)
		require.NoError(t, err)

		indexNames := make([]string, 0)
		for indexes.Next(ctx) {
			var index bson.M
			err := indexes.Decode(&index)
			require.NoError(t, err)
			indexNames = append(indexNames, index["name"].(string))
		}

		expectedIndexes := []string{
			"_id_",
			"history_node_primary_key",
			"history_node_tree_lookup",
		}

		for _, expected := range expectedIndexes {
			assert.Contains(t, indexNames, expected, "Index %s should exist", expected)
		}
	})

	// Clean up
	err = db.Drop(ctx)
	require.NoError(t, err)
}

// TestSchemaValidation tests that validation schemas are properly applied
func TestSchemaValidation(t *testing.T) {
	// Skip if not running integration tests
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI("mongodb://localhost:27017"))
	require.NoError(t, err)
	defer func() {
		err := client.Disconnect(ctx)
		require.NoError(t, err)
	}()

	dbName := "temporal_test"
	schema := NewSchema(dbName)
	err = schema.CreateSchema(ctx, client)
	require.NoError(t, err)

	db := client.Database(dbName)

	// Test that invalid documents are rejected
	t.Run("InvalidDocumentRejection", func(t *testing.T) {
		coll := db.Collection("executions")

		// Try to insert document without required fields
		invalidDoc := bson.M{
			"shard_id": 1,
			// Missing required fields: type, namespace_id, workflow_id, run_id
		}

		_, err := coll.InsertOne(ctx, invalidDoc)
		assert.Error(t, err, "Should reject document without required fields")
	})

	// Test that valid documents are accepted
	t.Run("ValidDocumentAcceptance", func(t *testing.T) {
		coll := db.Collection("executions")

		validDoc := bson.M{
			"shard_id":     1,
			"type":         1,
			"namespace_id": []byte("12345678901234567890123456789012"),
			"workflow_id":  "test-workflow",
			"run_id":       []byte("12345678901234567890123456789012"),
		}

		result, err := coll.InsertOne(ctx, validDoc)
		assert.NoError(t, err, "Should accept valid document")
		assert.NotNil(t, result.InsertedID)

		// Clean up
		_, err = coll.DeleteOne(ctx, bson.M{"_id": result.InsertedID})
		require.NoError(t, err)
	})

	// Clean up
	err = db.Drop(ctx)
	require.NoError(t, err)
}
