package mongodb

import (
	"context"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.temporal.io/server/schema/mongodb"
)

// TestMongoDBHandler tests the MongoDB handler functionality
func TestMongoDBHandler(t *testing.T) {
	// Skip if no MongoDB connection available
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI("mongodb://localhost:27017"))
	if err != nil {
		t.Skip("MongoDB not available, skipping test")
		return
	}
	defer client.Disconnect(ctx)

	// Test database name
	testDB := "temporal_test"

	// Create handler
	handler := NewMongoDBHandler(client, testDB)

	// Test health check
	t.Run("HealthCheck", func(t *testing.T) {
		err := handler.HealthCheck(ctx)
		if err != nil {
			t.Errorf("Health check failed: %v", err)
		}
	})

	// Test list collections
	t.Run("ListCollections", func(t *testing.T) {
		err := handler.ListCollections(ctx)
		if err != nil {
			t.Errorf("List collections failed: %v", err)
		}
	})

	// Test database info
	t.Run("GetDatabaseInfo", func(t *testing.T) {
		err := handler.GetDatabaseInfo(ctx)
		if err != nil {
			t.Errorf("Get database info failed: %v", err)
		}
	})
}

// TestMongoDBHandlerWithTestData tests handler with test data
func TestMongoDBHandlerWithTestData(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI("mongodb://localhost:27017"))
	if err != nil {
		t.Skip("MongoDB not available, skipping test")
		return
	}
	defer client.Disconnect(ctx)

	testDB := "temporal_test_handler"
	handler := NewMongoDBHandler(client, testDB)

	// Clean up test database
	db := client.Database(testDB)
	defer db.Drop(ctx)

	// Create test collection
	testCollection := "test_collection"
	coll := db.Collection(testCollection)

	// Insert test data
	testDoc := bson.M{
		"name":       "test_document",
		"value":      42,
		"created_at": time.Now(),
	}
	_, err = coll.InsertOne(ctx, testDoc)
	if err != nil {
		t.Fatalf("Failed to insert test document: %v", err)
	}

	// Test collection stats
	t.Run("GetCollectionStats", func(t *testing.T) {
		err := handler.GetCollectionStats(ctx, testCollection)
		if err != nil {
			t.Errorf("Get collection stats failed: %v", err)
		}
	})

	// Test get indexes
	t.Run("GetIndexes", func(t *testing.T) {
		err := handler.GetIndexes(ctx, testCollection)
		if err != nil {
			t.Errorf("Get indexes failed: %v", err)
		}
	})

	// Test backup and restore
	t.Run("BackupAndRestore", func(t *testing.T) {
		backupName := "test_collection_backup"
		targetName := "test_collection_restored"

		// Backup
		err := handler.BackupCollection(ctx, testCollection, backupName)
		if err != nil {
			t.Errorf("Backup collection failed: %v", err)
		}

		// Restore
		err = handler.RestoreCollection(ctx, backupName, targetName)
		if err != nil {
			t.Errorf("Restore collection failed: %v", err)
		}

		// Verify restore
		originalCount, _ := coll.CountDocuments(ctx, bson.M{})
		restoredCount, _ := db.Collection(targetName).CountDocuments(ctx, bson.M{})
		if originalCount != restoredCount {
			t.Errorf("Restore verification failed: expected %d documents, got %d", originalCount, restoredCount)
		}
	})
}

// TestCleanupOldData tests the cleanup functionality
func TestCleanupOldData(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI("mongodb://localhost:27017"))
	if err != nil {
		t.Skip("MongoDB not available, skipping test")
		return
	}
	defer client.Disconnect(ctx)

	testDB := "temporal_test_cleanup"
	handler := NewMongoDBHandler(client, testDB)

	// Clean up test database
	db := client.Database(testDB)
	defer db.Drop(ctx)

	testCollection := "test_cleanup"
	coll := db.Collection(testCollection)

	// Insert test data with different timestamps
	now := time.Now()
	oldTime := now.Add(-2 * time.Hour)
	newTime := now.Add(-30 * time.Minute)

	docs := []interface{}{
		bson.M{"name": "old_doc", "created_at": oldTime},
		bson.M{"name": "new_doc", "created_at": newTime},
		bson.M{"name": "current_doc", "created_at": now},
	}

	_, err = coll.InsertMany(ctx, docs)
	if err != nil {
		t.Fatalf("Failed to insert test documents: %v", err)
	}

	// Test cleanup with 1 hour threshold
	t.Run("CleanupOldData", func(t *testing.T) {
		err := handler.CleanupOldData(ctx, testCollection, time.Hour)
		if err != nil {
			t.Errorf("Cleanup old data failed: %v", err)
		}

		// Verify cleanup
		count, err := coll.CountDocuments(ctx, bson.M{})
		if err != nil {
			t.Errorf("Failed to count documents after cleanup: %v", err)
		}

		// Should have 2 documents left (new_doc and current_doc)
		if count != 2 {
			t.Errorf("Expected 2 documents after cleanup, got %d", count)
		}
	})
}

// TestSchemaValidation tests schema validation functions
func TestSchemaValidation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI("mongodb://localhost:27017"))
	if err != nil {
		t.Skip("MongoDB not available, skipping test")
		return
	}
	defer client.Disconnect(ctx)

	testDB := "temporal_test_schema"
	db := client.Database(testDB)
	defer db.Drop(ctx)

	// Test validateIndexes with non-existent collection
	t.Run("ValidateIndexesNonExistent", func(t *testing.T) {
		err := validateIndexes(ctx, db, false)
		if err == nil {
			t.Error("Expected error when validating indexes on non-existent collection")
		}
	})

	// Test validateSchema with non-existent collections
	t.Run("ValidateSchemaNonExistent", func(t *testing.T) {
		schema := &mongodb.Schema{DatabaseName: testDB}
		err := validateSchema(ctx, client, schema, false)
		if err == nil {
			t.Error("Expected error when validating schema with non-existent collections")
		}
	})
}

// TestDurationParsing tests the duration parsing functionality
func TestDurationParsing(t *testing.T) {
	testCases := []struct {
		input    string
		expected time.Duration
		valid    bool
	}{
		{"30d", 30 * 24 * time.Hour, true},
		{"24h", 24 * time.Hour, true},
		{"1h30m", 90 * time.Minute, true},
		{"invalid", 0, false},
		{"", 0, false},
	}

	for _, tc := range testCases {
		t.Run(tc.input, func(t *testing.T) {
			duration, err := time.ParseDuration(tc.input)
			if tc.valid && err != nil {
				t.Errorf("Expected valid duration for %s, got error: %v", tc.input, err)
			}
			if !tc.valid && err == nil {
				t.Errorf("Expected invalid duration for %s, but got no error", tc.input)
			}
			if tc.valid && duration != tc.expected {
				t.Errorf("Expected duration %v for %s, got %v", tc.expected, tc.input, duration)
			}
		})
	}
}

// BenchmarkMongoDBHandler benchmarks the handler operations
func BenchmarkMongoDBHandler(b *testing.B) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI("mongodb://localhost:27017"))
	if err != nil {
		b.Skip("MongoDB not available, skipping benchmark")
		return
	}
	defer client.Disconnect(ctx)

	testDB := "temporal_benchmark"
	handler := NewMongoDBHandler(client, testDB)

	// Clean up test database
	db := client.Database(testDB)
	defer db.Drop(ctx)

	b.Run("HealthCheck", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			handler.HealthCheck(ctx)
		}
	})

	b.Run("ListCollections", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			handler.ListCollections(ctx)
		}
	})

	b.Run("GetDatabaseInfo", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			handler.GetDatabaseInfo(ctx)
		}
	})
}
