package mongodb

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

// MongoDBHandler provides utilities for MongoDB operations
type MongoDBHandler struct {
	client   *mongo.Client
	database string
}

// NewMongoDBHandler creates a new MongoDB handler
func NewMongoDBHandler(client *mongo.Client, database string) *MongoDBHandler {
	return &MongoDBHandler{
		client:   client,
		database: database,
	}
}

// HealthCheck performs a health check on the MongoDB connection
func (h *MongoDBHandler) HealthCheck(ctx context.Context) error {
	// Ping the database
	if err := h.client.Ping(ctx, nil); err != nil {
		return fmt.Errorf("failed to ping MongoDB: %w", err)
	}

	// Get database stats
	db := h.client.Database(h.database)
	stats, err := db.RunCommand(ctx, bson.M{"dbStats": 1}).DecodeBytes()
	if err != nil {
		return fmt.Errorf("failed to get database stats: %w", err)
	}

	// Check if database exists and has collections
	collections, err := db.ListCollectionNames(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to list collections: %w", err)
	}

	fmt.Printf("✅ MongoDB health check passed\n")
	fmt.Printf("   Database: %s\n", h.database)
	fmt.Printf("   Collections: %d\n", len(collections))
	fmt.Printf("   Data size: %v bytes\n", stats.Lookup("dataSize"))

	return nil
}

// GetCollectionStats returns statistics for a specific collection
func (h *MongoDBHandler) GetCollectionStats(ctx context.Context, collectionName string) error {
	db := h.client.Database(h.database)
	coll := db.Collection(collectionName)

	// Get collection stats
	stats, err := db.RunCommand(ctx, bson.M{"collStats": collectionName}).DecodeBytes()
	if err != nil {
		return fmt.Errorf("failed to get collection stats for %s: %w", collectionName, err)
	}

	// Get document count
	count, err := coll.CountDocuments(ctx, bson.M{})
	if err != nil {
		return fmt.Errorf("failed to count documents in %s: %w", collectionName, err)
	}

	fmt.Printf("Collection: %s\n", collectionName)
	fmt.Printf("  Documents: %d\n", count)
	fmt.Printf("  Size: %v bytes\n", stats.Lookup("size"))
	fmt.Printf("  Storage size: %v bytes\n", stats.Lookup("storageSize"))
	fmt.Printf("  Indexes: %v\n", stats.Lookup("nindexes"))

	return nil
}

// ListCollections returns all collections in the database
func (h *MongoDBHandler) ListCollections(ctx context.Context) error {
	db := h.client.Database(h.database)
	collections, err := db.ListCollectionNames(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to list collections: %w", err)
	}

	fmt.Printf("Collections in database '%s':\n", h.database)
	for i, collection := range collections {
		fmt.Printf("  %d. %s\n", i+1, collection)
	}

	return nil
}

// GetIndexes returns all indexes for a specific collection
func (h *MongoDBHandler) GetIndexes(ctx context.Context, collectionName string) error {
	db := h.client.Database(h.database)
	coll := db.Collection(collectionName)

	indexes, err := coll.Indexes().List(ctx)
	if err != nil {
		return fmt.Errorf("failed to list indexes for %s: %w", collectionName, err)
	}

	fmt.Printf("Indexes for collection '%s':\n", collectionName)
	count := 0
	for indexes.Next(ctx) {
		var index map[string]interface{}
		if err := indexes.Decode(&index); err != nil {
			return fmt.Errorf("failed to decode index: %w", err)
		}
		count++
		fmt.Printf("  %d. %s\n", count, index["name"])
	}

	if count == 0 {
		fmt.Printf("  No indexes found\n")
	}

	return nil
}

// BackupCollection creates a backup of a collection
func (h *MongoDBHandler) BackupCollection(ctx context.Context, collectionName, backupCollectionName string) error {
	db := h.client.Database(h.database)
	sourceColl := db.Collection(collectionName)
	backupColl := db.Collection(backupCollectionName)

	// Check if source collection exists
	count, err := sourceColl.CountDocuments(ctx, bson.M{})
	if err != nil {
		return fmt.Errorf("source collection %s does not exist: %w", collectionName, err)
	}

	fmt.Printf("Backing up collection '%s' (%d documents) to '%s'...\n", collectionName, count, backupCollectionName)

	// Use aggregation pipeline to copy documents
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: bson.M{}}},
		{{Key: "$out", Value: backupCollectionName}},
	}

	_, err = sourceColl.Aggregate(ctx, pipeline)
	if err != nil {
		return fmt.Errorf("failed to backup collection: %w", err)
	}

	// Verify backup
	backupCount, err := backupColl.CountDocuments(ctx, bson.M{})
	if err != nil {
		return fmt.Errorf("failed to verify backup: %w", err)
	}

	if backupCount != count {
		return fmt.Errorf("backup verification failed: expected %d documents, got %d", count, backupCount)
	}

	fmt.Printf("✅ Backup completed successfully\n")
	return nil
}

// RestoreCollection restores a collection from backup
func (h *MongoDBHandler) RestoreCollection(ctx context.Context, backupCollectionName, targetCollectionName string) error {
	db := h.client.Database(h.database)
	backupColl := db.Collection(backupCollectionName)
	targetColl := db.Collection(targetCollectionName)

	// Check if backup collection exists
	count, err := backupColl.CountDocuments(ctx, bson.M{})
	if err != nil {
		return fmt.Errorf("backup collection %s does not exist: %w", backupCollectionName, err)
	}

	fmt.Printf("Restoring collection '%s' (%d documents) to '%s'...\n", backupCollectionName, count, targetCollectionName)

	// Drop target collection if it exists
	err = targetColl.Drop(ctx)
	if err != nil {
		// Ignore error if collection doesn't exist
	}

	// Use aggregation pipeline to restore documents
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: bson.M{}}},
		{{Key: "$out", Value: targetCollectionName}},
	}

	_, err = backupColl.Aggregate(ctx, pipeline)
	if err != nil {
		return fmt.Errorf("failed to restore collection: %w", err)
	}

	// Verify restore
	restoreCount, err := targetColl.CountDocuments(ctx, bson.M{})
	if err != nil {
		return fmt.Errorf("failed to verify restore: %w", err)
	}

	if restoreCount != count {
		return fmt.Errorf("restore verification failed: expected %d documents, got %d", count, restoreCount)
	}

	fmt.Printf("✅ Restore completed successfully\n")
	return nil
}

// CleanupOldData removes old data based on criteria
func (h *MongoDBHandler) CleanupOldData(ctx context.Context, collectionName string, olderThan time.Duration) error {
	db := h.client.Database(h.database)
	coll := db.Collection(collectionName)

	// Calculate cutoff time
	cutoffTime := time.Now().Add(-olderThan)

	// Build filter for old documents
	filter := bson.M{
		"$or": []bson.M{
			{"visibility_ts": bson.M{"$lt": cutoffTime}},
			{"created_at": bson.M{"$lt": cutoffTime}},
			{"updated_at": bson.M{"$lt": cutoffTime}},
		},
	}

	// Count documents to be deleted
	count, err := coll.CountDocuments(ctx, filter)
	if err != nil {
		return fmt.Errorf("failed to count documents for cleanup: %w", err)
	}

	if count == 0 {
		fmt.Printf("No documents older than %v found in collection '%s'\n", olderThan, collectionName)
		return nil
	}

	fmt.Printf("Deleting %d documents older than %v from collection '%s'...\n", count, olderThan, collectionName)

	// Delete old documents
	result, err := coll.DeleteMany(ctx, filter)
	if err != nil {
		return fmt.Errorf("failed to delete old documents: %w", err)
	}

	fmt.Printf("✅ Cleanup completed: %d documents deleted\n", result.DeletedCount)
	return nil
}

// GetDatabaseInfo returns comprehensive database information
func (h *MongoDBHandler) GetDatabaseInfo(ctx context.Context) error {
	db := h.client.Database(h.database)

	// Get database stats
	stats, err := db.RunCommand(ctx, bson.M{"dbStats": 1}).DecodeBytes()
	if err != nil {
		return fmt.Errorf("failed to get database stats: %w", err)
	}

	// Get collections
	collections, err := db.ListCollectionNames(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to list collections: %w", err)
	}

	fmt.Printf("Database Information for '%s':\n", h.database)
	fmt.Printf("  Collections: %d\n", len(collections))
	fmt.Printf("  Data size: %v bytes\n", stats.Lookup("dataSize"))
	fmt.Printf("  Storage size: %v bytes\n", stats.Lookup("storageSize"))
	fmt.Printf("  Indexes: %v\n", stats.Lookup("nindexes"))
	fmt.Printf("  Objects: %v\n", stats.Lookup("objects"))

	// Get collection details
	fmt.Printf("\nCollections:\n")
	for i, collection := range collections {
		collStats, err := db.RunCommand(ctx, bson.M{"collStats": collection}).DecodeBytes()
		if err != nil {
			fmt.Printf("  %d. %s (error getting stats)\n", i+1, collection)
			continue
		}
		fmt.Printf("  %d. %s (%v documents, %v bytes)\n",
			i+1, collection,
			collStats.Lookup("count"),
			collStats.Lookup("size"))
	}

	return nil
}
