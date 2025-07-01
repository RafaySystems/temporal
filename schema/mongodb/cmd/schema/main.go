package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"go.temporal.io/server/schema/mongodb"
)

func main() {
	var (
		uri      = flag.String("uri", "mongodb://localhost:27017", "MongoDB connection URI")
		database = flag.String("database", "temporal", "Database name")
		action   = flag.String("action", "create", "Action to perform: create, validate, drop")
		timeout  = flag.Duration("timeout", 30*time.Second, "Connection timeout")
		verbose  = flag.Bool("verbose", false, "Enable verbose output")
	)
	flag.Parse()

	if *verbose {
		log.Printf("Connecting to MongoDB at %s", *uri)
	}

	// Connect to MongoDB
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(*uri))
	if err != nil {
		log.Fatalf("Failed to connect to MongoDB: %v", err)
	}
	defer client.Disconnect(ctx)

	// Ping the database
	if err := client.Ping(ctx, nil); err != nil {
		log.Fatalf("Failed to ping MongoDB: %v", err)
	}

	if *verbose {
		log.Printf("Connected to MongoDB successfully")
	}

	// Create schema instance
	schema := mongodb.NewSchema(*database)

	switch *action {
	case "create":
		if err := createSchema(ctx, client, schema, *verbose); err != nil {
			log.Fatalf("Failed to create schema: %v", err)
		}
		fmt.Println("✅ MongoDB schema created successfully")

	case "validate":
		if err := validateSchema(ctx, client, schema, *verbose); err != nil {
			log.Fatalf("Failed to validate schema: %v", err)
		}
		fmt.Println("✅ MongoDB schema validation passed")

	case "drop":
		if err := dropSchema(ctx, client, *database, *verbose); err != nil {
			log.Fatalf("Failed to drop schema: %v", err)
		}
		fmt.Println("✅ MongoDB schema dropped successfully")

	default:
		fmt.Printf("Unknown action: %s\n", *action)
		fmt.Println("Available actions: create, validate, drop")
		os.Exit(1)
	}
}

func createSchema(ctx context.Context, client *mongo.Client, schema *mongodb.Schema, verbose bool) error {
	if verbose {
		log.Println("Creating MongoDB schema...")
	}

	return schema.CreateSchema(ctx, client)
}

func validateSchema(ctx context.Context, client *mongo.Client, schema *mongodb.Schema, verbose bool) error {
	if verbose {
		log.Println("Validating MongoDB schema...")
	}

	db := client.Database(schema.DatabaseName)

	// Check if collections exist
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

	collections, err := db.ListCollectionNames(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to list collections: %w", err)
	}

	collectionMap := make(map[string]bool)
	for _, collection := range collections {
		collectionMap[collection] = true
	}

	missingCollections := []string{}
	for _, expected := range expectedCollections {
		if !collectionMap[expected] {
			missingCollections = append(missingCollections, expected)
		}
	}

	if len(missingCollections) > 0 {
		return fmt.Errorf("missing collections: %v", missingCollections)
	}

	if verbose {
		log.Printf("Found %d collections", len(collections))
	}

	// Check indexes for key collections
	if err := validateIndexes(ctx, db, verbose); err != nil {
		return fmt.Errorf("index validation failed: %w", err)
	}

	return nil
}

func validateIndexes(ctx context.Context, db *mongo.Database, verbose bool) error {
	// Check executions collection indexes
	coll := db.Collection("executions")
	indexes, err := coll.Indexes().List(ctx)
	if err != nil {
		return fmt.Errorf("failed to list indexes for executions: %w", err)
	}

	expectedIndexes := []string{
		"_id_",
		"executions_primary_key",
		"executions_workflow_lookup",
		"executions_visibility",
	}

	indexMap := make(map[string]bool)
	for indexes.Next(ctx) {
		var index map[string]interface{}
		if err := indexes.Decode(&index); err != nil {
			return fmt.Errorf("failed to decode index: %w", err)
		}
		indexMap[index["name"].(string)] = true
	}

	missingIndexes := []string{}
	for _, expected := range expectedIndexes {
		if !indexMap[expected] {
			missingIndexes = append(missingIndexes, expected)
		}
	}

	if len(missingIndexes) > 0 {
		return fmt.Errorf("missing indexes in executions collection: %v", missingIndexes)
	}

	if verbose {
		log.Printf("Found %d indexes in executions collection", len(indexMap))
	}

	return nil
}

func dropSchema(ctx context.Context, client *mongo.Client, databaseName string, verbose bool) error {
	if verbose {
		log.Printf("Dropping database: %s", databaseName)
	}

	db := client.Database(databaseName)
	return db.Drop(ctx)
}
