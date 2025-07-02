package mongodb

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

// RunTool runs the temporal-mongodb-tool command line tool
func RunTool(args []string) error {
	// Set up command line flags
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)

	var (
		uri        = flag.String("uri", "mongodb://localhost:27017", "MongoDB connection URI")
		database   = flag.String("database", "temporal", "Database name")
		action     = flag.String("action", "create", "Action to perform")
		version    = flag.String("version", "", "Schema version for setup-schema or update-schema")
		schemaDir  = flag.String("schema-dir", "", "Directory containing schema files for update-schema")
		timeout    = flag.Duration("timeout", 30*time.Second, "Connection timeout")
		verbose    = flag.Bool("verbose", false, "Enable verbose output")
		collection = flag.String("collection", "", "Collection name for specific operations")
		backupName = flag.String("backup-name", "", "Backup collection name")
		targetName = flag.String("target-name", "", "Target collection name for restore")
		olderThan  = flag.String("older-than", "", "Time duration for cleanup (e.g., 30d, 24h)")
	)

	// Parse the provided arguments
	flag.CommandLine.Parse(args[1:])

	if *verbose {
		log.Printf("Connecting to MongoDB at %s", *uri)
	}

	// Connect to MongoDB
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(*uri))
	if err != nil {
		return fmt.Errorf("failed to connect to MongoDB: %w", err)
	}
	defer func() {
		if err := client.Disconnect(ctx); err != nil {
			log.Printf("Failed to disconnect from MongoDB: %v", err)
		}
	}()

	// Ping the database
	if err := client.Ping(ctx, nil); err != nil {
		return fmt.Errorf("failed to ping MongoDB: %w", err)
	}

	if *verbose {
		log.Printf("Connected to MongoDB successfully")
	}

	// Create schema instance
	schema := mongodb.NewSchema(*database)

	// Create handler for advanced operations
	handler := NewMongoDBHandler(client, *database)

	switch *action {
	case "create":
		if err := createSchema(ctx, client, schema, *verbose); err != nil {
			return fmt.Errorf("failed to create schema: %w", err)
		}
		fmt.Println("✅ MongoDB schema created successfully")

	case "validate":
		if err := validateSchema(ctx, client, schema, *verbose); err != nil {
			return fmt.Errorf("failed to validate schema: %w", err)
		}
		fmt.Println("✅ MongoDB schema validation passed")

	case "drop":
		if err := dropSchema(ctx, client, *database, *verbose); err != nil {
			return fmt.Errorf("failed to drop schema: %w", err)
		}
		fmt.Println("✅ MongoDB schema dropped successfully")

	case "setup-schema":
		if *version == "" {
			return fmt.Errorf("version is required for setup-schema action")
		}
		if err := setupSchema(ctx, client, *database, *version, *verbose); err != nil {
			return fmt.Errorf("failed to setup schema: %w", err)
		}
		fmt.Printf("✅ MongoDB schema setup completed for version %s\n", *version)

	case "update-schema":
		if *version == "" {
			return fmt.Errorf("version is required for update-schema action")
		}
		if err := updateSchema(ctx, client, *database, *version, *schemaDir, *verbose); err != nil {
			return fmt.Errorf("failed to update schema: %w", err)
		}
		fmt.Printf("✅ MongoDB schema updated to version %s\n", *version)

	case "health-check":
		if err := handler.HealthCheck(ctx); err != nil {
			return fmt.Errorf("health check failed: %w", err)
		}

	case "info":
		if err := handler.GetDatabaseInfo(ctx); err != nil {
			return fmt.Errorf("failed to get database info: %w", err)
		}

	case "list-collections":
		if err := handler.ListCollections(ctx); err != nil {
			return fmt.Errorf("failed to list collections: %w", err)
		}

	case "collection-stats":
		if *collection == "" {
			return fmt.Errorf("collection name is required for collection-stats action")
		}
		if err := handler.GetCollectionStats(ctx, *collection); err != nil {
			return fmt.Errorf("failed to get collection stats: %w", err)
		}

	case "list-indexes":
		if *collection == "" {
			return fmt.Errorf("collection name is required for list-indexes action")
		}
		if err := handler.GetIndexes(ctx, *collection); err != nil {
			return fmt.Errorf("failed to list indexes: %w", err)
		}

	case "backup":
		if *collection == "" {
			return fmt.Errorf("collection name is required for backup action")
		}
		if *backupName == "" {
			return fmt.Errorf("backup name is required for backup action")
		}
		if err := handler.BackupCollection(ctx, *collection, *backupName); err != nil {
			return fmt.Errorf("failed to backup collection: %w", err)
		}

	case "restore":
		if *backupName == "" {
			return fmt.Errorf("backup name is required for restore action")
		}
		if *targetName == "" {
			return fmt.Errorf("target name is required for restore action")
		}
		if err := handler.RestoreCollection(ctx, *backupName, *targetName); err != nil {
			return fmt.Errorf("failed to restore collection: %w", err)
		}

	case "cleanup":
		if *collection == "" {
			return fmt.Errorf("collection name is required for cleanup action")
		}
		if *olderThan == "" {
			return fmt.Errorf("older-than duration is required for cleanup action")
		}
		duration, err := time.ParseDuration(*olderThan)
		if err != nil {
			return fmt.Errorf("invalid duration format: %w", err)
		}
		if err := handler.CleanupOldData(ctx, *collection, duration); err != nil {
			return fmt.Errorf("failed to cleanup old data: %w", err)
		}

	default:
		fmt.Printf("Unknown action: %s\n", *action)
		fmt.Println("Available actions: create, validate, drop, setup-schema, update-schema, health-check, info, list-collections, collection-stats, list-indexes, backup, restore, cleanup")
		return fmt.Errorf("unknown action: %s", *action)
	}

	return nil
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

func setupSchema(ctx context.Context, client *mongo.Client, databaseName, version string, verbose bool) error {
	if verbose {
		log.Printf("Setting up schema version %s for database %s", version, databaseName)
	}

	db := client.Database(databaseName)

	// Create schema version collection
	schemaVersionColl := db.Collection("schema_version")

	// Check if schema version document exists
	var existingVersion map[string]interface{}
	err := schemaVersionColl.FindOne(ctx, map[string]interface{}{}).Decode(&existingVersion)
	if err == nil {
		return fmt.Errorf("schema version already exists, cannot setup schema")
	}

	// Insert initial schema version
	_, err = schemaVersionColl.InsertOne(ctx, map[string]interface{}{
		"version":     version,
		"created_at":  time.Now(),
		"description": "Initial schema setup",
	})
	if err != nil {
		return fmt.Errorf("failed to insert schema version: %w", err)
	}

	// Create the actual schema
	schema := mongodb.NewSchema(databaseName)
	return schema.CreateSchema(ctx, client)
}

func updateSchema(ctx context.Context, client *mongo.Client, databaseName, version, schemaDir string, verbose bool) error {
	if verbose {
		log.Printf("Updating schema to version %s for database %s", version, databaseName)
	}

	db := client.Database(databaseName)

	// Check current schema version
	schemaVersionColl := db.Collection("schema_version")
	var currentVersion map[string]interface{}
	err := schemaVersionColl.FindOne(ctx, map[string]interface{}{}).Decode(&currentVersion)
	if err != nil {
		return fmt.Errorf("no schema version found, run setup-schema first: %w", err)
	}

	if verbose {
		log.Printf("Current schema version: %v", currentVersion["version"])
	}

	// Update schema version
	_, err = schemaVersionColl.UpdateOne(
		ctx,
		map[string]interface{}{},
		map[string]interface{}{
			"$set": map[string]interface{}{
				"version":     version,
				"updated_at":  time.Now(),
				"description": "Schema updated",
			},
		},
	)
	if err != nil {
		return fmt.Errorf("failed to update schema version: %w", err)
	}

	// If schema directory is provided, use it; otherwise use embedded schema
	if schemaDir != "" {
		if verbose {
			log.Printf("Using schema from directory: %s", schemaDir)
		}
		// TODO: Implement schema loading from directory
		return fmt.Errorf("schema directory loading not yet implemented")
	}

	// Use embedded schema
	schema := mongodb.NewSchema(databaseName)
	return schema.CreateSchema(ctx, client)
}
