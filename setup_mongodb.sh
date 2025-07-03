#!/bin/bash

# Setup MongoDB for Temporal
# This script creates the MongoDB schema and initializes cluster metadata

set -e

echo "Setting up MongoDB for Temporal..."

# Configuration
MONGODB_URI="mongodb://aisrvdbuser:aisrvdbuser@rafay-mongodb-0.rafay-mongodb-svc.rafay-core.svc.cluster.local:27017/admin?replicaSet=rafay-mongodb&ssl=false"
DATABASE="temporal"
CLUSTER_NAME="active"

echo "MongoDB URI: $MONGODB_URI"
echo "Database: $DATABASE"
echo "Cluster Name: $CLUSTER_NAME"

# Step 1: Create MongoDB schema
echo ""
echo "Step 1: Creating MongoDB schema..."
go run ./cmd/tools/mongodb/main.go \
    -uri "$MONGODB_URI" \
    -database "$DATABASE" \
    -action create \
    -verbose

if [ $? -eq 0 ]; then
    echo "✅ MongoDB schema created successfully"
else
    echo "❌ Failed to create MongoDB schema"
    exit 1
fi

# Step 2: Validate schema
echo ""
echo "Step 2: Validating MongoDB schema..."
go run ./cmd/tools/mongodb/main.go \
    -uri "$MONGODB_URI" \
    -database "$DATABASE" \
    -action validate \
    -verbose

if [ $? -eq 0 ]; then
    echo "✅ MongoDB schema validation successful"
else
    echo "❌ MongoDB schema validation failed"
    exit 1
fi

# Step 3: Initialize cluster metadata
echo ""
echo "Step 3: Initializing cluster metadata..."

# Create a temporary Go program to initialize cluster metadata
cat > /tmp/init_cluster_metadata.go << 'EOF'
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"go.temporal.io/server/common/persistence"
	persistencespb "go.temporal.io/server/common/persistence/persistence"
)

func main() {
	// MongoDB connection
	uri := "mongodb://aisrvdbuser:aisrvdbuser@rafay-mongodb-0.rafay-mongodb-svc.rafay-core.svc.cluster.local:27017/admin?replicaSet=rafay-mongodb&ssl=false"
	database := "temporal"
	clusterName := "active"

	// Connect to MongoDB
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		log.Fatalf("Failed to connect to MongoDB: %v", err)
	}
	defer client.Disconnect(ctx)

	// Ping the database
	if err := client.Ping(ctx, nil); err != nil {
		log.Fatalf("Failed to ping MongoDB: %v", err)
	}

	db := client.Database(database)
	collection := db.Collection("cluster_metadata_info")

	// Create cluster metadata
	clusterMetadata := &persistencespb.ClusterMetadata{
		ClusterName:              clusterName,
		HistoryShardCount:        4, // Default from config
		ClusterId:                "temporal-cluster-1",
		ClusterAddress:           "127.0.0.1:7233",
		HttpAddress:              "127.0.0.1:7243",
		FailoverVersionIncrement: 10,
		InitialFailoverVersion:   1,
		IsGlobalNamespaceEnabled: false,
		IsConnectionEnabled:      true,
		VersionInfo: &persistencespb.VersionInfo{
			Current: &persistencespb.VersionInfo_Version{
				Version: "1.0",
			},
		},
		LastUpdatedTime: timestamppb.Now(),
	}

	// Serialize the cluster metadata
	data, err := proto.Marshal(clusterMetadata)
	if err != nil {
		log.Fatalf("Failed to marshal cluster metadata: %v", err)
	}

	// Create the document
	doc := bson.M{
		"metadata_partition": 0,
		"cluster_name":       clusterName,
		"data":               data,
		"data_encoding":      "proto3",
		"version":            1,
	}

	// Upsert the cluster metadata
	filter := bson.M{
		"metadata_partition": 0,
		"cluster_name":       clusterName,
	}

	update := bson.M{
		"$set": doc,
	}

	opts := options.Update().SetUpsert(true)
	result, err := collection.UpdateOne(ctx, filter, update, opts)
	if err != nil {
		log.Fatalf("Failed to upsert cluster metadata: %v", err)
	}

	if result.UpsertedCount > 0 {
		fmt.Printf("✅ Cluster metadata created for cluster: %s\n", clusterName)
	} else {
		fmt.Printf("✅ Cluster metadata updated for cluster: %s\n", clusterName)
	}

	// Verify the cluster metadata was saved
	var savedDoc bson.M
	err = collection.FindOne(ctx, filter).Decode(&savedDoc)
	if err != nil {
		log.Fatalf("Failed to verify cluster metadata: %v", err)
	}

	fmt.Printf("✅ Cluster metadata verified in database\n")
	fmt.Printf("   Cluster Name: %s\n", savedDoc["cluster_name"])
	fmt.Printf("   Version: %v\n", savedDoc["version"])
}
EOF

# Run the cluster metadata initialization
echo "Running cluster metadata initialization..."
go run /tmp/init_cluster_metadata.go

if [ $? -eq 0 ]; then
    echo "✅ Cluster metadata initialized successfully"
else
    echo "❌ Failed to initialize cluster metadata"
    exit 1
fi

# Clean up
rm -f /tmp/init_cluster_metadata.go

echo ""
echo "🎉 MongoDB setup completed successfully!"
echo ""
echo "You can now start Temporal with MongoDB:"
echo "  TEMPORAL_ENVIRONMENT=docker \\"
echo "  TEMPORAL_CONFIG_DIR=config \\"
echo "  DB=mongodb \\"
echo "  MONGODB_SEEDS=rafay-mongodb-0.rafay-mongodb-svc.rafay-core.svc.cluster.local:27017 \\"
echo "  MONGODB_USER=aisrvdbuser \\"
echo "  MONGODB_PWD=aisrvdbuser \\"
echo "  go run ./cmd/server/main.go --allow-no-auth start" 