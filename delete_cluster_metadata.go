package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func main() {
	// MongoDB connection
	uri := "mongodb://aisrvdbuser:aisrvdbuser@rafay-mongodb-0.rafay-mongodb-svc.rafay-core.svc.cluster.local:27017/admin?replicaSet=rafay-mongodb&ssl=false"
	database := "temporal"

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

	fmt.Println("✅ Connected to MongoDB successfully")

	db := client.Database(database)
	collection := db.Collection("cluster_metadata_info")

	// Delete the corrupted cluster metadata document
	filter := bson.M{
		"metadata_partition": 0,
		"cluster_name":       "active",
	}

	result, err := collection.DeleteOne(ctx, filter)
	if err != nil {
		log.Fatalf("Failed to delete cluster metadata: %v", err)
	}

	if result.DeletedCount > 0 {
		fmt.Printf("✅ Deleted corrupted cluster metadata document (Deleted: %d)\n", result.DeletedCount)
	} else {
		fmt.Println("ℹ️  No cluster metadata document found to delete")
	}

	// Verify the document was deleted
	var doc bson.M
	err = collection.FindOne(ctx, filter).Decode(&doc)
	if err == mongo.ErrNoDocuments {
		fmt.Println("✅ Confirmed: cluster metadata document has been deleted")
		fmt.Println("\n🎉 Ready for Temporal to auto-populate cluster metadata!")
		fmt.Println("You can now start Temporal and it will create the proper cluster metadata.")
	} else if err != nil {
		log.Fatalf("Failed to verify deletion: %v", err)
	} else {
		fmt.Println("⚠️  Warning: Document still exists after deletion attempt")
	}
}
