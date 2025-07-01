package mongodb

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Schema represents the MongoDB schema for Temporal
type Schema struct {
	DatabaseName string
	Collections  []Collection
}

// Collection represents a MongoDB collection with its schema and indexes
type Collection struct {
	Name       string
	Validation bson.M
	Indexes    []Index
}

// Index represents a MongoDB index
type Index struct {
	Keys    bson.D
	Options *options.IndexOptions
}

// NewSchema creates a new MongoDB schema for Temporal
func NewSchema(databaseName string) *Schema {
	return &Schema{
		DatabaseName: databaseName,
		Collections:  getCollections(),
	}
}

// CreateSchema creates all collections and indexes in the database
func (s *Schema) CreateSchema(ctx context.Context, client *mongo.Client) error {
	db := client.Database(s.DatabaseName)

	for _, collection := range s.Collections {
		if err := s.createCollection(ctx, db, collection); err != nil {
			return fmt.Errorf("failed to create collection %s: %w", collection.Name, err)
		}
	}

	return nil
}

// createCollection creates a single collection with validation and indexes
func (s *Schema) createCollection(ctx context.Context, db *mongo.Database, collection Collection) error {
	// Create collection with validation
	opts := options.CreateCollection()
	if collection.Validation != nil {
		opts.SetValidator(collection.Validation)
	}

	if err := db.CreateCollection(ctx, collection.Name, opts); err != nil {
		// Collection might already exist, which is fine
		if !isCollectionExistsError(err) {
			return err
		}
	}

	// Create indexes
	coll := db.Collection(collection.Name)
	for _, index := range collection.Indexes {
		if _, err := coll.Indexes().CreateOne(ctx, mongo.IndexModel{
			Keys:    index.Keys,
			Options: index.Options,
		}); err != nil {
			return fmt.Errorf("failed to create index for collection %s: %w", collection.Name, err)
		}
	}

	return nil
}

// isCollectionExistsError checks if the error is due to collection already existing
func isCollectionExistsError(err error) bool {
	var mongoErr mongo.CommandError
	if ok := err.(*mongo.CommandError); ok != nil {
		return mongoErr.Code == 48 // Collection already exists
	}
	return false
}

// getCollections returns all collections for Temporal
func getCollections() []Collection {
	return []Collection{
		executionsCollection(),
		historyNodeCollection(),
		historyTreeCollection(),
		tasksCollection(),
		taskQueueUserDataCollection(),
		namespacesByIdCollection(),
		namespacesCollection(),
		queueMetadataCollection(),
		queueCollection(),
		clusterMetadataInfoCollection(),
		clusterMembershipCollection(),
		queuesCollection(),
		queueMessagesCollection(),
		nexusEndpointsCollection(),
	}
}

// executionsCollection defines the executions collection schema
func executionsCollection() Collection {
	return Collection{
		Name: "executions",
		Validation: bson.M{
			"$jsonSchema": bson.M{
				"bsonType": "object",
				"required": []string{"shard_id", "type", "namespace_id", "workflow_id", "run_id"},
				"properties": bson.M{
					"shard_id":                      bson.M{"bsonType": "int"},
					"type":                          bson.M{"bsonType": "int"},
					"namespace_id":                  bson.M{"bsonType": "binData"},
					"workflow_id":                   bson.M{"bsonType": "string"},
					"run_id":                        bson.M{"bsonType": "binData"},
					"current_run_id":                bson.M{"bsonType": "binData"},
					"visibility_ts":                 bson.M{"bsonType": "date"},
					"task_id":                       bson.M{"bsonType": "long"},
					"shard":                         bson.M{"bsonType": "binData"},
					"shard_encoding":                bson.M{"bsonType": "string"},
					"execution":                     bson.M{"bsonType": "binData"},
					"execution_encoding":            bson.M{"bsonType": "string"},
					"execution_state":               bson.M{"bsonType": "binData"},
					"execution_state_encoding":      bson.M{"bsonType": "string"},
					"transfer":                      bson.M{"bsonType": "binData"},
					"transfer_encoding":             bson.M{"bsonType": "string"},
					"replication":                   bson.M{"bsonType": "binData"},
					"replication_encoding":          bson.M{"bsonType": "string"},
					"timer":                         bson.M{"bsonType": "binData"},
					"timer_encoding":                bson.M{"bsonType": "string"},
					"visibility_task_data":          bson.M{"bsonType": "binData"},
					"visibility_task_encoding":      bson.M{"bsonType": "string"},
					"task_data":                     bson.M{"bsonType": "binData"},
					"task_encoding":                 bson.M{"bsonType": "string"},
					"next_event_id":                 bson.M{"bsonType": "long"},
					"range_id":                      bson.M{"bsonType": "long"},
					"activity_map":                  bson.M{"bsonType": "object"},
					"activity_map_encoding":         bson.M{"bsonType": "string"},
					"timer_map":                     bson.M{"bsonType": "object"},
					"timer_map_encoding":            bson.M{"bsonType": "string"},
					"child_executions_map":          bson.M{"bsonType": "object"},
					"child_executions_map_encoding": bson.M{"bsonType": "string"},
					"request_cancel_map":            bson.M{"bsonType": "object"},
					"request_cancel_map_encoding":   bson.M{"bsonType": "string"},
					"signal_map":                    bson.M{"bsonType": "object"},
					"signal_map_encoding":           bson.M{"bsonType": "string"},
					"signal_requested":              bson.M{"bsonType": "array"},
					"chasm_node_map":                bson.M{"bsonType": "object"},
					"chasm_node_map_encoding":       bson.M{"bsonType": "string"},
					"buffered_events_list":          bson.M{"bsonType": "array"},
					"workflow_last_write_version":   bson.M{"bsonType": "long"},
					"workflow_state":                bson.M{"bsonType": "int"},
					"checksum":                      bson.M{"bsonType": "binData"},
					"checksum_encoding":             bson.M{"bsonType": "string"},
					"db_record_version":             bson.M{"bsonType": "long"},
				},
			},
		},
		Indexes: []Index{
			{
				Keys: bson.D{
					{"shard_id", 1},
					{"type", 1},
					{"namespace_id", 1},
					{"workflow_id", 1},
					{"run_id", 1},
					{"visibility_ts", 1},
					{"task_id", 1},
				},
				Options: options.Index().SetUnique(true).SetName("executions_primary_key"),
			},
			{
				Keys: bson.D{
					{"namespace_id", 1},
					{"workflow_id", 1},
					{"run_id", 1},
				},
				Options: options.Index().SetName("executions_workflow_lookup"),
			},
			{
				Keys: bson.D{
					{"namespace_id", 1},
					{"visibility_ts", 1},
				},
				Options: options.Index().SetName("executions_visibility"),
			},
		},
	}
}

// historyNodeCollection defines the history_node collection schema
func historyNodeCollection() Collection {
	return Collection{
		Name: "history_node",
		Validation: bson.M{
			"$jsonSchema": bson.M{
				"bsonType": "object",
				"required": []string{"tree_id", "branch_id", "node_id", "txn_id"},
				"properties": bson.M{
					"tree_id":       bson.M{"bsonType": "binData"},
					"branch_id":     bson.M{"bsonType": "binData"},
					"node_id":       bson.M{"bsonType": "long"},
					"txn_id":        bson.M{"bsonType": "long"},
					"prev_txn_id":   bson.M{"bsonType": "long"},
					"data":          bson.M{"bsonType": "binData"},
					"data_encoding": bson.M{"bsonType": "string"},
				},
			},
		},
		Indexes: []Index{
			{
				Keys: bson.D{
					{"tree_id", 1},
					{"branch_id", 1},
					{"node_id", 1},
					{"txn_id", -1},
				},
				Options: options.Index().SetUnique(true).SetName("history_node_primary_key"),
			},
			{
				Keys: bson.D{
					{"tree_id", 1},
					{"branch_id", 1},
					{"node_id", 1},
				},
				Options: options.Index().SetName("history_node_tree_lookup"),
			},
		},
	}
}

// historyTreeCollection defines the history_tree collection schema
func historyTreeCollection() Collection {
	return Collection{
		Name: "history_tree",
		Validation: bson.M{
			"$jsonSchema": bson.M{
				"bsonType": "object",
				"required": []string{"tree_id", "branch_id"},
				"properties": bson.M{
					"tree_id":         bson.M{"bsonType": "binData"},
					"branch_id":       bson.M{"bsonType": "binData"},
					"branch":          bson.M{"bsonType": "binData"},
					"branch_encoding": bson.M{"bsonType": "string"},
				},
			},
		},
		Indexes: []Index{
			{
				Keys: bson.D{
					{"tree_id", 1},
					{"branch_id", 1},
				},
				Options: options.Index().SetUnique(true).SetName("history_tree_primary_key"),
			},
		},
	}
}

// tasksCollection defines the tasks collection schema
func tasksCollection() Collection {
	return Collection{
		Name: "tasks",
		Validation: bson.M{
			"$jsonSchema": bson.M{
				"bsonType": "object",
				"required": []string{"namespace_id", "task_queue_name", "task_queue_type", "type", "task_id"},
				"properties": bson.M{
					"namespace_id":        bson.M{"bsonType": "binData"},
					"task_queue_name":     bson.M{"bsonType": "string"},
					"task_queue_type":     bson.M{"bsonType": "int"},
					"type":                bson.M{"bsonType": "int"},
					"task_id":             bson.M{"bsonType": "long"},
					"range_id":            bson.M{"bsonType": "long"},
					"task":                bson.M{"bsonType": "binData"},
					"task_encoding":       bson.M{"bsonType": "string"},
					"task_queue":          bson.M{"bsonType": "binData"},
					"task_queue_encoding": bson.M{"bsonType": "string"},
				},
			},
		},
		Indexes: []Index{
			{
				Keys: bson.D{
					{"namespace_id", 1},
					{"task_queue_name", 1},
					{"task_queue_type", 1},
					{"type", 1},
					{"task_id", 1},
				},
				Options: options.Index().SetUnique(true).SetName("tasks_primary_key"),
			},
			{
				Keys: bson.D{
					{"namespace_id", 1},
					{"task_queue_name", 1},
					{"task_queue_type", 1},
				},
				Options: options.Index().SetName("tasks_queue_lookup"),
			},
		},
	}
}

// taskQueueUserDataCollection defines the task_queue_user_data collection schema
func taskQueueUserDataCollection() Collection {
	return Collection{
		Name: "task_queue_user_data",
		Validation: bson.M{
			"$jsonSchema": bson.M{
				"bsonType": "object",
				"required": []string{"namespace_id", "task_queue_name"},
				"properties": bson.M{
					"namespace_id":    bson.M{"bsonType": "binData"},
					"task_queue_name": bson.M{"bsonType": "string"},
					"build_id":        bson.M{"bsonType": "string"},
					"data":            bson.M{"bsonType": "binData"},
					"data_encoding":   bson.M{"bsonType": "string"},
					"version":         bson.M{"bsonType": "long"},
				},
			},
		},
		Indexes: []Index{
			{
				Keys: bson.D{
					{"namespace_id", 1},
					{"build_id", 1},
					{"task_queue_name", 1},
				},
				Options: options.Index().SetUnique(true).SetName("task_queue_user_data_primary_key"),
			},
			{
				Keys: bson.D{
					{"namespace_id", 1},
				},
				Options: options.Index().SetName("task_queue_user_data_namespace"),
			},
		},
	}
}

// namespacesByIdCollection defines the namespaces_by_id collection schema
func namespacesByIdCollection() Collection {
	return Collection{
		Name: "namespaces_by_id",
		Validation: bson.M{
			"$jsonSchema": bson.M{
				"bsonType": "object",
				"required": []string{"id", "name"},
				"properties": bson.M{
					"id":   bson.M{"bsonType": "binData"},
					"name": bson.M{"bsonType": "string"},
				},
			},
		},
		Indexes: []Index{
			{
				Keys: bson.D{
					{"id", 1},
				},
				Options: options.Index().SetUnique(true).SetName("namespaces_by_id_primary_key"),
			},
		},
	}
}

// namespacesCollection defines the namespaces collection schema
func namespacesCollection() Collection {
	return Collection{
		Name: "namespaces",
		Validation: bson.M{
			"$jsonSchema": bson.M{
				"bsonType": "object",
				"required": []string{"namespaces_partition", "name", "id"},
				"properties": bson.M{
					"namespaces_partition": bson.M{"bsonType": "int"},
					"name":                 bson.M{"bsonType": "string"},
					"id":                   bson.M{"bsonType": "binData"},
					"detail":               bson.M{"bsonType": "binData"},
					"detail_encoding":      bson.M{"bsonType": "string"},
					"is_global_namespace":  bson.M{"bsonType": "bool"},
					"notification_version": bson.M{"bsonType": "long"},
				},
			},
		},
		Indexes: []Index{
			{
				Keys: bson.D{
					{"namespaces_partition", 1},
					{"name", 1},
				},
				Options: options.Index().SetUnique(true).SetName("namespaces_primary_key"),
			},
			{
				Keys: bson.D{
					{"id", 1},
				},
				Options: options.Index().SetUnique(true).SetName("namespaces_id_lookup"),
			},
		},
	}
}

// queueMetadataCollection defines the queue_metadata collection schema
func queueMetadataCollection() Collection {
	return Collection{
		Name: "queue_metadata",
		Validation: bson.M{
			"$jsonSchema": bson.M{
				"bsonType": "object",
				"required": []string{"queue_type"},
				"properties": bson.M{
					"queue_type":        bson.M{"bsonType": "int"},
					"cluster_ack_level": bson.M{"bsonType": "object"},
					"data":              bson.M{"bsonType": "binData"},
					"data_encoding":     bson.M{"bsonType": "string"},
					"version":           bson.M{"bsonType": "long"},
				},
			},
		},
		Indexes: []Index{
			{
				Keys: bson.D{
					{"queue_type", 1},
				},
				Options: options.Index().SetUnique(true).SetName("queue_metadata_primary_key"),
			},
		},
	}
}

// queueCollection defines the queue collection schema
func queueCollection() Collection {
	return Collection{
		Name: "queue",
		Validation: bson.M{
			"$jsonSchema": bson.M{
				"bsonType": "object",
				"required": []string{"queue_type", "message_id"},
				"properties": bson.M{
					"queue_type":       bson.M{"bsonType": "int"},
					"message_id":       bson.M{"bsonType": "long"},
					"message_payload":  bson.M{"bsonType": "binData"},
					"message_encoding": bson.M{"bsonType": "string"},
				},
			},
		},
		Indexes: []Index{
			{
				Keys: bson.D{
					{"queue_type", 1},
					{"message_id", 1},
				},
				Options: options.Index().SetUnique(true).SetName("queue_primary_key"),
			},
		},
	}
}

// clusterMetadataInfoCollection defines the cluster_metadata_info collection schema
func clusterMetadataInfoCollection() Collection {
	return Collection{
		Name: "cluster_metadata_info",
		Validation: bson.M{
			"$jsonSchema": bson.M{
				"bsonType": "object",
				"required": []string{"metadata_partition", "cluster_name"},
				"properties": bson.M{
					"metadata_partition": bson.M{"bsonType": "int"},
					"cluster_name":       bson.M{"bsonType": "string"},
					"data":               bson.M{"bsonType": "binData"},
					"data_encoding":      bson.M{"bsonType": "string"},
					"version":            bson.M{"bsonType": "long"},
				},
			},
		},
		Indexes: []Index{
			{
				Keys: bson.D{
					{"metadata_partition", 1},
					{"cluster_name", 1},
				},
				Options: options.Index().SetUnique(true).SetName("cluster_metadata_info_primary_key"),
			},
		},
	}
}

// clusterMembershipCollection defines the cluster_membership collection schema
func clusterMembershipCollection() Collection {
	return Collection{
		Name: "cluster_membership",
		Validation: bson.M{
			"$jsonSchema": bson.M{
				"bsonType": "object",
				"required": []string{"membership_partition", "host_id", "role"},
				"properties": bson.M{
					"membership_partition": bson.M{"bsonType": "int"},
					"host_id":              bson.M{"bsonType": "binData"},
					"rpc_address":          bson.M{"bsonType": "string"},
					"rpc_port":             bson.M{"bsonType": "int"},
					"role":                 bson.M{"bsonType": "int"},
					"session_start":        bson.M{"bsonType": "date"},
					"last_heartbeat":       bson.M{"bsonType": "date"},
				},
			},
		},
		Indexes: []Index{
			{
				Keys: bson.D{
					{"membership_partition", 1},
					{"role", 1},
					{"host_id", 1},
				},
				Options: options.Index().SetUnique(true).SetName("cluster_membership_primary_key"),
			},
			{
				Keys: bson.D{
					{"last_heartbeat", 1},
				},
				Options: options.Index().SetName("cluster_membership_last_heartbeat"),
			},
			{
				Keys: bson.D{
					{"session_start", 1},
				},
				Options: options.Index().SetName("cluster_membership_session_start"),
			},
		},
	}
}

// queuesCollection defines the queues collection schema
func queuesCollection() Collection {
	return Collection{
		Name: "queues",
		Validation: bson.M{
			"$jsonSchema": bson.M{
				"bsonType": "object",
				"required": []string{"queue_type", "queue_name"},
				"properties": bson.M{
					"queue_type":        bson.M{"bsonType": "int"},
					"queue_name":        bson.M{"bsonType": "string"},
					"metadata_payload":  bson.M{"bsonType": "binData"},
					"metadata_encoding": bson.M{"bsonType": "string"},
					"version":           bson.M{"bsonType": "long"},
				},
			},
		},
		Indexes: []Index{
			{
				Keys: bson.D{
					{"queue_type", 1},
					{"queue_name", 1},
				},
				Options: options.Index().SetUnique(true).SetName("queues_primary_key"),
			},
		},
	}
}

// queueMessagesCollection defines the queue_messages collection schema
func queueMessagesCollection() Collection {
	return Collection{
		Name: "queue_messages",
		Validation: bson.M{
			"$jsonSchema": bson.M{
				"bsonType": "object",
				"required": []string{"queue_type", "queue_name", "queue_partition", "message_id"},
				"properties": bson.M{
					"queue_type":       bson.M{"bsonType": "int"},
					"queue_name":       bson.M{"bsonType": "string"},
					"queue_partition":  bson.M{"bsonType": "int"},
					"message_id":       bson.M{"bsonType": "long"},
					"message_payload":  bson.M{"bsonType": "binData"},
					"message_encoding": bson.M{"bsonType": "string"},
				},
			},
		},
		Indexes: []Index{
			{
				Keys: bson.D{
					{"queue_type", 1},
					{"queue_name", 1},
					{"queue_partition", 1},
					{"message_id", 1},
				},
				Options: options.Index().SetUnique(true).SetName("queue_messages_primary_key"),
			},
		},
	}
}

// nexusEndpointsCollection defines the nexus_endpoints collection schema
func nexusEndpointsCollection() Collection {
	return Collection{
		Name: "nexus_endpoints",
		Validation: bson.M{
			"$jsonSchema": bson.M{
				"bsonType": "object",
				"required": []string{"partition", "type", "id"},
				"properties": bson.M{
					"partition":     bson.M{"bsonType": "int"},
					"type":          bson.M{"bsonType": "int"},
					"id":            bson.M{"bsonType": "binData"},
					"data":          bson.M{"bsonType": "binData"},
					"data_encoding": bson.M{"bsonType": "string"},
					"version":       bson.M{"bsonType": "long"},
				},
			},
		},
		Indexes: []Index{
			{
				Keys: bson.D{
					{"partition", 1},
					{"type", 1},
					{"id", 1},
				},
				Options: options.Index().SetUnique(true).SetName("nexus_endpoints_primary_key"),
			},
			{
				Keys: bson.D{
					{"partition", 1},
					{"type", 1},
				},
				Options: options.Index().SetName("nexus_endpoints_lookup"),
			},
		},
	}
}
