# Temporal MongoDB Schema

## What
This directory contains the MongoDB schema for Temporal, implemented in Go to integrate seamlessly with the Temporal codebase. The directory structure is as follows:

```
./schema
   - mongodb/
        - schema.go           -- Main schema definition with collections and indexes
        - schema_test.go      -- Tests for schema validation and data insertion
        - version.go          -- Schema version tracking
        - versioned/
             - v1.0/
                - schema.go   -- Version-specific schema implementation
                - manifest.json -- Version metadata
        - MIGRATION_GUIDE.md  -- Migration guide from Cassandra to MongoDB
        - CASSANDRA_MONGODB_COMPARISON.md -- Detailed comparison document
```

## How

### Q: How do I update existing schema?
* Add your changes to `schema.go`
* Create a new schema version directory under `./schema/mongodb/versioned/vx.x`
  * Add a `manifest.json` with version metadata
  * Add a `schema.go` with version-specific implementation
* Update the version in `version.go`
* Add tests in `schema_test.go`

### Q: What's the format of manifest.json?

Example below:
* CurrVersion is the current schema version
* MinCompatibleVersion is the minimum schema version that your code can handle
* Description explains the changes in this version

```json
{
    "CurrVersion": "1.0",
    "MinCompatibleVersion": "1.0",
    "Description": "Initial MongoDB schema for Temporal"
}
```

## MongoDB Collections Overview

The MongoDB schema includes the following collections:

1. **executions** - Stores workflow execution data, tasks, and state
2. **history_node** - Stores workflow history events in a tree structure
3. **history_tree** - Stores workflow history tree metadata
4. **tasks** - Stores activity and workflow tasks
5. **task_queue_user_data** - Stores task queue user data and build ID mappings
6. **namespaces_by_id** - Maps namespace UUID to namespace name
7. **namespaces** - Stores namespace information
8. **queue_metadata** - Stores queue metadata
9. **queue** - Stores queue messages
10. **cluster_metadata_info** - Stores cluster metadata
11. **cluster_membership** - Stores cluster membership information
12. **queues** - Stores queue information
13. **queue_messages** - Stores queue messages with partitioning
14. **nexus_endpoints** - Stores Nexus endpoint information

## Usage

### Creating Schema Programmatically

```go
package main

import (
    "context"
    "log"
    
    "go.mongodb.org/mongo-driver/mongo"
    "go.mongodb.org/mongo-driver/mongo/options"
    
    "temporal/schema/mongodb"
)

func main() {
    // Connect to MongoDB
    ctx := context.Background()
    client, err := mongo.Connect(ctx, options.Client().ApplyURI("mongodb://localhost:27017"))
    if err != nil {
        log.Fatal(err)
    }
    defer client.Disconnect(ctx)
    
    // Create schema
    schema := mongodb.NewSchema("temporal")
    err = schema.CreateSchema(ctx, client)
    if err != nil {
        log.Fatal(err)
    }
    
    log.Println("MongoDB schema created successfully")
}
```

### Running Tests

```bash
# Run all tests
go test ./schema/mongodb/...

# Run integration tests (requires MongoDB running)
go test ./schema/mongodb/... -tags=integration

# Run specific test
go test ./schema/mongodb/... -run TestSchemaCreation
```

## Indexes

Each collection includes appropriate indexes for efficient querying:
- **Primary key indexes** with unique constraints
- **Secondary indexes** for frequently queried fields
- **Compound indexes** for complex queries
- **Optimized ordering** (e.g., descending `txn_id` for history nodes)

## Data Types

MongoDB schema uses:
- `binData` for UUIDs and binary data (equivalent to Cassandra `uuid` and `blob`)
- `date` for timestamps (equivalent to Cassandra `timestamp`)
- `string` for text fields (equivalent to Cassandra `text`)
- `int` for 32-bit integers (equivalent to Cassandra `int`)
- `long` for 64-bit integers (equivalent to Cassandra `bigint`)
- `bool` for boolean values (equivalent to Cassandra `boolean`)
- `object` for key-value mappings (equivalent to Cassandra `map<key,value>`)
- `array` for collections (equivalent to Cassandra `set<type>` and `list<type>`)

## Validation

Each collection includes JSON Schema validation to ensure data integrity:
- Required field validation
- Data type validation
- Field constraints

## Performance Considerations

1. **Indexes**: All required indexes are created automatically
2. **Connection Pooling**: Configure appropriate connection pool sizes in your application
3. **Read Preferences**: Use appropriate read preferences for your use case
4. **Write Concerns**: Configure write concerns based on durability requirements

## Integration with Temporal

The Go-based schema integrates seamlessly with Temporal's persistence layer:

```go
// Example persistence configuration
persistence:
  defaultStore: mongodb
  visibilityStore: mongodb
  datastores:
    mongodb:
      pluginName: "mongodb"
      databaseName: "temporal"
      connectAddr: "localhost:27017"
      connectTimeout: 5s
      maxConns: 10
      maxIdleConns: 5
      maxConnLifetime: 1h
```

## Migration

See `MIGRATION_GUIDE.md` for detailed instructions on migrating from Cassandra to MongoDB.

## Comparison with Cassandra

See `CASSANDRA_MONGODB_COMPARISON.md` for a detailed comparison between Cassandra and MongoDB schemas. 