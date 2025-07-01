# Cassandra vs MongoDB Schema Comparison

This document provides a detailed comparison between the Cassandra and MongoDB schemas for Temporal, highlighting the key differences and mapping between the two databases.

## Table/Collection Mapping

| Cassandra Table | MongoDB Collection | Purpose |
|-----------------|-------------------|---------|
| `executions` | `executions` | Stores workflow execution data, tasks, and state |
| `history_node` | `history_node` | Stores workflow history events in a tree structure |
| `history_tree` | `history_tree` | Stores workflow history tree metadata |
| `tasks` | `tasks` | Stores activity or workflow tasks |
| `task_queue_user_data` | `task_queue_user_data` | Stores task queue user data and build ID mappings |
| `namespaces_by_id` | `namespaces_by_id` | Maps namespace UUID to namespace name |
| `namespaces` | `namespaces` | Stores namespace information |
| `queue_metadata` | `queue_metadata` | Stores queue metadata |
| `queue` | `queue` | Stores queue messages |
| `cluster_metadata_info` | `cluster_metadata_info` | Stores cluster metadata |
| `cluster_membership` | `cluster_membership` | Stores cluster membership information |
| `queues` | `queues` | Stores queue information |
| `queue_messages` | `queue_messages` | Stores queue messages with partitioning |
| `nexus_endpoints` | `nexus_endpoints` | Stores Nexus endpoint information |

## Data Type Mapping

### Cassandra to MongoDB Data Types

| Cassandra Type | MongoDB Type | Description |
|----------------|--------------|-------------|
| `uuid` | `binData` | Binary data for UUIDs |
| `timestamp` | `date` | Date/time values |
| `blob` | `binData` | Binary data |
| `text` | `string` | Text strings |
| `int` | `int` | 32-bit integers |
| `bigint` | `long` | 64-bit integers |
| `boolean` | `bool` | Boolean values |
| `map<key, value>` | `object` | Key-value mappings |
| `set<type>` | `array` | Collections of unique values |
| `list<type>` | `array` | Ordered collections |

## Primary Key Structure

### Cassandra Primary Keys
Cassandra uses composite primary keys with partition and clustering keys:

```sql
-- Example: executions table
PRIMARY KEY (shard_id, type, namespace_id, workflow_id, run_id, visibility_ts, task_id)
-- shard_id is the partition key
-- Others are clustering keys
```

### MongoDB Primary Keys
MongoDB uses compound indexes to achieve similar functionality:

```javascript
// Example: executions collection
db.executions.createIndex(
  { 
    "shard_id": 1, 
    "type": 1, 
    "namespace_id": 1, 
    "workflow_id": 1, 
    "run_id": 1, 
    "visibility_ts": 1, 
    "task_id": 1 
  }, 
  { unique: true }
);
```

## Detailed Schema Comparison

### 1. Executions Table/Collection

#### Cassandra Schema:
```sql
CREATE TABLE executions (
  shard_id                       int,
  type                           int,
  namespace_id                   uuid,
  workflow_id                    text,
  run_id                         uuid,
  current_run_id                 uuid,
  visibility_ts                  timestamp,
  task_id                        bigint,
  shard                          blob,
  shard_encoding                 text,
  execution                      blob,
  execution_encoding             text,
  execution_state                blob,
  execution_state_encoding       text,
  transfer                       blob,
  transfer_encoding              text,
  replication                    blob,
  replication_encoding           text,
  timer                          blob,
  timer_encoding                 text,
  visibility_task_data           blob,
  visibility_task_encoding       text,
  task_data                      blob,
  task_encoding                  text,
  next_event_id                  bigint,
  range_id                       bigint,
  activity_map                   map<bigint, blob>,
  activity_map_encoding          text,
  timer_map                      map<text, blob>,
  timer_map_encoding             text,
  child_executions_map           map<bigint, blob>,
  child_executions_map_encoding  text,
  request_cancel_map             map<bigint, blob>,
  request_cancel_map_encoding    text,
  signal_map                     map<bigint, blob>,
  signal_map_encoding            text,
  signal_requested               set<uuid>,
  chasm_node_map                 map<text, blob>,
  chasm_node_map_encoding        text,
  buffered_events_list           list<frozen<serialized_event_batch>>,
  workflow_last_write_version    bigint,
  workflow_state                 int,
  checksum                       blob,
  checksum_encoding              text,
  db_record_version              bigint,
  PRIMARY KEY (shard_id, type, namespace_id, workflow_id, run_id, visibility_ts, task_id)
);
```

#### MongoDB Schema:
```javascript
// Collection validation schema
{
  bsonType: "object",
  required: ["shard_id", "type", "namespace_id", "workflow_id", "run_id"],
  properties: {
    shard_id: { bsonType: "int" },
    type: { bsonType: "int" },
    namespace_id: { bsonType: "binData" },
    workflow_id: { bsonType: "string" },
    run_id: { bsonType: "binData" },
    current_run_id: { bsonType: "binData" },
    visibility_ts: { bsonType: "date" },
    task_id: { bsonType: "long" },
    shard: { bsonType: "binData" },
    shard_encoding: { bsonType: "string" },
    execution: { bsonType: "binData" },
    execution_encoding: { bsonType: "string" },
    execution_state: { bsonType: "binData" },
    execution_state_encoding: { bsonType: "string" },
    transfer: { bsonType: "binData" },
    transfer_encoding: { bsonType: "string" },
    replication: { bsonType: "binData" },
    replication_encoding: { bsonType: "string" },
    timer: { bsonType: "binData" },
    timer_encoding: { bsonType: "string" },
    visibility_task_data: { bsonType: "binData" },
    visibility_task_encoding: { bsonType: "string" },
    task_data: { bsonType: "binData" },
    task_encoding: { bsonType: "string" },
    next_event_id: { bsonType: "long" },
    range_id: { bsonType: "long" },
    activity_map: { bsonType: "object" },
    activity_map_encoding: { bsonType: "string" },
    timer_map: { bsonType: "object" },
    timer_map_encoding: { bsonType: "string" },
    child_executions_map: { bsonType: "object" },
    child_executions_map_encoding: { bsonType: "string" },
    request_cancel_map: { bsonType: "object" },
    request_cancel_map_encoding: { bsonType: "string" },
    signal_map: { bsonType: "object" },
    signal_map_encoding: { bsonType: "string" },
    signal_requested: { bsonType: "array" },
    chasm_node_map: { bsonType: "object" },
    chasm_node_map_encoding: { bsonType: "string" },
    buffered_events_list: { bsonType: "array" },
    workflow_last_write_version: { bsonType: "long" },
    workflow_state: { bsonType: "int" },
    checksum: { bsonType: "binData" },
    checksum_encoding: { bsonType: "string" },
    db_record_version: { bsonType: "long" }
  }
}
```

### 2. History Node Table/Collection

#### Cassandra Schema:
```sql
CREATE TABLE history_node (
  tree_id           uuid,
  branch_id         uuid,
  node_id           bigint,
  txn_id            bigint,
  prev_txn_id       bigint,
  data              blob,
  data_encoding     text,
  PRIMARY KEY ((tree_id), branch_id, node_id, txn_id)
) WITH CLUSTERING ORDER BY (branch_id ASC, node_id ASC, txn_id DESC);
```

#### MongoDB Schema:
```javascript
// Collection validation schema
{
  bsonType: "object",
  required: ["tree_id", "branch_id", "node_id", "txn_id"],
  properties: {
    tree_id: { bsonType: "binData" },
    branch_id: { bsonType: "binData" },
    node_id: { bsonType: "long" },
    txn_id: { bsonType: "long" },
    prev_txn_id: { bsonType: "long" },
    data: { bsonType: "binData" },
    data_encoding: { bsonType: "string" }
  }
}

// Index with descending order for txn_id
db.history_node.createIndex(
  { 
    "tree_id": 1, 
    "branch_id": 1, 
    "node_id": 1, 
    "txn_id": -1 
  }, 
  { unique: true }
);
```

## Query Patterns

### Cassandra Queries:
```sql
-- Get workflow execution
SELECT * FROM executions 
WHERE shard_id = ? AND type = ? AND namespace_id = ? AND workflow_id = ? AND run_id = ?;

-- Get history events
SELECT * FROM history_node 
WHERE tree_id = ? AND branch_id = ? AND node_id >= ? AND node_id <= ?;
```

### MongoDB Queries:
```javascript
// Get workflow execution
db.executions.findOne({
  shard_id: shardId,
  type: type,
  namespace_id: namespaceId,
  workflow_id: workflowId,
  run_id: runId
});

// Get history events
db.history_node.find({
  tree_id: treeId,
  branch_id: branchId,
  node_id: { $gte: startNodeId, $lte: endNodeId }
}).sort({ node_id: 1, txn_id: -1 });
```

## Performance Considerations

### Cassandra Strengths:
- Excellent write performance
- Linear scalability
- Built-in partitioning
- Strong consistency within partitions

### MongoDB Strengths:
- Flexible schema
- Rich query capabilities
- Better read performance for complex queries
- Built-in aggregation framework
- Multi-document transactions

### Indexing Strategies:

#### Cassandra:
- Primary key automatically creates indexes
- Secondary indexes on non-primary key columns
- Limited to one secondary index per query

#### MongoDB:
- Compound indexes for complex queries
- Text indexes for full-text search
- Geospatial indexes for location data
- Multiple indexes per collection

## Migration Considerations

### Data Volume:
- Large datasets may require batch processing
- Consider using MongoDB's bulk operations
- Monitor index creation time for large collections

### Consistency:
- Cassandra provides tunable consistency
- MongoDB offers different write concerns
- Consider eventual consistency vs strong consistency

### Transactions:
- Cassandra: Limited transaction support
- MongoDB: Multi-document transactions (4.0+)

### Backup and Recovery:
- Both databases support point-in-time recovery
- Consider backup strategies during migration
- Test restore procedures before production migration

## Monitoring and Maintenance

### Cassandra:
- Monitor compaction metrics
- Check SSTable counts
- Monitor read/write latencies

### MongoDB:
- Monitor index usage with `explain()`
- Check collection statistics
- Monitor connection pool utilization
- Track operation latencies

## Conclusion

The MongoDB schema provides equivalent functionality to the Cassandra schema while offering:

1. **Flexibility**: Easier schema evolution
2. **Query Power**: Rich query capabilities
3. **Developer Experience**: JSON-like documents
4. **Ecosystem**: Rich tooling and drivers

The migration requires careful planning but offers long-term benefits in terms of operational simplicity and query flexibility. 