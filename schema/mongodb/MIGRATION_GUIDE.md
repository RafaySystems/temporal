# Migration Guide: Cassandra to MongoDB

This guide helps you migrate your Temporal deployment from Cassandra to MongoDB.

## Prerequisites

1. MongoDB 4.4+ installed and running
2. Temporal server with MongoDB persistence support
3. Backup of your existing Cassandra data
4. Downtime window for migration

## Migration Steps

### 1. Prepare MongoDB Environment

```bash
# Start MongoDB (if not already running)
mongod --dbpath /path/to/mongodb/data

# Create temporal database and user
mongo
use temporal
db.createUser({
  user: "temporal",
  pwd: "your_password",
  roles: ["readWrite"]
})
```

### 2. Initialize MongoDB Schema

```bash
# Run the database initialization script
mongo temporal schema/mongodb/database.js

# Run the schema creation script
mongo temporal schema/mongodb/schema.js
```

### 3. Update Temporal Configuration

Update your Temporal configuration to use MongoDB persistence:

```yaml
persistence:
  defaultStore: mongodb
  visibilityStore: mongodb
  numHistoryShards: 4
  datastores:
    mongodb:
      pluginName: "mongodb"
      databaseName: "temporal"
      connectAddr: "localhost:27017"
      connectTimeout: 5s
      username: "temporal"
      password: "your_password"
      maxConns: 10
      maxIdleConns: 5
      maxConnLifetime: 1h
      tls:
        enabled: false
        certFile: ""
        keyFile: ""
        caFile: ""
        serverName: ""
        disableHostVerification: false
```

### 4. Data Migration

#### Option A: Using Temporal's Built-in Migration Tools

If Temporal provides built-in migration tools:

```bash
# Run migration tool
temporal-mongodb-tool migrate --from-cassandra --to-mongodb
```

#### Option B: Manual Migration Script

Create a custom migration script to transfer data:

```javascript
// migration-script.js
const { MongoClient } = require('mongodb');
const cassandra = require('cassandra-driver');

// Connect to Cassandra
const cassandraClient = new cassandra.Client({
  contactPoints: ['localhost'],
  localDataCenter: 'datacenter1',
  keyspace: 'temporal'
});

// Connect to MongoDB
const mongoClient = new MongoClient('mongodb://localhost:27017');
const db = mongoClient.db('temporal');

async function migrateExecutions() {
  const query = 'SELECT * FROM executions';
  const result = await cassandraClient.execute(query);
  
  for (const row of result.rows) {
    const doc = {
      shard_id: row.shard_id,
      type: row.type,
      namespace_id: row.namespace_id,
      workflow_id: row.workflow_id,
      run_id: row.run_id,
      current_run_id: row.current_run_id,
      visibility_ts: row.visibility_ts,
      task_id: row.task_id,
      shard: row.shard,
      shard_encoding: row.shard_encoding,
      execution: row.execution,
      execution_encoding: row.execution_encoding,
      // ... map all other fields
    };
    
    await db.collection('executions').insertOne(doc);
  }
}

// Run migration for each collection
async function migrateAll() {
  await migrateExecutions();
  // Add other collection migrations
}

migrateAll().then(() => {
  console.log('Migration completed');
  process.exit(0);
}).catch(console.error);
```

### 5. Verify Migration

After migration, verify data integrity:

```javascript
// verification-script.js
const { MongoClient } = require('mongodb');

const mongoClient = new MongoClient('mongodb://localhost:27017');
const db = mongoClient.db('temporal');

async function verifyData() {
  // Check collection counts
  const executionsCount = await db.collection('executions').countDocuments();
  const tasksCount = await db.collection('tasks').countDocuments();
  const namespacesCount = await db.collection('namespaces').countDocuments();
  
  console.log(`Executions: ${executionsCount}`);
  console.log(`Tasks: ${tasksCount}`);
  console.log(`Namespaces: ${namespacesCount}`);
  
  // Verify sample data
  const sampleExecution = await db.collection('executions').findOne();
  console.log('Sample execution:', sampleExecution);
}

verifyData().then(() => {
  console.log('Verification completed');
  process.exit(0);
}).catch(console.error);
```

### 6. Update Application Code

If your application code has Cassandra-specific queries, update them for MongoDB:

#### Before (Cassandra):
```go
query := `SELECT * FROM executions WHERE namespace_id = ? AND workflow_id = ?`
result := session.Query(query, namespaceID, workflowID)
```

#### After (MongoDB):
```go
filter := bson.M{
    "namespace_id": namespaceID,
    "workflow_id":  workflowID,
}
result := collection.Find(ctx, filter)
```

## Key Differences

### Data Types
- **Cassandra**: `uuid`, `timestamp`, `blob`, `text`
- **MongoDB**: `binData` (for UUIDs), `date`, `binData` (for blobs), `string`

### Indexing
- **Cassandra**: Primary keys define partition and clustering keys
- **MongoDB**: Compound indexes with explicit ordering

### Queries
- **Cassandra**: CQL with specific partition key requirements
- **MongoDB**: Flexible queries with index support

### Transactions
- **Cassandra**: Limited transaction support
- **MongoDB**: Multi-document transactions (4.0+)

## Performance Considerations

1. **Indexes**: Ensure all required indexes are created
2. **Connection Pooling**: Configure appropriate connection pool sizes
3. **Read Preferences**: Use appropriate read preferences for your use case
4. **Write Concerns**: Configure write concerns based on durability requirements

## Troubleshooting

### Common Issues

1. **Index Creation Fails**: Check MongoDB version compatibility
2. **Data Type Mismatches**: Verify UUID and timestamp conversions
3. **Performance Issues**: Review index usage with `explain()`
4. **Connection Issues**: Check network connectivity and authentication

### Monitoring

Monitor these metrics during and after migration:
- MongoDB operation latency
- Index usage statistics
- Connection pool utilization
- Disk space usage

## Rollback Plan

If issues arise, you can rollback to Cassandra:

1. Stop Temporal services
2. Revert configuration to Cassandra
3. Restart services
4. Verify data integrity

## Support

For issues with MongoDB migration:
1. Check Temporal documentation
2. Review MongoDB best practices
3. Consult Temporal community forums
4. Contact Temporal support if needed 