# MongoDB Persistence Plugin for Temporal

This directory contains a MongoDB persistence plugin implementation for Temporal. The plugin provides a complete MongoDB backend for Temporal's persistence layer, allowing you to use MongoDB as your primary datastore.

## Overview

The MongoDB persistence plugin implements all the required interfaces from Temporal's persistence layer:

- **TaskStore**: Manages task queues and tasks
- **ShardStore**: Manages shard information and ownership
- **MetadataStore**: Manages namespace metadata
- **ExecutionStore**: Manages workflow executions and history
- **ClusterMetadataStore**: Manages cluster-wide metadata
- **NexusEndpointStore**: Manages Nexus endpoints
- **Queue/QueueV2**: Manages internal queues and messages

## Features

- **Full Persistence Interface Support**: Implements all required Temporal persistence interfaces
- **MongoDB Native**: Uses MongoDB's native features for optimal performance
- **Connection Pooling**: Configurable connection pool settings
- **Indexing**: Proper indexing for efficient queries
- **Error Handling**: Comprehensive error handling with Temporal-specific error types
- **Logging**: Structured logging with Temporal's logging framework
- **Metrics**: Built-in metrics for monitoring MongoDB operations
- **Transactions**: Support for MongoDB transactions for complex operations
- **Bulk Operations**: Optimized bulk operations for better performance

## Configuration

### MongoDB Configuration

Add the MongoDB configuration to your Temporal configuration file:

```yaml
persistence:
  defaultStore: mongodb
  numHistoryShards: 4
  datastores:
    mongodb:
      mongodb:
        connectAddr: "localhost:27017"
        databaseName: "temporal"
        username: "temporal"
        password: "your_password"
        maxConns: 100
        maxIdleConns: 10
        maxConnLifetime: 30m
        connectTimeout: 5s
        writeConcern: "majority"
        readConcern: "majority"
        readPreference: "primary"
        tls:
          enabled: false
          certFile: ""
          keyFile: ""
          caFile: ""
          serverName: ""
          disableHostVerification: false
```

### Configuration Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `connectAddr` | string | - | MongoDB connection address (e.g., "localhost:27017") |
| `databaseName` | string | - | Database name to use |
| `username` | string | - | MongoDB username for authentication |
| `password` | string | - | MongoDB password for authentication |
| `maxConns` | int | 100 | Maximum connection pool size |
| `maxIdleConns` | int | 10 | Minimum connection pool size |
| `maxConnLifetime` | duration | 30m | Maximum time a connection can be alive |
| `connectTimeout` | duration | 5s | Connection timeout |
| `writeConcern` | string | "majority" | MongoDB write concern |
| `readConcern` | string | "majority" | MongoDB read concern |
| `readPreference` | string | "primary" | MongoDB read preference |
| `tls` | object | - | TLS configuration |

## Usage

### Basic Usage

```go
package main

import (
    "go.temporal.io/server/common/config"
    "go.temporal.io/server/common/log"
    "go.temporal.io/server/common/persistence/mongodb"
)

func main() {
    // Create MongoDB configuration
    cfg := config.MongoDB{
        ConnectAddr:    "localhost:27017",
        DatabaseName:   "temporal",
        Username:       "temporal",
        Password:       "your_password",
        MaxConns:       100,
        MaxIdleConns:   10,
        ConnectTimeout: 5 * time.Second,
    }

    // Create logger
    logger := log.NewNoopLogger()

    // Create MongoDB factory
    factory := mongodb.NewFactory(cfg, resolver.NewNoopResolver(), "my-cluster", logger, metrics.NoopMetricsHandler)
    defer factory.Close()

    // Create stores
    taskStore, err := factory.NewTaskStore()
    if err != nil {
        panic(err)
    }
    defer taskStore.Close()

    shardStore, err := factory.NewShardStore()
    if err != nil {
        panic(err)
    }
    defer shardStore.Close()

    // Use the stores...
}
```

### Integration with Temporal Server

To use MongoDB as your persistence backend in Temporal Server:

1. **Update Configuration**: Add the MongoDB configuration to your Temporal server configuration.

2. **Register Plugin**: The plugin is automatically registered when you import the MongoDB package.

3. **Start Server**: Start Temporal server with the MongoDB configuration.

## Collections

The plugin creates the following MongoDB collections:

### Core Collections

- **`shards`**: Stores shard information and ownership
- **`tasks`**: Stores task queue tasks
- **`namespaces`**: Stores namespace metadata
- **`workflow_executions`**: Stores workflow execution state
- **`cluster_metadata`**: Stores cluster-wide metadata
- **`nexus_endpoints`**: Stores Nexus endpoint configurations

### History Collections

- **`history_nodes`**: Stores workflow history nodes
- **`history_trees`**: Stores workflow history trees

### Queue Collections

- **`queues`**: Stores queue metadata
- **`queue_messages`**: Stores queue messages

## Indexes

The plugin automatically creates the following indexes for optimal performance:

### Executions Collection
- Primary key: `shard_id`, `type`, `namespace_id`, `workflow_id`, `run_id`, `visibility_ts`, `task_id`
- Workflow lookup: `namespace_id`, `workflow_id`, `run_id`
- Visibility: `namespace_id`, `visibility_ts`

### Tasks Collection
- Primary key: `namespace_id`, `task_queue_name`, `task_queue_type`, `type`, `task_id`
- Task queue: `namespace_id`, `task_queue_name`, `task_queue_type`
- Expiry: `expiry_time`

### History Nodes Collection
- Primary key: `shard_id`, `branch_token`, `node_id`
- Transaction: `shard_id`, `branch_token`, `transaction_id`

## Error Handling

The plugin provides comprehensive error handling with Temporal-specific error types:

- **ShardOwnershipLostError**: When shard ownership is lost
- **ConditionFailedError**: When conditional operations fail
- **WorkflowConditionFailedError**: When workflow conditions fail
- **CurrentWorkflowConditionFailedError**: When current workflow conditions fail

## Performance Considerations

### Connection Pooling
- Configure appropriate `maxConns` and `maxIdleConns` based on your workload
- Monitor connection pool metrics

### Indexing
- All required indexes are created automatically
- Monitor index usage and performance

### Write Concerns
- Use `majority` write concern for durability
- Consider using `1` for better performance in single-node deployments

### Read Preferences
- Use `primary` for consistency
- Consider `secondary` for read-heavy workloads

## Monitoring

The plugin provides built-in metrics for monitoring MongoDB operations:

- **Operation Latency**: Time taken for various operations
- **Operation Counts**: Number of operations performed
- **Error Rates**: Rate of errors by operation type
- **Connection Pool**: Connection pool statistics
- **Bulk Operations**: Bulk operation performance metrics

## Limitations

- **Shard IO Concurrency**: MongoDB implementation only supports `ShardIOConcurrency = 1`
- **Transaction Size**: Limited by MongoDB's transaction size limits
- **Consistency**: Eventual consistency model (can be configured with write/read concerns)

## Development

### Running Tests

```bash
# Run all MongoDB persistence tests
go test ./common/persistence/mongodb/...

# Run specific test suites
go test ./common/persistence/mongodb/ -run TestTaskStore
go test ./common/persistence/mongodb/ -run TestExecutionStore
```

### Adding New Features

1. **Implement Store Interface**: Add new methods to the appropriate store
2. **Add Tests**: Create comprehensive tests for new functionality
3. **Update Documentation**: Update this README with new features
4. **Add Metrics**: Include metrics for new operations

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Update documentation
6. Submit a pull request

## Support

For issues and questions:

1. Check the [Temporal documentation](https://docs.temporal.io/)
2. Search existing [GitHub issues](https://github.com/temporalio/temporal/issues)
3. Create a new issue with detailed information

## License

This project is licensed under the MIT License - see the [LICENSE](../../../LICENSE) file for details. 