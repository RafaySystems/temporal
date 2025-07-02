## Using the MongoDB schema tool

This package contains the tooling for temporal MongoDB operations. The tool provides comprehensive schema management, validation, and maintenance capabilities for MongoDB-based Temporal deployments.

## Features

- **Schema Management**: Create, validate, and update MongoDB schemas
- **Version Control**: Track schema versions and manage upgrades
- **Health Checks**: Monitor database health and performance
- **Backup & Restore**: Backup and restore collections
- **Data Cleanup**: Remove old data based on time criteria
- **Statistics**: Get detailed database and collection statistics

## For localhost development

For the very first time run:
```bash
make
```

then run:
```bash
make install-schema-mongodb
```

to create schema in your `mongodb` instance.

## For production

### Create the binary
- Run `make temporal-mongodb-tool` on the root of repository
- You should see an executable `temporal-mongodb-tool`

### Basic Operations

#### Create schema
```bash
./temporal-mongodb-tool --uri mongodb://localhost:27017 --database temporal --action create
```

#### Validate schema
```bash
./temporal-mongodb-tool --uri mongodb://localhost:27017 --database temporal --action validate
```

#### Drop schema (⚠️ Destructive)
```bash
./temporal-mongodb-tool --uri mongodb://localhost:27017 --database temporal --action drop
```

### Schema Version Management

#### Initial schema setup
```bash
./temporal-mongodb-tool --uri mongodb://localhost:27017 --database temporal --action setup-schema --version 1.0
```

This sets up the schema version tables with initial version of 1.0 and creates all required collections and indexes.

#### Update schema to a new version
```bash
./temporal-mongodb-tool --uri mongodb://localhost:27017 --database temporal --action update-schema --version 1.1
```

This upgrades your schema to version 1.1. You can only upgrade to a new version after the initial setup.

#### Update schema with custom schema directory
```bash
./temporal-mongodb-tool --uri mongodb://localhost:27017 --database temporal --action update-schema --version 1.1 --schema-dir ./schema/mongodb/versioned/v1.1
```

### Advanced Operations

#### Health check
```bash
./temporal-mongodb-tool --uri mongodb://localhost:27017 --database temporal --action health-check
```

#### Get database information
```bash
./temporal-mongodb-tool --uri mongodb://localhost:27017 --database temporal --action info
```

#### List collections
```bash
./temporal-mongodb-tool --uri mongodb://localhost:27017 --database temporal --action list-collections
```

#### Get collection statistics
```bash
./temporal-mongodb-tool --uri mongodb://localhost:27017 --database temporal --action collection-stats --collection executions
```

#### Get collection indexes
```bash
./temporal-mongodb-tool --uri mongodb://localhost:27017 --database temporal --action list-indexes --collection executions
```

#### Backup collection
```bash
./temporal-mongodb-tool --uri mongodb://localhost:27017 --database temporal --action backup --collection executions --backup-name executions_backup
```

#### Restore collection
```bash
./temporal-mongodb-tool --uri mongodb://localhost:27017 --database temporal --action restore --backup-name executions_backup --target-name executions_restored
```

#### Cleanup old data
```bash
./temporal-mongodb-tool --uri mongodb://localhost:27017 --database temporal --action cleanup --collection executions --older-than 30d
```

## Command Line Options

| Option | Description | Default |
|--------|-------------|---------|
| `--uri` | MongoDB connection URI | `mongodb://localhost:27017` |
| `--database` | Database name | `temporal` |
| `--action` | Action to perform | `create` |
| `--version` | Schema version for setup/update | `` |
| `--schema-dir` | Directory containing schema files | `` |
| `--timeout` | Connection timeout | `30s` |
| `--verbose` | Enable verbose output | `false` |
| `--collection` | Collection name for specific operations | `` |
| `--backup-name` | Backup collection name | `` |
| `--target-name` | Target collection name for restore | `` |
| `--older-than` | Time duration for cleanup (e.g., 30d, 24h) | `` |

## Available Actions

- `create` - Create MongoDB schema
- `validate` - Validate existing schema
- `drop` - Drop entire database (⚠️ Destructive)
- `setup-schema` - Initial schema setup with version tracking
- `update-schema` - Update schema to a new version
- `health-check` - Perform database health check
- `info` - Get comprehensive database information
- `list-collections` - List all collections
- `collection-stats` - Get collection statistics
- `list-indexes` - List indexes for a collection
- `backup` - Backup a collection
- `restore` - Restore a collection from backup
- `cleanup` - Clean up old data

## MongoDB Collections

The Temporal MongoDB schema includes the following collections:

- `executions` - Workflow execution data
- `history_node` - Workflow history nodes
- `history_tree` - Workflow history tree structure
- `tasks` - Task queue data
- `task_queue_user_data` - User data for task queues
- `namespaces_by_id` - Namespace data indexed by ID
- `namespaces` - Namespace information
- `queue_metadata` - Queue metadata
- `queue` - Queue data
- `cluster_metadata_info` - Cluster metadata
- `cluster_membership` - Cluster membership information
- `queues` - Queue definitions
- `queue_messages` - Queue messages
- `nexus_endpoints` - Nexus endpoint configurations
- `schema_version` - Schema version tracking

## Indexes

Key indexes are automatically created for optimal performance:

- **executions_primary_key**: Unique index on shard_id, type, namespace_id, workflow_id, run_id, visibility_ts, task_id
- **executions_workflow_lookup**: Index on namespace_id, workflow_id, run_id
- **executions_visibility**: Index on namespace_id, visibility_ts

## Security Considerations

- Use authentication and authorization in production
- Configure TLS/SSL for encrypted connections
- Use network security groups to restrict access
- Regularly backup your data
- Monitor database performance and resource usage

## Troubleshooting

### Connection Issues
- Verify MongoDB is running and accessible
- Check network connectivity and firewall settings
- Ensure correct connection URI format
- Verify authentication credentials if using auth

### Schema Issues
- Run validation to check schema integrity
- Check MongoDB logs for detailed error messages
- Ensure sufficient disk space for operations
- Verify MongoDB version compatibility

### Performance Issues
- Monitor index usage and create missing indexes
- Check collection statistics for large collections
- Consider data archiving for old records
- Monitor MongoDB server resources

## Examples

### Development Setup
```bash
# Start MongoDB (if using Docker)
docker run -d -p 27017:27017 --name mongodb mongo:latest

# Create schema
./temporal-mongodb-tool --uri mongodb://localhost:27017 --database temporal --action setup-schema --version 1.0 --verbose

# Validate schema
./temporal-mongodb-tool --uri mongodb://localhost:27017 --database temporal --action validate --verbose
```

### Production Deployment
```bash
# Setup initial schema
./temporal-mongodb-tool --uri mongodb://user:pass@cluster.example.com:27017 --database temporal --action setup-schema --version 1.0

# Health check
./temporal-mongodb-tool --uri mongodb://user:pass@cluster.example.com:27017 --database temporal --action health-check

# Get database info
./temporal-mongodb-tool --uri mongodb://user:pass@cluster.example.com:27017 --database temporal --action info
```

### Maintenance Operations
```bash
# Backup executions collection
./temporal-mongodb-tool --uri mongodb://localhost:27017 --database temporal --action backup --collection executions --backup-name executions_backup_$(date +%Y%m%d)

# Cleanup old data (older than 90 days)
./temporal-mongodb-tool --uri mongodb://localhost:27017 --database temporal --action cleanup --collection executions --older-than 90d

# Get collection statistics
./temporal-mongodb-tool --uri mongodb://localhost:27017 --database temporal --action collection-stats --collection executions
``` 