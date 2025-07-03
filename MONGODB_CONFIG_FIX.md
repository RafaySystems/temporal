# MongoDB Configuration Fix Summary

## Problem
The Temporal server was failing to start with MongoDB configuration due to three main issues:

1. **Missing MongoDB support in docker.yaml**: The `config/docker.yaml` file only supported Cassandra, MySQL, and PostgreSQL, but not MongoDB.
2. **SSL parsing error**: The MongoDB connection string was being constructed incorrectly, causing an error: `"invalid value for \"ssl\": \"false/temporal\""`.
3. **Missing cluster metadata**: After fixing the connection issues, the server failed with `"cluster metadata not found"` because the MongoDB database didn't have the required schema and cluster metadata initialized.

## Root Cause Analysis

### Issue 1: Missing MongoDB Configuration
The `config/docker.yaml` file was a template that only included configuration blocks for:
- `cassandra`
- `mysql8` 
- `postgres12`
- `postgres12_pgx`

When `DB=mongodb` was set, the template would fail to generate the required MongoDB configuration blocks, resulting in missing `connectAddr` and `databaseName` fields.

### Issue 2: Incorrect URI Construction
In `common/persistence/mongodb/factory.go`, the `buildMongoURI()` function was incorrectly adding the database name to the MongoDB connection URI path:

```go
// WRONG - This was causing the SSL parsing error
uri += "/" + f.cfg.DatabaseName
```

The MongoDB Go driver expects the database name to be specified separately when creating the database instance, not as part of the connection URI.

### Issue 3: Missing Database Schema and Cluster Metadata
After fixing the connection issues, Temporal requires:
1. **MongoDB Schema**: 14 collections must be created with proper indexes and validation
2. **Cluster Metadata**: The `cluster_metadata_info` collection must contain at least one cluster metadata record
3. **Initialization**: The cluster metadata must be properly serialized and stored in the database

### Issue 4: Collection Name Mismatch
The `ClusterMetadataStore` was looking for a collection called `cluster_metadata`, but the MongoDB schema creates a collection called `cluster_metadata_info`. This mismatch caused the "cluster metadata not found" error.

## Solutions Implemented

### 1. Added MongoDB Support to docker.yaml

**File**: `config/docker.yaml`

**Changes**:
- Added `"mongodb"` to the supported database types validation
- Added MongoDB configuration blocks for both `default` and `visibility` datastores
- Included all necessary MongoDB configuration options:
  - `connectAddr`
  - `databaseName`
  - `username`
  - `password`
  - `maxConns`
  - `maxIdleConns`
  - `maxConnLifetime`
  - `writeConcern`
  - `readConcern`
  - `readPreference`
  - TLS configuration support

**Example configuration generated**:
```yaml
datastores:
  default:
    mongodb:
      connectAddr: localhost:27017
      databaseName: temporal
      username: temporal
      password: temporal123
      maxConns: 20
      maxIdleConns: 20
      maxConnLifetime: 1h
      writeConcern: majority
      readConcern: majority
      readPreference: primary
  visibility:
    mongodb:
      connectAddr: localhost:27017
      databaseName: temporal_visibility
      username: temporal
      password: temporal123
      maxConns: 10
      maxIdleConns: 10
      maxConnLifetime: 1h
      writeConcern: majority
      readConcern: majority
      readPreference: primary
```

### 2. Fixed MongoDB URI Construction

**File**: `common/persistence/mongodb/factory.go`

**Changes**:
- Removed the database name from the URI construction
- The database name is now only used when creating the database instance with `client.Database(f.cfg.DatabaseName)`

**Before**:
```go
// Add the connection address
uri += f.cfg.ConnectAddr

// Add database name (WRONG)
if f.cfg.DatabaseName != "" {
    uri += "/" + f.cfg.DatabaseName
}
```

**After**:
```go
// Add the connection address
uri += f.cfg.ConnectAddr
// Database name is handled separately by client.Database()
```

### 3. Created MongoDB Setup Tools

**Files Created**:
- `setup_mongodb.sh` - Automated setup script
- `MONGODB_MANUAL_SETUP.md` - Manual setup guide

**Features**:
- **Schema Creation**: Creates all 14 required MongoDB collections with proper indexes
- **Cluster Metadata Initialization**: Inserts the required cluster metadata record
- **Validation**: Verifies that the setup was successful
- **Error Handling**: Provides clear error messages and troubleshooting steps

### 4. Fixed Collection Name Mismatch

**File**: `common/persistence/mongodb/cluster_metadata_store.go`

**Changes**:
- Changed collection name from `cluster_metadata` to `cluster_metadata_info` to match the schema
- Updated document structure to match the MongoDB schema fields:
  - `metadata_partition` (int)
  - `cluster_name` (string)
  - `data` (binData)
  - `data_encoding` (string)
  - `version` (long)
- Fixed all query filters to use the correct field names
- Updated sorting to use `cluster_name` instead of `_id`

**Required Collections**:
1. `executions` - Workflow executions
2. `history_node` - Workflow history nodes
3. `history_tree` - Workflow history tree
4. `tasks` - Tasks
5. `task_queue_user_data` - Task queue user data
6. `namespaces_by_id` - Namespaces by ID
7. `namespaces` - Namespaces
8. `queue_metadata` - Queue metadata
9. `queue` - Queue
10. `cluster_metadata_info` - **Cluster metadata (required for startup)**
11. `cluster_membership` - Cluster membership
12. `queues` - Queues
13. `queue_messages` - Queue messages
14. `nexus_endpoints` - Nexus endpoints

## Environment Variables Required

To use MongoDB with Temporal, set these environment variables:

```bash
export TEMPORAL_ENVIRONMENT=docker
export TEMPORAL_CONFIG_DIR=config
export DB=mongodb
export MONGODB_SEEDS=localhost:27017
export MONGODB_USER=temporal
export MONGODB_PWD=temporal123
```

## Testing

The configuration can be tested by running:

```bash
# Test configuration rendering
TEMPORAL_ENVIRONMENT=docker \
TEMPORAL_CONFIG_DIR=config \
DB=mongodb \
MONGODB_SEEDS=rafay-mongodb-0.rafay-mongodb-svc.rafay-core.svc.cluster.local:27017 \
MONGODB_USER=aisrvdbuser \
MONGODB_PWD=aisrvdbuser \
go run ./cmd/server/main.go render-config

# Test server startup (help command)
TEMPORAL_ENVIRONMENT=docker \
TEMPORAL_CONFIG_DIR=config \
DB=mongodb \
MONGODB_SEEDS=rafay-mongodb-0.rafay-mongodb-svc.rafay-core.svc.cluster.local:27017 \
MONGODB_USER=aisrvdbuser \
MONGODB_PWD=aisrvdbuser \
go run ./cmd/server/main.go --allow-no-auth start --help
```

## Setup Instructions

### Automated Setup (Recommended)
Run the setup script to automatically create the schema and initialize cluster metadata:

```bash
./setup_mongodb.sh
```

### Manual Setup
If the automated script doesn't work in your environment, follow the manual instructions in `MONGODB_MANUAL_SETUP.md`.

### Verification
After setup, verify that everything is working:

```bash
go run ./cmd/tools/mongodb/main.go \
    -uri "mongodb://aisrvdbuser:aisrvdbuser@rafay-mongodb-0.rafay-mongodb-svc.rafay-core.svc.cluster.local:27017/admin?replicaSet=rafay-mongodb&ssl=false" \
    -database temporal \
    -action validate \
    -verbose
```

## Usage

To start Temporal with MongoDB:

```bash
TEMPORAL_ENVIRONMENT=docker \
TEMPORAL_CONFIG_DIR=config \
DB=mongodb \
MONGODB_SEEDS=rafay-mongodb-0.rafay-mongodb-svc.rafay-core.svc.cluster.local:27017 \
MONGODB_USER=aisrvdbuser \
MONGODB_PWD=aisrvdbuser \
go run ./cmd/server/main.go --allow-no-auth start
```

**Note**: Make sure to run the setup script or manual setup first to create the required schema and cluster metadata.

## Additional Notes

- The `--allow-no-auth` flag is recommended to suppress the authorizer warning
- MongoDB support is now fully integrated into the docker configuration template
- The configuration supports both default and visibility datastores with separate settings
- TLS configuration is supported but disabled by default
- Connection pooling and timeout settings are configurable via environment variables

## Files Modified

1. `config/docker.yaml` - Added MongoDB configuration support
2. `common/persistence/mongodb/factory.go` - Fixed URI construction
3. `common/persistence/mongodb/cluster_metadata_store.go` - Fixed collection name and document structure
4. `setup_mongodb.sh` - Created automated setup script (new file)
5. `MONGODB_MANUAL_SETUP.md` - Created manual setup guide (new file)
6. `MONGODB_CONFIG_FIX.md` - This documentation (new file) 