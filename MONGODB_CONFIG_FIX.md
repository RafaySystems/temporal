# MongoDB Configuration Fix Summary

## Problem
The Temporal server was failing to start with MongoDB configuration due to two main issues:

1. **Missing MongoDB support in docker.yaml**: The `config/docker.yaml` file only supported Cassandra, MySQL, and PostgreSQL, but not MongoDB.
2. **SSL parsing error**: The MongoDB connection string was being constructed incorrectly, causing an error: `"invalid value for \"ssl\": \"false/temporal\""`.

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

A test script `test_mongodb_config.sh` was created to verify the configuration works correctly:

```bash
./test_mongodb_config.sh
```

The script tests:
1. Configuration rendering
2. Server startup (help command)
3. All required environment variables

## Usage

To start Temporal with MongoDB:

```bash
TEMPORAL_ENVIRONMENT=docker \
TEMPORAL_CONFIG_DIR=config \
DB=mongodb \
MONGODB_SEEDS=localhost:27017 \
MONGODB_USER=temporal \
MONGODB_PWD=temporal123 \
go run ./cmd/server/main.go --allow-no-auth start
```

## Additional Notes

- The `--allow-no-auth` flag is recommended to suppress the authorizer warning
- MongoDB support is now fully integrated into the docker configuration template
- The configuration supports both default and visibility datastores with separate settings
- TLS configuration is supported but disabled by default
- Connection pooling and timeout settings are configurable via environment variables

## Files Modified

1. `config/docker.yaml` - Added MongoDB configuration support
2. `common/persistence/mongodb/factory.go` - Fixed URI construction
3. `test_mongodb_config.sh` - Created test script (new file)
4. `MONGODB_CONFIG_FIX.md` - This documentation (new file) 