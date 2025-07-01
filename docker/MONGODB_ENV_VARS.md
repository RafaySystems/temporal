# MongoDB Environment Variables for Docker

This document describes the MongoDB environment variables that can be used with the Temporal Docker configuration template (`docker/config_template.yaml`).

## Basic Configuration

### Database Selection
- `DB=mongodb` - Set to "mongodb" to use MongoDB as the persistence backend

### Connection Settings
- `MONGODB_SEEDS` - MongoDB server address (default: empty, must be provided)
- `MONGODB_PORT` - MongoDB server port (default: "27017")
- `MONGODB_USER` - MongoDB username (default: empty, no authentication)
- `MONGODB_PWD` - MongoDB password (default: empty, no authentication)

### Database Names
- `DBNAME` - Database name for Temporal data (default: "temporal")
- `VISIBILITY_DBNAME` - Database name for visibility data (default: "temporal_visibility")

## Connection Pool Settings

### Default Store (Temporal Data)
- `MONGODB_MAX_POOL_SIZE` - Maximum connection pool size (default: "20")
- `MONGODB_MIN_POOL_SIZE` - Minimum connection pool size (default: "5")
- `MONGODB_MAX_CONN_IDLE_TIME` - Maximum connection idle time (default: "1h")

### Visibility Store
- `MONGODB_VIS_MAX_POOL_SIZE` - Maximum connection pool size for visibility (default: "10")
- `MONGODB_VIS_MIN_POOL_SIZE` - Minimum connection pool size for visibility (default: "2")
- `MONGODB_VIS_MAX_CONN_IDLE_TIME` - Maximum connection idle time for visibility (default: "1h")

## MongoDB-Specific Settings

### Read/Write Concerns
- `MONGODB_WRITE_CONCERN` - Write concern level (default: "majority")
- `MONGODB_READ_CONCERN` - Read concern level (default: "majority")
- `MONGODB_READ_PREFERENCE` - Read preference (default: "primary")

## TLS Configuration

### TLS Settings
- `MONGODB_TLS_ENABLED` - Enable TLS for MongoDB connections (default: "false")
- `MONGODB_CA` - Path to CA certificate file
- `MONGODB_CERT` - Path to client certificate file
- `MONGODB_CERT_KEY` - Path to client private key file
- `MONGODB_HOST_VERIFICATION` - Enable host verification (default: "false")
- `MONGODB_HOST_NAME` - Server name for TLS verification

## Visibility Store Override

You can configure separate MongoDB instances for visibility data:

- `VISIBILITY_MONGODB_SEEDS` - MongoDB server for visibility (defaults to `MONGODB_SEEDS`)
- `VISIBILITY_MONGODB_PORT` - MongoDB port for visibility (defaults to `MONGODB_PORT`)
- `VISIBILITY_MONGODB_USER` - MongoDB user for visibility (defaults to `MONGODB_USER`)
- `VISIBILITY_MONGODB_PWD` - MongoDB password for visibility (defaults to `MONGODB_PWD`)

## Usage Examples

### Basic MongoDB Setup
```bash
export DB=mongodb
export MONGODB_SEEDS=mongodb.example.com
export MONGODB_PORT=27017
export DBNAME=temporal
export VISIBILITY_DBNAME=temporal_visibility
```

### MongoDB with Authentication
```bash
export DB=mongodb
export MONGODB_SEEDS=mongodb.example.com
export MONGODB_USER=temporal_user
export MONGODB_PWD=secure_password
export DBNAME=temporal
```

### MongoDB with TLS
```bash
export DB=mongodb
export MONGODB_SEEDS=mongodb.example.com
export MONGODB_TLS_ENABLED=true
export MONGODB_CA=/path/to/ca.crt
export MONGODB_CERT=/path/to/client.crt
export MONGODB_CERT_KEY=/path/to/client.key
export MONGODB_HOST_VERIFICATION=true
export MONGODB_HOST_NAME=mongodb.example.com
```

### MongoDB with Custom Connection Pool
```bash
export DB=mongodb
export MONGODB_SEEDS=mongodb.example.com
export MONGODB_MAX_POOL_SIZE=50
export MONGODB_MIN_POOL_SIZE=10
export MONGODB_MAX_CONN_IDLE_TIME=30m
```

### Separate Visibility MongoDB
```bash
export DB=mongodb
export MONGODB_SEEDS=mongodb-primary.example.com
export VISIBILITY_MONGODB_SEEDS=mongodb-secondary.example.com
export VISIBILITY_MONGODB_PORT=27018
export DBNAME=temporal
export VISIBILITY_DBNAME=temporal_visibility
```

## Docker Compose Example

```yaml
version: '3.8'
services:
  temporal:
    image: temporalio/auto-setup:latest
    environment:
      - DB=mongodb
      - MONGODB_SEEDS=mongodb:27017
      - MONGODB_USER=temporal
      - MONGODB_PWD=temporal123
      - DBNAME=temporal
      - VISIBILITY_DBNAME=temporal_visibility
      - MONGODB_MAX_POOL_SIZE=20
      - MONGODB_MIN_POOL_SIZE=5
      - MONGODB_WRITE_CONCERN=majority
      - MONGODB_READ_CONCERN=majority
    depends_on:
      - mongodb

  mongodb:
    image: mongo:6.0
    environment:
      - MONGO_INITDB_ROOT_USERNAME=temporal
      - MONGO_INITDB_ROOT_PASSWORD=temporal123
    ports:
      - "27017:27017"
```

## Notes

1. **Required Variables**: You must set `DB=mongodb` and provide `MONGODB_SEEDS`
2. **Authentication**: If using authentication, both `MONGODB_USER` and `MONGODB_PWD` must be set
3. **TLS**: TLS configuration requires all certificate-related variables to be set
4. **Visibility Store**: By default, visibility uses the same MongoDB instance as the default store
5. **Connection Pool**: Adjust pool sizes based on your workload and MongoDB server capacity
6. **Read/Write Concerns**: These affect consistency and performance - adjust based on your requirements

## Validation

The configuration template will validate that:
- MongoDB is selected as the database (`DB=mongodb`)
- MongoDB seeds are provided (`MONGODB_SEEDS`)
- TLS configuration is complete if enabled
- Connection pool settings are reasonable 