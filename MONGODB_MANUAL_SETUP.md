# MongoDB Manual Setup Guide

## Problem
The Temporal server is failing with the error:
```
error while fetching cluster metadata: cluster metadata not found
```

This happens because the MongoDB database doesn't have the required schema and cluster metadata initialized.

## Solution

### Step 1: Create MongoDB Schema

Run the MongoDB schema creation tool:

```bash
go run ./cmd/tools/mongodb/main.go \
    -uri "mongodb://aisrvdbuser:aisrvdbuser@rafay-mongodb-0.rafay-mongodb-svc.rafay-core.svc.cluster.local:27017/admin?replicaSet=rafay-mongodb&ssl=false" \
    -database temporal \
    -action create \
    -verbose
```

### Step 2: Initialize Cluster Metadata

The cluster metadata needs to be manually inserted into the `cluster_metadata_info` collection. You can do this using MongoDB shell or any MongoDB client.

#### Option A: Using MongoDB Shell

Connect to your MongoDB instance and run:

```javascript
use temporal

// Insert cluster metadata
db.cluster_metadata_info.insertOne({
    metadata_partition: 0,
    cluster_name: "active",
    data: BinData(0, "CgZhY3RpdmUQBBoVdGVtcG9yYWwtY2x1c3Rlci0xKggxMjcuMC4wLjEQBDIICgExLjAiB2FjdGl2ZQ=="), // This is a sample protobuf data
    data_encoding: "proto3",
    version: 1
})
```

#### Option B: Using the Setup Script

Run the provided setup script:

```bash
./setup_mongodb.sh
```

### Step 3: Verify Setup

Check that the collections were created:

```bash
go run ./cmd/tools/mongodb/main.go \
    -uri "mongodb://aisrvdbuser:aisrvdbuser@rafay-mongodb-0.rafay-mongodb-svc.rafay-core.svc.cluster.local:27017/admin?replicaSet=rafay-mongodb&ssl=false" \
    -database temporal \
    -action validate \
    -verbose
```

### Step 4: Start Temporal Server

Once the schema and cluster metadata are set up, start the Temporal server:

```bash
TEMPORAL_ENVIRONMENT=docker \
TEMPORAL_CONFIG_DIR=config \
DB=mongodb \
MONGODB_SEEDS=rafay-mongodb-0.rafay-mongodb-svc.rafay-core.svc.cluster.local:27017 \
MONGODB_USER=aisrvdbuser \
MONGODB_PWD=aisrvdbuser \
go run ./cmd/server/main.go --allow-no-auth start
```

## Required Collections

The MongoDB schema creates the following collections:

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

## Troubleshooting

### Connection Issues
- Ensure MongoDB is accessible from your environment
- Check network connectivity to the MongoDB cluster
- Verify credentials and connection string

### Schema Creation Issues
- Ensure you have write permissions to the database
- Check MongoDB logs for any errors
- Verify the MongoDB version is compatible

### Cluster Metadata Issues
- The `cluster_metadata_info` collection must exist
- At least one cluster metadata record must be present
- The cluster name in the metadata should match your configuration

## Configuration Reference

Your current MongoDB configuration:
- **Host**: `rafay-mongodb-0.rafay-mongodb-svc.rafay-core.svc.cluster.local:27017`
- **Database**: `temporal`
- **Username**: `aisrvdbuser`
- **Password**: `aisrvdbuser`
- **Replica Set**: `rafay-mongodb`
- **SSL**: `false` 