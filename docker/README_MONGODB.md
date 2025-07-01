# Temporal with MongoDB in Docker

This directory contains Docker configuration files for running Temporal with MongoDB as the persistence backend.

## Quick Start

### Using Docker Compose (Recommended)

1. **Start the services:**
   ```bash
   docker-compose -f docker-compose-mongodb.yml up -d
   ```

2. **Check the services:**
   ```bash
   docker-compose -f docker-compose-mongodb.yml ps
   ```

3. **View logs:**
   ```bash
   docker-compose -f docker-compose-mongodb.yml logs -f temporal
   ```

4. **Access Temporal:**
   - **gRPC Endpoint**: `localhost:7233`
   - **HTTP Endpoint**: `localhost:7243`
   - **Web UI**: `http://localhost:8088`

### Using Environment Variables

You can also use the `config_template.yaml` with environment variables:

```bash
export DB=mongodb
export MONGODB_SEEDS=localhost:27017
export MONGODB_USER=temporal
export MONGODB_PWD=temporal123
export DBNAME=temporal
export VISIBILITY_DBNAME=temporal_visibility

# Start Temporal with these environment variables
docker run -e DB=mongodb -e MONGODB_SEEDS=localhost:27017 \
  -e MONGODB_USER=temporal -e MONGODB_PWD=temporal123 \
  temporalio/auto-setup:latest
```

## Configuration Files

### `config_template.yaml`
The main configuration template that supports MongoDB. Set `DB=mongodb` to use MongoDB persistence.

### `docker-compose-mongodb.yml`
Complete Docker Compose setup with:
- MongoDB 6.0 database
- Temporal server with MongoDB persistence
- Temporal Web UI (optional)

### `init-mongo.js`
MongoDB initialization script that:
- Creates the `temporal` user with appropriate permissions
- Creates necessary databases (`temporal`, `temporal_visibility`)
- Creates required collections

### `MONGODB_ENV_VARS.md`
Complete documentation of all MongoDB environment variables.

## Environment Variables

### Required
- `DB=mongodb` - Set database type to MongoDB
- `MONGODB_SEEDS` - MongoDB server address

### Optional
- `MONGODB_PORT` - MongoDB port (default: 27017)
- `MONGODB_USER` - MongoDB username
- `MONGODB_PWD` - MongoDB password
- `DBNAME` - Database name (default: temporal)
- `VISIBILITY_DBNAME` - Visibility database name (default: temporal_visibility)

### Connection Pool
- `MONGODB_MAX_POOL_SIZE` - Max connection pool size (default: 20)
- `MONGODB_MIN_POOL_SIZE` - Min connection pool size (default: 5)
- `MONGODB_MAX_CONN_IDLE_TIME` - Max connection idle time (default: 1h)

### MongoDB Settings
- `MONGODB_WRITE_CONCERN` - Write concern (default: majority)
- `MONGODB_READ_CONCERN` - Read concern (default: majority)
- `MONGODB_READ_PREFERENCE` - Read preference (default: primary)

## MongoDB Collections

The MongoDB persistence plugin creates the following collections:

### Temporal Database (`temporal`)
- `executions` - Workflow execution data
- `tasks` - Task queue data
- `shards` - History shard data
- `metadata` - Namespace and workflow metadata
- `cluster_metadata` - Cluster configuration
- `nexus_endpoints` - Nexus endpoint data
- `queues` - Task queue metadata

### Visibility Database (`temporal_visibility`)
- `visibility` - Workflow visibility records
- `visibility_tasks` - Visibility task data

## Troubleshooting

### Connection Issues
1. **Check MongoDB is running:**
   ```bash
   docker-compose -f docker-compose-mongodb.yml logs mongodb
   ```

2. **Verify MongoDB connection:**
   ```bash
   docker exec -it temporal-mongodb mongosh -u temporal -p temporal123
   ```

3. **Check Temporal logs:**
   ```bash
   docker-compose -f docker-compose-mongodb.yml logs temporal
   ```

### Authentication Issues
- Ensure `MONGODB_USER` and `MONGODB_PWD` match the MongoDB user credentials
- Check that the user has appropriate permissions on both databases

### Performance Issues
- Adjust connection pool settings based on your workload
- Monitor MongoDB performance with built-in tools
- Consider using MongoDB Atlas for production deployments

## Production Considerations

### Security
- Use strong passwords for MongoDB users
- Enable TLS encryption
- Use MongoDB authentication
- Restrict network access

### Performance
- Use MongoDB replica sets for high availability
- Configure appropriate indexes
- Monitor connection pool usage
- Use MongoDB Atlas for managed MongoDB

### Backup
- Set up regular MongoDB backups
- Test backup and restore procedures
- Consider point-in-time recovery

## Migration from Other Databases

If you're migrating from Cassandra, MySQL, or PostgreSQL:

1. **Export data** from your current database
2. **Set up MongoDB** with the provided configuration
3. **Import data** using Temporal's migration tools
4. **Update application configuration** to use MongoDB
5. **Test thoroughly** before switching production traffic

See the main `MIGRATION_GUIDE.md` for detailed migration instructions.

## Support

For issues with MongoDB persistence:
1. Check the Temporal documentation
2. Review MongoDB logs and Temporal logs
3. Verify configuration settings
4. Test with the provided Docker Compose setup 