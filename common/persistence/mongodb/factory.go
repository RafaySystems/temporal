package mongodb

import (
	"context"
	"strings"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/resolver"
)

type (
	// Factory vends datastore implementations backed by MongoDB
	Factory struct {
		sync.RWMutex
		cfg         config.MongoDB
		clusterName string
		logger      log.Logger
		client      *mongo.Client
		database    *mongo.Database
	}
)

// NewFactory returns an instance of a factory object which can be used to create
// data stores that are backed by MongoDB
func NewFactory(
	cfg config.MongoDB,
	r resolver.ServiceResolver,
	clusterName string,
	logger log.Logger,
	metricsHandler metrics.Handler,
) *Factory {
	return &Factory{
		cfg:         cfg,
		clusterName: clusterName,
		logger:      logger,
	}
}

// Close closes the factory
func (f *Factory) Close() {
	f.Lock()
	defer f.Unlock()

	if f.client != nil {
		if err := f.client.Disconnect(context.Background()); err != nil {
			f.logger.Error("failed to disconnect MongoDB client", tag.Error(err))
		}
		f.client = nil
		f.database = nil
	}
}

// NewTaskStore returns a new task store
func (f *Factory) NewTaskStore() (p.TaskStore, error) {
	database, err := f.getDatabase()
	if err != nil {
		return nil, err
	}

	return NewTaskStore(database, f.logger), nil
}

// NewShardStore returns a new shard store
func (f *Factory) NewShardStore() (p.ShardStore, error) {
	database, err := f.getDatabase()
	if err != nil {
		return nil, err
	}

	return NewShardStore(f.clusterName, database, f.logger), nil
}

// NewMetadataStore returns a new metadata store
func (f *Factory) NewMetadataStore() (p.MetadataStore, error) {
	database, err := f.getDatabase()
	if err != nil {
		return nil, err
	}

	return NewMetadataStore(database, f.logger), nil
}

// NewExecutionStore returns a new execution store
func (f *Factory) NewExecutionStore() (p.ExecutionStore, error) {
	database, err := f.getDatabase()
	if err != nil {
		return nil, err
	}

	return NewExecutionStore(database, f.logger), nil
}

// NewQueue returns a new queue
func (f *Factory) NewQueue(queueType p.QueueType) (p.Queue, error) {
	database, err := f.getDatabase()
	if err != nil {
		return nil, err
	}

	return NewQueue(queueType, database, f.logger), nil
}

// NewQueueV2 returns a new QueueV2
func (f *Factory) NewQueueV2() (p.QueueV2, error) {
	database, err := f.getDatabase()
	if err != nil {
		return nil, err
	}

	return NewQueueV2(database, f.logger), nil
}

// NewClusterMetadataStore returns a new cluster metadata store
func (f *Factory) NewClusterMetadataStore() (p.ClusterMetadataStore, error) {
	database, err := f.getDatabase()
	if err != nil {
		return nil, err
	}

	return NewClusterMetadataStore(database, f.logger), nil
}

// NewNexusEndpointStore returns a new nexus endpoint store
func (f *Factory) NewNexusEndpointStore() (p.NexusEndpointStore, error) {
	database, err := f.getDatabase()
	if err != nil {
		return nil, err
	}

	return NewNexusEndpointStore(database, f.logger), nil
}

// getDatabase returns the MongoDB database instance, creating it if necessary
func (f *Factory) getDatabase() (*mongo.Database, error) {
	f.RLock()
	if f.database != nil {
		defer f.RUnlock()
		return f.database, nil
	}
	f.RUnlock()

	f.Lock()
	defer f.Unlock()

	// Double-check after acquiring write lock
	if f.database != nil {
		return f.database, nil
	}

	// Build MongoDB URI from configuration
	uri := f.buildMongoURI()

	// Create MongoDB client
	clientOptions := options.Client().ApplyURI(uri)

	// Set connection pool options
	if f.cfg.MaxConns > 0 {
		clientOptions.SetMaxPoolSize(uint64(f.cfg.MaxConns))
	}
	if f.cfg.MaxIdleConns > 0 {
		clientOptions.SetMinPoolSize(uint64(f.cfg.MaxIdleConns))
	}
	if f.cfg.MaxConnLifetime > 0 {
		clientOptions.SetMaxConnIdleTime(f.cfg.MaxConnLifetime)
	}
	if f.cfg.ConnectTimeout > 0 {
		clientOptions.SetConnectTimeout(f.cfg.ConnectTimeout)
	}

	client, err := mongo.Connect(context.Background(), clientOptions)
	if err != nil {
		return nil, err
	}

	// Test the connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx, nil); err != nil {
		_ = client.Disconnect(context.Background())
		return nil, err
	}

	f.client = client
	f.database = client.Database(f.cfg.DatabaseName)

	f.logger.Info("MongoDB connection established",
		tag.NewStringTag("uri", uri),
		tag.NewStringTag("database", f.cfg.DatabaseName))

	return f.database, nil
}

// buildMongoURI builds the MongoDB connection URI from the configuration
func (f *Factory) buildMongoURI() string {
	// Start with the basic URI
	uri := "mongodb://"

	// Add authentication if provided
	if f.cfg.Username != "" {
		uri += f.cfg.Username
		if f.cfg.Password != "" {
			uri += ":" + f.cfg.Password
		}
		uri += "@"
	}

	// Add the connection address
	uri += f.cfg.ConnectAddr

	// Add query parameters
	params := make([]string, 0)

	if f.cfg.WriteConcern != "" {
		params = append(params, "w="+f.cfg.WriteConcern)
	}

	if f.cfg.ReadConcern != "" {
		params = append(params, "readConcernLevel="+f.cfg.ReadConcern)
	}

	if f.cfg.ReadPreference != "" {
		params = append(params, "readPreference="+f.cfg.ReadPreference)
	}

	if len(params) > 0 {
		uri += "/?" + strings.Join(params, "&")
	}

	return uri
}
