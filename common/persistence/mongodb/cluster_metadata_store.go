package mongodb

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	p "go.temporal.io/server/common/persistence"
)

type (
	// ClusterMetadataStore implements the ClusterMetadataStore interface for MongoDB
	ClusterMetadataStore struct {
		client     *mongo.Client
		database   *mongo.Database
		collection *mongo.Collection
		logger     log.Logger
	}

	// ClusterMetadataDocument represents a cluster metadata document in MongoDB
	ClusterMetadataDocument struct {
		MetadataPartition int    `bson:"metadata_partition"`
		ClusterName       string `bson:"cluster_name"`
		Data              []byte `bson:"data"`
		DataEncoding      string `bson:"data_encoding"`
		Version           int64  `bson:"version"`
	}

	// ClusterMemberDocument represents a cluster member document in MongoDB
	ClusterMemberDocument struct {
		Role          int32     `bson:"role"`
		HostID        string    `bson:"host_id"`
		RPCAddress    string    `bson:"rpc_address"`
		RPCPort       uint16    `bson:"rpc_port"`
		SessionStart  time.Time `bson:"session_start"`
		LastHeartbeat time.Time `bson:"last_heartbeat"`
		RecordExpiry  time.Time `bson:"record_expiry"`
		CreatedAt     time.Time `bson:"created_at"`
	}
)

// NewClusterMetadataStore creates a new MongoDB cluster metadata store
func NewClusterMetadataStore(database *mongo.Database, logger log.Logger) *ClusterMetadataStore {
	return &ClusterMetadataStore{
		client:     database.Client(),
		database:   database,
		collection: database.Collection("cluster_metadata_info"),
		logger:     logger,
	}
}

// Close closes the cluster metadata store
func (s *ClusterMetadataStore) Close() {
	// MongoDB client is managed by the factory
}

// GetName returns the name of the cluster metadata store
func (s *ClusterMetadataStore) GetName() string {
	return "mongodb-cluster-metadata-store"
}

// ListClusterMetadata lists cluster metadata
func (s *ClusterMetadataStore) ListClusterMetadata(ctx context.Context, request *p.InternalListClusterMetadataRequest) (*p.InternalListClusterMetadataResponse, error) {
	filter := bson.M{}

	opts := options.Find().
		SetSort(bson.D{{Key: "cluster_name", Value: 1}}).
		SetLimit(int64(request.PageSize))

	// Handle pagination if page token is provided
	if len(request.NextPageToken) > 0 {
		// For now, we'll skip the first N documents based on the page token
		// In a real implementation, you'd decode the page token to get the last seen ID
		skipCount := len(request.NextPageToken) // Simplified approach
		opts.SetSkip(int64(skipCount))
	}

	cursor, err := s.collection.Find(ctx, filter, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to list cluster metadata: %w", err)
	}
	defer cursor.Close(ctx)

	var clusterMetadata []*p.InternalGetClusterMetadataResponse
	for cursor.Next(ctx) {
		var doc ClusterMetadataDocument
		if err := cursor.Decode(&doc); err != nil {
			s.logger.Error("failed to decode cluster metadata document", tag.Error(err))
			continue
		}

		encodingType, err := enumspb.EncodingTypeFromString(doc.DataEncoding)
		if err != nil {
			encodingType = enumspb.ENCODING_TYPE_UNSPECIFIED
		}

		clusterMetadata = append(clusterMetadata, &p.InternalGetClusterMetadataResponse{
			ClusterMetadata: &commonpb.DataBlob{
				Data:         doc.Data,
				EncodingType: encodingType,
			},
			Version: doc.Version,
		})
	}

	return &p.InternalListClusterMetadataResponse{
		ClusterMetadata: clusterMetadata,
	}, nil
}

// GetClusterMetadata retrieves cluster metadata
func (s *ClusterMetadataStore) GetClusterMetadata(ctx context.Context, request *p.InternalGetClusterMetadataRequest) (*p.InternalGetClusterMetadataResponse, error) {
	filter := bson.M{
		"metadata_partition": 0,
		"cluster_name":       request.ClusterName,
	}

	var doc ClusterMetadataDocument
	err := s.collection.FindOne(ctx, filter).Decode(&doc)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, &p.ConditionFailedError{
				Msg: "cluster metadata not found",
			}
		}
		return nil, fmt.Errorf("failed to get cluster metadata: %w", err)
	}

	encodingType, err := enumspb.EncodingTypeFromString(doc.DataEncoding)
	if err != nil {
		encodingType = enumspb.ENCODING_TYPE_UNSPECIFIED
	}

	return &p.InternalGetClusterMetadataResponse{
		ClusterMetadata: &commonpb.DataBlob{
			Data:         doc.Data,
			EncodingType: encodingType,
		},
		Version: doc.Version,
	}, nil
}

// SaveClusterMetadata saves cluster metadata
func (s *ClusterMetadataStore) SaveClusterMetadata(ctx context.Context, request *p.InternalSaveClusterMetadataRequest) (bool, error) {
	doc := &ClusterMetadataDocument{
		MetadataPartition: 0,
		ClusterName:       request.ClusterName,
		Data:              request.ClusterMetadata.Data,
		DataEncoding:      request.ClusterMetadata.EncodingType.String(),
		Version:           request.Version,
	}

	// Use upsert to handle both create and update cases
	filter := bson.M{
		"metadata_partition": doc.MetadataPartition,
		"cluster_name":       doc.ClusterName,
	}

	update := bson.M{
		"$set": doc,
	}

	opts := options.Update().SetUpsert(true)
	_, err := s.collection.UpdateOne(ctx, filter, update, opts)
	if err != nil {
		return false, fmt.Errorf("failed to save cluster metadata: %w", err)
	}

	return true, nil
}

// DeleteClusterMetadata deletes cluster metadata
func (s *ClusterMetadataStore) DeleteClusterMetadata(ctx context.Context, request *p.InternalDeleteClusterMetadataRequest) error {
	filter := bson.M{
		"metadata_partition": 0,
		"cluster_name":       request.ClusterName,
	}

	_, err := s.collection.DeleteOne(ctx, filter)
	if err != nil {
		return fmt.Errorf("failed to delete cluster metadata: %w", err)
	}

	return nil
}

// GetClusterMembers retrieves cluster members
func (s *ClusterMetadataStore) GetClusterMembers(ctx context.Context, request *p.GetClusterMembersRequest) (*p.GetClusterMembersResponse, error) {
	// This is a simplified implementation
	// In a real implementation, you'd handle getting cluster members
	return &p.GetClusterMembersResponse{
		ActiveMembers: []*p.ClusterMember{},
	}, nil
}

// UpsertClusterMembership upserts cluster membership
func (s *ClusterMetadataStore) UpsertClusterMembership(ctx context.Context, request *p.UpsertClusterMembershipRequest) error {
	// This is a simplified implementation
	// In a real implementation, you'd handle upserting cluster membership
	return nil
}

// PruneClusterMembership prunes cluster membership
func (s *ClusterMetadataStore) PruneClusterMembership(ctx context.Context, request *p.PruneClusterMembershipRequest) error {
	// This is a simplified implementation
	// In a real implementation, you'd handle pruning cluster membership
	return nil
}
