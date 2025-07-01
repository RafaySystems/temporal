package mongodb

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/log"
	p "go.temporal.io/server/common/persistence"
)

type (
	// ShardStore implements the ShardStore interface for MongoDB
	ShardStore struct {
		clusterName string
		client      *mongo.Client
		database    *mongo.Database
		collection  *mongo.Collection
		logger      log.Logger
	}

	// ShardDocument represents a shard document in MongoDB
	ShardDocument struct {
		ShardID          int32     `bson:"shard_id"`
		RangeID          int64     `bson:"range_id"`
		Owner            string    `bson:"owner"`
		Shard            []byte    `bson:"shard,omitempty"`
		ShardEncoding    string    `bson:"shard_encoding,omitempty"`
		StolenSinceRenew int32     `bson:"stolen_since_renew"`
		UpdatedAt        time.Time `bson:"updated_at"`
		CreatedAt        time.Time `bson:"created_at"`
	}
)

// NewShardStore creates a new MongoDB shard store
func NewShardStore(clusterName string, database *mongo.Database, logger log.Logger) *ShardStore {
	return &ShardStore{
		clusterName: clusterName,
		client:      database.Client(),
		database:    database,
		collection:  database.Collection("shards"),
		logger:      logger,
	}
}

// Close closes the shard store
func (s *ShardStore) Close() {
	// MongoDB client is managed by the factory
}

// GetName returns the name of the shard store
func (s *ShardStore) GetName() string {
	return "mongodb-shard-store"
}

// GetClusterName returns the cluster name
func (s *ShardStore) GetClusterName() string {
	return s.clusterName
}

// GetOrCreateShard retrieves or creates a shard
func (s *ShardStore) GetOrCreateShard(ctx context.Context, request *p.InternalGetOrCreateShardRequest) (*p.InternalGetOrCreateShardResponse, error) {
	filter := bson.M{
		"shard_id": request.ShardID,
	}

	var doc ShardDocument
	err := s.collection.FindOne(ctx, filter).Decode(&doc)
	if err == nil {
		// Shard exists, return it
		encodingType, err := enumspb.EncodingTypeFromString(doc.ShardEncoding)
		if err != nil {
			encodingType = enumspb.ENCODING_TYPE_UNSPECIFIED
		}

		return &p.InternalGetOrCreateShardResponse{
			ShardInfo: &commonpb.DataBlob{
				Data:         doc.Shard,
				EncodingType: encodingType,
			},
		}, nil
	}

	if err != mongo.ErrNoDocuments {
		return nil, fmt.Errorf("failed to get shard: %w", err)
	}

	// Shard doesn't exist, create it
	rangeID, shardInfo, err := request.CreateShardInfo()
	if err != nil {
		return nil, fmt.Errorf("failed to create shard info: %w", err)
	}

	now := time.Now()
	newDoc := &ShardDocument{
		ShardID:          request.ShardID,
		RangeID:          rangeID,
		Owner:            "", // Will be set when shard is acquired
		Shard:            shardInfo.Data,
		ShardEncoding:    shardInfo.EncodingType.String(),
		StolenSinceRenew: 0,
		CreatedAt:        now,
		UpdatedAt:        now,
	}

	_, err = s.collection.InsertOne(ctx, newDoc)
	if err != nil {
		return nil, fmt.Errorf("failed to create shard: %w", err)
	}

	return &p.InternalGetOrCreateShardResponse{
		ShardInfo: shardInfo,
	}, nil
}

// UpdateShard updates a shard
func (s *ShardStore) UpdateShard(ctx context.Context, request *p.InternalUpdateShardRequest) error {
	filter := bson.M{
		"shard_id": request.ShardID,
		"range_id": request.PreviousRangeID, // Optimistic concurrency control
	}

	update := bson.M{
		"$set": bson.M{
			"range_id":       request.RangeID,
			"owner":          request.Owner,
			"shard":          request.ShardInfo.Data,
			"shard_encoding": request.ShardInfo.EncodingType.String(),
			"updated_at":     time.Now(),
		},
	}

	result, err := s.collection.UpdateOne(ctx, filter, update)
	if err != nil {
		return fmt.Errorf("failed to update shard: %w", err)
	}

	if result.MatchedCount == 0 {
		return &p.ShardOwnershipLostError{
			ShardID: request.ShardID,
			Msg:     "shard not found or range ID mismatch",
		}
	}

	return nil
}

// AssertShardOwnership asserts shard ownership
func (s *ShardStore) AssertShardOwnership(ctx context.Context, request *p.AssertShardOwnershipRequest) error {
	filter := bson.M{
		"shard_id": request.ShardID,
		"range_id": request.RangeID,
	}

	var doc ShardDocument
	err := s.collection.FindOne(ctx, filter).Decode(&doc)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return &p.ShardOwnershipLostError{
				ShardID: request.ShardID,
				Msg:     "shard not found",
			}
		}
		return fmt.Errorf("failed to assert shard ownership: %w", err)
	}

	// For AssertShardOwnership, we just verify the shard exists with the expected range ID
	// The owner field is not part of the request, so we don't check it
	return nil
}
