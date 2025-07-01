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
	// NexusEndpointStore implements the NexusEndpointStore interface for MongoDB
	NexusEndpointStore struct {
		client     *mongo.Client
		database   *mongo.Database
		collection *mongo.Collection
		logger     log.Logger
	}

	// NexusEndpointDocument represents a nexus endpoint document in MongoDB
	NexusEndpointDocument struct {
		ID           string    `bson:"_id"`
		Version      int64     `bson:"version"`
		Data         []byte    `bson:"data,omitempty"`
		DataEncoding string    `bson:"data_encoding,omitempty"`
		CreatedAt    time.Time `bson:"created_at"`
		UpdatedAt    time.Time `bson:"updated_at"`
	}
)

// NewNexusEndpointStore creates a new MongoDB nexus endpoint store
func NewNexusEndpointStore(database *mongo.Database, logger log.Logger) *NexusEndpointStore {
	return &NexusEndpointStore{
		client:     database.Client(),
		database:   database,
		collection: database.Collection("nexus_endpoints"),
		logger:     logger,
	}
}

// Close closes the nexus endpoint store
func (s *NexusEndpointStore) Close() {
	// MongoDB client is managed by the factory
}

// GetName returns the name of the nexus endpoint store
func (s *NexusEndpointStore) GetName() string {
	return "mongodb-nexus-endpoint-store"
}

// CreateOrUpdateNexusEndpoint creates or updates a nexus endpoint
func (s *NexusEndpointStore) CreateOrUpdateNexusEndpoint(ctx context.Context, request *p.InternalCreateOrUpdateNexusEndpointRequest) error {
	now := time.Now()
	doc := &NexusEndpointDocument{
		ID:           request.Endpoint.ID,
		Version:      request.Endpoint.Version,
		Data:         request.Endpoint.Data.Data,
		DataEncoding: request.Endpoint.Data.EncodingType.String(),
		CreatedAt:    now,
		UpdatedAt:    now,
	}

	// Use upsert to handle both create and update cases
	filter := bson.M{
		"_id": doc.ID,
	}

	update := bson.M{
		"$set": doc,
		"$setOnInsert": bson.M{
			"created_at": now,
		},
	}

	opts := options.Update().SetUpsert(true)
	_, err := s.collection.UpdateOne(ctx, filter, update, opts)
	if err != nil {
		return fmt.Errorf("failed to create or update nexus endpoint: %w", err)
	}

	return nil
}

// DeleteNexusEndpoint deletes a nexus endpoint
func (s *NexusEndpointStore) DeleteNexusEndpoint(ctx context.Context, request *p.DeleteNexusEndpointRequest) error {
	filter := bson.M{
		"_id": request.ID,
	}

	_, err := s.collection.DeleteOne(ctx, filter)
	if err != nil {
		return fmt.Errorf("failed to delete nexus endpoint: %w", err)
	}

	return nil
}

// GetNexusEndpoint retrieves a nexus endpoint
func (s *NexusEndpointStore) GetNexusEndpoint(ctx context.Context, request *p.GetNexusEndpointRequest) (*p.InternalNexusEndpoint, error) {
	filter := bson.M{
		"_id": request.ID,
	}

	var doc NexusEndpointDocument
	err := s.collection.FindOne(ctx, filter).Decode(&doc)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, &p.ConditionFailedError{
				Msg: "nexus endpoint not found",
			}
		}
		return nil, fmt.Errorf("failed to get nexus endpoint: %w", err)
	}

	encodingType, err := enumspb.EncodingTypeFromString(doc.DataEncoding)
	if err != nil {
		encodingType = enumspb.ENCODING_TYPE_UNSPECIFIED
	}

	return &p.InternalNexusEndpoint{
		ID:      doc.ID,
		Version: doc.Version,
		Data: &commonpb.DataBlob{
			Data:         doc.Data,
			EncodingType: encodingType,
		},
	}, nil
}

// ListNexusEndpoints lists nexus endpoints
func (s *NexusEndpointStore) ListNexusEndpoints(ctx context.Context, request *p.ListNexusEndpointsRequest) (*p.InternalListNexusEndpointsResponse, error) {
	filter := bson.M{}

	opts := options.Find().
		SetSort(bson.D{{"_id", 1}}).
		SetLimit(int64(request.PageSize))

	if len(request.NextPageToken) > 0 {
		// In a real implementation, you'd decode the page token
		// and use it for pagination
	}

	cursor, err := s.collection.Find(ctx, filter, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to list nexus endpoints: %w", err)
	}
	defer cursor.Close(ctx)

	var endpoints []p.InternalNexusEndpoint
	for cursor.Next(ctx) {
		var doc NexusEndpointDocument
		if err := cursor.Decode(&doc); err != nil {
			s.logger.Error("failed to decode nexus endpoint document", tag.Error(err))
			continue
		}

		encodingType, err := enumspb.EncodingTypeFromString(doc.DataEncoding)
		if err != nil {
			encodingType = enumspb.ENCODING_TYPE_UNSPECIFIED
		}

		endpoints = append(endpoints, p.InternalNexusEndpoint{
			ID:      doc.ID,
			Version: doc.Version,
			Data: &commonpb.DataBlob{
				Data:         doc.Data,
				EncodingType: encodingType,
			},
		})
	}

	return &p.InternalListNexusEndpointsResponse{
		TableVersion: 1, // This should be tracked properly in a real implementation
		Endpoints:    endpoints,
	}, nil
}
