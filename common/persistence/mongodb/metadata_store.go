package mongodb

import (
	"context"
	"errors"
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
	// MetadataStore implements the MetadataStore interface for MongoDB
	MetadataStore struct {
		client     *mongo.Client
		database   *mongo.Database
		collection *mongo.Collection
		logger     log.Logger
	}

	// NamespaceDocument represents a namespace document in MongoDB
	NamespaceDocument struct {
		ID                  string    `bson:"_id"`
		Name                string    `bson:"name"`
		Namespace           []byte    `bson:"namespace,omitempty"`
		NamespaceEncoding   string    `bson:"namespace_encoding,omitempty"`
		IsGlobal            bool      `bson:"is_global"`
		NotificationVersion int64     `bson:"notification_version"`
		CreatedAt           time.Time `bson:"created_at"`
		UpdatedAt           time.Time `bson:"updated_at"`
	}
)

// NewMetadataStore creates a new MongoDB metadata store
func NewMetadataStore(database *mongo.Database, logger log.Logger) *MetadataStore {
	return &MetadataStore{
		client:     database.Client(),
		database:   database,
		collection: database.Collection("namespaces"),
		logger:     logger,
	}
}

// Close closes the metadata store
func (s *MetadataStore) Close() {
	// MongoDB client is managed by the factory
}

// GetName returns the name of the metadata store
func (s *MetadataStore) GetName() string {
	return "mongodb-metadata-store"
}

// CreateNamespace creates a new namespace
func (s *MetadataStore) CreateNamespace(ctx context.Context, request *p.InternalCreateNamespaceRequest) (*p.CreateNamespaceResponse, error) {
	now := time.Now()
	doc := &NamespaceDocument{
		ID:                  request.ID,
		Name:                request.Name,
		Namespace:           request.Namespace.Data,
		NamespaceEncoding:   request.Namespace.EncodingType.String(),
		IsGlobal:            request.IsGlobal,
		NotificationVersion: 1, // Start with version 1
		CreatedAt:           now,
		UpdatedAt:           now,
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
		return nil, fmt.Errorf("failed to create namespace: %w", err)
	}

	return &p.CreateNamespaceResponse{
		ID: request.ID,
	}, nil
}

// GetNamespace retrieves a namespace
func (s *MetadataStore) GetNamespace(ctx context.Context, request *p.GetNamespaceRequest) (*p.InternalGetNamespaceResponse, error) {
	var filter bson.M
	if request.ID != "" {
		filter = bson.M{"_id": request.ID}
	} else if request.Name != "" {
		filter = bson.M{"name": request.Name}
	} else {
		return nil, errors.New("either ID or Name must be provided")
	}

	var doc NamespaceDocument
	err := s.collection.FindOne(ctx, filter).Decode(&doc)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, &p.ConditionFailedError{
				Msg: "namespace not found",
			}
		}
		return nil, fmt.Errorf("failed to get namespace: %w", err)
	}

	encodingType, err := enumspb.EncodingTypeFromString(doc.NamespaceEncoding)
	if err != nil {
		encodingType = enumspb.ENCODING_TYPE_UNSPECIFIED
	}

	return &p.InternalGetNamespaceResponse{
		Namespace: &commonpb.DataBlob{
			Data:         doc.Namespace,
			EncodingType: encodingType,
		},
		IsGlobal:            doc.IsGlobal,
		NotificationVersion: doc.NotificationVersion,
	}, nil
}

// UpdateNamespace updates a namespace
func (s *MetadataStore) UpdateNamespace(ctx context.Context, request *p.InternalUpdateNamespaceRequest) error {
	filter := bson.M{
		"_id": request.Id,
	}

	update := bson.M{
		"$set": bson.M{
			"name":                 request.Name,
			"namespace":            request.Namespace.Data,
			"namespace_encoding":   request.Namespace.EncodingType.String(),
			"is_global":            request.IsGlobal,
			"notification_version": request.NotificationVersion,
			"updated_at":           time.Now(),
		},
	}

	result, err := s.collection.UpdateOne(ctx, filter, update)
	if err != nil {
		return fmt.Errorf("failed to update namespace: %w", err)
	}

	if result.MatchedCount == 0 {
		return &p.ConditionFailedError{
			Msg: "namespace not found",
		}
	}

	return nil
}

// RenameNamespace renames a namespace
func (s *MetadataStore) RenameNamespace(ctx context.Context, request *p.InternalRenameNamespaceRequest) error {
	// First, check if the new name already exists
	existingFilter := bson.M{"name": request.Name}
	var existingDoc NamespaceDocument
	err := s.collection.FindOne(ctx, existingFilter).Decode(&existingDoc)
	if err == nil {
		return &p.ConditionFailedError{
			Msg: "namespace with new name already exists",
		}
	}

	// Update the namespace
	filter := bson.M{
		"name": request.PreviousName,
	}

	update := bson.M{
		"$set": bson.M{
			"name":       request.Name,
			"updated_at": time.Now(),
		},
	}

	result, err := s.collection.UpdateOne(ctx, filter, update)
	if err != nil {
		return fmt.Errorf("failed to rename namespace: %w", err)
	}

	if result.MatchedCount == 0 {
		return &p.ConditionFailedError{
			Msg: "namespace not found",
		}
	}

	return nil
}

// DeleteNamespace deletes a namespace
func (s *MetadataStore) DeleteNamespace(ctx context.Context, request *p.DeleteNamespaceRequest) error {
	filter := bson.M{
		"_id": request.ID,
	}

	_, err := s.collection.DeleteOne(ctx, filter)
	if err != nil {
		return fmt.Errorf("failed to delete namespace: %w", err)
	}

	return nil
}

// DeleteNamespaceByName deletes a namespace by name
func (s *MetadataStore) DeleteNamespaceByName(ctx context.Context, request *p.DeleteNamespaceByNameRequest) error {
	filter := bson.M{
		"name": request.Name,
	}

	_, err := s.collection.DeleteOne(ctx, filter)
	if err != nil {
		return fmt.Errorf("failed to delete namespace by name: %w", err)
	}

	return nil
}

// ListNamespaces lists namespaces
func (s *MetadataStore) ListNamespaces(ctx context.Context, request *p.InternalListNamespacesRequest) (*p.InternalListNamespacesResponse, error) {
	filter := bson.M{}

	opts := options.Find().
		SetSort(bson.D{{Key: "name", Value: 1}}).
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
		return nil, fmt.Errorf("failed to list namespaces: %w", err)
	}
	defer cursor.Close(ctx)

	var namespaces []*p.InternalGetNamespaceResponse
	for cursor.Next(ctx) {
		var doc NamespaceDocument
		if err := cursor.Decode(&doc); err != nil {
			s.logger.Error("failed to decode namespace document", tag.Error(err))
			continue
		}

		encodingType, err := enumspb.EncodingTypeFromString(doc.NamespaceEncoding)
		if err != nil {
			encodingType = enumspb.ENCODING_TYPE_UNSPECIFIED
		}

		namespaces = append(namespaces, &p.InternalGetNamespaceResponse{
			Namespace: &commonpb.DataBlob{
				Data:         doc.Namespace,
				EncodingType: encodingType,
			},
			IsGlobal:            doc.IsGlobal,
			NotificationVersion: doc.NotificationVersion,
		})
	}

	return &p.InternalListNamespacesResponse{
		Namespaces: namespaces,
	}, nil
}

// GetMetadata retrieves metadata
func (s *MetadataStore) GetMetadata(ctx context.Context) (*p.GetMetadataResponse, error) {
	// This is a simplified implementation
	// In a real implementation, you'd track notification version across all namespaces
	return &p.GetMetadataResponse{
		NotificationVersion: 1,
	}, nil
}
