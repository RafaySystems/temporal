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
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/persistence/visibility/store"
	"go.temporal.io/server/common/persistence/visibility/store/query"
	"go.temporal.io/server/common/resolver"
	"go.temporal.io/server/common/searchattribute"
)

type (
	// VisibilityStore implements the VisibilityStore interface for MongoDB
	VisibilityStore struct {
		client                         *mongo.Client
		database                       *mongo.Database
		collection                     *mongo.Collection
		searchAttributesProvider       searchattribute.Provider
		searchAttributesMapperProvider searchattribute.MapperProvider
		logger                         log.Logger
		metricsHandler                 metrics.Handler
	}

	// VisibilityDocument represents a visibility document in MongoDB
	VisibilityDocument struct {
		ID                   string                 `bson:"_id"`
		NamespaceID          string                 `bson:"namespace_id"`
		WorkflowID           string                 `bson:"workflow_id"`
		RunID                string                 `bson:"run_id"`
		WorkflowTypeName     string                 `bson:"workflow_type_name"`
		StartTime            time.Time              `bson:"start_time"`
		ExecutionTime        time.Time              `bson:"execution_time"`
		CloseTime            *time.Time             `bson:"close_time,omitempty"`
		Status               int32                  `bson:"status"`
		HistoryLength        *int64                 `bson:"history_length,omitempty"`
		HistorySizeBytes     *int64                 `bson:"history_size_bytes,omitempty"`
		ExecutionDuration    *time.Duration         `bson:"execution_duration,omitempty"`
		StateTransitionCount *int64                 `bson:"state_transition_count,omitempty"`
		Memo                 []byte                 `bson:"memo,omitempty"`
		MemoEncoding         string                 `bson:"memo_encoding,omitempty"`
		TaskQueue            string                 `bson:"task_queue"`
		SearchAttributes     map[string]interface{} `bson:"search_attributes,omitempty"`
		ParentWorkflowID     *string                `bson:"parent_workflow_id,omitempty"`
		ParentRunID          *string                `bson:"parent_run_id,omitempty"`
		RootWorkflowID       string                 `bson:"root_workflow_id"`
		RootRunID            string                 `bson:"root_run_id"`
		Version              int64                  `bson:"version"`
		CreatedAt            time.Time              `bson:"created_at"`
		UpdatedAt            time.Time              `bson:"updated_at"`
	}
)

var _ store.VisibilityStore = (*VisibilityStore)(nil)

var maxTime, _ = time.Parse(time.RFC3339, "9999-12-31T23:59:59Z")

// NewMongoDBVisibilityStore creates an instance of VisibilityStore
func NewMongoDBVisibilityStore(
	cfg config.MongoDB,
	r resolver.ServiceResolver,
	searchAttributesProvider searchattribute.Provider,
	searchAttributesMapperProvider searchattribute.MapperProvider,
	logger log.Logger,
	metricsHandler metrics.Handler,
) (*VisibilityStore, error) {
	// Create MongoDB client
	clientOptions := options.Client().ApplyURI(fmt.Sprintf("mongodb://%s", cfg.ConnectAddr))
	if cfg.Username != "" && cfg.Password != "" {
		clientOptions.SetAuth(options.Credential{
			Username: cfg.Username,
			Password: cfg.Password,
		})
	}
	if cfg.MaxConns > 0 {
		clientOptions.SetMaxPoolSize(uint64(cfg.MaxConns))
	}
	if cfg.MaxIdleConns > 0 {
		clientOptions.SetMinPoolSize(uint64(cfg.MaxIdleConns))
	}
	if cfg.ConnectTimeout > 0 {
		clientOptions.SetServerSelectionTimeout(cfg.ConnectTimeout)
	}

	client, err := mongo.Connect(context.Background(), clientOptions)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to MongoDB: %w", err)
	}

	// Test the connection
	err = client.Ping(context.Background(), nil)
	if err != nil {
		return nil, fmt.Errorf("failed to ping MongoDB: %w", err)
	}

	database := client.Database(cfg.DatabaseName)
	collection := database.Collection("visibility")

	// Create indexes for optimal performance
	indexes := []mongo.IndexModel{
		{
			Keys: bson.D{
				{Key: "namespace_id", Value: 1},
				{Key: "workflow_id", Value: 1},
				{Key: "run_id", Value: 1},
			},
			Options: options.Index().SetUnique(true),
		},
		{
			Keys: bson.D{
				{Key: "namespace_id", Value: 1},
				{Key: "start_time", Value: -1},
			},
		},
		{
			Keys: bson.D{
				{Key: "namespace_id", Value: 1},
				{Key: "close_time", Value: -1},
			},
		},
		{
			Keys: bson.D{
				{Key: "namespace_id", Value: 1},
				{Key: "workflow_type_name", Value: 1},
			},
		},
		{
			Keys: bson.D{
				{Key: "namespace_id", Value: 1},
				{Key: "status", Value: 1},
			},
		},
	}

	_, err = collection.Indexes().CreateMany(context.Background(), indexes)
	if err != nil {
		return nil, fmt.Errorf("failed to create indexes: %w", err)
	}

	return &VisibilityStore{
		client:                         client,
		database:                       database,
		collection:                     collection,
		searchAttributesProvider:       searchAttributesProvider,
		searchAttributesMapperProvider: searchAttributesMapperProvider,
		logger:                         logger,
		metricsHandler:                 metricsHandler,
	}, nil
}

func (s *VisibilityStore) Close() {
	if s.client != nil {
		err := s.client.Disconnect(context.Background())
		if err != nil {
			s.logger.Error("failed to disconnect MongoDB client", tag.Error(err))
		}
	}
}

func (s *VisibilityStore) GetName() string {
	return "mongodb-visibility-store"
}

func (s *VisibilityStore) GetIndexName() string {
	return s.database.Name()
}

func (s *VisibilityStore) ValidateCustomSearchAttributes(
	searchAttributes map[string]any,
) (map[string]any, error) {
	return searchAttributes, nil
}

func (s *VisibilityStore) RecordWorkflowExecutionStarted(
	ctx context.Context,
	request *store.InternalRecordWorkflowExecutionStartedRequest,
) error {
	doc, err := s.generateVisibilityDocument(request.InternalVisibilityRequestBase)
	if err != nil {
		return err
	}

	_, err = s.collection.InsertOne(ctx, doc)
	if err != nil {
		if mongo.IsDuplicateKeyError(err) {
			// Document already exists, update it
			filter := bson.M{
				"namespace_id": request.NamespaceID,
				"workflow_id":  request.WorkflowID,
				"run_id":       request.RunID,
			}
			update := bson.M{
				"$set": doc,
			}
			_, err = s.collection.UpdateOne(ctx, filter, update)
		}
		return err
	}

	return nil
}

func (s *VisibilityStore) RecordWorkflowExecutionClosed(
	ctx context.Context,
	request *store.InternalRecordWorkflowExecutionClosedRequest,
) error {
	doc, err := s.generateVisibilityDocument(request.InternalVisibilityRequestBase)
	if err != nil {
		return err
	}

	doc.CloseTime = &request.CloseTime
	doc.HistoryLength = &request.HistoryLength
	doc.HistorySizeBytes = &request.HistorySizeBytes
	doc.ExecutionDuration = &request.ExecutionDuration
	doc.StateTransitionCount = &request.StateTransitionCount

	filter := bson.M{
		"namespace_id": request.NamespaceID,
		"workflow_id":  request.WorkflowID,
		"run_id":       request.RunID,
	}

	update := bson.M{
		"$set": doc,
	}

	opts := options.Update().SetUpsert(true)
	result, err := s.collection.UpdateOne(ctx, filter, update, opts)
	if err != nil {
		return err
	}

	if result.MatchedCount > 1 || result.UpsertedCount > 1 {
		return fmt.Errorf("RecordWorkflowExecutionClosed unexpected number of documents affected")
	}

	return nil
}

func (s *VisibilityStore) UpsertWorkflowExecution(
	ctx context.Context,
	request *store.InternalUpsertWorkflowExecutionRequest,
) error {
	doc, err := s.generateVisibilityDocument(request.InternalVisibilityRequestBase)
	if err != nil {
		return err
	}

	filter := bson.M{
		"namespace_id": request.NamespaceID,
		"workflow_id":  request.WorkflowID,
		"run_id":       request.RunID,
	}

	update := bson.M{
		"$set": doc,
	}

	opts := options.Update().SetUpsert(true)
	result, err := s.collection.UpdateOne(ctx, filter, update, opts)
	if err != nil {
		return err
	}

	if result.MatchedCount > 1 || result.UpsertedCount > 1 {
		return fmt.Errorf("UpsertWorkflowExecution unexpected number of documents affected")
	}

	return nil
}

func (s *VisibilityStore) DeleteWorkflowExecution(
	ctx context.Context,
	request *manager.VisibilityDeleteWorkflowExecutionRequest,
) error {
	filter := bson.M{
		"namespace_id": request.NamespaceID.String(),
		"run_id":       request.RunID,
	}

	_, err := s.collection.DeleteOne(ctx, filter)
	if err != nil {
		return serviceerror.NewUnavailable(err.Error())
	}

	return nil
}

func (s *VisibilityStore) ListWorkflowExecutions(
	ctx context.Context,
	request *manager.ListWorkflowExecutionsRequestV2,
) (*store.InternalListWorkflowExecutionsResponse, error) {
	saTypeMap, err := s.searchAttributesProvider.GetSearchAttributes(s.GetIndexName(), false)
	if err != nil {
		return nil, err
	}

	saMapper, err := s.searchAttributesMapperProvider.GetMapper(request.Namespace)
	if err != nil {
		return nil, err
	}

	// Build MongoDB filter from query
	filter, err := s.buildMongoFilter(request.Query, request.NamespaceID.String(), saTypeMap, saMapper)
	if err != nil {
		var converterErr *query.ConverterError
		if errors.As(err, &converterErr) {
			return nil, converterErr.ToInvalidArgument()
		}
		return nil, err
	}

	// Build sort options
	sort := bson.D{
		{Key: "close_time", Value: -1},
		{Key: "start_time", Value: -1},
		{Key: "run_id", Value: -1},
	}

	// Build find options
	findOptions := options.Find().
		SetSort(sort).
		SetLimit(int64(request.PageSize))

	// Handle pagination
	if len(request.NextPageToken) > 0 {
		// For simplicity, we'll use skip-based pagination
		// In a production implementation, you might want to use cursor-based pagination
		var skip int64
		// Parse the page token to get skip value
		// This is a simplified implementation
		findOptions.SetSkip(skip)
	}

	cursor, err := s.collection.Find(ctx, filter, findOptions)
	if err != nil {
		return nil, serviceerror.NewUnavailable(
			fmt.Sprintf("ListWorkflowExecutions operation failed. Query failed: %v", err))
	}
	defer cursor.Close(ctx)

	var documents []VisibilityDocument
	if err = cursor.All(ctx, &documents); err != nil {
		return nil, serviceerror.NewUnavailable(
			fmt.Sprintf("ListWorkflowExecutions operation failed. Cursor failed: %v", err))
	}

	if len(documents) == 0 {
		return &store.InternalListWorkflowExecutionsResponse{}, nil
	}

	// Convert documents to workflow execution info
	executions := make([]*store.InternalWorkflowExecutionInfo, 0, len(documents))
	for _, doc := range documents {
		info, err := s.documentToInfo(&doc, request.Namespace)
		if err != nil {
			return nil, err
		}
		executions = append(executions, info)
	}

	response := &store.InternalListWorkflowExecutionsResponse{
		Executions: executions,
	}

	// Generate next page token if there are more results
	if len(documents) == request.PageSize {
		// In a real implementation, you would generate a proper cursor-based token
		// For now, we'll use a simple offset-based token
		response.NextPageToken = []byte(fmt.Sprintf("%d", len(documents)))
	}

	return response, nil
}

func (s *VisibilityStore) ScanWorkflowExecutions(
	ctx context.Context,
	request *manager.ListWorkflowExecutionsRequestV2,
) (*store.InternalListWorkflowExecutionsResponse, error) {
	// For MongoDB, scan is similar to list but without ordering guarantees
	return s.ListWorkflowExecutions(ctx, request)
}

func (s *VisibilityStore) CountWorkflowExecutions(
	ctx context.Context,
	request *manager.CountWorkflowExecutionsRequest,
) (*manager.CountWorkflowExecutionsResponse, error) {
	saTypeMap, err := s.searchAttributesProvider.GetSearchAttributes(s.GetIndexName(), false)
	if err != nil {
		return nil, err
	}

	saMapper, err := s.searchAttributesMapperProvider.GetMapper(request.Namespace)
	if err != nil {
		return nil, err
	}

	// Build MongoDB filter from query
	filter, err := s.buildMongoFilter(request.Query, request.NamespaceID.String(), saTypeMap, saMapper)
	if err != nil {
		var converterErr *query.ConverterError
		if errors.As(err, &converterErr) {
			return nil, converterErr.ToInvalidArgument()
		}
		return nil, err
	}

	// If group by is specified, use aggregation
	/*if len(request.Query.GroupBy) > 0 {
		return s.countGroupByWorkflowExecutions(ctx, filter, request, saTypeMap)
	}*/

	// Simple count
	count, err := s.collection.CountDocuments(ctx, filter)
	if err != nil {
		return nil, serviceerror.NewUnavailable(
			fmt.Sprintf("CountWorkflowExecutions operation failed. Count failed: %v", err))
	}

	return &manager.CountWorkflowExecutionsResponse{
		Count: count,
	}, nil
}

func (s *VisibilityStore) countGroupByWorkflowExecutions(
	ctx context.Context,
	filter bson.M,
	request *manager.CountWorkflowExecutionsRequest,
	saTypeMap searchattribute.NameTypeMap,
) (*manager.CountWorkflowExecutionsResponse, error) {
	// Build group stage
	groupStage := bson.M{
		"_id": bson.M{},
		"count": bson.M{
			"$sum": 1,
		},
	}

	pipeline := []bson.M{
		{"$match": filter},
		{"$group": groupStage},
	}

	cursor, err := s.collection.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, serviceerror.NewUnavailable(
			fmt.Sprintf("CountWorkflowExecutions operation failed. Aggregation failed: %v", err))
	}
	defer cursor.Close(ctx)

	var results []bson.M
	if err = cursor.All(ctx, &results); err != nil {
		return nil, serviceerror.NewUnavailable(
			fmt.Sprintf("CountWorkflowExecutions operation failed. Cursor failed: %v", err))
	}

	resp := &manager.CountWorkflowExecutionsResponse{
		Count:  0,
		Groups: make([]*workflowservice.CountWorkflowExecutionsResponse_AggregationGroup, 0, len(results)),
	}

	for _, result := range results {
		count := result["count"].(int64)
		resp.Count += count

		// Convert group values to payloads
		groupValues := make([]*commonpb.Payload, 0)
		if groupID, ok := result["_id"].(bson.M); ok {
			for _, value := range groupID {
				payload, err := searchattribute.EncodeValue(value, enumspb.INDEXED_VALUE_TYPE_KEYWORD)
				if err != nil {
					return nil, err
				}
				groupValues = append(groupValues, payload)
			}
		}

		resp.Groups = append(resp.Groups, &workflowservice.CountWorkflowExecutionsResponse_AggregationGroup{
			GroupValues: groupValues,
			Count:       count,
		})
	}

	return resp, nil
}

func (s *VisibilityStore) GetWorkflowExecution(
	ctx context.Context,
	request *manager.GetWorkflowExecutionRequest,
) (*store.InternalGetWorkflowExecutionResponse, error) {
	filter := bson.M{
		"namespace_id": request.NamespaceID.String(),
		"run_id":       request.RunID,
	}

	var doc VisibilityDocument
	err := s.collection.FindOne(ctx, filter).Decode(&doc)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, serviceerror.NewNotFound("workflow execution not found")
		}
		return nil, serviceerror.NewUnavailable(
			fmt.Sprintf("GetWorkflowExecution operation failed. Query failed: %v", err))
	}

	info, err := s.documentToInfo(&doc, request.Namespace)
	if err != nil {
		return nil, err
	}

	return &store.InternalGetWorkflowExecutionResponse{
		Execution: info,
	}, nil
}

func (s *VisibilityStore) generateVisibilityDocument(
	request *store.InternalVisibilityRequestBase,
) (*VisibilityDocument, error) {
	searchAttributes, err := s.prepareSearchAttributesForDB(request)
	if err != nil {
		return nil, err
	}

	now := time.Now()
	doc := &VisibilityDocument{
		ID:               fmt.Sprintf("%s:%s:%s", request.NamespaceID, request.WorkflowID, request.RunID),
		NamespaceID:      request.NamespaceID,
		WorkflowID:       request.WorkflowID,
		RunID:            request.RunID,
		WorkflowTypeName: request.WorkflowTypeName,
		StartTime:        request.StartTime,
		ExecutionTime:    request.ExecutionTime,
		Status:           int32(request.Status),
		TaskQueue:        request.TaskQueue,
		SearchAttributes: searchAttributes,
		RootWorkflowID:   request.RootWorkflowID,
		RootRunID:        request.RootRunID,
		Version:          request.TaskID,
		CreatedAt:        now,
		UpdatedAt:        now,
	}

	if request.Memo != nil {
		doc.Memo = request.Memo.Data
		doc.MemoEncoding = request.Memo.EncodingType.String()
	}

	if request.ParentWorkflowID != nil {
		doc.ParentWorkflowID = request.ParentWorkflowID
	}
	if request.ParentRunID != nil {
		doc.ParentRunID = request.ParentRunID
	}

	return doc, nil
}

func (s *VisibilityStore) prepareSearchAttributesForDB(
	request *store.InternalVisibilityRequestBase,
) (map[string]interface{}, error) {
	if request.SearchAttributes == nil || len(request.SearchAttributes.IndexedFields) == 0 {
		return nil, nil
	}

	saTypeMap, err := s.searchAttributesProvider.GetSearchAttributes(s.GetIndexName(), false)
	if err != nil {
		return nil, err
	}

	searchAttributes := make(map[string]interface{})
	for key, payload := range request.SearchAttributes.IndexedFields {
		valueType, err := saTypeMap.GetType(key)
		if err != nil {
			return nil, err
		}
		value, err := searchattribute.DecodeValue(payload, valueType, false)
		if err != nil {
			return nil, err
		}
		searchAttributes[key] = value
	}

	return searchAttributes, nil
}

func (s *VisibilityStore) documentToInfo(
	doc *VisibilityDocument,
	nsName namespace.Name,
) (*store.InternalWorkflowExecutionInfo, error) {
	if doc.ExecutionTime.UnixNano() == 0 {
		doc.ExecutionTime = doc.StartTime
	}

	info := &store.InternalWorkflowExecutionInfo{
		WorkflowID:     doc.WorkflowID,
		RunID:          doc.RunID,
		TypeName:       doc.WorkflowTypeName,
		StartTime:      doc.StartTime,
		ExecutionTime:  doc.ExecutionTime,
		Status:         enumspb.WorkflowExecutionStatus(doc.Status),
		TaskQueue:      doc.TaskQueue,
		RootWorkflowID: doc.RootWorkflowID,
		RootRunID:      doc.RootRunID,
		Memo:           persistence.NewDataBlob(doc.Memo, doc.MemoEncoding),
	}

	if doc.SearchAttributes != nil && len(doc.SearchAttributes) > 0 {
		searchAttributes, err := s.processDocumentSearchAttributes(doc.SearchAttributes, nsName)
		if err != nil {
			return nil, err
		}
		info.SearchAttributes = searchAttributes
	}

	if doc.CloseTime != nil {
		info.CloseTime = *doc.CloseTime
	}
	if doc.ExecutionDuration != nil {
		info.ExecutionDuration = *doc.ExecutionDuration
	}
	if doc.HistoryLength != nil {
		info.HistoryLength = *doc.HistoryLength
	}
	if doc.HistorySizeBytes != nil {
		info.HistorySizeBytes = *doc.HistorySizeBytes
	}
	if doc.StateTransitionCount != nil {
		info.StateTransitionCount = *doc.StateTransitionCount
	}
	if doc.ParentWorkflowID != nil {
		info.ParentWorkflowID = *doc.ParentWorkflowID
	}
	if doc.ParentRunID != nil {
		info.ParentRunID = *doc.ParentRunID
	}

	return info, nil
}

func (s *VisibilityStore) processDocumentSearchAttributes(
	docSearchAttributes map[string]interface{},
	nsName namespace.Name,
) (*commonpb.SearchAttributes, error) {
	saTypeMap, err := s.searchAttributesProvider.GetSearchAttributes(
		s.GetIndexName(),
		false,
	)
	if err != nil {
		return nil, serviceerror.NewUnavailable(
			fmt.Sprintf("Unable to read search attributes types: %v", err))
	}

	searchAttributes, err := searchattribute.Encode(docSearchAttributes, &saTypeMap)
	if err != nil {
		return nil, err
	}

	aliasedSas, err := searchattribute.AliasFields(
		s.searchAttributesMapperProvider,
		searchAttributes,
		nsName.String(),
	)
	if err != nil {
		return nil, err
	}

	return aliasedSas, nil
}

func (s *VisibilityStore) buildMongoFilter(
	query string,
	namespaceID string,
	saTypeMap searchattribute.NameTypeMap,
	saMapper searchattribute.Mapper,
) (bson.M, error) {
	// This is a simplified implementation
	// In a real implementation, you would parse the query string and convert it to MongoDB filter
	// For now, we'll return a basic filter that matches the namespace
	filter := bson.M{
		"namespace_id": namespaceID,
	}

	// TODO: Implement proper query parsing and conversion to MongoDB filter
	// This would involve parsing the Temporal query language and converting it to MongoDB query syntax

	return filter, nil
}

func (s *VisibilityStore) AddSearchAttributes(
	ctx context.Context,
	request *manager.AddSearchAttributesRequest,
) error {
	// MongoDB Visibility does not support modifying schema to add search attributes at this moment.
	return serviceerror.NewUnimplemented("AddSearchAttributes operation not supported in MongoDB visibility")
}
