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
	// Queue implements the Queue interface for MongoDB
	Queue struct {
		queueType  p.QueueType
		client     *mongo.Client
		database   *mongo.Database
		collection *mongo.Collection
		logger     log.Logger
	}

	// QueueMessageDocument represents a queue message document in MongoDB
	QueueMessageDocument struct {
		QueueType    int32     `bson:"queue_type"`
		MessageID    int64     `bson:"message_id"`
		Data         []byte    `bson:"data,omitempty"`
		DataEncoding string    `bson:"data_encoding,omitempty"`
		CreatedAt    time.Time `bson:"created_at"`
	}

	// QueueMetadataDocument represents queue metadata in MongoDB
	QueueMetadataDocument struct {
		QueueType    int32     `bson:"queue_type"`
		Blob         []byte    `bson:"blob,omitempty"`
		BlobEncoding string    `bson:"blob_encoding,omitempty"`
		Version      int64     `bson:"version"`
		CreatedAt    time.Time `bson:"created_at"`
		UpdatedAt    time.Time `bson:"updated_at"`
	}
)

// NewQueue creates a new MongoDB queue
func NewQueue(queueType p.QueueType, database *mongo.Database, logger log.Logger) *Queue {
	return &Queue{
		queueType:  queueType,
		client:     database.Client(),
		database:   database,
		collection: database.Collection("queue_messages"),
		logger:     logger,
	}
}

// Close closes the queue
func (q *Queue) Close() {
	// MongoDB client is managed by the factory
}

// Init initializes the queue
func (q *Queue) Init(ctx context.Context, blob *commonpb.DataBlob) error {
	// This is a simplified implementation
	// In a real implementation, you'd handle queue initialization
	return nil
}

// EnqueueMessage enqueues a message
func (q *Queue) EnqueueMessage(ctx context.Context, blob *commonpb.DataBlob) error {
	now := time.Now()

	// Get the next message ID
	filter := bson.M{"queue_type": int32(q.queueType)}
	opts := options.FindOne().SetSort(bson.D{{"message_id", -1}})

	var lastMessage QueueMessageDocument
	err := q.collection.FindOne(ctx, filter, opts).Decode(&lastMessage)

	var messageID int64 = 1
	if err == nil {
		messageID = lastMessage.MessageID + 1
	} else if err != mongo.ErrNoDocuments {
		return fmt.Errorf("failed to get last message ID: %w", err)
	}

	doc := &QueueMessageDocument{
		QueueType:    int32(q.queueType),
		MessageID:    messageID,
		Data:         blob.Data,
		DataEncoding: blob.EncodingType.String(),
		CreatedAt:    now,
	}

	_, err = q.collection.InsertOne(ctx, doc)
	if err != nil {
		return fmt.Errorf("failed to enqueue message: %w", err)
	}

	return nil
}

// ReadMessages reads messages
func (q *Queue) ReadMessages(ctx context.Context, lastMessageID int64, maxCount int) ([]*p.QueueMessage, error) {
	filter := bson.M{
		"queue_type": int32(q.queueType),
		"message_id": bson.M{"$gt": lastMessageID},
	}

	opts := options.Find().
		SetSort(bson.D{{"message_id", 1}}).
		SetLimit(int64(maxCount))

	cursor, err := q.collection.Find(ctx, filter, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to read messages: %w", err)
	}
	defer cursor.Close(ctx)

	var messages []*p.QueueMessage
	for cursor.Next(ctx) {
		var doc QueueMessageDocument
		if err := cursor.Decode(&doc); err != nil {
			q.logger.Error("failed to decode queue message document", tag.Error(err))
			continue
		}

		encodingType, err := enumspb.EncodingTypeFromString(doc.DataEncoding)
		if err != nil {
			encodingType = enumspb.ENCODING_TYPE_UNSPECIFIED
		}

		messages = append(messages, &p.QueueMessage{
			QueueType: q.queueType,
			ID:        doc.MessageID,
			Data:      doc.Data,
			Encoding:  encodingType.String(),
		})
	}

	return messages, nil
}

// DeleteMessagesBefore deletes messages before the given message ID
func (q *Queue) DeleteMessagesBefore(ctx context.Context, messageID int64) error {
	filter := bson.M{
		"queue_type": int32(q.queueType),
		"message_id": bson.M{"$lt": messageID},
	}

	_, err := q.collection.DeleteMany(ctx, filter)
	if err != nil {
		return fmt.Errorf("failed to delete messages: %w", err)
	}

	return nil
}

// UpdateAckLevel updates the acknowledgment level
func (q *Queue) UpdateAckLevel(ctx context.Context, metadata *p.InternalQueueMetadata) error {
	// This is a simplified implementation
	// In a real implementation, you'd handle updating acknowledgment levels
	return nil
}

// GetAckLevels gets the acknowledgment levels
func (q *Queue) GetAckLevels(ctx context.Context) (*p.InternalQueueMetadata, error) {
	// This is a simplified implementation
	// In a real implementation, you'd handle getting acknowledgment levels
	return &p.InternalQueueMetadata{
		Blob:    &commonpb.DataBlob{},
		Version: 1,
	}, nil
}

// EnqueueMessageToDLQ enqueues a message to DLQ
func (q *Queue) EnqueueMessageToDLQ(ctx context.Context, blob *commonpb.DataBlob) (int64, error) {
	// This is a simplified implementation
	// In a real implementation, you'd handle enqueueing to DLQ
	return 1, nil
}

// ReadMessagesFromDLQ reads messages from DLQ
func (q *Queue) ReadMessagesFromDLQ(ctx context.Context, firstMessageID int64, lastMessageID int64, pageSize int, pageToken []byte) ([]*p.QueueMessage, []byte, error) {
	// This is a simplified implementation
	// In a real implementation, you'd handle reading from DLQ
	return []*p.QueueMessage{}, nil, nil
}

// DeleteMessageFromDLQ deletes a message from DLQ
func (q *Queue) DeleteMessageFromDLQ(ctx context.Context, messageID int64) error {
	// This is a simplified implementation
	// In a real implementation, you'd handle deleting from DLQ
	return nil
}

// RangeDeleteMessagesFromDLQ deletes a range of messages from DLQ
func (q *Queue) RangeDeleteMessagesFromDLQ(ctx context.Context, firstMessageID int64, lastMessageID int64) error {
	// This is a simplified implementation
	// In a real implementation, you'd handle range deleting from DLQ
	return nil
}

// UpdateDLQAckLevel updates the DLQ acknowledgment level
func (q *Queue) UpdateDLQAckLevel(ctx context.Context, metadata *p.InternalQueueMetadata) error {
	// This is a simplified implementation
	// In a real implementation, you'd handle updating DLQ acknowledgment levels
	return nil
}

// GetDLQAckLevels gets the DLQ acknowledgment levels
func (q *Queue) GetDLQAckLevels(ctx context.Context) (*p.InternalQueueMetadata, error) {
	// This is a simplified implementation
	// In a real implementation, you'd handle getting DLQ acknowledgment levels
	return &p.InternalQueueMetadata{
		Blob:    &commonpb.DataBlob{},
		Version: 1,
	}, nil
}

// QueueV2 implements the QueueV2 interface for MongoDB
type QueueV2 struct {
	client     *mongo.Client
	database   *mongo.Database
	collection *mongo.Collection
	logger     log.Logger
}

// NewQueueV2 creates a new MongoDB QueueV2
func NewQueueV2(database *mongo.Database, logger log.Logger) *QueueV2 {
	return &QueueV2{
		client:     database.Client(),
		database:   database,
		collection: database.Collection("queue_v2_messages"),
		logger:     logger,
	}
}

// Close closes the queue
func (q *QueueV2) Close() {
	// MongoDB client is managed by the factory
}

// EnqueueMessage enqueues a message
func (q *QueueV2) EnqueueMessage(ctx context.Context, request *p.InternalEnqueueMessageRequest) (*p.InternalEnqueueMessageResponse, error) {
	// This is a simplified implementation
	// In a real implementation, you'd handle enqueueing messages
	return &p.InternalEnqueueMessageResponse{
		Metadata: p.MessageMetadata{ID: 1},
	}, nil
}

// ReadMessages reads messages
func (q *QueueV2) ReadMessages(ctx context.Context, request *p.InternalReadMessagesRequest) (*p.InternalReadMessagesResponse, error) {
	// This is a simplified implementation
	// In a real implementation, you'd handle reading messages
	return &p.InternalReadMessagesResponse{
		Messages: []p.QueueV2Message{},
	}, nil
}

// CreateQueue creates a queue
func (q *QueueV2) CreateQueue(ctx context.Context, request *p.InternalCreateQueueRequest) (*p.InternalCreateQueueResponse, error) {
	// This is a simplified implementation
	// In a real implementation, you'd handle creating queues
	return &p.InternalCreateQueueResponse{}, nil
}

// RangeDeleteMessages deletes a range of messages
func (q *QueueV2) RangeDeleteMessages(ctx context.Context, request *p.InternalRangeDeleteMessagesRequest) (*p.InternalRangeDeleteMessagesResponse, error) {
	// This is a simplified implementation
	// In a real implementation, you'd handle range deleting messages
	return &p.InternalRangeDeleteMessagesResponse{
		MessagesDeleted: 0,
	}, nil
}

// ListQueues lists queues
func (q *QueueV2) ListQueues(ctx context.Context, request *p.InternalListQueuesRequest) (*p.InternalListQueuesResponse, error) {
	// This is a simplified implementation
	// In a real implementation, you'd handle listing queues
	return &p.InternalListQueuesResponse{
		Queues: []p.QueueInfo{},
	}, nil
}
