package mongodb

import (
	"context"
	"time"

	"go.temporal.io/server/common/metrics"
)

// Metrics represents MongoDB operation metrics
type Metrics struct {
	handler metrics.Handler
}

// NewMetrics creates a new metrics instance
func NewMetrics(handler metrics.Handler) *Metrics {
	return &Metrics{
		handler: handler,
	}
}

// OperationMetrics tracks operation-specific metrics
type OperationMetrics struct {
	metrics *Metrics
	start   time.Time
	op      string
	tags    []metrics.Tag
}

// NewOperationMetrics creates new operation metrics
func (m *Metrics) NewOperationMetrics(operation string, tags []metrics.Tag) *OperationMetrics {
	return &OperationMetrics{
		metrics: m,
		start:   time.Now(),
		op:      operation,
		tags:    tags,
	}
}

// Complete marks the operation as complete and records metrics
func (om *OperationMetrics) Complete(err error) {
	duration := time.Since(om.start)

	// Record operation duration
	om.metrics.handler.Timer("mongodb_operation_latency").Record(duration, om.tags...)

	// Record operation count
	om.metrics.handler.Counter("mongodb_operation_count").Record(1, om.tags...)

	// Record error count if applicable
	if err != nil {
		om.metrics.handler.Counter("mongodb_operation_error_count").Record(1, om.tags...)
	}

	// Record operation-specific metrics
	om.recordOperationSpecificMetrics(duration, err)
}

// recordOperationSpecificMetrics records operation-specific metrics
func (om *OperationMetrics) recordOperationSpecificMetrics(duration time.Duration, err error) {
	// Define metric mappings to reduce cyclomatic complexity
	metricMappings := map[string]struct {
		latencyMetric string
		errorMetric   string
	}{
		"create_workflow_execution": {
			latencyMetric: "mongodb_create_workflow_execution_latency",
			errorMetric:   "mongodb_create_workflow_execution_error_count",
		},
		"update_workflow_execution": {
			latencyMetric: "mongodb_update_workflow_execution_latency",
			errorMetric:   "mongodb_update_workflow_execution_error_count",
		},
		"get_workflow_execution": {
			latencyMetric: "mongodb_get_workflow_execution_latency",
			errorMetric:   "mongodb_get_workflow_execution_error_count",
		},
		"delete_workflow_execution": {
			latencyMetric: "mongodb_delete_workflow_execution_latency",
			errorMetric:   "mongodb_delete_workflow_execution_error_count",
		},
		"create_task_queue": {
			latencyMetric: "mongodb_create_task_queue_latency",
			errorMetric:   "mongodb_create_task_queue_error_count",
		},
		"update_task_queue": {
			latencyMetric: "mongodb_update_task_queue_latency",
			errorMetric:   "mongodb_update_task_queue_error_count",
		},
		"get_task_queue": {
			latencyMetric: "mongodb_get_task_queue_latency",
			errorMetric:   "mongodb_get_task_queue_error_count",
		},
		"create_tasks": {
			latencyMetric: "mongodb_create_tasks_latency",
			errorMetric:   "mongodb_create_tasks_error_count",
		},
		"get_tasks": {
			latencyMetric: "mongodb_get_tasks_latency",
			errorMetric:   "mongodb_get_tasks_error_count",
		},
		"complete_tasks": {
			latencyMetric: "mongodb_complete_tasks_latency",
			errorMetric:   "mongodb_complete_tasks_error_count",
		},
		"append_history_nodes": {
			latencyMetric: "mongodb_append_history_nodes_latency",
			errorMetric:   "mongodb_append_history_nodes_error_count",
		},
		"read_history_branch": {
			latencyMetric: "mongodb_read_history_branch_latency",
			errorMetric:   "mongodb_read_history_branch_error_count",
		},
		"bulk_write": {
			latencyMetric: "mongodb_bulk_write_latency",
			errorMetric:   "mongodb_bulk_write_error_count",
		},
		"transaction": {
			latencyMetric: "mongodb_transaction_latency",
			errorMetric:   "mongodb_transaction_error_count",
		},
	}

	// Look up metrics for the operation
	if mapping, exists := metricMappings[om.op]; exists {
		om.metrics.handler.Timer(mapping.latencyMetric).Record(duration, om.tags...)
		if err != nil {
			om.metrics.handler.Counter(mapping.errorMetric).Record(1, om.tags...)
		}
	}
}

// RecordConnectionMetrics records connection-related metrics
func (m *Metrics) RecordConnectionMetrics(operation string, err error) {
	m.handler.Counter("mongodb_connection_count").Record(1)

	if err != nil {
		m.handler.Counter("mongodb_connection_error_count").Record(1)
	}

	switch operation {
	case "connect":
		m.handler.Counter("mongodb_connect_count").Record(1)
		if err != nil {
			m.handler.Counter("mongodb_connect_error_count").Record(1)
		}

	case "disconnect":
		m.handler.Counter("mongodb_disconnect_count").Record(1)
		if err != nil {
			m.handler.Counter("mongodb_disconnect_error_count").Record(1)
		}

	case "ping":
		m.handler.Counter("mongodb_ping_count").Record(1)
		if err != nil {
			m.handler.Counter("mongodb_ping_error_count").Record(1)
		}
	}
}

// RecordBulkOperationMetrics records bulk operation metrics
func (m *Metrics) RecordBulkOperationMetrics(operation string, count int, duration time.Duration, err error) {
	m.handler.Timer("mongodb_bulk_operation_latency").Record(duration)
	m.handler.Counter("mongodb_bulk_operation_count").Record(1)
	m.handler.Counter("mongodb_bulk_operation_document_count").Record(int64(count))

	if err != nil {
		m.handler.Counter("mongodb_bulk_operation_error_count").Record(1)
	}

	switch operation {
	case "bulk_insert":
		m.handler.Timer("mongodb_bulk_insert_latency").Record(duration)
		m.handler.Counter("mongodb_bulk_insert_count").Record(1)
		m.handler.Counter("mongodb_bulk_insert_document_count").Record(int64(count))
		if err != nil {
			m.handler.Counter("mongodb_bulk_insert_error_count").Record(1)
		}

	case "bulk_update":
		m.handler.Timer("mongodb_bulk_update_latency").Record(duration)
		m.handler.Counter("mongodb_bulk_update_count").Record(1)
		m.handler.Counter("mongodb_bulk_update_document_count").Record(int64(count))
		if err != nil {
			m.handler.Counter("mongodb_bulk_update_error_count").Record(1)
		}

	case "bulk_delete":
		m.handler.Timer("mongodb_bulk_delete_latency").Record(duration)
		m.handler.Counter("mongodb_bulk_delete_count").Record(1)
		m.handler.Counter("mongodb_bulk_delete_document_count").Record(int64(count))
		if err != nil {
			m.handler.Counter("mongodb_bulk_delete_error_count").Record(1)
		}
	}
}

// RecordQueryMetrics records query-related metrics
func (m *Metrics) RecordQueryMetrics(collection string, duration time.Duration, resultCount int, err error) {
	m.handler.Timer("mongodb_query_latency").Record(duration)
	m.handler.Counter("mongodb_query_count").Record(1)
	m.handler.Counter("mongodb_query_result_count").Record(int64(resultCount))

	if err != nil {
		m.handler.Counter("mongodb_query_error_count").Record(1)
	}

	// Record collection-specific metrics
	collectionHandler := m.handler.WithTags(metrics.StringTag("collection", collection))
	collectionHandler.Timer("mongodb_collection_query_latency").Record(duration)
	collectionHandler.Counter("mongodb_collection_query_count").Record(1)
}

// RecordIndexMetrics records index-related metrics
func (m *Metrics) RecordIndexMetrics(operation string, collection string, duration time.Duration, err error) {
	m.handler.Timer("mongodb_index_operation_latency").Record(duration)
	m.handler.Counter("mongodb_index_operation_count").Record(1)

	if err != nil {
		m.handler.Counter("mongodb_index_operation_error_count").Record(1)
	}

	switch operation {
	case "create_index":
		m.handler.Timer("mongodb_create_index_latency").Record(duration)
		m.handler.Counter("mongodb_create_index_count").Record(1)
		if err != nil {
			m.handler.Counter("mongodb_create_index_error_count").Record(1)
		}

	case "drop_index":
		m.handler.Timer("mongodb_drop_index_latency").Record(duration)
		m.handler.Counter("mongodb_drop_index_count").Record(1)
		if err != nil {
			m.handler.Counter("mongodb_drop_index_error_count").Record(1)
		}

	case "list_indexes":
		m.handler.Timer("mongodb_list_indexes_latency").Record(duration)
		m.handler.Counter("mongodb_list_indexes_count").Record(1)
		if err != nil {
			m.handler.Counter("mongodb_list_indexes_error_count").Record(1)
		}
	}
}

// TransactionMetrics tracks transaction-specific metrics
type TransactionMetrics struct {
	metrics *Metrics
	start   time.Time
}

// NewTransactionMetrics creates new transaction metrics
func (m *Metrics) NewTransactionMetrics() *TransactionMetrics {
	return &TransactionMetrics{
		metrics: m,
		start:   time.Now(),
	}
}

// Complete marks the transaction as complete and records metrics
func (tm *TransactionMetrics) Complete(operation string, err error) {
	duration := time.Since(tm.start)
	tm.RecordTransactionMetrics(operation, duration, err)
}

// RecordTransactionMetrics records transaction-related metrics
func (tm *TransactionMetrics) RecordTransactionMetrics(operation string, duration time.Duration, err error) {
	tm.metrics.handler.Timer("mongodb_transaction_latency").Record(duration)
	tm.metrics.handler.Counter("mongodb_transaction_count").Record(1)

	if err != nil {
		tm.metrics.handler.Counter("mongodb_transaction_error_count").Record(1)
	}

	switch operation {
	case "begin":
		tm.metrics.handler.Counter("mongodb_transaction_begin_count").Record(1)
		if err != nil {
			tm.metrics.handler.Counter("mongodb_transaction_begin_error_count").Record(1)
		}

	case "commit":
		tm.metrics.handler.Counter("mongodb_transaction_commit_count").Record(1)
		if err != nil {
			tm.metrics.handler.Counter("mongodb_transaction_commit_error_count").Record(1)
		}

	case "abort":
		tm.metrics.handler.Counter("mongodb_transaction_abort_count").Record(1)
		if err != nil {
			tm.metrics.handler.Counter("mongodb_transaction_abort_error_count").Record(1)
		}
	}
}

// MetricsWithContext creates metrics with context
func (m *Metrics) MetricsWithContext(ctx context.Context) *Metrics {
	return &Metrics{
		handler: m.handler,
	}
}

// MetricsWithTags creates metrics with additional tags
func (m *Metrics) MetricsWithTags(tags []metrics.Tag) *Metrics {
	return &Metrics{
		handler: m.handler.WithTags(tags...),
	}
}
