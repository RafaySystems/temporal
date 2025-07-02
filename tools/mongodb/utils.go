package mongodb

import (
	"fmt"
	"net/url"
	"strings"
	"time"
)

// ConnectionConfig holds MongoDB connection configuration
type ConnectionConfig struct {
	URI      string
	Database string
	Timeout  time.Duration
}

// ParseConnectionString parses and validates a MongoDB connection string
func ParseConnectionString(uri string) (*ConnectionConfig, error) {
	if uri == "" {
		return nil, fmt.Errorf("connection URI cannot be empty")
	}

	// Basic validation of MongoDB URI format
	if !strings.HasPrefix(uri, "mongodb://") && !strings.HasPrefix(uri, "mongodb+srv://") {
		return nil, fmt.Errorf("invalid MongoDB URI format: must start with mongodb:// or mongodb+srv://")
	}

	// Parse the URI
	parsedURL, err := url.Parse(uri)
	if err != nil {
		return nil, fmt.Errorf("failed to parse MongoDB URI: %w", err)
	}

	// Extract database name from path
	database := strings.TrimPrefix(parsedURL.Path, "/")
	if database == "" {
		database = "temporal" // Default database name
	}

	config := &ConnectionConfig{
		URI:      uri,
		Database: database,
		Timeout:  30 * time.Second, // Default timeout
	}

	return config, nil
}

// ValidateDatabaseName validates a MongoDB database name
func ValidateDatabaseName(name string) error {
	if name == "" {
		return fmt.Errorf("database name cannot be empty")
	}

	// MongoDB database name restrictions
	if len(name) > 64 {
		return fmt.Errorf("database name cannot exceed 64 characters")
	}

	// Check for invalid characters
	invalidChars := []string{"/", "\\", ".", " ", "\"", "$", "*", "<", ">", ":", "|", "?"}
	for _, char := range invalidChars {
		if strings.Contains(name, char) {
			return fmt.Errorf("database name cannot contain character: %s", char)
		}
	}

	// Check for reserved names
	reservedNames := []string{"admin", "local", "config"}
	for _, reserved := range reservedNames {
		if strings.EqualFold(name, reserved) {
			return fmt.Errorf("database name cannot be reserved name: %s", reserved)
		}
	}

	return nil
}

// ValidateCollectionName validates a MongoDB collection name
func ValidateCollectionName(name string) error {
	if name == "" {
		return fmt.Errorf("collection name cannot be empty")
	}

	// MongoDB collection name restrictions
	if len(name) > 255 {
		return fmt.Errorf("collection name cannot exceed 255 characters")
	}

	// Check for invalid characters
	invalidChars := []string{"/", "\\", ".", " ", "\"", "$", "*", "<", ">", ":", "|", "?"}
	for _, char := range invalidChars {
		if strings.Contains(name, char) {
			return fmt.Errorf("collection name cannot contain character: %s", char)
		}
	}

	// Check for reserved prefixes
	reservedPrefixes := []string{"system."}
	for _, prefix := range reservedPrefixes {
		if strings.HasPrefix(name, prefix) {
			return fmt.Errorf("collection name cannot start with reserved prefix: %s", prefix)
		}
	}

	return nil
}

// FormatBytes formats bytes into human-readable format
func FormatBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

// FormatDuration formats duration into human-readable format
func FormatDuration(d time.Duration) string {
	if d < time.Minute {
		return fmt.Sprintf("%.0fs", d.Seconds())
	}
	if d < time.Hour {
		return fmt.Sprintf("%.0fm", d.Minutes())
	}
	if d < 24*time.Hour {
		return fmt.Sprintf("%.0fh", d.Hours())
	}
	return fmt.Sprintf("%.0fd", d.Hours()/24)
}

// ParseTimeDuration parses time duration with support for days
func ParseTimeDuration(s string) (time.Duration, error) {
	// Handle days (not supported by time.ParseDuration)
	if strings.HasSuffix(s, "d") {
		days := strings.TrimSuffix(s, "d")
		if days == "" {
			return 0, fmt.Errorf("invalid duration format: empty days value")
		}
		// Parse the number of days
		var dayCount int
		_, err := fmt.Sscanf(days, "%d", &dayCount)
		if err != nil {
			return 0, fmt.Errorf("invalid duration format: cannot parse days value")
		}
		// Convert days to hours (24 hours per day)
		hours := dayCount * 24
		return time.Duration(hours) * time.Hour, nil
	}

	// Use standard time.ParseDuration for other formats
	return time.ParseDuration(s)
}

// GetTemporalCollections returns the list of expected Temporal collections
func GetTemporalCollections() []string {
	return []string{
		"executions",
		"history_node",
		"history_tree",
		"tasks",
		"task_queue_user_data",
		"namespaces_by_id",
		"namespaces",
		"queue_metadata",
		"queue",
		"cluster_metadata_info",
		"cluster_membership",
		"queues",
		"queue_messages",
		"nexus_endpoints",
		"schema_version",
	}
}

// GetTemporalIndexes returns the expected indexes for Temporal collections
func GetTemporalIndexes() map[string][]string {
	return map[string][]string{
		"executions": {
			"_id_",
			"executions_primary_key",
			"executions_workflow_lookup",
			"executions_visibility",
		},
		"history_node": {
			"_id_",
			"history_node_primary_key",
		},
		"history_tree": {
			"_id_",
			"history_tree_primary_key",
		},
		"tasks": {
			"_id_",
			"tasks_primary_key",
		},
		"namespaces_by_id": {
			"_id_",
			"namespaces_by_id_primary_key",
		},
		"namespaces": {
			"_id_",
			"namespaces_primary_key",
		},
	}
}

// IsValidAction checks if the provided action is valid
func IsValidAction(action string) bool {
	validActions := []string{
		"create",
		"validate",
		"drop",
		"setup-schema",
		"update-schema",
		"health-check",
		"info",
		"list-collections",
		"collection-stats",
		"list-indexes",
		"backup",
		"restore",
		"cleanup",
	}

	for _, valid := range validActions {
		if action == valid {
			return true
		}
	}
	return false
}

// GetActionDescription returns a description for each action
func GetActionDescription(action string) string {
	descriptions := map[string]string{
		"create":           "Create MongoDB schema",
		"validate":         "Validate existing schema",
		"drop":             "Drop entire database (⚠️ Destructive)",
		"setup-schema":     "Initial schema setup with version tracking",
		"update-schema":    "Update schema to a new version",
		"health-check":     "Perform database health check",
		"info":             "Get comprehensive database information",
		"list-collections": "List all collections",
		"collection-stats": "Get collection statistics",
		"list-indexes":     "List indexes for a collection",
		"backup":           "Backup a collection",
		"restore":          "Restore a collection from backup",
		"cleanup":          "Clean up old data",
	}

	if desc, exists := descriptions[action]; exists {
		return desc
	}
	return "Unknown action"
}
