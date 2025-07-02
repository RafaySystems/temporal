package mongodb

import (
	"testing"
	"time"
)

func TestParseConnectionString(t *testing.T) {
	testCases := []struct {
		name        string
		uri         string
		expectError bool
		expectedDB  string
	}{
		{
			name:        "Valid MongoDB URI",
			uri:         "mongodb://localhost:27017/temporal",
			expectError: false,
			expectedDB:  "temporal",
		},
		{
			name:        "Valid MongoDB URI with default database",
			uri:         "mongodb://localhost:27017",
			expectError: false,
			expectedDB:  "temporal",
		},
		{
			name:        "Valid MongoDB SRV URI",
			uri:         "mongodb+srv://user:pass@cluster.mongodb.net/temporal",
			expectError: false,
			expectedDB:  "temporal",
		},
		{
			name:        "Invalid URI format",
			uri:         "http://localhost:27017",
			expectError: true,
		},
		{
			name:        "Empty URI",
			uri:         "",
			expectError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			config, err := ParseConnectionString(tc.uri)
			if tc.expectError {
				if err == nil {
					t.Errorf("Expected error for URI: %s", tc.uri)
				}
				return
			}
			if err != nil {
				t.Errorf("Unexpected error for URI %s: %v", tc.uri, err)
				return
			}
			if config.Database != tc.expectedDB {
				t.Errorf("Expected database %s, got %s", tc.expectedDB, config.Database)
			}
		})
	}
}

func TestValidateDatabaseName(t *testing.T) {
	testCases := []struct {
		name        string
		dbName      string
		expectError bool
	}{
		{
			name:        "Valid database name",
			dbName:      "temporal",
			expectError: false,
		},
		{
			name:        "Empty database name",
			dbName:      "",
			expectError: true,
		},
		{
			name:        "Database name too long",
			dbName:      "a" + string(make([]byte, 65)),
			expectError: true,
		},
		{
			name:        "Database name with invalid character",
			dbName:      "temporal/test",
			expectError: true,
		},
		{
			name:        "Reserved database name",
			dbName:      "admin",
			expectError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateDatabaseName(tc.dbName)
			if tc.expectError && err == nil {
				t.Errorf("Expected error for database name: %s", tc.dbName)
			}
			if !tc.expectError && err != nil {
				t.Errorf("Unexpected error for database name %s: %v", tc.dbName, err)
			}
		})
	}
}

func TestValidateCollectionName(t *testing.T) {
	testCases := []struct {
		name        string
		collection  string
		expectError bool
	}{
		{
			name:        "Valid collection name",
			collection:  "executions",
			expectError: false,
		},
		{
			name:        "Empty collection name",
			collection:  "",
			expectError: true,
		},
		{
			name:        "Collection name with invalid character",
			collection:  "executions/test",
			expectError: true,
		},
		{
			name:        "Reserved prefix",
			collection:  "system.test",
			expectError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateCollectionName(tc.collection)
			if tc.expectError && err == nil {
				t.Errorf("Expected error for collection name: %s", tc.collection)
			}
			if !tc.expectError && err != nil {
				t.Errorf("Unexpected error for collection name %s: %v", tc.collection, err)
			}
		})
	}
}

func TestFormatBytes(t *testing.T) {
	testCases := []struct {
		bytes    int64
		expected string
	}{
		{1023, "1023 B"},
		{1024, "1.0 KB"},
		{1024 * 1024, "1.0 MB"},
		{1024 * 1024 * 1024, "1.0 GB"},
	}

	for _, tc := range testCases {
		t.Run(tc.expected, func(t *testing.T) {
			result := FormatBytes(tc.bytes)
			if result != tc.expected {
				t.Errorf("Expected %s, got %s", tc.expected, result)
			}
		})
	}
}

func TestFormatDuration(t *testing.T) {
	testCases := []struct {
		duration time.Duration
		expected string
	}{
		{30 * time.Second, "30s"},
		{2 * time.Minute, "2m"},
		{3 * time.Hour, "3h"},
		{25 * time.Hour, "1d"},
	}

	for _, tc := range testCases {
		t.Run(tc.expected, func(t *testing.T) {
			result := FormatDuration(tc.duration)
			if result != tc.expected {
				t.Errorf("Expected %s, got %s", tc.expected, result)
			}
		})
	}
}

func TestParseTimeDuration(t *testing.T) {
	testCases := []struct {
		input    string
		expected time.Duration
		valid    bool
	}{
		{"30s", 30 * time.Second, true},
		{"2m", 2 * time.Minute, true},
		{"1h", 1 * time.Hour, true},
		{"30d", 720 * time.Hour, true},
		{"invalid", 0, false},
		{"", 0, false},
	}

	for _, tc := range testCases {
		t.Run(tc.input, func(t *testing.T) {
			result, err := ParseTimeDuration(tc.input)
			if tc.valid && err != nil {
				t.Errorf("Expected valid duration for %s, got error: %v", tc.input, err)
			}
			if !tc.valid && err == nil {
				t.Errorf("Expected invalid duration for %s, but got no error", tc.input)
			}
			if tc.valid && result != tc.expected {
				t.Errorf("Expected duration %v for %s, got %v", tc.expected, tc.input, result)
			}
		})
	}
}

func TestGetTemporalCollections(t *testing.T) {
	collections := GetTemporalCollections()
	expectedCount := 15 // Including schema_version

	if len(collections) != expectedCount {
		t.Errorf("Expected %d collections, got %d", expectedCount, len(collections))
	}

	// Check for key collections
	keyCollections := []string{"executions", "history_node", "schema_version"}
	for _, key := range keyCollections {
		found := false
		for _, collection := range collections {
			if collection == key {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Expected collection %s not found", key)
		}
	}
}

func TestGetTemporalIndexes(t *testing.T) {
	indexes := GetTemporalIndexes()

	// Check executions collection indexes
	executionsIndexes, exists := indexes["executions"]
	if !exists {
		t.Error("Expected executions collection indexes not found")
	}

	expectedExecutionsIndexes := []string{"_id_", "executions_primary_key", "executions_workflow_lookup", "executions_visibility"}
	for _, expected := range expectedExecutionsIndexes {
		found := false
		for _, index := range executionsIndexes {
			if index == expected {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Expected index %s not found in executions collection", expected)
		}
	}
}

func TestIsValidAction(t *testing.T) {
	testCases := []struct {
		action string
		valid  bool
	}{
		{"create", true},
		{"validate", true},
		{"drop", true},
		{"setup-schema", true},
		{"update-schema", true},
		{"health-check", true},
		{"info", true},
		{"list-collections", true},
		{"collection-stats", true},
		{"list-indexes", true},
		{"backup", true},
		{"restore", true},
		{"cleanup", true},
		{"invalid", false},
		{"", false},
	}

	for _, tc := range testCases {
		t.Run(tc.action, func(t *testing.T) {
			result := IsValidAction(tc.action)
			if result != tc.valid {
				t.Errorf("Expected %v for action %s, got %v", tc.valid, tc.action, result)
			}
		})
	}
}

func TestGetActionDescription(t *testing.T) {
	testCases := []struct {
		action     string
		expected   string
		shouldFind bool
	}{
		{"create", "Create MongoDB schema", true},
		{"validate", "Validate existing schema", true},
		{"drop", "Drop entire database (⚠️ Destructive)", true},
		{"invalid", "Unknown action", false},
	}

	for _, tc := range testCases {
		t.Run(tc.action, func(t *testing.T) {
			result := GetActionDescription(tc.action)
			if result != tc.expected {
				t.Errorf("Expected description '%s' for action %s, got '%s'", tc.expected, tc.action, result)
			}
		})
	}
}
