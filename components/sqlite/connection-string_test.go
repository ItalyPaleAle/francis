package sqlite

import (
	"log/slog"
	"net/url"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSQLiteConnectionString_IsInMemoryDB(t *testing.T) {
	tests := []struct {
		name     string
		connStr  sqliteConnectionString
		expected bool
	}{
		{
			name:     "memory database with :memory:",
			connStr:  ":memory:",
			expected: true,
		},
		{
			name:     "memory database with file::memory:",
			connStr:  "file::memory:",
			expected: true,
		},
		{
			name:     "memory database with :MEMORY: (uppercase)",
			connStr:  ":MEMORY:",
			expected: true,
		},
		{
			name:     "memory database with FILE::MEMORY: (uppercase)",
			connStr:  "FILE::MEMORY:",
			expected: true,
		},
		{
			name:     "memory database with mixed case",
			connStr:  ":Memory:",
			expected: true,
		},
		{
			name:     "has mode=memory",
			connStr:  "file:data?mode=memory",
			expected: true,
		},
		{
			name:     "file database",
			connStr:  "data.db",
			expected: false,
		},
		{
			name:     "file database with path",
			connStr:  "/path/to/data.db",
			expected: false,
		},
		{
			name:     "file database with file: prefix",
			connStr:  "file:data.db",
			expected: false,
		},
		{
			name:     "empty string",
			connStr:  "",
			expected: false,
		},
		{
			name:     "string containing memory but not at start",
			connStr:  "data:memory:.db",
			expected: false,
		},
		{
			name:     "has mode=ro",
			connStr:  "file:data?mode=ro",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.connStr.IsInMemoryDB()
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestSQLiteConnectionString_Parse(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	tests := []struct {
		name           string
		input          sqliteConnectionString
		expectedResult string
		shouldError    bool
		description    string
	}{
		{
			name:           "empty connection string",
			input:          "",
			expectedResult: "file:" + DefaultConnectionString + "?_pragma=busy_timeout%282500%29&_pragma=foreign_keys%281%29&_pragma=journal_mode%28WAL%29&_txlock=immediate",
			shouldError:    false,
			description:    "should use default connection string",
		},
		{
			name:           "simple file database",
			input:          "test.db",
			expectedResult: "file:test.db?_pragma=busy_timeout%282500%29&_pragma=foreign_keys%281%29&_pragma=journal_mode%28WAL%29&_txlock=immediate",
			shouldError:    false,
			description:    "should add file prefix and default pragmas",
		},
		{
			name:           "in-memory database",
			input:          ":memory:",
			expectedResult: "file::memory:?_pragma=busy_timeout%282500%29&_pragma=foreign_keys%281%29&_pragma=journal_mode%28MEMORY%29&_txlock=immediate&cache=shared",
			shouldError:    false,
			description:    "should set cache=shared and journal_mode=MEMORY for in-memory DB",
		},
		{
			name:           "file in-memory database",
			input:          "file::memory:",
			expectedResult: "file::memory:?_pragma=busy_timeout%282500%29&_pragma=foreign_keys%281%29&_pragma=journal_mode%28MEMORY%29&_txlock=immediate&cache=shared",
			shouldError:    false,
			description:    "should set cache=shared and journal_mode=MEMORY for file in-memory DB",
		},
		{
			name:           "mode=memory",
			input:          "file:data?mode=memory",
			expectedResult: "file:data?mode=memory&_pragma=busy_timeout%282500%29&_pragma=foreign_keys%281%29&_pragma=journal_mode%28MEMORY%29&_txlock=immediate&cache=shared",
			shouldError:    false,
			description:    "should set cache=shared and journal_mode=MEMORY for file in-memory DB",
		},
		{
			name:           "read-only database with mode=ro",
			input:          "test.db?mode=ro",
			expectedResult: "file:test.db?_pragma=busy_timeout%282500%29&_pragma=foreign_keys%281%29&_pragma=journal_mode%28DELETE%29&_txlock=immediate&mode=ro",
			shouldError:    false,
			description:    "should set journal_mode=DELETE for read-only DB",
		},
		{
			name:           "immutable database",
			input:          "test.db?immutable=1",
			expectedResult: "file:test.db?_pragma=busy_timeout%282500%29&_pragma=foreign_keys%281%29&_pragma=journal_mode%28DELETE%29&_txlock=immediate&immutable=1",
			shouldError:    false,
			description:    "should set journal_mode=DELETE for immutable DB",
		},
		{
			name:           "database with existing _txlock",
			input:          "test.db?_txlock=deferred",
			expectedResult: "file:test.db?_pragma=busy_timeout%282500%29&_pragma=foreign_keys%281%29&_pragma=journal_mode%28WAL%29&_txlock=deferred",
			shouldError:    false,
			description:    "should keep existing _txlock value",
		},
		{
			name:           "database with existing busy_timeout pragma",
			input:          "test.db?_pragma=busy_timeout(5000)",
			expectedResult: "file:test.db?_pragma=busy_timeout%285000%29&_pragma=foreign_keys%281%29&_pragma=journal_mode%28WAL%29&_txlock=immediate",
			shouldError:    false,
			description:    "should keep existing busy_timeout pragma",
		},
		{
			name:           "database with existing journal_mode pragma",
			input:          "test.db?_pragma=journal_mode(DELETE)",
			expectedResult: "file:test.db?_pragma=busy_timeout%282500%29&_pragma=journal_mode%28DELETE%29&_pragma=foreign_keys%281%29&_txlock=immediate",
			shouldError:    false,
			description:    "should keep existing journal_mode pragma",
		},
		{
			name:        "database with forbidden foreign_keys pragma",
			input:       "test.db?_pragma=foreign_keys(0)",
			shouldError: true,
			description: "should return error for forbidden foreign_keys pragma",
		},
		{
			name:           "database with multiple existing pragmas",
			input:          "test.db?_pragma=busy_timeout(1000)&_pragma=journal_mode(TRUNCATE)",
			expectedResult: "file:test.db?_pragma=busy_timeout%281000%29&_pragma=journal_mode%28TRUNCATE%29&_pragma=foreign_keys%281%29&_txlock=immediate",
			shouldError:    false,
			description:    "should keep existing pragmas and add missing ones",
		},
		{
			name:           "database already with file: prefix",
			input:          "file:test.db",
			expectedResult: "file:test.db?_pragma=busy_timeout%282500%29&_pragma=foreign_keys%281%29&_pragma=journal_mode%28WAL%29&_txlock=immediate",
			shouldError:    false,
			description:    "should not add duplicate file: prefix",
		},
		{
			name:           "database with uppercase FILE: prefix",
			input:          "FILE:test.db",
			expectedResult: "FILE:test.db?_pragma=busy_timeout%282500%29&_pragma=foreign_keys%281%29&_pragma=journal_mode%28WAL%29&_txlock=immediate",
			shouldError:    false,
			description:    "should not add file: prefix when FILE: exists",
		},
		{
			name:           "database with multiple mode values",
			input:          "test.db?mode=rw&mode=ro",
			expectedResult: "file:test.db?_pragma=busy_timeout%282500%29&_pragma=foreign_keys%281%29&_pragma=journal_mode%28WAL%29&_txlock=immediate&mode=rw",
			shouldError:    false,
			description:    "should keep first mode value only",
		},
		{
			name:           "database with multiple immutable values",
			input:          "test.db?immutable=0&immutable=1",
			expectedResult: "file:test.db?_pragma=busy_timeout%282500%29&_pragma=foreign_keys%281%29&_pragma=journal_mode%28WAL%29&_txlock=immediate&immutable=0",
			shouldError:    false,
			description:    "should keep first immutable value only",
		},
		{
			name:           "database with multiple _txlock values",
			input:          "test.db?_txlock=EXCLUSIVE&_txlock=immediate",
			expectedResult: "file:test.db?_pragma=busy_timeout%282500%29&_pragma=foreign_keys%281%29&_pragma=journal_mode%28WAL%29&_txlock=exclusive",
			shouldError:    false,
			description:    "should keep first _txlock value only and normalize to lowercase",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.input.Parse(logger)

			if tt.shouldError {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)

			result := string(tt.input)
			compareConnectionStrings(t, result, tt.expectedResult)
		})
	}
}

func TestSQLiteConnectionString_Parse_LogWarning(t *testing.T) {
	// Create a buffer to capture log output
	var logOutput strings.Builder
	logger := slog.New(slog.NewTextHandler(&logOutput, &slog.HandlerOptions{
		Level: slog.LevelWarn,
	}))

	connStr := sqliteConnectionString("test.db?_txlock=exclusive")
	err := connStr.Parse(logger)

	require.NoError(t, err, "Parse() returned unexpected error")

	logContent := logOutput.String()
	assert.Contains(t, logContent, "_txlock different from the recommended value 'immediate'",
		"Expected warning about _txlock not found in log output")
}

// Helper function to compare query strings (order-independent)
func compareConnectionStrings(t *testing.T, actual string, expected string) {
	t.Helper()

	actualUrl, err := url.Parse(actual)
	require.NoError(t, err)

	expectedUrl, err := url.Parse(expected)
	require.NoError(t, err)

	assert.Equal(t, expectedUrl.Scheme, actualUrl.Scheme, "have connection string '%s', want '%s'", actualUrl, expectedUrl)
	assert.Equal(t, expectedUrl.Path, actualUrl.Path, "have connection string '%s', want '%s'", actualUrl, expectedUrl)

	// Query strings may contain slices, which we'll sort since order doesn't matter
	expectedQs := expectedUrl.Query()
	actualQs := actualUrl.Query()
	sortSlicesInQS(expectedQs)
	sortSlicesInQS(actualQs)

	assert.Equal(t, expectedQs, actualQs, "have connection string '%s', want '%s'", actualUrl, expectedUrl)
}

func sortSlicesInQS(vals url.Values) {
	for _, v := range vals {
		slices.Sort(v)
	}
}
