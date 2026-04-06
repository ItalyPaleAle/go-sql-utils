// This code was adapted from https://github.com/pocket-id/pocket-id/tree/v2.5.0
// Copyright (c) 2024 Elias Schneider
// License: BSD-2

package sqlite

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIsInMemory(t *testing.T) {
	tests := []struct {
		name     string
		connStr  string
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
			result := isInMemory(tt.connStr)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestAddDefaultParameters(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		isMemoryDB  bool
		expected    string
		expectError bool
	}{
		{
			name:       "basic file database",
			input:      "file:test.db",
			isMemoryDB: false,
			expected:   "file:test.db?_pragma=busy_timeout%282500%29&_pragma=foreign_keys%281%29&_pragma=journal_mode%28WAL%29&_txlock=immediate",
		},
		{
			name:       "in-memory database",
			input:      "file::memory:",
			isMemoryDB: true,
			expected:   "file::memory:?_pragma=busy_timeout%282500%29&_pragma=foreign_keys%281%29&_pragma=journal_mode%28MEMORY%29&_txlock=immediate",
		},
		{
			name:       "read-only database with mode=ro",
			input:      "file:test.db?mode=ro",
			isMemoryDB: false,
			expected:   "file:test.db?_pragma=busy_timeout%282500%29&_pragma=foreign_keys%281%29&_pragma=journal_mode%28DELETE%29&_txlock=immediate&mode=ro",
		},
		{
			name:       "immutable database",
			input:      "file:test.db?immutable=1",
			isMemoryDB: false,
			expected:   "file:test.db?_pragma=busy_timeout%282500%29&_pragma=foreign_keys%281%29&_pragma=journal_mode%28DELETE%29&_txlock=immediate&immutable=1",
		},
		{
			name:       "database with existing _txlock",
			input:      "file:test.db?_txlock=deferred",
			isMemoryDB: false,
			expected:   "file:test.db?_pragma=busy_timeout%282500%29&_pragma=foreign_keys%281%29&_pragma=journal_mode%28WAL%29&_txlock=deferred",
		},
		{
			name:       "database with existing busy_timeout pragma",
			input:      "file:test.db?_pragma=busy_timeout%285000%29",
			isMemoryDB: false,
			expected:   "file:test.db?_pragma=busy_timeout%285000%29&_pragma=foreign_keys%281%29&_pragma=journal_mode%28WAL%29&_txlock=immediate",
		},
		{
			name:       "database with existing journal_mode pragma",
			input:      "file:test.db?_pragma=journal_mode%28DELETE%29",
			isMemoryDB: false,
			expected:   "file:test.db?_pragma=busy_timeout%282500%29&_pragma=foreign_keys%281%29&_pragma=journal_mode%28DELETE%29&_txlock=immediate",
		},
		{
			name:        "database with forbidden foreign_keys pragma",
			input:       "file:test.db?_pragma=foreign_keys%280%29",
			isMemoryDB:  false,
			expectError: true,
		},
		{
			name:       "database with multiple existing pragmas",
			input:      "file:test.db?_pragma=busy_timeout%283000%29&_pragma=journal_mode%28TRUNCATE%29&_pragma=synchronous%28NORMAL%29",
			isMemoryDB: false,
			expected:   "file:test.db?_pragma=busy_timeout%283000%29&_pragma=foreign_keys%281%29&_pragma=journal_mode%28TRUNCATE%29&_pragma=synchronous%28NORMAL%29&_txlock=immediate",
		},
		{
			name:       "database with mode=rw (not read-only)",
			input:      "file:test.db?mode=rw",
			isMemoryDB: false,
			expected:   "file:test.db?_pragma=busy_timeout%282500%29&_pragma=foreign_keys%281%29&_pragma=journal_mode%28WAL%29&_txlock=immediate&mode=rw",
		},
		{
			name:       "database with immutable=0 (not immutable)",
			input:      "file:test.db?immutable=0",
			isMemoryDB: false,
			expected:   "file:test.db?_pragma=busy_timeout%282500%29&_pragma=foreign_keys%281%29&_pragma=journal_mode%28WAL%29&_txlock=immediate&immutable=0",
		},
		{
			name:       "database with mixed case mode=RO",
			input:      "file:test.db?mode=RO",
			isMemoryDB: false,
			expected:   "file:test.db?_pragma=busy_timeout%282500%29&_pragma=foreign_keys%281%29&_pragma=journal_mode%28DELETE%29&_txlock=immediate&mode=ro",
		},
		{
			name:       "database with mixed case immutable=1",
			input:      "file:test.db?immutable=1",
			isMemoryDB: false,
			expected:   "file:test.db?_pragma=busy_timeout%282500%29&_pragma=foreign_keys%281%29&_pragma=journal_mode%28DELETE%29&_txlock=immediate&immutable=1",
		},
		{
			name:       "complex database configuration",
			input:      "file:test.db?cache=shared&mode=rwc&_txlock=immediate&_pragma=synchronous%28FULL%29",
			isMemoryDB: false,
			expected:   "file:test.db?_pragma=busy_timeout%282500%29&_pragma=foreign_keys%281%29&_pragma=journal_mode%28WAL%29&_pragma=synchronous%28FULL%29&_txlock=immediate&cache=shared&mode=rwc",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resultURL, err := url.Parse(tt.input)
			require.NoError(t, err)

			err = addDefaultParameters(resultURL, tt.isMemoryDB, nil)

			if tt.expectError {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)

			expectedURL, err := url.Parse(tt.expected)
			require.NoError(t, err)

			compareQueryStrings(t, expectedURL, resultURL)
		})
	}
}

func compareQueryStrings(t *testing.T, expectedURL *url.URL, resultURL *url.URL) {
	t.Helper()

	// Compare scheme and path components
	assert.Equal(t, expectedURL.Scheme, resultURL.Scheme)
	assert.Equal(t, expectedURL.Path, resultURL.Path)

	// Compare query parameters regardless of order
	expectedQuery := expectedURL.Query()
	resultQuery := resultURL.Query()

	assert.Len(t, expectedQuery, len(resultQuery))

	for key, expectedValues := range expectedQuery {
		resultValues, ok := resultQuery[key]
		_ = assert.True(t, ok) &&
			assert.ElementsMatch(t, expectedValues, resultValues)
	}
}
