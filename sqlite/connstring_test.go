// This code was adapted from https://github.com/pocket-id/pocket-id/tree/v2.9.0
// Copyright (c) 2024 Elias Schneider
// License: BSD-2

package sqlite

import (
	"net/url"
	"path/filepath"
	"strings"
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
			expected:   "file:test.db?_pragma=busy_timeout%282500%29&_pragma=foreign_keys%281%29&_pragma=temp_store%28MEMORY%29&_pragma=journal_mode%28WAL%29&_txlock=immediate",
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
			expected:   "file:test.db?_pragma=busy_timeout%282500%29&_pragma=foreign_keys%281%29&_pragma=temp_store%28MEMORY%29&_pragma=journal_mode%28WAL%29&_txlock=deferred",
		},
		{
			name:       "database with existing busy_timeout pragma",
			input:      "file:test.db?_pragma=busy_timeout%285000%29",
			isMemoryDB: false,
			expected:   "file:test.db?_pragma=busy_timeout%285000%29&_pragma=foreign_keys%281%29&_pragma=temp_store%28MEMORY%29&_pragma=journal_mode%28WAL%29&_txlock=immediate",
		},
		{
			name:       "database with existing journal_mode pragma",
			input:      "file:test.db?_pragma=journal_mode%28DELETE%29",
			isMemoryDB: false,
			expected:   "file:test.db?_pragma=busy_timeout%282500%29&_pragma=foreign_keys%281%29&_pragma=temp_store%28MEMORY%29&_pragma=journal_mode%28DELETE%29&_txlock=immediate",
		},
		{
			name:       "database with existing temp_store pragma",
			input:      "file:test.db?_pragma=temp_store%28FILE%29",
			isMemoryDB: false,
			expected:   "file:test.db?_pragma=busy_timeout%282500%29&_pragma=foreign_keys%281%29&_pragma=temp_store%28FILE%29&_pragma=journal_mode%28WAL%29&_txlock=immediate",
		},
		{
			name:        "database with forbidden foreign_keys pragma",
			input:       "file:test.db?_pragma=foreign_keys%280%29",
			isMemoryDB:  false,
			expectError: true,
		},
		{
			name:       "database with multiple existing pragmas",
			input:      "file:test.db?_pragma=busy_timeout%283000%29&_pragma=journal_mode%28TRUNCATE%29&_pragma=temp_store%28FILE%29&_pragma=synchronous%28NORMAL%29",
			isMemoryDB: false,
			expected:   "file:test.db?_pragma=busy_timeout%283000%29&_pragma=foreign_keys%281%29&_pragma=temp_store%28FILE%29&_pragma=journal_mode%28TRUNCATE%29&_pragma=synchronous%28NORMAL%29&_txlock=immediate",
		},
		{
			name:       "database with mode=rw (not read-only)",
			input:      "file:test.db?mode=rw",
			isMemoryDB: false,
			expected:   "file:test.db?_pragma=busy_timeout%282500%29&_pragma=foreign_keys%281%29&_pragma=temp_store%28MEMORY%29&_pragma=journal_mode%28WAL%29&_txlock=immediate&mode=rw",
		},
		{
			name:       "database with immutable=0 (not immutable)",
			input:      "file:test.db?immutable=0",
			isMemoryDB: false,
			expected:   "file:test.db?_pragma=busy_timeout%282500%29&_pragma=foreign_keys%281%29&_pragma=temp_store%28MEMORY%29&_pragma=journal_mode%28WAL%29&_txlock=immediate&immutable=0",
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
			input:      "file:test.db?cache=shared&mode=rwc&_txlock=immediate&_pragma=synchronous%28FULL%29&_pragma=temp_store%28FILE%29",
			isMemoryDB: false,
			expected:   "file:test.db?_pragma=busy_timeout%282500%29&_pragma=foreign_keys%281%29&_pragma=temp_store%28FILE%29&_pragma=journal_mode%28WAL%29&_pragma=synchronous%28FULL%29&_txlock=immediate&cache=shared&mode=rwc",
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

func TestConvertSqlitePragmaArgs(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "basic file path",
			input:    "file:test.db",
			expected: "file:test.db",
		},
		{
			name:     "converts _busy_timeout to pragma",
			input:    "file:test.db?_busy_timeout=5000",
			expected: "file:test.db?_pragma=busy_timeout%285000%29",
		},
		{
			name:     "converts _timeout to pragma",
			input:    "file:test.db?_timeout=5000",
			expected: "file:test.db?_pragma=busy_timeout%285000%29",
		},
		{
			name:     "converts _foreign_keys to pragma",
			input:    "file:test.db?_foreign_keys=1",
			expected: "file:test.db?_pragma=foreign_keys%281%29",
		},
		{
			name:     "converts _fk to pragma",
			input:    "file:test.db?_fk=1",
			expected: "file:test.db?_pragma=foreign_keys%281%29",
		},
		{
			name:     "converts _synchronous to pragma",
			input:    "file:test.db?_synchronous=NORMAL",
			expected: "file:test.db?_pragma=synchronous%28NORMAL%29",
		},
		{
			name:     "converts _sync to pragma",
			input:    "file:test.db?_sync=NORMAL",
			expected: "file:test.db?_pragma=synchronous%28NORMAL%29",
		},
		{
			name:     "converts _auto_vacuum to pragma",
			input:    "file:test.db?_auto_vacuum=FULL",
			expected: "file:test.db?_pragma=auto_vacuum%28FULL%29",
		},
		{
			name:     "converts _vacuum to pragma",
			input:    "file:test.db?_vacuum=FULL",
			expected: "file:test.db?_pragma=auto_vacuum%28FULL%29",
		},
		{
			name:     "converts _case_sensitive_like to pragma",
			input:    "file:test.db?_case_sensitive_like=1",
			expected: "file:test.db?_pragma=case_sensitive_like%281%29",
		},
		{
			name:     "converts _cslike to pragma",
			input:    "file:test.db?_cslike=1",
			expected: "file:test.db?_pragma=case_sensitive_like%281%29",
		},
		{
			name:     "converts _locking_mode to pragma",
			input:    "file:test.db?_locking_mode=EXCLUSIVE",
			expected: "file:test.db?_pragma=locking_mode%28EXCLUSIVE%29",
		},
		{
			name:     "converts _locking to pragma",
			input:    "file:test.db?_locking=EXCLUSIVE",
			expected: "file:test.db?_pragma=locking_mode%28EXCLUSIVE%29",
		},
		{
			name:     "converts _secure_delete to pragma",
			input:    "file:test.db?_secure_delete=1",
			expected: "file:test.db?_pragma=secure_delete%281%29",
		},
		{
			name:     "preserves unrecognized parameters",
			input:    "file:test.db?mode=rw&cache=shared",
			expected: "file:test.db?cache=shared&mode=rw",
		},
		{
			name:     "handles multiple parameters",
			input:    "file:test.db?_fk=1&mode=rw&_timeout=5000",
			expected: "file:test.db?_pragma=foreign_keys%281%29&_pragma=busy_timeout%285000%29&mode=rw",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resultURL, _ := url.Parse(tt.input)
			convertSqlitePragmaArgs(resultURL)

			// Parse both URLs to compare components independently
			expectedURL, err := url.Parse(tt.expected)
			require.NoError(t, err)

			// Compare scheme and path components
			compareQueryStrings(t, expectedURL, resultURL)
		})
	}
}

func TestParseConnectionString(t *testing.T) {
	tests := []struct {
		name            string
		connString      string
		expectError     bool
		isMemoryDB      bool
		checkConnString func(t *testing.T, cs string)
		checkDbPath     func(t *testing.T, path string)
	}{
		{
			name:       "plain filename gets file: prefix",
			connString: "test.db",
			isMemoryDB: false,
			checkConnString: func(t *testing.T, cs string) {
				assert.True(t, strings.HasPrefix(cs, "file:"))
			},
			checkDbPath: func(t *testing.T, path string) {
				assert.True(t, filepath.IsAbs(path))
				assert.True(t, strings.HasSuffix(path, "test.db"))
			},
		},
		{
			name:       "file: prefix is not duplicated",
			connString: "file:test.db",
			isMemoryDB: false,
			checkConnString: func(t *testing.T, cs string) {
				assert.True(t, strings.HasPrefix(cs, "file:"))
				assert.False(t, strings.HasPrefix(cs, "file:file:"))
			},
			checkDbPath: func(t *testing.T, path string) {
				assert.True(t, filepath.IsAbs(path))
				assert.True(t, strings.HasSuffix(path, "test.db"))
			},
		},
		{
			name:       "absolute path is preserved",
			connString: "/tmp/mydb.db",
			isMemoryDB: false,
			checkDbPath: func(t *testing.T, path string) {
				assert.True(t, filepath.IsAbs(path))
				assert.True(t, strings.HasSuffix(path, "mydb.db"))
			},
		},
		{
			name:       "in-memory database :memory:",
			connString: ":memory:",
			isMemoryDB: true,
			checkConnString: func(t *testing.T, cs string) {
				u, err := url.Parse(cs)
				require.NoError(t, err)
				hasMem := false
				for _, p := range u.Query()["_pragma"] {
					if p == "journal_mode(MEMORY)" {
						hasMem = true
					}
				}
				assert.True(t, hasMem, "expected journal_mode(MEMORY) pragma for in-memory DB")
			},
		},
		{
			name:       "in-memory database file::memory:",
			connString: "file::memory:",
			isMemoryDB: true,
		},
		{
			name:       "in-memory database via mode=memory",
			connString: "file:test?mode=memory",
			isMemoryDB: true,
		},
		{
			name:       "read-only database gets DELETE journal mode",
			connString: "file:test.db?mode=ro",
			isMemoryDB: false,
			checkConnString: func(t *testing.T, cs string) {
				u, err := url.Parse(cs)
				require.NoError(t, err)
				hasDel := false
				for _, p := range u.Query()["_pragma"] {
					if p == "journal_mode(DELETE)" {
						hasDel = true
					}
				}
				assert.True(t, hasDel, "expected journal_mode(DELETE) pragma for read-only DB")
			},
		},
		{
			name:       "regular file database gets WAL journal mode",
			connString: "test.db",
			isMemoryDB: false,
			checkConnString: func(t *testing.T, cs string) {
				u, err := url.Parse(cs)
				require.NoError(t, err)
				hasWAL := false
				for _, p := range u.Query()["_pragma"] {
					if p == "journal_mode(WAL)" {
						hasWAL = true
					}
				}
				assert.True(t, hasWAL, "expected journal_mode(WAL) pragma for regular DB")
			},
		},
		{
			name:       "foreign_keys(1) pragma is always added",
			connString: "test.db",
			isMemoryDB: false,
			checkConnString: func(t *testing.T, cs string) {
				u, err := url.Parse(cs)
				require.NoError(t, err)
				hasFK := false
				for _, p := range u.Query()["_pragma"] {
					if p == "foreign_keys(1)" {
						hasFK = true
					}
				}
				assert.True(t, hasFK, "expected foreign_keys(1) pragma")
			},
		},
		{
			name:       "default _txlock=immediate is added",
			connString: "test.db",
			isMemoryDB: false,
			checkConnString: func(t *testing.T, cs string) {
				u, err := url.Parse(cs)
				require.NoError(t, err)
				assert.Equal(t, []string{"immediate"}, u.Query()["_txlock"])
			},
		},
		{
			name:       "existing _txlock is preserved",
			connString: "file:test.db?_txlock=deferred",
			isMemoryDB: false,
			checkConnString: func(t *testing.T, cs string) {
				u, err := url.Parse(cs)
				require.NoError(t, err)
				assert.Equal(t, []string{"deferred"}, u.Query()["_txlock"])
			},
		},
		{
			name:       "legacy _busy_timeout param is converted to pragma",
			connString: "file:test.db?_busy_timeout=5000",
			isMemoryDB: false,
			checkConnString: func(t *testing.T, cs string) {
				u, err := url.Parse(cs)
				require.NoError(t, err)
				hasBT := false
				for _, p := range u.Query()["_pragma"] {
					if p == "busy_timeout(5000)" {
						hasBT = true
					}
				}
				assert.True(t, hasBT, "expected busy_timeout(5000) pragma from converted _busy_timeout")
				// The original _busy_timeout param must not appear in the output
				_, hasOld := u.Query()["_busy_timeout"]
				assert.False(t, hasOld, "_busy_timeout should have been converted and removed")
			},
		},
		{
			name:        "forbidden foreign_keys pragma returns error",
			connString:  "file:test.db?_pragma=foreign_keys(0)",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsedConnString, dbPath, isMemoryDB, _, err := ParseConnectionString(tt.connString, nil)

			if tt.expectError {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.isMemoryDB, isMemoryDB)

			if tt.checkConnString != nil {
				tt.checkConnString(t, parsedConnString)
			}
			if tt.checkDbPath != nil {
				tt.checkDbPath(t, dbPath)
			}
		})
	}
}

func TestParseConnectionStringMaxConns(t *testing.T) {
	tests := []struct {
		name        string
		connString  string
		expectError bool
		maxConns    int
	}{
		{
			name:       "no _maxconn param",
			connString: "file:test.db",
			maxConns:   0,
		},
		{
			name:       "_maxconn set to a positive value",
			connString: "file:test.db?_maxconn=10",
			maxConns:   10,
		},
		{
			name:       "_maxconn set to zero means use the default",
			connString: "file:test.db?_maxconn=0",
			maxConns:   0,
		},
		{
			name:       "_maxconn set to a negative value means use the default",
			connString: "file:test.db?_maxconn=-5",
			maxConns:   0,
		},
		{
			name:        "_maxconn set to a non-numeric value returns an error",
			connString:  "file:test.db?_maxconn=abc",
			expectError: true,
		},
		{
			name:       "_maxconn is ignored for in-memory databases",
			connString: "file::memory:?_maxconn=10",
			maxConns:   10,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsedConnString, _, _, maxConns, err := ParseConnectionString(tt.connString, nil)

			if tt.expectError {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.maxConns, maxConns)

			// The "_maxconn" parameter must never be passed through to the driver
			u, err := url.Parse(parsedConnString)
			require.NoError(t, err)
			_, ok := u.Query()["_maxconn"]
			assert.False(t, ok, "_maxconn should have been removed from the parsed connection string")
		})
	}
}
