package sqlite

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	// Blank import for the SQLite driver
	_ "modernc.org/sqlite"

	"github.com/italypaleale/go-sql-utils/migrations"
)

func getTestDB(t *testing.T) *sql.DB {
	t.Helper()

	hash := sha256.Sum256([]byte(t.Name()))
	dbName := hex.EncodeToString(hash[:])

	db, err := sql.Open("sqlite", "file:"+dbName+"?mode=memory")
	require.NoError(t, err)

	// Verify connection
	err = db.Ping()
	require.NoError(t, err)

	return db
}

func TestMigrations_Perform(t *testing.T) {
	db := getTestDB(t)
	t.Cleanup(func() { _ = db.Close() })

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	m := &Migrations{
		Pool:              db,
		MetadataTableName: "test_metadata",
		MetadataKey:       "migration_level",
	}

	var migrationRan bool
	migrationFns := []migrations.MigrationFn{
		func(ctx context.Context) error {
			migrationRan = true
			return nil
		},
	}

	err := m.Perform(t.Context(), migrationFns, logger)
	require.NoError(t, err)
	assert.True(t, migrationRan, "migration function should have been called")

	// Verify migration level was updated
	var level string
	err = db.
		QueryRowContext(t.Context(), "SELECT value FROM test_metadata WHERE key = ?", "migration_level").
		Scan(&level)
	require.NoError(t, err)
	assert.Equal(t, "1", level)
}

func TestMigrations_Perform_MultipleMigrations(t *testing.T) {
	db := getTestDB(t)
	t.Cleanup(func() { _ = db.Close() })

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	m := &Migrations{
		Pool:              db,
		MetadataTableName: "test_metadata",
		MetadataKey:       "migration_level",
	}

	executionOrder := []int{}
	migrationFns := []migrations.MigrationFn{
		func(ctx context.Context) error {
			executionOrder = append(executionOrder, 1)
			return nil
		},
		func(ctx context.Context) error {
			executionOrder = append(executionOrder, 2)
			return nil
		},
		func(ctx context.Context) error {
			executionOrder = append(executionOrder, 3)
			return nil
		},
	}

	err := m.Perform(t.Context(), migrationFns, logger)
	require.NoError(t, err)
	assert.Equal(t, []int{1, 2, 3}, executionOrder)

	// Verify final migration level
	var level string
	err = db.
		QueryRowContext(t.Context(), "SELECT value FROM test_metadata WHERE key = ?", "migration_level").
		Scan(&level)
	require.NoError(t, err)
	assert.Equal(t, "3", level)
}

func TestMigrations_Perform_ResumesFromLevel(t *testing.T) {
	db := getTestDB(t)
	t.Cleanup(func() { _ = db.Close() })

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	m := &Migrations{
		Pool:              db,
		MetadataTableName: "test_metadata",
		MetadataKey:       "migration_level",
	}

	// Run first migration
	firstMigrationRan := false
	err := m.Perform(t.Context(), []migrations.MigrationFn{
		func(ctx context.Context) error {
			firstMigrationRan = true
			return nil
		},
	}, logger)
	require.NoError(t, err)
	assert.True(t, firstMigrationRan)

	// Now run with more migrations - only new ones should run
	secondMigrationRan := false
	thirdMigrationRan := false
	err = m.Perform(t.Context(), []migrations.MigrationFn{
		func(ctx context.Context) error {
			t.Error("first migration should not run again")
			return nil
		},
		func(ctx context.Context) error {
			secondMigrationRan = true
			return nil
		},
		func(ctx context.Context) error {
			thirdMigrationRan = true
			return nil
		},
	}, logger)
	require.NoError(t, err)
	assert.True(t, secondMigrationRan)
	assert.True(t, thirdMigrationRan)

	// Verify final migration level
	var level string
	err = db.
		QueryRowContext(t.Context(), "SELECT value FROM test_metadata WHERE key = ?", "migration_level").
		Scan(&level)
	require.NoError(t, err)
	assert.Equal(t, "3", level)
}

func TestMigrations_Perform_MigrationError(t *testing.T) {
	db := getTestDB(t)
	t.Cleanup(func() { _ = db.Close() })

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	m := &Migrations{
		Pool:              db,
		MetadataTableName: "test_metadata",
		MetadataKey:       "migration_level",
	}

	expectedErr := errors.New("migration failed")
	migrationFns := []migrations.MigrationFn{
		func(ctx context.Context) error {
			return expectedErr
		},
	}

	err := m.Perform(t.Context(), migrationFns, logger)
	require.Error(t, err)
	require.ErrorIs(t, err, expectedErr)
}

func TestMigrations_Perform_NoMigrations(t *testing.T) {
	db := getTestDB(t)
	t.Cleanup(func() { _ = db.Close() })

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	m := &Migrations{
		Pool:              db,
		MetadataTableName: "test_metadata",
		MetadataKey:       "migration_level",
	}

	err := m.Perform(t.Context(), []migrations.MigrationFn{}, logger)
	require.NoError(t, err)
}

func TestMigrations_GetConn(t *testing.T) {
	db := getTestDB(t)
	t.Cleanup(func() { _ = db.Close() })

	m := &Migrations{
		Pool:              db,
		MetadataTableName: "test_metadata",
		MetadataKey:       "migration_level",
	}

	// Initially nil
	require.Nil(t, m.GetConn())

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	// After Perform, the conn is set but then closed
	err := m.Perform(t.Context(), []migrations.MigrationFn{}, logger)
	require.NoError(t, err)
}

func TestMigrations_Perform_CreatesTables(t *testing.T) {
	db := getTestDB(t)
	t.Cleanup(func() { _ = db.Close() })

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	m := &Migrations{
		Pool:              db,
		MetadataTableName: "test_metadata",
		MetadataKey:       "migration_level",
	}

	migrationFns := []migrations.MigrationFn{
		func(ctx context.Context) error {
			conn := m.GetConn()
			_, err := conn.ExecContext(ctx, `
				CREATE TABLE IF NOT EXISTS users (
					id INTEGER PRIMARY KEY AUTOINCREMENT,
					name TEXT NOT NULL
				)
			`)
			return err
		},
	}

	err := m.Perform(t.Context(), migrationFns, logger)
	require.NoError(t, err)

	// Verify the table was created
	_, err = db.ExecContext(t.Context(), "INSERT INTO users (name) VALUES (?)", "test_name")
	require.NoError(t, err)
}

func TestMigrations_tableExists(t *testing.T) {
	db := getTestDB(t)
	t.Cleanup(func() { _ = db.Close() })

	m := &Migrations{
		Pool:              db,
		MetadataTableName: "test_table_exists",
		MetadataKey:       "migration_level",
	}

	conn, err := db.Conn(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	// Table should not exist initially
	exists, err := m.tableExists(t.Context(), conn)
	require.NoError(t, err)
	require.False(t, exists)

	// Create the table
	_, err = conn.ExecContext(t.Context(), `
		CREATE TABLE test_table_exists (
			key TEXT NOT NULL PRIMARY KEY,
			value TEXT NOT NULL
		)
	`)
	require.NoError(t, err)

	// Now it should exist
	exists, err = m.tableExists(t.Context(), conn)
	require.NoError(t, err)
	require.True(t, exists)
}

func TestMigrations_createMetadataTable(t *testing.T) {
	db := getTestDB(t)
	t.Cleanup(func() { _ = db.Close() })

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	m := &Migrations{
		Pool:              db,
		MetadataTableName: "test_create_metadata",
		MetadataKey:       "migration_level",
	}

	conn, err := db.Conn(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	err = m.createMetadataTable(t.Context(), conn, logger)
	require.NoError(t, err)

	// Verify table was created by inserting a row
	_, err = conn.ExecContext(t.Context(), "INSERT INTO test_create_metadata (key, value) VALUES (?, ?)", "test_key", "test_value")
	require.NoError(t, err)
}

func TestMigrations_createMetadataTable_Idempotent(t *testing.T) {
	db := getTestDB(t)
	t.Cleanup(func() { _ = db.Close() })

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	m := &Migrations{
		Pool:              db,
		MetadataTableName: "test_create_metadata_idempotent",
		MetadataKey:       "migration_level",
	}

	conn, err := db.Conn(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	// Call createMetadataTable multiple times in parallel - should not error
	var wg sync.WaitGroup
	errs := make([]error, 5)

	for i := range 5 {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			errs[idx] = m.createMetadataTable(t.Context(), conn, logger)
		}(i)
	}

	wg.Wait()

	for i, err := range errs {
		require.NoError(t, err, "goroutine %d should not error", i)
	}
}

func TestMigrations_Perform_PartialMigrationFailure(t *testing.T) {
	db := getTestDB(t)
	t.Cleanup(func() { _ = db.Close() })

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	m := &Migrations{
		Pool:              db,
		MetadataTableName: "test_metadata_partial",
		MetadataKey:       "migration_level",
	}

	var firstRan, secondRan bool

	migrationFns := []migrations.MigrationFn{
		func(ctx context.Context) error {
			firstRan = true
			return nil
		},
		func(ctx context.Context) error {
			secondRan = true
			return errors.New("second migration failed")
		},
	}

	err := m.Perform(t.Context(), migrationFns, logger)
	require.Error(t, err)
	assert.True(t, firstRan)
	assert.True(t, secondRan)
	require.ErrorContains(t, err, "second migration failed")
}

func TestMigrations_Perform_ConcurrentSafety(t *testing.T) {
	// Use a file-based database to test concurrency across connections
	// Memory databases are isolated per connection
	dbPath := t.TempDir() + "/test_concurrent.db"
	db, err := sql.Open("sqlite", "file:"+dbPath+"?_pragma=busy_timeout%2810000%29")
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	// Set connection pool to allow multiple connections
	db.SetMaxOpenConns(10)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	const numGoroutines = 5
	executionCount := &atomic.Int32{}
	var wg sync.WaitGroup

	migrationFns := []migrations.MigrationFn{
		func(ctx context.Context) error {
			// Atomically increment the counter to track how many times the migration actually ran
			executionCount.Add(1)
			return nil
		},
	}

	// Launch multiple goroutines to perform migrations concurrently
	wg.Add(numGoroutines)
	errs := make([]error, numGoroutines)

	for i := range numGoroutines {
		go func(idx int) {
			defer wg.Done()

			m := &Migrations{
				Pool:              db,
				MetadataTableName: "test_metadata_concurrent",
				MetadataKey:       "migration_level",
			}

			errs[idx] = m.Perform(t.Context(), migrationFns, logger)
		}(i)
	}

	wg.Wait()

	// All goroutines should complete without error
	for i, err := range errs {
		require.NoError(t, err, "goroutine %d should not error", i)
	}

	// The migration should have been executed exactly once due to the exclusive lock
	assert.Equal(t, int32(1), executionCount.Load(), "migration should execute exactly once")

	// Verify the migration level is correct
	var level string
	err = db.
		QueryRowContext(t.Context(), "SELECT value FROM test_metadata_concurrent WHERE key = ?", "migration_level").
		Scan(&level)
	require.NoError(t, err)
	assert.Equal(t, "1", level)
}
