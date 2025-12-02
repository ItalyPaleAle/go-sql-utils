package postgres

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/italypaleale/go-sql-utils/migrations"
)

func getTestDB(t *testing.T) *pgxpool.Pool {
	t.Helper()

	connStr := os.Getenv("POSTGRES_TEST_DB")
	if connStr == "" {
		t.Skip("POSTGRES_TEST_DB environment variable not set")
	}

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, connStr)
	require.NoError(t, err)

	// Verify connection
	err = pool.Ping(ctx)
	require.NoError(t, err)

	return pool
}

func cleanupTestTable(t *testing.T, pool *pgxpool.Pool, tableName string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	_, _ = pool.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
}

func TestMigrations_Perform(t *testing.T) {
	pool := getTestDB(t)
	t.Cleanup(func() { pool.Close() })

	const tableName = "test_metadata_perform"
	cleanupTestTable(t, pool, tableName)
	t.Cleanup(func() { cleanupTestTable(t, pool, tableName) })

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	m := Migrations{
		DB:                pool,
		MetadataTableName: tableName,
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
	err = pool.
		QueryRow(t.Context(), fmt.Sprintf("SELECT value FROM %s WHERE key = $1", tableName), "migration_level").
		Scan(&level)
	require.NoError(t, err)
	assert.Equal(t, "1", level)
}

func TestMigrations_Perform_MultipleMigrations(t *testing.T) {
	pool := getTestDB(t)
	t.Cleanup(func() { pool.Close() })

	const tableName = "test_metadata_multi"
	cleanupTestTable(t, pool, tableName)
	t.Cleanup(func() { cleanupTestTable(t, pool, tableName) })

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	m := Migrations{
		DB:                pool,
		MetadataTableName: tableName,
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
	err = pool.QueryRow(t.Context(), fmt.Sprintf("SELECT value FROM %s WHERE key = $1", tableName), "migration_level").Scan(&level)
	require.NoError(t, err)
	assert.Equal(t, "3", level)
}

func TestMigrations_Perform_ResumesFromLevel(t *testing.T) {
	pool := getTestDB(t)
	t.Cleanup(func() { pool.Close() })

	const tableName = "test_metadata_resume"
	cleanupTestTable(t, pool, tableName)
	t.Cleanup(func() { cleanupTestTable(t, pool, tableName) })

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	m := Migrations{
		DB:                pool,
		MetadataTableName: tableName,
		MetadataKey:       "migration_level",
	}

	// Run first migration
	var firstMigrationRan bool
	err := m.Perform(t.Context(), []migrations.MigrationFn{
		func(ctx context.Context) error {
			firstMigrationRan = true
			return nil
		},
	}, logger)
	require.NoError(t, err)
	require.True(t, firstMigrationRan)

	// Now run with more migrations - only new ones should run
	var secondMigrationRan, thirdMigrationRan bool
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
	require.True(t, secondMigrationRan)
	require.True(t, thirdMigrationRan)

	// Verify final migration level
	var level string
	err = pool.
		QueryRow(t.Context(), fmt.Sprintf("SELECT value FROM %s WHERE key = $1", tableName), "migration_level").
		Scan(&level)
	require.NoError(t, err)
	assert.Equal(t, "3", level)
}

func TestMigrations_Perform_MigrationError(t *testing.T) {
	pool := getTestDB(t)
	t.Cleanup(func() { pool.Close() })

	const tableName = "test_metadata_error"
	cleanupTestTable(t, pool, tableName)
	t.Cleanup(func() { cleanupTestTable(t, pool, tableName) })

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	m := Migrations{
		DB:                pool,
		MetadataTableName: tableName,
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
	pool := getTestDB(t)
	t.Cleanup(func() { pool.Close() })

	const tableName = "test_metadata_empty"
	cleanupTestTable(t, pool, tableName)
	t.Cleanup(func() { cleanupTestTable(t, pool, tableName) })

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	m := Migrations{
		DB:                pool,
		MetadataTableName: tableName,
		MetadataKey:       "migration_level",
	}

	err := m.Perform(t.Context(), []migrations.MigrationFn{}, logger)
	require.NoError(t, err)
}

func TestMigrations_EnsureMetadataTable(t *testing.T) {
	pool := getTestDB(t)
	t.Cleanup(func() { pool.Close() })

	const tableName = "test_ensure_metadata"
	cleanupTestTable(t, pool, tableName)
	t.Cleanup(func() { cleanupTestTable(t, pool, tableName) })

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	m := Migrations{
		DB:                pool,
		MetadataTableName: tableName,
		MetadataKey:       "migration_level",
	}

	err := m.EnsureMetadataTable(t.Context(), logger)
	require.NoError(t, err)

	// Verify table was created by inserting a row
	_, err = pool.Exec(t.Context(), fmt.Sprintf("INSERT INTO %s (key, value) VALUES ($1, $2)", tableName), "test_key", "test_value")
	require.NoError(t, err)

	var value string
	err = pool.QueryRow(t.Context(), fmt.Sprintf("SELECT value FROM %s WHERE key = $1", tableName), "test_key").Scan(&value)
	require.NoError(t, err)
	assert.Equal(t, "test_value", value)
}

func TestMigrations_EnsureMetadataTable_Idempotent(t *testing.T) {
	pool := getTestDB(t)
	t.Cleanup(func() { pool.Close() })

	const tableName = "test_ensure_metadata_idempotent"
	cleanupTestTable(t, pool, tableName)
	t.Cleanup(func() { cleanupTestTable(t, pool, tableName) })

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	m := Migrations{
		DB:                pool,
		MetadataTableName: tableName,
		MetadataKey:       "migration_level",
	}

	// Call EnsureMetadataTable multiple times in parallel - should not error
	var wg sync.WaitGroup
	errs := make([]error, 5)

	for i := range 5 {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			errs[idx] = m.EnsureMetadataTable(t.Context(), logger)
		}(i)
	}

	wg.Wait()

	for i, err := range errs {
		require.NoError(t, err, "goroutine %d should not error", i)
	}
}

func TestMigrations_Perform_CreatesTables(t *testing.T) {
	pool := getTestDB(t)
	t.Cleanup(func() { pool.Close() })

	const tableName = "test_metadata_creates_tables"
	const testTableName = "test_user_table"
	cleanupTestTable(t, pool, tableName)
	cleanupTestTable(t, pool, testTableName)
	t.Cleanup(func() { cleanupTestTable(t, pool, tableName) })
	t.Cleanup(func() { cleanupTestTable(t, pool, testTableName) })

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	m := Migrations{
		DB:                pool,
		MetadataTableName: tableName,
		MetadataKey:       "migration_level",
	}

	migrationFns := []migrations.MigrationFn{
		func(ctx context.Context) error {
			_, err := pool.Exec(ctx, fmt.Sprintf(`
				CREATE TABLE IF NOT EXISTS %s (
					id SERIAL PRIMARY KEY,
					name TEXT NOT NULL
				)
			`, testTableName))
			return err
		},
	}

	err := m.Perform(t.Context(), migrationFns, logger)
	require.NoError(t, err)

	// Verify the table was created
	_, err = pool.Exec(t.Context(), fmt.Sprintf("INSERT INTO %s (name) VALUES ($1)", testTableName), "test_name")
	require.NoError(t, err)
}

func TestMigrations_Perform_ConcurrentSafety(t *testing.T) {
	pool := getTestDB(t)
	t.Cleanup(func() { pool.Close() })

	const tableName = "test_metadata_concurrent"
	cleanupTestTable(t, pool, tableName)
	t.Cleanup(func() { cleanupTestTable(t, pool, tableName) })

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

			m := Migrations{
				DB:                pool,
				MetadataTableName: tableName,
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

	// The migration should have been executed exactly once due to the row-level lock
	assert.Equal(t, int32(1), executionCount.Load(), "migration should execute exactly once")

	// Verify the migration level is correct
	var level string
	err := pool.
		QueryRow(t.Context(), fmt.Sprintf("SELECT value FROM %s WHERE key = $1", tableName), "migration_level").
		Scan(&level)
	require.NoError(t, err)
	assert.Equal(t, "1", level)
}
