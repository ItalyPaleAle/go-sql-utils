package migrations

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	// Blank import for the SQLite driver
	_ "modernc.org/sqlite"

	sqladapter "github.com/italypaleale/go-sql-utils/adapter/sql"
)

func getTestDB(t *testing.T) *sql.DB {
	t.Helper()

	hash := sha256.Sum256([]byte(t.Name()))
	dbName := hex.EncodeToString(hash[:])

	db, err := sql.Open("sqlite", "file:"+dbName+"?mode=memory")
	require.NoError(t, err)

	err = db.Ping()
	require.NoError(t, err)

	return db
}

func setupMetadataTable(t *testing.T, db *sql.DB) {
	t.Helper()

	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS metadata (
			key TEXT NOT NULL PRIMARY KEY,
			value TEXT NOT NULL
		)
	`)
	require.NoError(t, err)
}

func TestMigrate_Success(t *testing.T) {
	db := getTestDB(t)
	t.Cleanup(func() { _ = db.Close() })

	setupMetadataTable(t, db)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	migrationRan := false
	opts := MigrationOptions{
		Migrations: []MigrationFn{
			func(ctx context.Context) error {
				migrationRan = true
				return nil
			},
		},
		GetVersionQuery: "SELECT value FROM metadata WHERE key = 'version'",
		UpdateVersionQuery: func(version string) (string, any) {
			return "REPLACE INTO metadata (key, value) VALUES ('version', ?)", version
		},
	}

	ctx := t.Context()
	err := Migrate(ctx, sqladapter.AdaptDatabaseSQLConn(db), opts, logger)
	require.NoError(t, err)
	assert.True(t, migrationRan)

	// Verify version was updated
	var version string
	err = db.QueryRowContext(ctx, "SELECT value FROM metadata WHERE key = 'version'").Scan(&version)
	require.NoError(t, err)
	assert.Equal(t, "1", version)
}

func TestMigrate_MultipleMigrations(t *testing.T) {
	db := getTestDB(t)
	t.Cleanup(func() { _ = db.Close() })

	setupMetadataTable(t, db)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	executionOrder := []int{}
	opts := MigrationOptions{
		Migrations: []MigrationFn{
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
		},
		GetVersionQuery: "SELECT value FROM metadata WHERE key = 'version'",
		UpdateVersionQuery: func(version string) (string, any) {
			return "REPLACE INTO metadata (key, value) VALUES ('version', ?)", version
		},
	}

	ctx := t.Context()
	err := Migrate(ctx, sqladapter.AdaptDatabaseSQLConn(db), opts, logger)
	require.NoError(t, err)
	assert.Equal(t, []int{1, 2, 3}, executionOrder)

	// Verify final version
	var version string
	err = db.QueryRowContext(ctx, "SELECT value FROM metadata WHERE key = 'version'").Scan(&version)
	require.NoError(t, err)
	assert.Equal(t, "3", version)
}

func TestMigrate_ResumesFromLevel(t *testing.T) {
	db := getTestDB(t)
	t.Cleanup(func() { _ = db.Close() })

	setupMetadataTable(t, db)

	// Pre-set migration level to 2
	_, err := db.Exec("INSERT INTO metadata (key, value) VALUES ('version', '2')")
	require.NoError(t, err)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	executed := []int{}
	opts := MigrationOptions{
		Migrations: []MigrationFn{
			func(ctx context.Context) error {
				executed = append(executed, 1)
				return nil
			},
			func(ctx context.Context) error {
				executed = append(executed, 2)
				return nil
			},
			func(ctx context.Context) error {
				executed = append(executed, 3)
				return nil
			},
			func(ctx context.Context) error {
				executed = append(executed, 4)
				return nil
			},
		},
		GetVersionQuery: "SELECT value FROM metadata WHERE key = 'version'",
		UpdateVersionQuery: func(version string) (string, any) {
			return "REPLACE INTO metadata (key, value) VALUES ('version', ?)", version
		},
	}

	ctx := t.Context()
	err = Migrate(ctx, sqladapter.AdaptDatabaseSQLConn(db), opts, logger)
	require.NoError(t, err)

	// Only migrations 3 and 4 should have run
	assert.Equal(t, []int{3, 4}, executed)

	// Verify final version
	var version string
	err = db.QueryRowContext(ctx, "SELECT value FROM metadata WHERE key = 'version'").Scan(&version)
	require.NoError(t, err)
	assert.Equal(t, "4", version)
}

func TestMigrate_NoMigrations(t *testing.T) {
	db := getTestDB(t)
	t.Cleanup(func() { _ = db.Close() })

	setupMetadataTable(t, db)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	opts := MigrationOptions{
		Migrations:      []MigrationFn{},
		GetVersionQuery: "SELECT value FROM metadata WHERE key = 'version'",
		UpdateVersionQuery: func(version string) (string, any) {
			return "REPLACE INTO metadata (key, value) VALUES ('version', ?)", version
		},
	}

	ctx := t.Context()
	err := Migrate(ctx, sqladapter.AdaptDatabaseSQLConn(db), opts, logger)
	require.NoError(t, err)
}

func TestMigrate_MigrationError(t *testing.T) {
	db := getTestDB(t)
	t.Cleanup(func() { _ = db.Close() })

	setupMetadataTable(t, db)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	expectedErr := errors.New("migration failed")
	opts := MigrationOptions{
		Migrations: []MigrationFn{
			func(ctx context.Context) error {
				return expectedErr
			},
		},
		GetVersionQuery: "SELECT value FROM metadata WHERE key = 'version'",
		UpdateVersionQuery: func(version string) (string, any) {
			return "REPLACE INTO metadata (key, value) VALUES ('version', ?)", version
		},
	}

	ctx := t.Context()
	err := Migrate(ctx, sqladapter.AdaptDatabaseSQLConn(db), opts, logger)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "migration failed")
}

func TestMigrate_EnsureMetadataTableCalled(t *testing.T) {
	db := getTestDB(t)
	t.Cleanup(func() { _ = db.Close() })

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	ensureMetadataCalled := false
	opts := MigrationOptions{
		Migrations: []MigrationFn{
			func(ctx context.Context) error {
				return nil
			},
		},
		EnsureMetadataTable: func(ctx context.Context) error {
			ensureMetadataCalled = true
			// Create the metadata table
			_, err := db.ExecContext(ctx, `
				CREATE TABLE IF NOT EXISTS metadata (
					key TEXT NOT NULL PRIMARY KEY,
					value TEXT NOT NULL
				)
			`)
			return err
		},
		GetVersionQuery: "SELECT value FROM metadata WHERE key = 'version'",
		UpdateVersionQuery: func(version string) (string, any) {
			return "REPLACE INTO metadata (key, value) VALUES ('version', ?)", version
		},
	}

	ctx := t.Context()
	err := Migrate(ctx, sqladapter.AdaptDatabaseSQLConn(db), opts, logger)
	require.NoError(t, err)
	assert.True(t, ensureMetadataCalled)
}

func TestMigrate_EnsureMetadataTableError(t *testing.T) {
	db := getTestDB(t)
	t.Cleanup(func() { _ = db.Close() })

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	expectedErr := errors.New("failed to create metadata table")
	opts := MigrationOptions{
		Migrations: []MigrationFn{
			func(ctx context.Context) error {
				return nil
			},
		},
		EnsureMetadataTable: func(ctx context.Context) error {
			return expectedErr
		},
		GetVersionQuery: "SELECT value FROM metadata WHERE key = 'version'",
		UpdateVersionQuery: func(version string) (string, any) {
			return "REPLACE INTO metadata (key, value) VALUES ('version', ?)", version
		},
	}

	ctx := t.Context()
	err := Migrate(ctx, sqladapter.AdaptDatabaseSQLConn(db), opts, logger)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to ensure metadata table exists")
}

func TestMigrate_InvalidMigrationLevel(t *testing.T) {
	db := getTestDB(t)
	t.Cleanup(func() { _ = db.Close() })

	setupMetadataTable(t, db)

	// Set invalid migration level
	_, err := db.Exec("INSERT INTO metadata (key, value) VALUES ('version', 'invalid')")
	require.NoError(t, err)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	opts := MigrationOptions{
		Migrations: []MigrationFn{
			func(ctx context.Context) error {
				return nil
			},
		},
		GetVersionQuery: "SELECT value FROM metadata WHERE key = 'version'",
		UpdateVersionQuery: func(version string) (string, any) {
			return "REPLACE INTO metadata (key, value) VALUES ('version', ?)", version
		},
	}

	ctx := t.Context()
	err = Migrate(ctx, sqladapter.AdaptDatabaseSQLConn(db), opts, logger)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid migration level")
}

func TestMigrate_NegativeMigrationLevel(t *testing.T) {
	db := getTestDB(t)
	t.Cleanup(func() { _ = db.Close() })

	setupMetadataTable(t, db)

	// Set negative migration level
	_, err := db.Exec("INSERT INTO metadata (key, value) VALUES ('version', '-1')")
	require.NoError(t, err)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	opts := MigrationOptions{
		Migrations: []MigrationFn{
			func(ctx context.Context) error {
				return nil
			},
		},
		GetVersionQuery: "SELECT value FROM metadata WHERE key = 'version'",
		UpdateVersionQuery: func(version string) (string, any) {
			return "REPLACE INTO metadata (key, value) VALUES ('version', ?)", version
		},
	}

	ctx := t.Context()
	err = Migrate(ctx, sqladapter.AdaptDatabaseSQLConn(db), opts, logger)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid migration level")
}

func TestMigrate_AlreadyAtLatestVersion(t *testing.T) {
	db := getTestDB(t)
	t.Cleanup(func() { _ = db.Close() })

	setupMetadataTable(t, db)

	// Pre-set migration level to match number of migrations
	_, err := db.Exec("INSERT INTO metadata (key, value) VALUES ('version', '3')")
	require.NoError(t, err)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	executed := []int{}
	opts := MigrationOptions{
		Migrations: []MigrationFn{
			func(ctx context.Context) error {
				executed = append(executed, 1)
				return nil
			},
			func(ctx context.Context) error {
				executed = append(executed, 2)
				return nil
			},
			func(ctx context.Context) error {
				executed = append(executed, 3)
				return nil
			},
		},
		GetVersionQuery: "SELECT value FROM metadata WHERE key = 'version'",
		UpdateVersionQuery: func(version string) (string, any) {
			return "REPLACE INTO metadata (key, value) VALUES ('version', ?)", version
		},
	}

	ctx := t.Context()
	err = Migrate(ctx, sqladapter.AdaptDatabaseSQLConn(db), opts, logger)
	require.NoError(t, err)

	// No migrations should have run
	assert.Empty(t, executed)
}

func TestMigrate_PartialMigrationFailure(t *testing.T) {
	db := getTestDB(t)
	t.Cleanup(func() { _ = db.Close() })

	setupMetadataTable(t, db)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	executed := []int{}
	opts := MigrationOptions{
		Migrations: []MigrationFn{
			func(ctx context.Context) error {
				executed = append(executed, 1)
				return nil
			},
			func(ctx context.Context) error {
				executed = append(executed, 2)
				return fmt.Errorf("migration 2 failed")
			},
			func(ctx context.Context) error {
				executed = append(executed, 3)
				return nil
			},
		},
		GetVersionQuery: "SELECT value FROM metadata WHERE key = 'version'",
		UpdateVersionQuery: func(version string) (string, any) {
			return "REPLACE INTO metadata (key, value) VALUES ('version', ?)", version
		},
	}

	ctx := t.Context()
	err := Migrate(ctx, sqladapter.AdaptDatabaseSQLConn(db), opts, logger)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "migration 2 failed")

	// Only first two migrations should have attempted
	assert.Equal(t, []int{1, 2}, executed)

	// Version should be at 1 (last successful)
	var version string
	err = db.QueryRowContext(ctx, "SELECT value FROM metadata WHERE key = 'version'").Scan(&version)
	require.NoError(t, err)
	assert.Equal(t, "1", version)
}
