package cleanup

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"log/slog"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	testingclock "k8s.io/utils/clock/testing"

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

func setupTestTables(t *testing.T, db *sql.DB) {
	t.Helper()

	// Create metadata table for tracking last cleanup
	_, err := db.ExecContext(t.Context(), `
		CREATE TABLE IF NOT EXISTS metadata (
			key TEXT NOT NULL PRIMARY KEY,
			value TEXT NOT NULL
		)
	`)
	require.NoError(t, err)

	// Insert initial last_cleanup value
	_, err = db.ExecContext(t.Context(), `INSERT INTO metadata (key, value) VALUES ('last_cleanup', '0')`)
	require.NoError(t, err)

	// Create a table with expirable data
	_, err = db.ExecContext(t.Context(), `
		CREATE TABLE IF NOT EXISTS sessions (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			expires_at INTEGER NOT NULL
		)
	`)
	require.NoError(t, err)
}

func TestScheduleGarbageCollector_NilDB(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	gc, err := ScheduleGarbageCollector(GCOptions{
		Logger: logger,
		DB:     nil,
	})

	require.Error(t, err)
	require.ErrorContains(t, err, "property DB must be provided")
	assert.Nil(t, gc)
}

func TestScheduleGarbageCollector_Success(t *testing.T) {
	db := getTestDB(t)
	t.Cleanup(func() { _ = db.Close() })

	setupTestTables(t, db)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	gc, err := ScheduleGarbageCollector(GCOptions{
		Logger:          logger,
		DB:              sqladapter.AdaptDatabaseSQLConn(db),
		CleanupInterval: 0, // Disable background task for this test
		UpdateLastCleanupQuery: func(arg any) (string, []any) {
			return `UPDATE metadata SET value = ? WHERE key = 'last_cleanup' AND CAST(value AS INTEGER) < ?`, []any{time.Now().UnixMilli(), arg}
		},
		DeleteExpiredValuesQueries: map[string]DeleteExpiredValuesQueryFn{
			"sessions": func() (string, func() []any) {
				return "DELETE FROM sessions WHERE expires_at < ?", func() []any {
					return []any{time.Now().Unix()}
				}
			},
		},
	})

	require.NoError(t, err)
	require.NotNil(t, gc)

	err = gc.Close()
	require.NoError(t, err)
}

func TestGarbageCollector_CleanupExpired(t *testing.T) {
	db := getTestDB(t)
	t.Cleanup(func() { _ = db.Close() })

	setupTestTables(t, db)

	// Insert some expired sessions
	now := time.Now().Unix()
	_, err := db.ExecContext(t.Context(), "INSERT INTO sessions (expires_at) VALUES (?)", now-3600) // expired 1 hour ago
	require.NoError(t, err)
	_, err = db.ExecContext(t.Context(), "INSERT INTO sessions (expires_at) VALUES (?)", now-7200) // expired 2 hours ago
	require.NoError(t, err)
	_, err = db.ExecContext(t.Context(), "INSERT INTO sessions (expires_at) VALUES (?)", now+3600) // expires in 1 hour (not expired)
	require.NoError(t, err)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	gc, err := ScheduleGarbageCollector(GCOptions{
		Logger:              logger,
		DB:                  sqladapter.AdaptDatabaseSQLConn(db),
		CleanupInterval:     time.Hour, // Set a non-zero interval for the cleanup check
		CleanupQueryTimeout: 5 * time.Second,
		UpdateLastCleanupQuery: func(arg any) (string, []any) {
			return `UPDATE metadata SET value = ? WHERE key = 'last_cleanup' AND CAST(value AS INTEGER) < ?`, []any{time.Now().UnixMilli(), arg}
		},
		DeleteExpiredValuesQueries: map[string]DeleteExpiredValuesQueryFn{
			"sessions": func() (string, func() []any) {
				return "DELETE FROM sessions WHERE expires_at < ?", func() []any {
					return []any{time.Now().Unix()}
				}
			},
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = gc.Close() })

	// Manually trigger cleanup
	err = gc.CleanupExpired()
	require.NoError(t, err)

	// Verify only non-expired session remains
	var count int
	err = db.
		QueryRowContext(t.Context(), "SELECT COUNT(*) FROM sessions").
		Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 1, count)
}

func TestGarbageCollector_SkipsRecentCleanup(t *testing.T) {
	db := getTestDB(t)
	t.Cleanup(func() { _ = db.Close() })

	setupTestTables(t, db)

	// Set last_cleanup to recent time
	_, err := db.ExecContext(t.Context(), "UPDATE metadata SET value = ? WHERE key = 'last_cleanup'", time.Now().UnixMilli())
	require.NoError(t, err)

	// Insert an expired session
	now := time.Now().Unix()
	_, err = db.ExecContext(t.Context(), "INSERT INTO sessions (expires_at) VALUES (?)", now-3600)
	require.NoError(t, err)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	gc, err := ScheduleGarbageCollector(GCOptions{
		Logger:              logger,
		DB:                  sqladapter.AdaptDatabaseSQLConn(db),
		CleanupInterval:     time.Hour,
		CleanupQueryTimeout: 5 * time.Second,
		UpdateLastCleanupQuery: func(arg any) (string, []any) {
			// This query will not update if last cleanup was too recent
			return `UPDATE metadata SET value = ? WHERE key = 'last_cleanup' AND CAST(value AS INTEGER) < ?`, []any{time.Now().UnixMilli(), arg}
		},
		DeleteExpiredValuesQueries: map[string]DeleteExpiredValuesQueryFn{
			"sessions": func() (string, func() []any) {
				return "DELETE FROM sessions WHERE expires_at < ?", func() []any {
					return []any{time.Now().Unix()}
				}
			},
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = gc.Close() })

	// Manually trigger cleanup - should skip since last cleanup was recent
	err = gc.CleanupExpired()
	require.NoError(t, err)

	// Verify expired session still exists (cleanup was skipped)
	var count int
	err = db.
		QueryRowContext(t.Context(), "SELECT COUNT(*) FROM sessions").
		Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 1, count)
}

func TestGarbageCollector_Close(t *testing.T) {
	db := getTestDB(t)
	t.Cleanup(func() { _ = db.Close() })

	setupTestTables(t, db)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	gc, err := ScheduleGarbageCollector(GCOptions{
		Logger:          logger,
		DB:              sqladapter.AdaptDatabaseSQLConn(db),
		CleanupInterval: time.Hour,
		UpdateLastCleanupQuery: func(arg any) (string, []any) {
			return `UPDATE metadata SET value = ? WHERE key = 'last_cleanup' AND CAST(value AS INTEGER) < ?`, []any{time.Now().UnixMilli(), arg}
		},
		DeleteExpiredValuesQueries: map[string]DeleteExpiredValuesQueryFn{},
	})
	require.NoError(t, err)

	// Close should work without error
	err = gc.Close()
	require.NoError(t, err)

	// Double close should also work (idempotent)
	err = gc.Close()
	require.NoError(t, err)
}

func TestGarbageCollector_DefaultTimeout(t *testing.T) {
	db := getTestDB(t)
	t.Cleanup(func() { _ = db.Close() })

	setupTestTables(t, db)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	gc, err := ScheduleGarbageCollector(GCOptions{
		Logger:              logger,
		DB:                  sqladapter.AdaptDatabaseSQLConn(db),
		CleanupInterval:     0, // Disable background task
		CleanupQueryTimeout: 0, // Should default to 5 minutes
		UpdateLastCleanupQuery: func(arg any) (string, []any) {
			return `UPDATE metadata SET value = ? WHERE key = 'last_cleanup' AND CAST(value AS INTEGER) < ?`, []any{time.Now().UnixMilli(), arg}
		},
		DeleteExpiredValuesQueries: map[string]DeleteExpiredValuesQueryFn{},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = gc.Close() })

	// Just verify it was created successfully with default timeout
	require.NotNil(t, gc)
}

func TestGarbageCollector_CustomClock(t *testing.T) {
	db := getTestDB(t)
	t.Cleanup(func() { _ = db.Close() })

	setupTestTables(t, db)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	// Use a fake clock for testing
	fakeClock := testingclock.NewFakeClock(time.Now())

	cleanupCount := &atomic.Int32{}

	gc, err := ScheduleGarbageCollector(GCOptions{
		Logger:              logger,
		DB:                  sqladapter.AdaptDatabaseSQLConn(db),
		CleanupInterval:     time.Hour,
		CleanupQueryTimeout: 5 * time.Second,
		Clock:               fakeClock,
		UpdateLastCleanupQuery: func(arg any) (string, []any) {
			cleanupCount.Add(1)
			return `UPDATE metadata SET value = ? WHERE key = 'last_cleanup' AND CAST(value AS INTEGER) < ?`, []any{time.Now().UnixMilli(), arg}
		},
		DeleteExpiredValuesQueries: map[string]DeleteExpiredValuesQueryFn{},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = gc.Close() })

	// Wait for the goroutine to start and register with the ticker
	time.Sleep(50 * time.Millisecond)

	// Advance clock by cleanup interval to trigger cleanup
	fakeClock.Step(time.Hour + time.Second)

	// Give time for the goroutine to process the tick
	time.Sleep(200 * time.Millisecond)

	// Verify cleanup was triggered
	assert.GreaterOrEqual(t, cleanupCount.Load(), int32(1))
}

func TestGarbageCollector_MultipleDeleteQueries(t *testing.T) {
	db := getTestDB(t)
	t.Cleanup(func() { _ = db.Close() })

	setupTestTables(t, db)

	// Create another table for testing multiple delete queries
	_, err := db.ExecContext(t.Context(), `
		CREATE TABLE IF NOT EXISTS tokens (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			expires_at INTEGER NOT NULL
		)
	`)
	require.NoError(t, err)

	// Insert expired data into both tables
	now := time.Now().Unix()
	_, err = db.ExecContext(t.Context(), "INSERT INTO sessions (expires_at) VALUES (?)", now-3600)
	require.NoError(t, err)
	_, err = db.ExecContext(t.Context(), "INSERT INTO tokens (expires_at) VALUES (?)", now-3600)
	require.NoError(t, err)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	gc, err := ScheduleGarbageCollector(GCOptions{
		Logger:              logger,
		DB:                  sqladapter.AdaptDatabaseSQLConn(db),
		CleanupInterval:     time.Hour,
		CleanupQueryTimeout: 5 * time.Second,
		UpdateLastCleanupQuery: func(arg any) (string, []any) {
			return `UPDATE metadata SET value = ? WHERE key = 'last_cleanup' AND CAST(value AS INTEGER) < ?`, []any{time.Now().UnixMilli(), arg}
		},
		DeleteExpiredValuesQueries: map[string]DeleteExpiredValuesQueryFn{
			"sessions": func() (string, func() []any) {
				return "DELETE FROM sessions WHERE expires_at < ?", func() []any {
					return []any{time.Now().Unix()}
				}
			},
			"tokens": func() (string, func() []any) {
				return "DELETE FROM tokens WHERE expires_at < ?", func() []any {
					return []any{time.Now().Unix()}
				}
			},
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = gc.Close() })

	// Manually trigger cleanup
	err = gc.CleanupExpired()
	require.NoError(t, err)

	// Verify both tables were cleaned
	var sessionCount, tokenCount int
	err = db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM sessions").Scan(&sessionCount)
	require.NoError(t, err)
	assert.Equal(t, 0, sessionCount)

	err = db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM tokens").Scan(&tokenCount)
	require.NoError(t, err)
	assert.Equal(t, 0, tokenCount)
}

func TestScheduleGarbageCollector_DefaultClock(t *testing.T) {
	db := getTestDB(t)
	t.Cleanup(func() { _ = db.Close() })

	setupTestTables(t, db)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	gc, err := ScheduleGarbageCollector(GCOptions{
		Logger:          logger,
		DB:              sqladapter.AdaptDatabaseSQLConn(db),
		CleanupInterval: 0, // Disable background task
		Clock:           nil,
		UpdateLastCleanupQuery: func(arg any) (string, []any) {
			return `UPDATE metadata SET value = ? WHERE key = 'last_cleanup' AND CAST(value AS INTEGER) < ?`, []any{time.Now().UnixMilli(), arg}
		},
		DeleteExpiredValuesQueries: map[string]DeleteExpiredValuesQueryFn{},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = gc.Close() })

	// Just verify it was created successfully with default clock
	require.NotNil(t, gc)
}
