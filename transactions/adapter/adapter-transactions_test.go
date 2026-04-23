package adaptertransactions

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/italypaleale/go-sql-utils/adapter"
	postgresadapter "github.com/italypaleale/go-sql-utils/adapter/postgres"
	sqladapter "github.com/italypaleale/go-sql-utils/adapter/sql"

	// Blank import for the SQLite driver
	_ "modernc.org/sqlite"
)

func getTestSQLiteDB(t *testing.T) *sql.DB {
	t.Helper()

	hash := sha256.Sum256([]byte(t.Name()))
	dbName := hex.EncodeToString(hash[:])

	db, err := sql.Open("sqlite", "file:"+dbName+"?mode=memory")
	require.NoError(t, err)

	err = db.Ping()
	require.NoError(t, err)

	return db
}

func getTestPostgresDB(t *testing.T) *pgxpool.Pool {
	t.Helper()

	connStr := os.Getenv("POSTGRES_TEST_DB")
	if connStr == "" {
		t.Skip("POSTGRES_TEST_DB environment variable not set")
	}

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, connStr)
	require.NoError(t, err)

	err = pool.Ping(ctx)
	require.NoError(t, err)

	return pool
}

func TestExecuteInTransaction_SQL_Success(t *testing.T) {
	db := getTestSQLiteDB(t)
	t.Cleanup(func() { _ = db.Close() })

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	// Create a test table
	_, err := db.ExecContext(t.Context(), "CREATE TABLE test_tx_success (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)")
	require.NoError(t, err)

	conn := sqladapter.AdaptDatabaseSQLConn(db)
	result, err := ExecuteInTransaction(t.Context(), logger, conn, 30*time.Second, func(ctx context.Context, tx adapter.Querier) (string, error) {
		_, err := tx.Exec(ctx, "INSERT INTO test_tx_success (name) VALUES (?)", "test")
		if err != nil {
			return "", err
		}
		return "success", nil
	})

	require.NoError(t, err)
	assert.Equal(t, "success", result)

	// Verify data was committed
	var name string
	err = db.
		QueryRowContext(t.Context(), "SELECT name FROM test_tx_success WHERE name = ?", "test").
		Scan(&name)
	require.NoError(t, err)
	assert.Equal(t, "test", name)
}

func TestExecuteInTransaction_SQL_Rollback(t *testing.T) {
	db := getTestSQLiteDB(t)
	t.Cleanup(func() { _ = db.Close() })

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	// Create a test table
	_, err := db.ExecContext(t.Context(), "CREATE TABLE test_tx_rollback (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)")
	require.NoError(t, err)

	conn := sqladapter.AdaptDatabaseSQLConn(db)
	expectedErr := errors.New("intentional error")
	result, err := ExecuteInTransaction(t.Context(), logger, conn, 30*time.Second, func(ctx context.Context, tx adapter.Querier) (string, error) {
		_, err := tx.Exec(ctx, "INSERT INTO test_tx_rollback (name) VALUES (?)", "test_rollback")
		if err != nil {
			return "", err
		}
		return "", expectedErr
	})

	require.Error(t, err)
	assert.Equal(t, expectedErr, err)
	assert.Equal(t, "", result)

	// Verify data was not committed (rollback happened)
	var count int
	err = db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM test_tx_rollback").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 0, count)
}

func TestExecuteInTransaction_SQL_MultipleOperations(t *testing.T) {
	db := getTestSQLiteDB(t)
	t.Cleanup(func() { _ = db.Close() })

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	// Create a test table
	_, err := db.ExecContext(t.Context(), "CREATE TABLE test_tx_multi (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)")
	require.NoError(t, err)

	conn := sqladapter.AdaptDatabaseSQLConn(db)
	result, err := ExecuteInTransaction(t.Context(), logger, conn, 30*time.Second, func(ctx context.Context, tx adapter.Querier) (int, error) {
		for i := 1; i <= 5; i++ {
			_, err := tx.Exec(ctx, "INSERT INTO test_tx_multi (name) VALUES (?)", "test")
			if err != nil {
				return 0, err
			}
		}
		return 5, nil
	})

	require.NoError(t, err)
	assert.Equal(t, 5, result)

	// Verify all data was committed
	var count int
	err = db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM test_tx_multi").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 5, count)
}

func TestExecuteInTransaction_SQL_QueryWithinTransaction(t *testing.T) {
	db := getTestSQLiteDB(t)
	t.Cleanup(func() { _ = db.Close() })

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	// Create a test table
	_, err := db.ExecContext(t.Context(), "CREATE TABLE test_tx_query (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)")
	require.NoError(t, err)

	conn := sqladapter.AdaptDatabaseSQLConn(db)
	result, err := ExecuteInTransaction(t.Context(), logger, conn, 30*time.Second, func(ctx context.Context, tx adapter.Querier) (string, error) {
		// Insert
		_, err := tx.Exec(ctx, "INSERT INTO test_tx_query (name) VALUES (?)", "query_test")
		if err != nil {
			return "", err
		}

		// Query within same transaction
		var name string
		err = tx.QueryRow(ctx, "SELECT name FROM test_tx_query WHERE name = ?", "query_test").Scan(&name)
		if err != nil {
			return "", err
		}

		return name, nil
	})

	require.NoError(t, err)
	assert.Equal(t, "query_test", result)
}

func TestExecuteInTransaction_Postgres_Success(t *testing.T) {
	pool := getTestPostgresDB(t)
	t.Cleanup(func() { pool.Close() })

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	// Create a test table
	_, err := pool.Exec(t.Context(), "CREATE TEMPORARY TABLE test_pgx_tx_success (id SERIAL PRIMARY KEY, name TEXT)")
	require.NoError(t, err)

	conn := postgresadapter.AdaptPgxConn(pool)
	result, err := ExecuteInTransaction(t.Context(), logger, conn, 30*time.Second, func(ctx context.Context, tx adapter.Querier) (string, error) {
		_, err := tx.Exec(ctx, "INSERT INTO test_pgx_tx_success (name) VALUES ($1)", "test")
		if err != nil {
			return "", err
		}
		return "success", nil
	})

	require.NoError(t, err)
	assert.Equal(t, "success", result)

	// Verify data was committed
	var name string
	err = pool.QueryRow(t.Context(), "SELECT name FROM test_pgx_tx_success WHERE name = $1", "test").Scan(&name)
	require.NoError(t, err)
	assert.Equal(t, "test", name)
}

func TestExecuteInTransaction_Postgres_Rollback(t *testing.T) {
	pool := getTestPostgresDB(t)
	t.Cleanup(func() { pool.Close() })

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	// Create a test table
	_, err := pool.Exec(t.Context(), "CREATE TEMPORARY TABLE test_pgx_tx_rollback (id SERIAL PRIMARY KEY, name TEXT)")
	require.NoError(t, err)

	conn := postgresadapter.AdaptPgxConn(pool)
	expectedErr := errors.New("intentional error")
	result, err := ExecuteInTransaction(t.Context(), logger, conn, 30*time.Second, func(ctx context.Context, tx adapter.Querier) (string, error) {
		_, err := tx.Exec(ctx, "INSERT INTO test_pgx_tx_rollback (name) VALUES ($1)", "test_rollback")
		if err != nil {
			return "", err
		}
		return "", expectedErr
	})

	require.Error(t, err)
	require.ErrorIs(t, err, expectedErr)
	assert.Equal(t, "", result)

	// Verify data was not committed (rollback happened)
	var count int
	err = pool.
		QueryRow(t.Context(), "SELECT COUNT(*) FROM test_pgx_tx_rollback").
		Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 0, count)
}

func TestExecuteInTransaction_Postgres_MultipleOperations(t *testing.T) {
	pool := getTestPostgresDB(t)
	t.Cleanup(func() { pool.Close() })

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	// Create a test table
	_, err := pool.Exec(t.Context(), "CREATE TEMPORARY TABLE test_pgx_tx_multi (id SERIAL PRIMARY KEY, name TEXT)")
	require.NoError(t, err)

	conn := postgresadapter.AdaptPgxConn(pool)
	result, err := ExecuteInTransaction(t.Context(), logger, conn, 30*time.Second, func(ctx context.Context, tx adapter.Querier) (int, error) {
		for i := 1; i <= 5; i++ {
			_, rErr := tx.Exec(ctx, "INSERT INTO test_pgx_tx_multi (name) VALUES ($1)", "test")
			if rErr != nil {
				return 0, rErr
			}
		}
		return 5, nil
	})

	require.NoError(t, err)
	assert.Equal(t, 5, result)

	// Verify all data was committed
	var count int
	err = pool.
		QueryRow(t.Context(), "SELECT COUNT(*) FROM test_pgx_tx_multi").
		Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 5, count)
}
