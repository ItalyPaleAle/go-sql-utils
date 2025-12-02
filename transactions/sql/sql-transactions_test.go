package sqltransactions

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"log/slog"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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

func TestExecuteInTransaction_Success(t *testing.T) {
	db := getTestSQLiteDB(t)
	t.Cleanup(func() { _ = db.Close() })

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	// Create a test table
	_, err := db.ExecContext(t.Context(), "CREATE TABLE test_tx_success (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)")
	require.NoError(t, err)

	result, err := ExecuteInTransaction(t.Context(), logger, db, func(ctx context.Context, tx *sql.Tx) (string, error) {
		_, err := tx.ExecContext(ctx, "INSERT INTO test_tx_success (name) VALUES (?)", "test")
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

func TestExecuteInTransaction_Rollback(t *testing.T) {
	db := getTestSQLiteDB(t)
	t.Cleanup(func() { _ = db.Close() })

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	// Create a test table
	_, err := db.ExecContext(t.Context(), "CREATE TABLE test_tx_rollback (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)")
	require.NoError(t, err)

	expectedErr := errors.New("intentional error")
	result, err := ExecuteInTransaction(t.Context(), logger, db, func(ctx context.Context, tx *sql.Tx) (string, error) {
		_, err := tx.ExecContext(ctx, "INSERT INTO test_tx_rollback (name) VALUES (?)", "test_rollback")
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

func TestExecuteInTransaction_MultipleOperations(t *testing.T) {
	db := getTestSQLiteDB(t)
	t.Cleanup(func() { _ = db.Close() })

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	// Create a test table
	_, err := db.ExecContext(t.Context(), "CREATE TABLE test_tx_multi (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)")
	require.NoError(t, err)

	result, err := ExecuteInTransaction(t.Context(), logger, db, func(ctx context.Context, tx *sql.Tx) (int, error) {
		for i := 1; i <= 5; i++ {
			_, err := tx.ExecContext(ctx, "INSERT INTO test_tx_multi (name) VALUES (?)", "test")
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

func TestExecuteInTransaction_QueryWithinTransaction(t *testing.T) {
	db := getTestSQLiteDB(t)
	t.Cleanup(func() { _ = db.Close() })

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	// Create a test table
	_, err := db.ExecContext(t.Context(), "CREATE TABLE test_tx_query (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)")
	require.NoError(t, err)

	result, err := ExecuteInTransaction(t.Context(), logger, db, func(ctx context.Context, tx *sql.Tx) (string, error) {
		// Insert
		_, err := tx.ExecContext(ctx, "INSERT INTO test_tx_query (name) VALUES (?)", "query_test")
		if err != nil {
			return "", err
		}

		// Query within same transaction
		var name string
		err = tx.QueryRowContext(ctx, "SELECT name FROM test_tx_query WHERE name = ?", "query_test").Scan(&name)
		if err != nil {
			return "", err
		}

		return name, nil
	})

	require.NoError(t, err)
	assert.Equal(t, "query_test", result)
}
