package sqltransactions

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
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
	assert.Empty(t, result)

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

// getTestSQLiteFileDB opens a file database in WAL mode with _txlock=immediate, so a regular transaction takes the write lock at BEGIN while a read-only one does not
func getTestSQLiteFileDB(t *testing.T) *sql.DB {
	t.Helper()

	// A short busy timeout makes a blocked writer fail quickly instead of waiting
	connString := "file:" + filepath.Join(t.TempDir(), "test.db") + "?_txlock=immediate&_pragma=journal_mode(WAL)&_pragma=busy_timeout(200)"
	db, err := sql.Open("sqlite", connString)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	_, err = db.ExecContext(t.Context(), "CREATE TABLE test_ro (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)")
	require.NoError(t, err)
	_, err = db.ExecContext(t.Context(), "INSERT INTO test_ro (name) VALUES ('first')")
	require.NoError(t, err)

	return db
}

// rowQuerier is implemented by both *sql.DB and *sql.Tx
type rowQuerier interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

func countRows(ctx context.Context, q rowQuerier) (int, error) {
	var count int
	err := q.QueryRowContext(ctx, "SELECT COUNT(*) FROM test_ro").Scan(&count)
	return count, err
}

func TestExecuteInReadOnlyTransaction(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	t.Run("does not block writers and sees a consistent snapshot", func(t *testing.T) {
		db := getTestSQLiteFileDB(t)

		counts, err := ExecuteInReadOnlyTransaction(t.Context(), logger, db, func(ctx context.Context, tx *sql.Tx) ([2]int, error) {
			var res [2]int

			// The first read starts the snapshot
			var err error
			res[0], err = countRows(ctx, tx)
			if err != nil {
				return res, err
			}

			// Another connection can write while the transaction is open, because it holds no write lock
			_, err = db.ExecContext(ctx, "INSERT INTO test_ro (name) VALUES ('second')")
			if err != nil {
				return res, err
			}

			// The transaction still sees the snapshot from before the write
			res[1], err = countRows(ctx, tx)
			return res, err
		})
		require.NoError(t, err)
		assert.Equal(t, [2]int{1, 1}, counts)

		// The write is visible once the transaction is over
		count, err := countRows(t.Context(), db)
		require.NoError(t, err)
		assert.Equal(t, 2, count)
	})

	t.Run("a regular transaction blocks writers", func(t *testing.T) {
		// Confirms the test above is meaningful: with _txlock=immediate, a regular transaction holds the write lock from BEGIN
		db := getTestSQLiteFileDB(t)

		_, err := ExecuteInTransaction(t.Context(), logger, db, func(ctx context.Context, tx *sql.Tx) (struct{}, error) {
			_, err := countRows(ctx, tx)
			if err != nil {
				return struct{}{}, err
			}

			_, err = db.ExecContext(ctx, "INSERT INTO test_ro (name) VALUES ('second')")
			return struct{}{}, err
		})
		require.Error(t, err)
		assert.ErrorContains(t, err, "SQLITE_BUSY")
	})

	t.Run("returns the error of the function", func(t *testing.T) {
		db := getTestSQLiteFileDB(t)

		expectedErr := errors.New("intentional error")
		_, err := ExecuteInReadOnlyTransaction(t.Context(), logger, db, func(ctx context.Context, tx *sql.Tx) (int, error) {
			return 0, expectedErr
		})
		require.ErrorIs(t, err, expectedErr)
	})
}
