package postgrestransactions

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

func TestExecuteInTransaction_Success(t *testing.T) {
	pool := getTestPostgresDB(t)
	t.Cleanup(func() { pool.Close() })

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	// Create a test table
	_, err := pool.Exec(t.Context(), "CREATE TEMPORARY TABLE test_pgx_tx_success (id SERIAL PRIMARY KEY, name TEXT)")
	require.NoError(t, err)

	result, err := ExecuteInTransaction(t.Context(), logger, pool, 30*time.Second, func(ctx context.Context, tx pgx.Tx) (string, error) {
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

func TestExecuteInTransaction_Rollback(t *testing.T) {
	pool := getTestPostgresDB(t)
	t.Cleanup(func() { pool.Close() })

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	// Create a test table
	_, err := pool.Exec(t.Context(), "CREATE TEMPORARY TABLE test_pgx_tx_rollback (id SERIAL PRIMARY KEY, name TEXT)")
	require.NoError(t, err)

	expectedErr := errors.New("intentional error")
	result, err := ExecuteInTransaction(t.Context(), logger, pool, 30*time.Second, func(ctx context.Context, tx pgx.Tx) (string, error) {
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

func TestExecuteInTransaction_MultipleOperations(t *testing.T) {
	pool := getTestPostgresDB(t)
	t.Cleanup(func() { pool.Close() })

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	// Create a test table
	_, err := pool.Exec(t.Context(), "CREATE TEMPORARY TABLE test_pgx_tx_multi (id SERIAL PRIMARY KEY, name TEXT)")
	require.NoError(t, err)

	result, err := ExecuteInTransaction(t.Context(), logger, pool, 30*time.Second, func(ctx context.Context, tx pgx.Tx) (int, error) {
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
