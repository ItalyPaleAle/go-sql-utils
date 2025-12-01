package databasesqladapter

import (
	"context"
	"database/sql"
	"errors"

	"github.com/italypaleale/go-sql-utils/sqladapter"
	"github.com/italypaleale/go-sql-utils/sqladapter/internal"
)

// AdaptDatabaseSQLConn returns a databaseConn based on a database/sql connection.
//
// Note: when using transactions with database/sql, the context passed to Begin impacts the entire transaction.
// Canceling the context automatically rolls back the transaction.
func AdaptDatabaseSQLConn(db DatabaseSQLConn) sqladapter.DatabaseConn {
	return &DatabaseSQLAdapter{db}
}

// DatabaseSQLConn is the interface for connections that use database/sql.
type DatabaseSQLConn interface {
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// DatabaseSQLAdapter is an adapter for database/sql connections.
type DatabaseSQLAdapter struct {
	conn DatabaseSQLConn
}

func (sqla *DatabaseSQLAdapter) Begin(ctx context.Context) (internal.DatabaseConnTx, error) {
	tx, err := sqla.conn.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}

	return &databaseSQLTxAdapter{tx}, nil
}

func (sqla *DatabaseSQLAdapter) Exec(ctx context.Context, query string, args ...any) (int64, error) {
	res, err := sqla.conn.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (sqla *DatabaseSQLAdapter) QueryRow(ctx context.Context, query string, args ...any) internal.DatabaseConnRow {
	return sqla.conn.QueryRowContext(ctx, query, args...)
}

func (sqla *DatabaseSQLAdapter) IsNoRowsError(err error) bool {
	return errors.Is(err, sql.ErrNoRows)
}

type databaseSQLTxAdapter struct {
	tx *sql.Tx
}

func (sqltx *databaseSQLTxAdapter) Rollback(_ context.Context) error {
	return sqltx.tx.Rollback()
}

func (sqltx *databaseSQLTxAdapter) Commit(_ context.Context) error {
	return sqltx.tx.Commit()
}

func (sqltx *databaseSQLTxAdapter) Exec(ctx context.Context, query string, args ...any) (int64, error) {
	res, err := sqltx.tx.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (sqltx *databaseSQLTxAdapter) QueryRow(ctx context.Context, query string, args ...any) internal.DatabaseConnRow {
	return sqltx.tx.QueryRowContext(ctx, query, args...)
}
