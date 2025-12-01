// This code was adapted from https://github.com/dapr/components-contrib/blob/v1.14.6/
// Copyright (C) 2023 The Dapr Authors
// License: Apache2

package postgresadapter

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/italypaleale/go-sql-utils/sqladapter"
	"github.com/italypaleale/go-sql-utils/sqladapter/internal"
)

// AdaptPgxConn returns a databaseConn based on a pgx connection.
//
// Note: when using transactions with pgx, the context passed to Begin impacts the creation of the transaction only.
func AdaptPgxConn(db PgxConn) sqladapter.DatabaseConn {
	return &PgxAdapter{db}
}

// PgxConn is the interface for connections that use pgx.
type PgxConn interface {
	Begin(ctx context.Context) (pgx.Tx, error)
	Query(ctx context.Context, query string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, query string, args ...any) pgx.Row
	Exec(ctx context.Context, query string, args ...any) (pgconn.CommandTag, error)
}

// Interface that contains methods for querying.
// Applies to *pgx.Conn, *pgxpool.Pool, and pgx.Tx
type PGXQuerier interface {
	Exec(ctx context.Context, query string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, query string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, query string, args ...any) pgx.Row
}

// Interface that applies to *pgxpool.Pool.
type PGXPoolConn interface {
	PGXQuerier

	Begin(ctx context.Context) (pgx.Tx, error)
	BeginTx(ctx context.Context, txOptions pgx.TxOptions) (pgx.Tx, error)
	Ping(ctx context.Context) error
	Close()
}

// PgxAdapter is an adapter for pgx connections.
type PgxAdapter struct {
	conn PgxConn
}

func (pga *PgxAdapter) Begin(ctx context.Context) (internal.DatabaseConnTx, error) {
	tx, err := pga.conn.Begin(ctx)
	if err != nil {
		return nil, err
	}

	return &pgxTxAdapter{tx}, nil
}

func (pga *PgxAdapter) Exec(ctx context.Context, query string, args ...any) (int64, error) {
	res, err := pga.conn.Exec(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected(), nil
}

func (pga *PgxAdapter) QueryRow(ctx context.Context, query string, args ...any) internal.DatabaseConnRow {
	return pga.conn.QueryRow(ctx, query, args...)
}

func (pga *PgxAdapter) IsNoRowsError(err error) bool {
	return errors.Is(err, pgx.ErrNoRows)
}

type pgxTxAdapter struct {
	tx pgx.Tx
}

func (pgtx *pgxTxAdapter) Rollback(ctx context.Context) error {
	return pgtx.tx.Rollback(ctx)
}

func (pgtx *pgxTxAdapter) Commit(ctx context.Context) error {
	return pgtx.tx.Commit(ctx)
}

func (pgtx *pgxTxAdapter) Exec(ctx context.Context, query string, args ...any) (int64, error) {
	res, err := pgtx.tx.Exec(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected(), nil
}

func (pgtx *pgxTxAdapter) QueryRow(ctx context.Context, query string, args ...any) internal.DatabaseConnRow {
	return pgtx.tx.QueryRow(ctx, query, args...)
}
