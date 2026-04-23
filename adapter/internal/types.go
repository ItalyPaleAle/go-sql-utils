package internal

import (
	"context"
)

type DatabaseConnRow interface {
	Scan(into ...any) error
}

type DatabaseConnRows interface {
	Close() error
	Err() error
	Next() bool
	Scan(dest ...any) error
}

type DatabaseConnTx interface {
	Querier

	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
}

type DbHelpers interface {
	IsTransaction() bool
	IsNoRowsError(err error) bool
	IsTxDoneError(err error) bool
}

type Querier interface {
	DbHelpers

	Query(ctx context.Context, query string, args ...any) (DatabaseConnRows, error)
	QueryRow(ctx context.Context, query string, args ...any) DatabaseConnRow
	Exec(ctx context.Context, query string, args ...any) (int64, error)
}
