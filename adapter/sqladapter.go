package adapter

import (
	"context"

	"github.com/italypaleale/go-sql-utils/adapter/internal"
)

// DatabaseConn is the interface matched by all adapters.
type DatabaseConn interface {
	Begin(ctx context.Context) (internal.DatabaseConnTx, error)
	QueryRow(ctx context.Context, query string, args ...any) internal.DatabaseConnRow
	Exec(ctx context.Context, query string, args ...any) (int64, error)
	IsNoRowsError(err error) bool
}
