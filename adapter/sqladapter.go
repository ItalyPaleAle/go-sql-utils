package adapter

import (
	"context"

	"github.com/italypaleale/go-sql-utils/adapter/internal"
)

// Querier is the interface matched by all adapters and transactions
type Querier = internal.Querier

// DatabaseConn is the interface matched by all adapters.
type DatabaseConn interface {
	internal.Querier

	Begin(ctx context.Context) (internal.DatabaseConnTx, error)
}
