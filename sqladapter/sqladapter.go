// This code was adapted from https://github.com/dapr/components-contrib/blob/v1.14.6/
// Copyright (C) 2023 The Dapr Authors
// License: Apache2

package sqladapter

import (
	"context"

	"github.com/italypaleale/go-sql-utils/sqladapter/internal"
)

// DatabaseConn is the interface matched by all adapters.
type DatabaseConn interface {
	Begin(ctx context.Context) (internal.DatabaseConnTx, error)
	QueryRow(ctx context.Context, query string, args ...any) internal.DatabaseConnRow
	Exec(ctx context.Context, query string, args ...any) (int64, error)
	IsNoRowsError(err error) bool
}
