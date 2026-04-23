// This code was adapted from https://github.com/dapr/components-contrib/blob/v1.14.6/
// Copyright (C) 2023 The Dapr Authors
// License: Apache2

package adaptertransactions

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/italypaleale/go-sql-utils/adapter"
)

// ExecuteInTransaction executes a function in a transaction for the adapter.
// If the handler returns an error, the transaction is rolled back automatically.
// Note that the context for pgx is tied to the begin command only, while it's tied to the entire transaction for SQL adapters
func ExecuteInTransaction[T any](ctx context.Context, log *slog.Logger, db adapter.DatabaseConn, timeout time.Duration, fn func(ctx context.Context, tx adapter.Querier) (T, error)) (res T, err error) {
	// Start the transaction
	queryCtx, queryCancel := context.WithTimeout(ctx, timeout)
	defer queryCancel()
	tx, err := db.Begin(queryCtx)
	if err != nil {
		return res, fmt.Errorf("failed to begin transaction: %w", err)
	}

	// Rollback in case of failure
	var success bool
	defer func() {
		if success {
			return
		}
		rollbackCtx, rollbackCancel := context.WithTimeout(ctx, timeout)
		defer rollbackCancel()
		rollbackErr := tx.Rollback(rollbackCtx)
		if rollbackErr != nil {
			// Log errors only
			log.ErrorContext(ctx, "Error while attempting to roll back transaction", slog.Any("error", rollbackErr))
		}
	}()

	// Execute the action
	res, err = fn(ctx, tx)
	if err != nil {
		return res, err
	}

	// Commit the transaction
	queryCtx, queryCancel = context.WithTimeout(ctx, timeout)
	defer queryCancel()
	err = tx.Commit(queryCtx)
	if err != nil {
		return res, fmt.Errorf("failed to commit transaction: %w", err)
	}
	success = true

	return res, nil
}
