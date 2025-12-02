# go-sql-utils

A collection of Go utilities for working with SQL databases, providing utilities for migrations, transactions, and scheduled cleanup operations.

```bash
go get github.com/italypaleale/go-sql-utils
```

## Packages

- **[migrations](#migrations)**: Database schema versioning for Postgres and SQLite, with concurrency support
- **[cleanup](#cleanup)**: Scheduled garbage collection for expired database records, optimized for concurrency in distributed systems
- **[transactions](#transactions)**: Transaction helpers with automatic rollback
- **sqladapter** (utility): Unified database interface for `database/sql` and `pgx` drivers

---

## migrations

The migrations package manages database schema versioning with support for resumable migrations and concurrency safety. It is safe to run multiple instances of apps performing migrations concurrently.

Each migration is defined in a function written in Go. It can execute SQL statements and/or perform any other task related to database migrations.

### PostgreSQL

Supports Postgres using the [pgx v5](https://pkg.go.dev/github.com/jackc/pgx/v5) driver.

```go
import (
    "github.com/italypaleale/go-sql-utils/migrations"
    postgresmigrations "github.com/italypaleale/go-sql-utils/migrations/postgres"
)

m := postgresmigrations.Migrations{
    DB:                pool, // *pgxpool.Pool
    MetadataTableName: "schema_migrations",
    MetadataKey:       "version",
}

migrationFns := []migrations.MigrationFn{
    // Migration 1: Create users table
    func(ctx context.Context) error {
        _, err := pool.Exec(ctx, `
            CREATE TABLE users (
                id SERIAL PRIMARY KEY,
                email TEXT NOT NULL UNIQUE,
                created_at TIMESTAMPTZ DEFAULT NOW()
            )
        `)
        return err
    },
    // Migration 2: Add name column
    func(ctx context.Context) error {
        _, err := pool.Exec(ctx, `ALTER TABLE users ADD COLUMN name TEXT`)
        return err
    },
}

err := m.Perform(ctx, migrationFns, logger)
```

### SQLite

Supports SQLite using the [modernc.org/sqlite](https://pkg.go.dev/modernc.org/sqlite) driver (does not require CGo)

```go
import (
    "github.com/italypaleale/go-sql-utils/migrations"
    sqlitemigrations "github.com/italypaleale/go-sql-utils/migrations/sqlite"
)

m := &sqlitemigrations.Migrations{
    Pool:              db, // *sql.DB
    MetadataTableName: "migrations",
    MetadataKey:       "version",
}

err := m.Perform(ctx, migrationFns, logger)
```

### How It Works

1. Creates a metadata table (if not exists) to track the current migration level
2. Queries the current migration level from the database
3. Executes only pending migrations in order (if at level 2, starts from migration 3)
4. Updates the migration level after each successful migration
5. If a migration fails, the level remains at the last successful migration

---

## cleanup

The cleanup package provides automated garbage collection for expired data in database tables, with support for distributed locking to coordinate between multiple processes.

### Usage

```go
import (
    "github.com/italypaleale/go-sql-utils/cleanup"
    sqladapter "github.com/italypaleale/go-sql-utils/adapter/sql"
)

gc, err := cleanup.ScheduleGarbageCollector(cleanup.GCOptions{
    Logger: slog.Default(),
    DB:     sqladapter.AdaptDatabaseSQLConn(db),

    // Run cleanup every hour
    CleanupInterval: 1 * time.Hour,

    // Optional: timeout for cleanup queries (default: 5 minutes)
    CleanupQueryTimeout: 5 * time.Minute,

    // Atomic update to track last cleanup time (for distributed coordination)
    UpdateLastCleanupQuery: func(arg any) (string, []any) {
        return `UPDATE metadata
                SET value = ?
                WHERE key = 'last_cleanup'
                AND CAST(value AS INTEGER) < ?`,
            []any{time.Now().UnixMilli(), arg}
    },

    // Define cleanup queries for each table
    DeleteExpiredValuesQueries: map[string]cleanup.DeleteExpiredValuesQueryFn{
        "sessions": func() (string, func() []any) {
            return "DELETE FROM sessions WHERE expires_at < ?",
                func() []any { return []any{time.Now().Unix()} }
        },
        "tokens": func() (string, func() []any) {
            return "DELETE FROM tokens WHERE expires_at < ?",
                func() []any { return []any{time.Now().Unix()} }
        },
    },
})
if err != nil {
    return err
}
defer gc.Close()
```

### How It Works

1. When `CleanupInterval > 0`, a background goroutine runs on a ticker
2. On each tick, it attempts to update the last cleanup time atomically
3. If the update succeeds (another process hasn't cleaned up recently), it executes all delete queries
4. The `UpdateLastCleanupQuery` provides distributed coordination - only one process will perform cleanup

### Manual Cleanup

You can trigger cleanup manually for testing:

```go
err := gc.CleanupExpired()
```

---

## transactions

The transactions package provides helpers for executing database operations within transactions, with automatic rollback on error.

### With database/sql

```go
import sqltransactions "github.com/italypaleale/go-sql-utils/transactions/sql"

result, err := sqltransactions.ExecuteInTransaction(ctx, logger, db,
    func(ctx context.Context, tx *sql.Tx) (int64, error) {
        res, err := tx.ExecContext(ctx,
            "INSERT INTO users (name, email) VALUES (?, ?)",
            "Alice", "alice@example.com")
        if err != nil {
            return 0, err // Transaction will be rolled back
        }
        return res.LastInsertId()
    })
```

### With pgx

The pgx package accepts an additional "timeout" parameter that is used to control the timeout for starting, committing and/or rolling back a transaction.

```go
import postgrestransactions "github.com/italypaleale/go-sql-utils/transactions/postgres"

user, err := postgrestransactions.ExecuteInTransaction(ctx, logger, pool, 30*time.Second,
    func(ctx context.Context, tx pgx.Tx) (*User, error) {
        var user User
        err := tx.QueryRow(ctx,
            "INSERT INTO users (name) VALUES ($1) RETURNING id, name",
            "Bob",
        ).Scan(&user.ID, &user.Name)
        if err != nil {
            return nil, err // Transaction will be rolled back
        }
        return &user, nil
    })
```

### Features

- **Generic return type**: Return any type from your transaction function
- **Automatic rollback**: If your function returns an error, the transaction is rolled back
- **Timeout support** (pgx): The timeout parameter controls query execution time
- **Error logging**: Rollback errors are logged but don't override the original error
