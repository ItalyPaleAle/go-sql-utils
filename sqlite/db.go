package sqlite

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"

	sqlitedriver "modernc.org/sqlite"
)

// ConnectOpts configures Connect and NewConnector
type ConnectOpts struct {
	// ConnString is a SQLite path or a URL beginning with "file:"
	ConnString string
	// Logger is an optional slog instance used to log setup warnings
	// If nil, uses the default slog instance
	Logger *slog.Logger
}

// Connector is a prepared modernc.org/sqlite connector
// It wraps the connector for the normalized DSN, and adds the metadata needed to configure the resulting sql.DB
// It can be passed to packages such as instrument/sqlite before opening the connection pool
type Connector struct {
	base     driver.Connector
	inMemory bool
}

// NewConnector validates and normalizes SQLite configuration, performs the filesystem safety setup used by Connect, and returns a reusable connector
func NewConnector(opts ConnectOpts) (*Connector, error) {
	if opts.ConnString == "" {
		return nil, errors.New("connection string is empty")
	}

	// Use the default slog instance if no specific logger is passed
	log := opts.Logger
	if log == nil {
		log = slog.Default()
	}

	// Parse the connection string
	connString, dbPath, isMemoryDB, err := ParseConnectionString(opts.ConnString, log)
	if err != nil {
		return nil, err
	}

	// Make sure that there's a temporary folder for SQLite to write its data
	// Note that this may be necessary for in-memory databases too, as SQLite may use a temporary file for overflow storage
	err = EnsureTempDir(filepath.Dir(dbPath), log)
	if err != nil {
		return nil, err
	}

	if !isMemoryDB {
		// Ensure that the folder where the database is stored exists
		err = EnsureDatabaseDir(dbPath)
		if err != nil {
			return nil, err
		}

		// SQLite locking is not reliable on filesystems such as NFS, SMB, and FUSE
		networked, err := IsNetworkedFileSystem(filepath.Dir(dbPath))
		switch {
		case err != nil:
			// Print out a warning log only
			log.Warn("Failed to detect filesystem type for the SQLite database directory", slog.String("path", filepath.Dir(dbPath)), slog.Any("error", err))
		case networked:
			log.Warn("⚠️⚠️⚠️ SQLite databases should not be stored on a networked file system like NFS, SMB, or FUSE, as there's a risk of crashes and even database corruption", slog.String("path", filepath.Dir(dbPath)))
		}
	}

	// Get the connector from the driver registered by modernc.org/sqlite, which carries all functions, collations, and connection hooks registered at the package level
	base, err := sqlitedriver.NewConnector(connString)
	if err != nil {
		return nil, fmt.Errorf("failed to create the SQLite connector: %w", err)
	}

	return &Connector{
		base:     base,
		inMemory: isMemoryDB,
	}, nil
}

// Connect implements driver.Connector
func (c *Connector) Connect(ctx context.Context) (driver.Conn, error) {
	// The underlying connector honors ctx only up to the point the open begins, since sqlite3_open_v2 has no cancellation hook, so cancellation is checked again immediately after
	conn, err := c.base.Connect(ctx)
	if err != nil {
		return nil, err
	}

	err = ctx.Err()
	if err != nil {
		_ = conn.Close()
		return nil, err
	}

	return conn, nil
}

// Driver implements driver.Connector
func (c *Connector) Driver() driver.Driver {
	return c.base.Driver()
}

// OpenDB opens a plain sql.DB from the prepared connector and applies pool settings required for correct SQLite behavior
func (c *Connector) OpenDB() *sql.DB {
	db := sql.OpenDB(c)
	c.ConfigureDB(db)
	return db
}

// ConfigureDB applies connector-specific pool constraints to db
// It is used by wrappers that decorate the connector before opening the pool
func (c *Connector) ConfigureDB(db *sql.DB) {
	if c.inMemory {
		// For in-memory SQLite databases, we must limit to 1 open connection at the same time, or they won't see the whole data
		// The other workaround, of using shared caches, doesn't work well with multiple write transactions trying to happen at once
		db.SetMaxOpenConns(1)
	}
}

// Connect opens a SQLite database
func Connect(opts ConnectOpts) (*sql.DB, error) {
	connector, err := NewConnector(opts)
	if err != nil {
		return nil, err
	}

	return connector.OpenDB(), nil
}
