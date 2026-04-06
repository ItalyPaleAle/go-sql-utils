package sqlite

import (
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"

	_ "modernc.org/sqlite"
)

// ConnectOpts is the parameters struct for the Connect method
type ConnectOpts struct {
	// SQLite database connection string
	// Could be the path to a file, or a URL beginning with "file:"
	ConnString string
	// Optional instance of a slog logger
	// If nil, uses the default slog instance
	Logger *slog.Logger
}

// Connect to a SQLite database using the modernc.org/sqlite driver
func Connect(opts ConnectOpts) (*sql.DB, error) {
	if opts.ConnString == "" {
		return nil, errors.New("connection string is empty")
	}

	// Use the default slog instance if no specific logger is passed
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}

	// Parse the connection string
	connString, dbPath, isMemoryDB, err := ParseConnectionString(opts.ConnString, opts.Logger)
	if err != nil {
		return nil, err
	}

	// Make sure that there's a temporary folder for SQLite to write its data
	// Note that this may be necessary for in-memory databases too, as SQLite may use a temporary file for overflow storage
	err = ensureTempDir(filepath.Dir(dbPath), opts.Logger)
	if err != nil {
		return nil, err
	}

	if !isMemoryDB {
		// Ensure that the folder where the database is stored exists
		err = ensureDatabaseDir(dbPath)
		if err != nil {
			return nil, err
		}

		// Running SQLite on a networked file system (like NFS, SMB, FUSE) is strongly discouraged because of bugs
		sqliteNetworkFilesystem, err := isNetworkedFileSystem(filepath.Dir(dbPath))
		if err != nil {
			// Log the error only
			opts.Logger.Warn("Failed to detect filesystem type for the SQLite database directory", slog.String("path", filepath.Dir(dbPath)), slog.Any("error", err))
		} else if sqliteNetworkFilesystem {
			opts.Logger.Warn("⚠️⚠️⚠️ SQLite databases should not be stored on a networked file system like NFS, SMB, or FUSE, as there's a risk of crashes and even database corruption", slog.String("path", filepath.Dir(dbPath)))
		}
	}

	// Connect to the database
	db, err := sql.Open("sqlite", connString)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to SQLite database: %w", err)
	}

	// For in-memory SQLite databases, we must limit to 1 open connection at the same time, or they won't see the whole data
	// The other workaround, of using shared caches, doesn't work well with multiple write transactions trying to happen at once
	if isMemoryDB {
		db.SetMaxOpenConns(1)
	}

	return db, nil
}
