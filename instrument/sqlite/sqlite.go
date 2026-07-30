// Package sqlite instruments modernc.org/sqlite connections with OpenTelemetry spans and optional query logging
package sqlite

import (
	"database/sql"
	"errors"

	"github.com/italypaleale/go-sql-utils/instrument"
	sqliteutils "github.com/italypaleale/go-sql-utils/sqlite"
)

// Open opens an instrumented sql.DB from an existing prepared SQLite connector, created with sqlite.NewConnector
func Open(connector *sqliteutils.Connector, opts *instrument.Options) (*sql.DB, error) {
	if connector == nil {
		return nil, errors.New("SQLite connector is nil")
	}

	inst := instrument.NewInstrumentation("sqlite", opts)
	db := sql.OpenDB(newConnector(connector, inst))
	connector.ConfigureDB(db)

	return db, nil
}
