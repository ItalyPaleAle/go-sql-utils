// This code was adapted from https://github.com/pocket-id/pocket-id/tree/v2.5.0
// Copyright (c) 2024 Elias Schneider
// License: BSD-2

package sqlite

import (
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"path/filepath"
	"strings"
	"time"
)

// Default value for the SQLite busy timeout
const defaultBusyTimeout = 2500 * time.Millisecond

// ParseConnectionString parses the SQLite connection string, ensuring the required parameters are set
// This is optimized for the modernc.org/sqlite driver
func ParseConnectionString(connString string, log *slog.Logger) (parsedConnString string, dbPath string, isMemoryDB bool, err error) {
	// Ensure there's the "file:" prefix
	if !strings.HasPrefix(connString, "file:") {
		connString = "file:" + connString
	}

	// Check if we're using an in-memory database
	isMemoryDB = isInMemory(connString)

	// Parse the connection string
	connStringUrl, err := url.Parse(connString)
	if err != nil {
		return "", "", false, fmt.Errorf("failed to parse SQLite connection string: %w", err)
	}

	// Add the default and required params
	err = addDefaultParameters(connStringUrl, isMemoryDB, log)
	if err != nil {
		return "", "", false, fmt.Errorf("invalid SQLite connection string: %w", err)
	}

	// Get the absolute path to the database
	// Here, we know for a fact that the ? is present
	parsedConnString = connStringUrl.String()
	idx := strings.IndexRune(parsedConnString, '?')
	dbPath, err = filepath.Abs(parsedConnString[len("file:"):idx])
	if err != nil {
		return "", "", false, fmt.Errorf("failed to determine absolute path to the database: %w", err)
	}

	return parsedConnString, dbPath, isMemoryDB, nil
}

// isInMemory returns true if the connection string is for an in-memory database.
func isInMemory(connString string) bool {
	lc := strings.ToLower(connString)

	// First way to define an in-memory database is to use ":memory:" or "file::memory:" as connection string
	if strings.HasPrefix(lc, ":memory:") || strings.HasPrefix(lc, "file::memory:") {
		return true
	}

	// Another way is to pass "mode=memory" in the "query string"
	idx := strings.IndexRune(lc, '?')
	if idx < 0 {
		return false
	}
	qs, _ := url.ParseQuery(lc[(idx + 1):])

	return len(qs["mode"]) > 0 && qs["mode"][0] == "memory"
}

// Adds the default (and some required) parameters to the SQLite connection string.
// Note this function updates connStringUrl.
func addDefaultParameters(connStringUrl *url.URL, isMemoryDB bool, log *slog.Logger) error {
	// This function include code adapted from https://github.com/dapr/components-contrib/blob/v1.14.6/
	// Copyright (C) 2023 The Dapr Authors
	// License: Apache2

	// Get the "query string" from the connection string if present
	qs := connStringUrl.Query()
	if len(qs) == 0 {
		qs = make(url.Values, 2)
	}

	// Check if the database is read-only or immutable
	isReadOnly := false
	if len(qs["mode"]) > 0 {
		// Keep the first value only
		qs["mode"] = []string{
			strings.ToLower(qs["mode"][0]),
		}
		if qs["mode"][0] == "ro" {
			isReadOnly = true
		}
	}
	if len(qs["immutable"]) > 0 {
		// Keep the first value only
		qs["immutable"] = []string{
			strings.ToLower(qs["immutable"][0]),
		}
		if qs["immutable"][0] == "1" {
			isReadOnly = true
		}
	}

	// We do not want to override a _txlock if set, but we'll show a warning if it's not "immediate"
	if len(qs["_txlock"]) > 0 {
		// Keep the first value only
		qs["_txlock"] = []string{
			strings.ToLower(qs["_txlock"][0]),
		}
		if qs["_txlock"][0] != "immediate" && log != nil {
			log.Warn("SQLite connection is being created with a _txlock different from the recommended value 'immediate'")
		}
	} else {
		qs["_txlock"] = []string{"immediate"}
	}

	// Add pragma values
	var hasBusyTimeout, hasJournalMode bool
	if len(qs["_pragma"]) == 0 {
		qs["_pragma"] = make([]string, 0, 3)
	} else {
		for _, p := range qs["_pragma"] {
			p = strings.ToLower(p)
			switch {
			case strings.HasPrefix(p, "busy_timeout"):
				hasBusyTimeout = true
			case strings.HasPrefix(p, "journal_mode"):
				hasJournalMode = true
			case strings.HasPrefix(p, "foreign_keys"):
				return errors.New("found forbidden option '_pragma=foreign_keys' in the connection string")
			}
		}
	}
	if !hasBusyTimeout {
		qs["_pragma"] = append(qs["_pragma"], fmt.Sprintf("busy_timeout(%d)", defaultBusyTimeout.Milliseconds()))
	}
	if !hasJournalMode {
		switch {
		case isMemoryDB:
			// For in-memory databases, set the journal to MEMORY, the only allowed option besides OFF (which would make transactions ineffective)
			qs["_pragma"] = append(qs["_pragma"], "journal_mode(MEMORY)")
		case isReadOnly:
			// Set the journaling mode to "DELETE" (the default) if the database is read-only
			qs["_pragma"] = append(qs["_pragma"], "journal_mode(DELETE)")
		default:
			// Enable WAL
			qs["_pragma"] = append(qs["_pragma"], "journal_mode(WAL)")
		}
	}

	// Forcefully enable foreign keys
	qs["_pragma"] = append(qs["_pragma"], "foreign_keys(1)")

	// Update the connStringUrl object
	connStringUrl.RawQuery = qs.Encode()

	return nil
}
