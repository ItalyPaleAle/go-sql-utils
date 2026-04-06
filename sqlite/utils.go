package sqlite

// This code was adapted from https://github.com/pocket-id/pocket-id/tree/v2.5.0
// Copyright (c) 2024 Elias Schneider
// License: BSD-2

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"syscall"
)

// ensureDatabaseDir creates the parent directory for the SQLite database file if it doesn't exist yet
func ensureDatabaseDir(dbPath string) error {
	dir := filepath.Dir(dbPath)

	info, err := os.Stat(dir)
	switch {
	case err == nil:
		if !info.IsDir() {
			return fmt.Errorf("SQLite database directory '%s' is not a directory", dir)
		}
		return nil
	case os.IsNotExist(err):
		err = os.MkdirAll(dir, 0700)
		if err != nil {
			return fmt.Errorf("failed to create SQLite database directory '%s': %w", dir, err)
		}
		return nil
	default:
		return fmt.Errorf("failed to check SQLite database directory '%s': %w", dir, err)
	}
}

// ensureTempDir ensures that SQLite has a directory where it can write temporary files if needed
// The default directory may not be writable when using a container with a read-only root file system
// See: https://www.sqlite.org/tempfiles.html
func ensureTempDir(dbPath string, log *slog.Logger) error {
	// Per docs, SQLite tries these folders in order (excluding those that aren't applicable to us):
	//
	// - The SQLITE_TMPDIR environment variable
	// - The TMPDIR environment variable
	// - /var/tmp
	// - /usr/tmp
	// - /tmp
	//
	// Source: https://www.sqlite.org/tempfiles.html#temporary_file_storage_locations
	//
	// First, let's check if SQLITE_TMPDIR or TMPDIR are set, in which case we trust the user has taken care of the problem already
	if os.Getenv("SQLITE_TMPDIR") != "" || os.Getenv("TMPDIR") != "" {
		return nil
	}

	// Now, let's check if /var/tmp, /usr/tmp, or /tmp exist and are writable
	for _, dir := range []string{"/var/tmp", "/usr/tmp", "/tmp"} {
		ok, err := isWritableDir(dir)
		if err != nil {
			return fmt.Errorf("failed to check if %s is writable: %w", dir, err)
		}
		if ok {
			// We found a folder that's writable
			return nil
		}
	}

	// If we're here, there's no temporary directory that's writable (not unusual for containers with a read-only root file system), so we set SQLITE_TMPDIR to the folder where the SQLite database is set
	err := os.Setenv("SQLITE_TMPDIR", dbPath)
	if err != nil {
		return fmt.Errorf("failed to set SQLITE_TMPDIR environmental variable: %w", err)
	}

	if log != nil {
		log.Debug("Set SQLITE_TMPDIR to the database directory", "path", dbPath)
	}

	return nil
}

// isWritableDir checks if a directory exists and is writable
func isWritableDir(dir string) (bool, error) {
	// Check if directory exists and it's actually a directory
	info, err := os.Stat(dir)
	if os.IsNotExist(err) {
		return false, nil
	} else if err != nil {
		return false, fmt.Errorf("failed to stat '%s': %w", dir, err)
	}
	if !info.IsDir() {
		return false, nil
	}

	// Generate a random suffix for the test file to avoid conflicts
	randomBytes := make([]byte, 8)
	_, err = io.ReadFull(rand.Reader, randomBytes)
	if err != nil {
		return false, fmt.Errorf("failed to generate random bytes: %w", err)
	}

	// Check if directory is writable by trying to create a temporary file
	testFile := filepath.Join(dir, ".sqlite_test_write"+hex.EncodeToString(randomBytes))
	defer os.Remove(testFile) //nolint:errcheck

	file, err := os.Create(testFile)
	if os.IsPermission(err) || errors.Is(err, syscall.EROFS) {
		return false, nil
	} else if err != nil {
		return false, fmt.Errorf("failed to create test file: %w", err)
	}

	_ = file.Close()

	return true, nil
}
