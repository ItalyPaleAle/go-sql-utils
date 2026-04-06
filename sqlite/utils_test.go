// This code was adapted from https://github.com/pocket-id/pocket-id/tree/v2.5.0
// Copyright (c) 2024 Elias Schneider
// License: BSD-2

package sqlite

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEnsureSqliteDatabaseDir(t *testing.T) {
	t.Run("creates missing directory", func(t *testing.T) {
		tempDir := t.TempDir()
		dbPath := filepath.Join(tempDir, "nested", "pocket-id.db")

		err := ensureDatabaseDir(dbPath)
		require.NoError(t, err)

		info, err := os.Stat(filepath.Dir(dbPath))
		require.NoError(t, err)
		assert.True(t, info.IsDir())
	})

	t.Run("fails when parent is file", func(t *testing.T) {
		tempDir := t.TempDir()
		filePath := filepath.Join(tempDir, "file.txt")
		require.NoError(t, os.WriteFile(filePath, []byte("test"), 0o600))

		err := ensureDatabaseDir(filepath.Join(filePath, "data.db"))
		require.Error(t, err)
	})
}

func TestIsWritableDir(t *testing.T) {
	t.Run("returns true for writable directory", func(t *testing.T) {
		tempDir := t.TempDir()

		ok, err := isWritableDir(tempDir)
		require.NoError(t, err)
		assert.True(t, ok)
	})

	t.Run("returns false for missing directory", func(t *testing.T) {
		missingDir := filepath.Join(t.TempDir(), "missing")

		ok, err := isWritableDir(missingDir)
		require.NoError(t, err)
		assert.False(t, ok)
	})

	t.Run("returns false for file path", func(t *testing.T) {
		tempDir := t.TempDir()
		filePath := filepath.Join(tempDir, "file.txt")
		require.NoError(t, os.WriteFile(filePath, []byte("test"), 0o600))

		ok, err := isWritableDir(filePath)
		require.NoError(t, err)
		assert.False(t, ok)
	})

	t.Run("returns false for non-writable directory", func(t *testing.T) {
		if runtime.GOOS == "windows" {
			t.Skip("directory permissions are not enforced consistently on Windows")
		}

		tempDir := t.TempDir()
		readOnlyDir := filepath.Join(tempDir, "readonly")
		require.NoError(t, os.Mkdir(readOnlyDir, 0o700))
		t.Cleanup(func() {
			_ = os.Chmod(readOnlyDir, 0o700)
		})
		require.NoError(t, os.Chmod(readOnlyDir, 0o500))

		probePath := filepath.Join(readOnlyDir, "probe")
		if err := os.WriteFile(probePath, []byte("test"), 0o600); err == nil {
			_ = os.Remove(probePath)
			t.Skip("current environment allows writing to read-only directories")
		}

		ok, err := isWritableDir(readOnlyDir)
		require.NoError(t, err)
		assert.False(t, ok)
	})
}
