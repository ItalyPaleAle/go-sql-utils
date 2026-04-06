//go:build !linux

// This code was adapted from https://github.com/pocket-id/pocket-id/tree/v2.5.0
// Copyright (c) 2024 Elias Schneider
// License: BSD-2

package sqlite

// isNetworkedFileSystem returns false on non-Linux systems because this detection is only used for Linux-specific statfs(2) filesystem magic values.
func isNetworkedFileSystem(string) (bool, error) {
	return false, nil
}
