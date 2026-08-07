package sqlite

import (
	"context"
	"database/sql/driver"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Compile-time interface assertions
var _ driver.Connector = (*Connector)(nil)

func TestConnectorHonorsCanceledContext(t *testing.T) {
	connector, err := NewConnector(ConnectOpts{ConnString: ":memory:"})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	conn, err := connector.Connect(ctx)
	assert.Nil(t, conn)
	require.ErrorIs(t, err, context.Canceled)
}

func TestConnectorConfiguresInMemoryPool(t *testing.T) {
	connector, err := NewConnector(ConnectOpts{ConnString: ":memory:"})
	require.NoError(t, err)

	db := connector.OpenDB()
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})
	assert.Equal(t, 1, db.Stats().MaxOpenConnections)
}

func TestConnectorAppliesMaxConnsParam(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")

	connector, err := NewConnector(ConnectOpts{ConnString: "file:" + dbPath + "?_maxconn=7"})
	require.NoError(t, err)

	db := connector.OpenDB()
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})
	assert.Equal(t, 7, db.Stats().MaxOpenConnections)
}

func TestConnectorIgnoresNonPositiveMaxConnsParam(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")

	connector, err := NewConnector(ConnectOpts{ConnString: "file:" + dbPath + "?_maxconn=0"})
	require.NoError(t, err)

	db := connector.OpenDB()
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})
	assert.Equal(t, 0, db.Stats().MaxOpenConnections)
}

func TestConnectorIgnoresMaxConnsParamForInMemoryDB(t *testing.T) {
	connector, err := NewConnector(ConnectOpts{ConnString: "file::memory:?_maxconn=7"})
	require.NoError(t, err)

	db := connector.OpenDB()
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})
	assert.Equal(t, 1, db.Stats().MaxOpenConnections)
}
