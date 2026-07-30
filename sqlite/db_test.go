package sqlite

import (
	"context"
	"database/sql/driver"
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
