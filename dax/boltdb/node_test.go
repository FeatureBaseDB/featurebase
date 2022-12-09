package boltdb_test

import (
	"context"
	"testing"

	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/dax/boltdb"
	testbolt "github.com/featurebasedb/featurebase/v3/dax/test/boltdb"
	"github.com/featurebasedb/featurebase/v3/errors"
	"github.com/featurebasedb/featurebase/v3/logger"
	"github.com/stretchr/testify/assert"
)

func TestNodeService(t *testing.T) {
	db := testbolt.MustOpenDB(t)
	defer testbolt.MustCloseDB(t, db)

	t.Cleanup(func() {
		testbolt.CleanupDB(t, db.Path())
	})

	ctx := context.Background()

	// Initialize the buckets.
	assert.NoError(t, db.InitializeBuckets(boltdb.NodeServiceBuckets...))

	t.Run("Nodes", func(t *testing.T) {
		ns := boltdb.NewNodeService(db, logger.NopLogger)

		node1 := &dax.Node{
			Address: "localhost:10101",
			RoleTypes: []dax.RoleType{
				"compute",
			},
		}

		// Create node.
		assert.NoError(t, ns.CreateNode(ctx, node1.Address, node1))

		// Read node.
		n, err := ns.ReadNode(ctx, node1.Address)
		assert.NoError(t, err)
		assert.Equal(t, node1, n)

		// Delete node.
		assert.NoError(t, ns.DeleteNode(ctx, node1.Address))

		// Read node.
		_, err = ns.ReadNode(ctx, node1.Address)
		if assert.Error(t, err) {
			assert.True(t, errors.Is(err, dax.ErrNodeDoesNotExist))
		}
	})
}
