package inmem_test

import (
	"context"
	"testing"

	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/dax/inmem"
	"github.com/featurebasedb/featurebase/v3/errors"
	"github.com/stretchr/testify/assert"
)

func TestVersionStore(t *testing.T) {
	orgID := dax.OrganizationID("acme")
	dbID := dax.DatabaseID("db1")

	tableID := dax.TableID("0000000000000001")

	qual := dax.NewTableQualifier(orgID, dbID)
	qtid := dax.NewQualifiedTableID(qual, tableID)

	invalidQtid := dax.NewQualifiedTableID(qual, dax.TableID("0000000000000000"))

	ctx := context.Background()

	// Ensure that when using a Schemar not initiated with NewSchemar, the error
	// handling works as expected.
	t.Run("EmptyVersionStore", func(t *testing.T) {
		s := inmem.VersionStore{}

		t.Run("GetShardsInvalid", func(t *testing.T) {
			sh, ok, err := s.Shards(ctx, invalidQtid)
			assert.NoError(t, err)
			assert.False(t, ok)
			assert.Nil(t, sh)
		})

		// Add new table.
		assert.NoError(t, s.AddTable(ctx, qtid))
	})

	t.Run("NewVersionStore", func(t *testing.T) {
		s := inmem.NewVersionStore()

		// Add new table.
		assert.NoError(t, s.AddTable(ctx, qtid))

		t.Run("AddTableAgain", func(t *testing.T) {
			err := s.AddTable(ctx, qtid)
			if assert.Error(t, err) {
				assert.True(t, errors.Is(err, dax.ErrTableIDExists))
			}
		})

		t.Run("AddShards", func(t *testing.T) {
			err := s.AddShards(ctx, invalidQtid, dax.NewVersionedShard(1, 0))
			if assert.Error(t, err) {
				assert.True(t, errors.Is(err, dax.ErrTableIDDoesNotExist))
			}

			{
				_, ok, err := s.Shards(ctx, invalidQtid)
				assert.NoError(t, err)
				assert.False(t, ok)
			}

			// Shards is empty if no shards have been added.
			{
				sh, ok, err := s.Shards(ctx, qtid)
				assert.NoError(t, err)
				assert.True(t, ok)
				assert.Equal(t, sh, dax.VersionedShards{})
			}

			// Add the first set of shards (with a duplicate (8)).
			{
				err := s.AddShards(ctx, qtid,
					dax.NewVersionedShard(8, 0),
					dax.NewVersionedShard(9, 0),
					dax.NewVersionedShard(8, 0),
					dax.NewVersionedShard(10, 0),
				)
				assert.NoError(t, err)
			}

			{
				sh, ok, err := s.Shards(ctx, qtid)
				assert.NoError(t, err)
				assert.True(t, ok)
				assert.Equal(t, dax.VersionedShards{
					dax.NewVersionedShard(8, 0),
					dax.NewVersionedShard(9, 0),
					dax.NewVersionedShard(10, 0),
				}, sh)
			}

			// Add another set of shards (with one duplicate (11) and one
			// existing (10)).
			{
				err := s.AddShards(ctx, qtid,
					dax.NewVersionedShard(10, 0),
					dax.NewVersionedShard(11, 0),
					dax.NewVersionedShard(12, 0),
					dax.NewVersionedShard(11, 0),
				)
				assert.NoError(t, err)
			}

			{
				sh, ok, err := s.Shards(ctx, qtid)
				assert.NoError(t, err)
				assert.True(t, ok)
				assert.Equal(t, dax.VersionedShards{
					dax.NewVersionedShard(8, 0),
					dax.NewVersionedShard(9, 0),
					dax.NewVersionedShard(10, 0),
					dax.NewVersionedShard(11, 0),
					dax.NewVersionedShard(12, 0),
				}, sh)
			}
		})

		t.Run("RemoveTable", func(t *testing.T) {
			shards, partitions, err := s.RemoveTable(ctx, qtid)
			assert.NoError(t, err)
			assert.Equal(t, dax.VersionedPartitions{}, partitions)
			assert.Equal(t, dax.VersionedShards{
				dax.NewVersionedShard(8, 0),
				dax.NewVersionedShard(9, 0),
				dax.NewVersionedShard(10, 0),
				dax.NewVersionedShard(11, 0),
				dax.NewVersionedShard(12, 0),
			}, shards)

			// Make sure the table was removed.
			shards, ok, err := s.Shards(ctx, qtid)
			assert.NoError(t, err)
			assert.False(t, ok)
			assert.Nil(t, shards)
		})
	})

	t.Run("ErrorConditions", func(t *testing.T) {
		t.Run("JustSchemar", func(t *testing.T) {
			s := inmem.VersionStore{}

			shards, partitions, err := s.RemoveTable(ctx, qtid)
			assert.Nil(t, shards)
			assert.Nil(t, partitions)
			if assert.Error(t, err) {
				assert.True(t, errors.Is(err, dax.ErrTableIDDoesNotExist))
			}
		})

		t.Run("NewSchemar", func(t *testing.T) {
			s := inmem.NewVersionStore()

			shards, partitions, err := s.RemoveTable(ctx, qtid)
			assert.Nil(t, shards)
			assert.Nil(t, partitions)
			if assert.Error(t, err) {
				assert.True(t, errors.Is(err, dax.ErrTableIDDoesNotExist))
			}
		})
	})
}
