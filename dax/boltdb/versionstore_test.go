package boltdb_test

import (
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/molecula/featurebase/v3/dax"
	"github.com/molecula/featurebase/v3/dax/boltdb"
	testbolt "github.com/molecula/featurebase/v3/dax/test/boltdb"
	"github.com/molecula/featurebase/v3/logger"
	"github.com/stretchr/testify/assert"
)

func TestVersionStore(t *testing.T) {
	db := testbolt.MustOpenDB(t)
	defer testbolt.MustCloseDB(t, db)

	ctx := context.Background()

	t.Cleanup(func() {
		testbolt.CleanupDB(t, db.Path())
	})

	orgID := dax.OrganizationID("acme")
	dbID := dax.DatabaseID("db1")

	qual := dax.NewTableQualifier(orgID, dbID)

	// Initialize the buckets.
	assert.NoError(t, db.InitializeBuckets(boltdb.VersionStoreBuckets...))

	t.Run("Tables", func(t *testing.T) {
		vs := boltdb.NewVersionStore(db, logger.NopLogger)

		qtids := newQualifiedTableIDs(t, qual, 3)
		qtid1 := qtids[0]
		qtid2 := qtids[1]
		qtid3 := qtids[2]
		defer vs.RemoveTable(ctx, qtid1)
		defer vs.RemoveTable(ctx, qtid2)
		defer vs.RemoveTable(ctx, qtid3)

		// Add table 1.
		assert.NoError(t, vs.AddTable(ctx, qtid1))

		// Add table 2.
		assert.NoError(t, vs.AddTable(ctx, qtid2))

		// Add table 3.
		assert.NoError(t, vs.AddTable(ctx, qtid3))
	})

	t.Run("Shards", func(t *testing.T) {
		vs := boltdb.NewVersionStore(db, logger.NopLogger)

		qtids := newQualifiedTableIDs(t, qual, 3)
		qtid1 := qtids[0]
		qtid2 := qtids[1]
		qtid3 := qtids[2]

		// Add tables.
		assert.NoError(t, vs.AddTable(ctx, qtid1))
		assert.NoError(t, vs.AddTable(ctx, qtid2))
		assert.NoError(t, vs.AddTable(ctx, qtid3))
		defer vs.RemoveTable(ctx, qtid1)
		defer vs.RemoveTable(ctx, qtid2)
		defer vs.RemoveTable(ctx, qtid3)

		// Create some shards to insert into the table.
		shards := make(dax.VersionedShards, 3)
		for i := range shards {
			shards[i] = dax.VersionedShard{
				Num:     dax.ShardNum(i),
				Version: i * 2,
			}
		}

		// Add shards to table 1.
		{
			err := vs.AddShards(ctx, qtid1, shards...)
			assert.NoError(t, err)
		}

		// Add shards to table 2.
		{
			err := vs.AddShards(ctx, qtid2, shards...)
			assert.NoError(t, err)
		}

		// Fetch a shard and compare.
		{
			version, found, err := vs.ShardVersion(ctx, qtid1, 2)
			assert.NoError(t, err)
			assert.True(t, found)
			assert.Equal(t, 4, version)
		}

		// Fetch all shards and compare.
		{
			shrds, found, err := vs.Shards(ctx, qtid1)
			assert.NoError(t, err)
			assert.True(t, found)
			assert.Equal(t, shards, shrds)
		}

		// Fetch tables.
		{
			tblIDs, err := vs.ShardTables(ctx, qual)
			assert.NoError(t, err)
			exp := dax.TableIDs{qtid1.ID, qtid2.ID, qtid3.ID}
			assert.Equal(t, exp, tblIDs)
		}

		// Remove table 1.
		{
			shards, partitions, err := vs.RemoveTable(ctx, qtid1)
			assert.NoError(t, err)
			assert.Equal(t, shards, shards)
			assert.Equal(t, dax.VersionedPartitions{}, partitions)
		}

		// Fetch all shards and compare.
		{
			shrds, found, err := vs.Shards(ctx, qtid1)
			assert.NoError(t, err)
			assert.True(t, found)
			assert.Equal(t, dax.VersionedShards{}, shrds)
		}
	})

	t.Run("Partitions", func(t *testing.T) {
		vs := boltdb.NewVersionStore(db, logger.NopLogger)

		// Create some partitions to insert into the table.
		partitions := make(dax.VersionedPartitions, 3)
		for i := range partitions {
			partitions[i] = dax.VersionedPartition{
				Num:     dax.PartitionNum(i),
				Version: i * 2,
			}
		}

		qtids := newQualifiedTableIDs(t, qual, 2)
		qtid1 := qtids[0]
		qtid2 := qtids[1]

		// Add tables.
		assert.NoError(t, vs.AddTable(ctx, qtid1))
		assert.NoError(t, vs.AddTable(ctx, qtid2))
		defer vs.RemoveTable(ctx, qtid1)
		defer vs.RemoveTable(ctx, qtid2)

		// Add partitions to table 1.
		{
			err := vs.AddPartitions(ctx, qtid1, partitions...)
			assert.NoError(t, err)
		}

		// Add partitions to table 2.
		{
			err := vs.AddPartitions(ctx, qtid2, partitions...)
			assert.NoError(t, err)
		}

		// Fetch a partition and compare.
		{
			version, found, err := vs.PartitionVersion(ctx, qtid1, 2)
			assert.NoError(t, err)
			assert.True(t, found)
			assert.Equal(t, 4, version)
		}

		// Fetch all partitions and compare.
		{
			parts, found, err := vs.Partitions(ctx, qtid1)
			assert.NoError(t, err)
			assert.True(t, found)
			assert.Equal(t, partitions, parts)
		}

		// Fetch tables.
		{
			tblIDs, err := vs.PartitionTables(ctx, qual)
			assert.NoError(t, err)
			exp := dax.TableIDs{qtid1.ID, qtid2.ID}
			assert.Equal(t, exp, tblIDs)
		}

		// Remove table 1.
		{
			shards, partitions, err := vs.RemoveTable(ctx, qtid1)
			assert.NoError(t, err)
			assert.Equal(t, dax.VersionedShards{}, shards)
			assert.Equal(t, partitions, partitions)
		}

		// Fetch all partitions and compare.
		{
			parts, found, err := vs.Partitions(ctx, qtid1)
			assert.NoError(t, err)
			assert.True(t, found)
			assert.Equal(t, dax.VersionedPartitions{}, parts)
		}
	})

	t.Run("FieldVersions", func(t *testing.T) {
		vs := boltdb.NewVersionStore(db, logger.NopLogger)

		qtids := newQualifiedTableIDs(t, qual, 2)
		qtid1 := qtids[0]
		qtid2 := qtids[1]

		// Add tables.
		assert.NoError(t, vs.AddTable(ctx, qtid1))
		assert.NoError(t, vs.AddTable(ctx, qtid2))
		defer vs.RemoveTable(ctx, qtid1)
		defer vs.RemoveTable(ctx, qtid2)

		// Create some fieldVersions to insert into the table.
		fieldVersions := make(dax.VersionedFields, 3)
		for i := range fieldVersions {
			fieldVersions[i] = dax.VersionedField{
				Name:    dax.FieldName(fmt.Sprintf("fld-%d", i)),
				Version: i * 2,
			}
		}

		// Add fieldVersions to table 1.
		{
			err := vs.AddFields(ctx, qtid1, fieldVersions...)
			assert.NoError(t, err)
		}

		// Add fieldVersions to table 2.
		{
			err := vs.AddFields(ctx, qtid2, fieldVersions...)
			assert.NoError(t, err)
		}

		// Fetch a fieldVersion and compare.
		{
			version, found, err := vs.FieldVersion(ctx, qtid1, dax.FieldName("fld-2"))
			assert.NoError(t, err)
			assert.True(t, found)
			assert.Equal(t, 4, version)
		}

		// Fetch all fieldVersions and compare.
		{
			flds, found, err := vs.Fields(ctx, qtid1)
			assert.NoError(t, err)
			assert.True(t, found)
			assert.Equal(t, fieldVersions, flds)
		}

		// Fetch tables.
		{
			tblIDs, err := vs.FieldTables(ctx, qual)
			assert.NoError(t, err)
			exp := dax.TableIDs{qtid1.ID, qtid2.ID}
			assert.Equal(t, exp, tblIDs)
		}

		// Remove table 1.
		{
			shards, partitions, err := vs.RemoveTable(ctx, qtid1)
			assert.NoError(t, err)
			assert.Equal(t, dax.VersionedShards{}, shards)
			assert.Equal(t, dax.VersionedPartitions{}, partitions)
		}

		// Fetch all fieldVersions and compare.
		{
			flds, found, err := vs.Fields(ctx, qtid1)
			assert.NoError(t, err)
			assert.True(t, found)
			assert.Equal(t, dax.VersionedFields{}, flds)
		}
	})

	t.Run("Copy", func(t *testing.T) {
		vs := boltdb.NewVersionStore(db, logger.NopLogger)

		qtids := newQualifiedTableIDs(t, qual, 1)
		qtid1 := qtids[0]

		// Add tables.
		assert.NoError(t, vs.AddTable(ctx, qtid1))
		defer vs.RemoveTable(ctx, qtid1)

		// Create some shards to insert into the table.
		shards := make(dax.VersionedShards, 3)
		for i := range shards {
			shards[i] = dax.VersionedShard{
				Num:     dax.ShardNum(i),
				Version: i * 2,
			}
		}

		// Create some partitions to insert into the table.
		partitions := make(dax.VersionedPartitions, 3)
		for i := range partitions {
			partitions[i] = dax.VersionedPartition{
				Num:     dax.PartitionNum(i),
				Version: i * 2,
			}
		}

		// Create some fieldVersions to insert into the table.
		fieldVersions := make(dax.VersionedFields, 3)
		for i := range fieldVersions {
			fieldVersions[i] = dax.VersionedField{
				Name:    dax.FieldName(fmt.Sprintf("fld-%d", i)),
				Version: i * 2,
			}
		}

		// Add shards to table 1.
		{
			err := vs.AddShards(ctx, qtid1, shards...)
			assert.NoError(t, err)
		}

		// Add partitions to table 1.
		{
			err := vs.AddPartitions(ctx, qtid1, partitions...)
			assert.NoError(t, err)
		}

		// Add fieldVersions to table 1.
		{
			err := vs.AddFields(ctx, qtid1, fieldVersions...)
			assert.NoError(t, err)
		}

		copy, err := vs.Copy(ctx)
		assert.NoError(t, err)

		// Fetch a shard and compare.
		{
			version, found, err := copy.ShardVersion(ctx, qtid1, 2)
			assert.NoError(t, err)
			assert.True(t, found)
			assert.Equal(t, 4, version)
		}

		// Fetch all partitions and compare.
		{
			parts, found, err := copy.Partitions(ctx, qtid1)
			assert.NoError(t, err)
			assert.True(t, found)
			assert.Equal(t, partitions, parts)
		}

		// Fetch all fieldVersions and compare.
		{
			flds, found, err := copy.Fields(ctx, qtid1)
			assert.NoError(t, err)
			assert.True(t, found)
			assert.Equal(t, fieldVersions, flds)
		}
	})
}

// newQualifiedTableIDs is a test helper function which generates a slice of n
// qtid. The entries in the slice will be ordered by TableID.
func newQualifiedTableIDs(t *testing.T, qual dax.TableQualifier, n int) []dax.QualifiedTableID {
	t.Helper()

	qtids := make([]dax.QualifiedTableID, n)
	for i := range qtids {
		tbl := dax.NewTable("testvstore")
		tbl.CreateID()
		qtids[i] = dax.NewQualifiedTableID(
			qual,
			tbl.ID,
		)
	}

	// sort the qtids by ID
	sort.Slice(qtids, func(i, j int) bool {
		return qtids[i].ID < qtids[j].ID
	})

	return qtids
}
