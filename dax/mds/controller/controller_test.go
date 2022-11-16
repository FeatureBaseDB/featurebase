package controller_test

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"testing"

	"github.com/molecula/featurebase/v3/dax"
	"github.com/molecula/featurebase/v3/dax/mds/controller"
	"github.com/molecula/featurebase/v3/dax/mds/controller/naive/boltdb"
	daxtest "github.com/molecula/featurebase/v3/dax/test"
	testbolt "github.com/molecula/featurebase/v3/dax/test/boltdb"
	"github.com/molecula/featurebase/v3/errors"
	"github.com/molecula/featurebase/v3/logger"
	"github.com/stretchr/testify/assert"
)

func TestController(t *testing.T) {
	ctx := context.Background()
	qual := dax.NewTableQualifier("acme", "db1")

	t.Run("RegisterNode", func(t *testing.T) {
		director := newTestDirector()

		schemar, cleanup := daxtest.NewSchemar(t)
		defer cleanup()
		cfg := controller.Config{
			Director: director,
			Schemar:  schemar,
		}
		con := controller.New(cfg)

		// Register a node with an invalid role type.
		node0 := &dax.Node{
			Address: "10.0.0.1:80",
			RoleTypes: []dax.RoleType{
				"invalid-role-type",
			},
		}
		err := con.RegisterNodes(ctx, node0)
		if assert.Error(t, err) {
			assert.True(t, errors.Is(err, controller.ErrCodeRoleTypeInvalid))
		}

		// Register a node with no role type.
		node1 := &dax.Node{
			Address:   "10.0.0.1:81",
			RoleTypes: []dax.RoleType{},
		}
		err = con.RegisterNodes(ctx, node1)
		if assert.Error(t, err) {
			assert.True(t, errors.Is(err, controller.ErrCodeRoleTypeInvalid))
		}
	})

	t.Run("ComputeNodes", func(t *testing.T) {
		director := newTestDirector()
		schemar, cleanup := daxtest.NewSchemar(t)
		defer cleanup()

		db := testbolt.MustOpenDB(t)
		db.InitializeBuckets(boltdb.NaiveBalancerBuckets...)
		defer func() {
			testbolt.MustCloseDB(t, db)
			testbolt.CleanupDB(t, db.Path())
		}()

		cfg := controller.Config{
			Director:          director,
			Schemar:           schemar,
			BoltDB:            db,
			StorageMethod:     "boltdb",
			ComputeBalancer:   boltdb.NewBalancer("compute", db, logger.StderrLogger),
			TranslateBalancer: boltdb.NewBalancer("translate", db, logger.StderrLogger),
		}
		con := controller.New(cfg)

		var exp []*dax.Directive

		// Register a node.
		node0 := &dax.Node{
			Address: "10.0.0.1:80",
			RoleTypes: []dax.RoleType{
				dax.RoleTypeCompute,
			},
		}
		assert.NoError(t, con.RegisterNodes(ctx, node0))

		exp = []*dax.Directive{
			{
				Address:        node0.Address,
				Method:         dax.DirectiveMethodReset,
				Tables:         []*dax.QualifiedTable{},
				ComputeRoles:   []dax.ComputeRole{},
				TranslateRoles: []dax.TranslateRole{},
				Version:        1,
			},
		}
		assert.Equal(t, exp, director.flush())

		// Add a non-keyed table.
		tbl0 := daxtest.TestQualifiedTableWithID(t, qual, "2", "foo", 0, false)
		assert.NoError(t, schemar.CreateTable(ctx, tbl0))
		assert.NoError(t, con.CreateTable(ctx, tbl0))

		exp = []*dax.Directive{}
		assert.Equal(t, exp, director.flush())

		// Add the same non-keyed table again.
		err := con.CreateTable(ctx, tbl0)
		if assert.Error(t, err) {
			assert.True(t, errors.Is(err, dax.ErrTableIDExists))
		}

		exp = []*dax.Directive{}
		assert.Equal(t, exp, director.flush())

		// Add a shard.
		assert.NoError(t, con.AddShards(ctx, tbl0.QualifiedID(),
			dax.NewVersionedShard(0, 0),
		))

		exp = []*dax.Directive{
			{
				Address: node0.Address,
				Method:  dax.DirectiveMethodDiff,
				Tables: []*dax.QualifiedTable{
					tbl0,
				},
				ComputeRoles: []dax.ComputeRole{
					{
						TableKey: tbl0.Key(),
						Shards: dax.VersionedShards{
							dax.NewVersionedShard(0, 0),
						},
					},
				},
				TranslateRoles: []dax.TranslateRole{},
				Version:        2,
			},
		}
		assert.Equal(t, exp, director.flush())

		// Register two more nodes.
		node1 := &dax.Node{
			Address: "10.0.0.1:81",
			RoleTypes: []dax.RoleType{
				dax.RoleTypeCompute,
			},
		}
		assert.NoError(t, con.RegisterNodes(ctx, node1))

		exp = []*dax.Directive{
			{
				Address:        node1.Address,
				Method:         dax.DirectiveMethodReset,
				Tables:         []*dax.QualifiedTable{},
				ComputeRoles:   []dax.ComputeRole{},
				TranslateRoles: []dax.TranslateRole{},
				Version:        3,
			},
		}
		assert.Equal(t, exp, director.flush())

		node2 := &dax.Node{
			Address: "10.0.0.1:82",
			RoleTypes: []dax.RoleType{
				dax.RoleTypeCompute,
			},
		}
		assert.NoError(t, con.RegisterNodes(ctx, node2))

		exp = []*dax.Directive{
			{
				Address:        node2.Address,
				Method:         dax.DirectiveMethodReset,
				Tables:         []*dax.QualifiedTable{},
				ComputeRoles:   []dax.ComputeRole{},
				TranslateRoles: []dax.TranslateRole{},
				Version:        4,
			},
		}
		assert.Equal(t, exp, director.flush())

		// Add more shards.
		assert.NoError(t, con.AddShards(ctx, tbl0.QualifiedID(),
			dax.NewVersionedShard(1, 0),
			dax.NewVersionedShard(2, 0),
			dax.NewVersionedShard(3, 0),
			dax.NewVersionedShard(5, 0),
			dax.NewVersionedShard(8, 0),
		))

		exp = []*dax.Directive{
			{
				Address: node0.Address,
				Method:  dax.DirectiveMethodDiff,
				Tables: []*dax.QualifiedTable{
					tbl0,
				},
				ComputeRoles: []dax.ComputeRole{
					{
						TableKey: tbl0.Key(),
						Shards: dax.VersionedShards{
							dax.NewVersionedShard(0, 0),
							dax.NewVersionedShard(3, 0),
						},
					},
				},
				TranslateRoles: []dax.TranslateRole{},
				Version:        5,
			},
			{
				Address: node1.Address,
				Method:  dax.DirectiveMethodDiff,
				Tables: []*dax.QualifiedTable{
					tbl0,
				},
				ComputeRoles: []dax.ComputeRole{
					{
						TableKey: tbl0.Key(),
						Shards: dax.VersionedShards{
							dax.NewVersionedShard(1, 0),
							dax.NewVersionedShard(5, 0),
						},
					},
				},
				TranslateRoles: []dax.TranslateRole{},
				Version:        6,
			},
			{
				Address: node2.Address,
				Method:  dax.DirectiveMethodDiff,
				Tables: []*dax.QualifiedTable{
					tbl0,
				},
				ComputeRoles: []dax.ComputeRole{
					{
						TableKey: tbl0.Key(),
						Shards: dax.VersionedShards{
							dax.NewVersionedShard(2, 0),
							dax.NewVersionedShard(8, 0),
						},
					},
				},
				TranslateRoles: []dax.TranslateRole{},
				Version:        7,
			},
		}
		assert.Equal(t, exp, director.flush())

		// Add another non-keyed table.
		tbl1 := daxtest.TestQualifiedTableWithID(t, qual, "1", "bar", 0, false)
		assert.NoError(t, schemar.CreateTable(ctx, tbl1))
		assert.NoError(t, con.CreateTable(ctx, tbl1))

		// Add more shards.
		assert.NoError(t, con.AddShards(ctx, tbl1.QualifiedID(),
			dax.NewVersionedShard(3, 0),
			dax.NewVersionedShard(5, 0),
			dax.NewVersionedShard(8, 0),
			dax.NewVersionedShard(13, 0),
		))

		exp = []*dax.Directive{
			{
				Address: node0.Address,
				Method:  dax.DirectiveMethodDiff,
				Tables: []*dax.QualifiedTable{
					tbl1,
					tbl0,
				},
				ComputeRoles: []dax.ComputeRole{
					{
						TableKey: tbl1.Key(),
						Shards: dax.VersionedShards{
							dax.NewVersionedShard(3, 0),
							dax.NewVersionedShard(13, 0),
						},
					},
					{
						TableKey: tbl0.Key(),
						Shards: dax.VersionedShards{
							dax.NewVersionedShard(0, 0),
							dax.NewVersionedShard(3, 0),
						},
					},
				},
				TranslateRoles: []dax.TranslateRole{},
				Version:        8,
			},
			{
				Address: node1.Address,
				Method:  dax.DirectiveMethodDiff,
				Tables: []*dax.QualifiedTable{
					tbl1,
					tbl0,
				},
				ComputeRoles: []dax.ComputeRole{
					{
						TableKey: tbl1.Key(),
						Shards: dax.VersionedShards{
							dax.NewVersionedShard(5, 0),
						},
					},
					{
						TableKey: tbl0.Key(),
						Shards: dax.VersionedShards{
							dax.NewVersionedShard(1, 0),
							dax.NewVersionedShard(5, 0),
						},
					},
				},
				TranslateRoles: []dax.TranslateRole{},
				Version:        9,
			},
			{
				Address: node2.Address,
				Method:  dax.DirectiveMethodDiff,
				Tables: []*dax.QualifiedTable{
					tbl1,
					tbl0,
				},
				ComputeRoles: []dax.ComputeRole{
					{
						TableKey: tbl1.Key(),
						Shards: dax.VersionedShards{
							dax.NewVersionedShard(8, 0),
						},
					},
					{
						TableKey: tbl0.Key(),
						Shards: dax.VersionedShards{
							dax.NewVersionedShard(2, 0),
							dax.NewVersionedShard(8, 0),
						},
					},
				},
				TranslateRoles: []dax.TranslateRole{},
				Version:        10,
			},
		}
		assert.Equal(t, exp, director.flush())

		// Remove a node.
		assert.NoError(t, con.DeregisterNodes(ctx, node1.Address))

		exp = []*dax.Directive{
			{
				Address: node0.Address,
				Method:  dax.DirectiveMethodDiff,
				Tables: []*dax.QualifiedTable{
					tbl1,
					tbl0,
				},
				ComputeRoles: []dax.ComputeRole{
					{
						TableKey: tbl1.Key(),
						Shards: dax.VersionedShards{
							dax.NewVersionedShard(3, 0),
							dax.NewVersionedShard(13, 0),
						},
					},
					{
						TableKey: tbl0.Key(),
						Shards: dax.VersionedShards{
							dax.NewVersionedShard(0, 0),
							dax.NewVersionedShard(1, 0),
							dax.NewVersionedShard(3, 0),
						},
					},
				},
				TranslateRoles: []dax.TranslateRole{},
				Version:        11,
			},
			{
				Address: node2.Address,
				Method:  dax.DirectiveMethodDiff,
				Tables: []*dax.QualifiedTable{
					tbl1,
					tbl0,
				},
				ComputeRoles: []dax.ComputeRole{
					{
						TableKey: tbl1.Key(),
						Shards: dax.VersionedShards{
							dax.NewVersionedShard(5, 0),
							dax.NewVersionedShard(8, 0),
						},
					},
					{
						TableKey: tbl0.Key(),
						Shards: dax.VersionedShards{
							dax.NewVersionedShard(2, 0),
							dax.NewVersionedShard(5, 0),
							dax.NewVersionedShard(8, 0),
						},
					},
				},
				TranslateRoles: []dax.TranslateRole{},
				Version:        12,
			},
		}
		assert.Equal(t, exp, director.flush())

		// Remove another node.
		assert.NoError(t, con.DeregisterNodes(ctx, node0.Address))

		exp = []*dax.Directive{
			{
				Address: node2.Address,
				Method:  dax.DirectiveMethodDiff,
				Tables: []*dax.QualifiedTable{
					tbl1,
					tbl0,
				},
				ComputeRoles: []dax.ComputeRole{
					{
						TableKey: tbl1.Key(),
						Shards: dax.VersionedShards{
							dax.NewVersionedShard(3, 0),
							dax.NewVersionedShard(5, 0),
							dax.NewVersionedShard(8, 0),
							dax.NewVersionedShard(13, 0),
						},
					},
					{
						TableKey: tbl0.Key(),
						Shards: dax.VersionedShards{
							dax.NewVersionedShard(0, 0),
							dax.NewVersionedShard(1, 0),
							dax.NewVersionedShard(2, 0),
							dax.NewVersionedShard(3, 0),
							dax.NewVersionedShard(5, 0),
							dax.NewVersionedShard(8, 0),
						},
					},
				},
				TranslateRoles: []dax.TranslateRole{},
				Version:        13,
			},
		}
		assert.Equal(t, exp, director.flush())

		// Remove final node.
		assert.NoError(t, con.DeregisterNodes(ctx, node2.Address))

		exp = []*dax.Directive{}
		assert.Equal(t, exp, director.flush())

		// Add a new node and ensure that the free shards get assigned to it.
		node3 := &dax.Node{
			Address: "10.0.0.1:83",
			RoleTypes: []dax.RoleType{
				dax.RoleTypeCompute,
			},
		}
		assert.NoError(t, con.RegisterNodes(ctx, node3))

		exp = []*dax.Directive{
			{
				Address: node3.Address,
				Method:  dax.DirectiveMethodReset,
				Tables: []*dax.QualifiedTable{
					tbl1,
					tbl0,
				},
				ComputeRoles: []dax.ComputeRole{
					{
						TableKey: tbl1.Key(),
						Shards: dax.VersionedShards{
							dax.NewVersionedShard(3, 0),
							dax.NewVersionedShard(5, 0),
							dax.NewVersionedShard(8, 0),
							dax.NewVersionedShard(13, 0),
						},
					},
					{
						TableKey: tbl0.Key(),
						Shards: dax.VersionedShards{
							dax.NewVersionedShard(0, 0),
							dax.NewVersionedShard(1, 0),
							dax.NewVersionedShard(2, 0),
							dax.NewVersionedShard(3, 0),
							dax.NewVersionedShard(5, 0),
							dax.NewVersionedShard(8, 0),
						},
					},
				},
				TranslateRoles: []dax.TranslateRole{},
				Version:        14,
			},
		}
		assert.Equal(t, exp, director.flush())

		// Remove shards.
		assert.NoError(t, con.RemoveShards(ctx, tbl0.QualifiedID(),
			dax.NewVersionedShard(2, 0),
			dax.NewVersionedShard(5, 0),
		))

		exp = []*dax.Directive{
			{
				Address: node3.Address,
				Method:  dax.DirectiveMethodDiff,
				Tables: []*dax.QualifiedTable{
					tbl1,
					tbl0,
				},
				ComputeRoles: []dax.ComputeRole{
					{
						TableKey: tbl1.Key(),
						Shards: dax.VersionedShards{
							dax.NewVersionedShard(3, 0),
							dax.NewVersionedShard(5, 0),
							dax.NewVersionedShard(8, 0),
							dax.NewVersionedShard(13, 0),
						},
					},
					{
						TableKey: tbl0.Key(),
						Shards: dax.VersionedShards{
							dax.NewVersionedShard(0, 0),
							dax.NewVersionedShard(1, 0),
							dax.NewVersionedShard(3, 0),
							dax.NewVersionedShard(8, 0),
						},
					},
				},
				TranslateRoles: []dax.TranslateRole{},
				Version:        15,
			},
		}
		assert.Equal(t, exp, director.flush())

		// Remove shards, one which does not exist.
		// Currently that doesn't result in an error, it simply no-ops on trying
		// to remove 99.
		assert.NoError(t, con.RemoveShards(ctx, tbl0.QualifiedID(),
			dax.NewVersionedShard(3, 0),
			dax.NewVersionedShard(99, 0),
		))

		exp = []*dax.Directive{
			{
				Address: node3.Address,
				Method:  dax.DirectiveMethodDiff,
				Tables: []*dax.QualifiedTable{
					tbl1,
					tbl0,
				},
				ComputeRoles: []dax.ComputeRole{
					{
						TableKey: tbl1.Key(),
						Shards: dax.VersionedShards{
							dax.NewVersionedShard(3, 0),
							dax.NewVersionedShard(5, 0),
							dax.NewVersionedShard(8, 0),
							dax.NewVersionedShard(13, 0),
						},
					},
					{
						TableKey: tbl0.Key(),
						Shards: dax.VersionedShards{
							dax.NewVersionedShard(0, 0),
							dax.NewVersionedShard(1, 0),
							dax.NewVersionedShard(8, 0),
						},
					},
				},
				TranslateRoles: []dax.TranslateRole{},
				Version:        16,
			},
		}
		assert.Equal(t, exp, director.flush())

		// Remove a table.
		assert.NoError(t, con.DropTable(ctx, tbl0.QualifiedID()))

		exp = []*dax.Directive{
			{
				Address: node3.Address,
				Method:  dax.DirectiveMethodDiff,
				Tables: []*dax.QualifiedTable{
					tbl1,
				},
				ComputeRoles: []dax.ComputeRole{
					{
						TableKey: tbl1.Key(),
						Shards: dax.VersionedShards{
							dax.NewVersionedShard(3, 0),
							dax.NewVersionedShard(5, 0),
							dax.NewVersionedShard(8, 0),
							dax.NewVersionedShard(13, 0),
						},
					},
				},
				TranslateRoles: []dax.TranslateRole{},
				Version:        17,
			},
		}
		assert.Equal(t, exp, director.flush())

		// Remove a node which doesn't exist.
		err = con.DeregisterNodes(ctx, "invalidNode")
		if assert.Error(t, err) {
			assert.True(t, errors.Is(err, dax.ErrNodeDoesNotExist))
		}

	})

	t.Run("TranslateNodes", func(t *testing.T) {
		invalidQtid := dax.NewQualifiedTableID(
			dax.NewTableQualifier("", ""),
			dax.TableID("invalidID"),
		)

		director := newTestDirector()
		schemar, cleanup := daxtest.NewSchemar(t)
		defer cleanup()

		db := testbolt.MustOpenDB(t)
		db.InitializeBuckets(boltdb.NaiveBalancerBuckets...)
		defer func() {
			testbolt.MustCloseDB(t, db)
			testbolt.CleanupDB(t, db.Path())
		}()

		cfg := controller.Config{
			Director:          director,
			Schemar:           schemar,
			BoltDB:            db,
			StorageMethod:     "boltdb",
			ComputeBalancer:   boltdb.NewBalancer("compute", db, logger.StderrLogger),
			TranslateBalancer: boltdb.NewBalancer("translate", db, logger.StderrLogger),
		}
		con := controller.New(cfg)

		var exp []*dax.Directive

		// Register a node.
		node0 := &dax.Node{
			Address: "10.0.0.1:80",
			RoleTypes: []dax.RoleType{
				dax.RoleTypeTranslate,
			},
		}
		assert.NoError(t, con.RegisterNodes(ctx, node0))

		exp = []*dax.Directive{
			{
				Address:        node0.Address,
				Method:         dax.DirectiveMethodReset,
				Tables:         []*dax.QualifiedTable{},
				ComputeRoles:   []dax.ComputeRole{},
				TranslateRoles: []dax.TranslateRole{},
				Version:        1,
			},
		}
		assert.Equal(t, exp, director.flush())

		// Try registering the same node. This should be ok.
		assert.NoError(t, con.RegisterNodes(ctx, node0))

		exp = []*dax.Directive{}
		assert.Equal(t, exp, director.flush())

		// Add a keyed table.
		tbl0 := daxtest.TestQualifiedTableWithID(t, qual, "2", "foo", 8, true)
		assert.NoError(t, schemar.CreateTable(ctx, tbl0))
		assert.NoError(t, con.CreateTable(ctx, tbl0))

		// Check directives.
		exp = []*dax.Directive{
			{
				Address: node0.Address,
				Method:  dax.DirectiveMethodDiff,
				Tables: []*dax.QualifiedTable{
					tbl0,
				},
				ComputeRoles: []dax.ComputeRole{},
				TranslateRoles: []dax.TranslateRole{
					{
						TableKey: tbl0.Key(),
						Partitions: dax.VersionedPartitions{
							dax.NewVersionedPartition(0, 0),
							dax.NewVersionedPartition(1, 0),
							dax.NewVersionedPartition(2, 0),
							dax.NewVersionedPartition(3, 0),
							dax.NewVersionedPartition(4, 0),
							dax.NewVersionedPartition(5, 0),
							dax.NewVersionedPartition(6, 0),
							dax.NewVersionedPartition(7, 0),
						},
					},
				},
				Version: 2,
			},
		}
		assert.Equal(t, exp, director.flush())

		// Register two more nodes.
		node1 := &dax.Node{
			Address: "10.0.0.1:81",
			RoleTypes: []dax.RoleType{
				dax.RoleTypeTranslate,
			},
		}
		assert.NoError(t, con.RegisterNodes(ctx, node1))

		exp = []*dax.Directive{
			{
				Address: node0.Address,
				Method:  dax.DirectiveMethodDiff,
				Tables: []*dax.QualifiedTable{
					tbl0,
				},
				ComputeRoles: []dax.ComputeRole{},
				TranslateRoles: []dax.TranslateRole{
					{
						TableKey: tbl0.Key(),
						Partitions: dax.VersionedPartitions{
							dax.NewVersionedPartition(0, 0),
							dax.NewVersionedPartition(1, 0),
							dax.NewVersionedPartition(2, 0),
							dax.NewVersionedPartition(3, 0),
						},
					},
				},
				Version: 3,
			},
			{
				Address: node1.Address,
				Method:  dax.DirectiveMethodReset,
				Tables: []*dax.QualifiedTable{
					tbl0,
				},
				ComputeRoles: []dax.ComputeRole{},
				TranslateRoles: []dax.TranslateRole{
					{
						TableKey: tbl0.Key(),
						Partitions: dax.VersionedPartitions{
							dax.NewVersionedPartition(4, 0),
							dax.NewVersionedPartition(5, 0),
							dax.NewVersionedPartition(6, 0),
							dax.NewVersionedPartition(7, 0),
						},
					},
				},
				Version: 4,
			},
		}
		assert.Equal(t, exp, director.flush())

		node2 := &dax.Node{
			Address: "10.0.0.1:82",
			RoleTypes: []dax.RoleType{
				dax.RoleTypeTranslate,
			},
		}
		assert.NoError(t, con.RegisterNodes(ctx, node2))

		exp = []*dax.Directive{
			{
				Address: node0.Address,
				Method:  dax.DirectiveMethodDiff,
				Tables: []*dax.QualifiedTable{
					tbl0,
				},
				ComputeRoles: []dax.ComputeRole{},
				TranslateRoles: []dax.TranslateRole{
					{
						TableKey: tbl0.Key(),
						Partitions: dax.VersionedPartitions{
							dax.NewVersionedPartition(0, 0),
							dax.NewVersionedPartition(1, 0),
							dax.NewVersionedPartition(2, 0),
						},
					},
				},
				Version: 5,
			},
			{
				Address: node1.Address,
				Method:  dax.DirectiveMethodDiff,
				Tables: []*dax.QualifiedTable{
					tbl0,
				},
				ComputeRoles: []dax.ComputeRole{},
				TranslateRoles: []dax.TranslateRole{
					{
						TableKey: tbl0.Key(),
						Partitions: dax.VersionedPartitions{
							dax.NewVersionedPartition(4, 0),
							dax.NewVersionedPartition(5, 0),
							dax.NewVersionedPartition(6, 0),
						},
					},
				},
				Version: 6,
			},
			{
				Address: node2.Address,
				Method:  dax.DirectiveMethodReset,
				Tables: []*dax.QualifiedTable{
					tbl0,
				},
				ComputeRoles: []dax.ComputeRole{},
				TranslateRoles: []dax.TranslateRole{
					{
						TableKey: tbl0.Key(),
						Partitions: dax.VersionedPartitions{
							dax.NewVersionedPartition(3, 0),
							dax.NewVersionedPartition(7, 0),
						},
					},
				},
				Version: 7,
			},
		}
		assert.Equal(t, exp, director.flush())

		// Add another keyed table.
		// Make PartitionN double digit to ensure that partition ints aren't
		// sorted as strings. Also, it should be large enough to spill over
		// onto node0.
		tbl1 := daxtest.TestQualifiedTableWithID(t, qual, "1", "bar", 24, true)
		assert.NoError(t, schemar.CreateTable(ctx, tbl1))
		assert.NoError(t, con.CreateTable(ctx, tbl1))

		// Check directives.
		exp = []*dax.Directive{
			{
				Address: node0.Address,
				Method:  dax.DirectiveMethodDiff,
				Tables: []*dax.QualifiedTable{
					tbl1,
					tbl0,
				},
				ComputeRoles: []dax.ComputeRole{},
				TranslateRoles: []dax.TranslateRole{
					{
						TableKey: tbl1.Key(),
						Partitions: dax.VersionedPartitions{
							dax.NewVersionedPartition(1, 0),
							dax.NewVersionedPartition(4, 0),
							dax.NewVersionedPartition(7, 0),
							dax.NewVersionedPartition(10, 0),
							dax.NewVersionedPartition(13, 0),
							dax.NewVersionedPartition(16, 0),
							dax.NewVersionedPartition(19, 0),
							dax.NewVersionedPartition(22, 0),
						},
					},
					{
						TableKey: tbl0.Key(),
						Partitions: dax.VersionedPartitions{
							dax.NewVersionedPartition(0, 0),
							dax.NewVersionedPartition(1, 0),
							dax.NewVersionedPartition(2, 0),
						},
					},
				},
				Version: 8,
			},
			{
				Address: node1.Address,
				Method:  dax.DirectiveMethodDiff,
				Tables: []*dax.QualifiedTable{
					tbl1,
					tbl0,
				},
				ComputeRoles: []dax.ComputeRole{},
				TranslateRoles: []dax.TranslateRole{
					{
						TableKey: tbl1.Key(),
						Partitions: dax.VersionedPartitions{
							dax.NewVersionedPartition(2, 0),
							dax.NewVersionedPartition(5, 0),
							dax.NewVersionedPartition(8, 0),
							dax.NewVersionedPartition(11, 0),
							dax.NewVersionedPartition(14, 0),
							dax.NewVersionedPartition(17, 0),
							dax.NewVersionedPartition(20, 0),
							dax.NewVersionedPartition(23, 0),
						},
					},
					{
						TableKey: tbl0.Key(),
						Partitions: dax.VersionedPartitions{
							dax.NewVersionedPartition(4, 0),
							dax.NewVersionedPartition(5, 0),
							dax.NewVersionedPartition(6, 0),
						},
					},
				},
				Version: 9,
			},
			{
				Address: node2.Address,
				Method:  dax.DirectiveMethodDiff,
				Tables: []*dax.QualifiedTable{
					tbl1,
					tbl0,
				},
				ComputeRoles: []dax.ComputeRole{},
				TranslateRoles: []dax.TranslateRole{
					{
						TableKey: tbl1.Key(),
						Partitions: dax.VersionedPartitions{
							dax.NewVersionedPartition(0, 0),
							dax.NewVersionedPartition(3, 0),
							dax.NewVersionedPartition(6, 0),
							dax.NewVersionedPartition(9, 0),
							dax.NewVersionedPartition(12, 0),
							dax.NewVersionedPartition(15, 0),
							dax.NewVersionedPartition(18, 0),
							dax.NewVersionedPartition(21, 0),
						},
					},
					{
						TableKey: tbl0.Key(),
						Partitions: dax.VersionedPartitions{
							dax.NewVersionedPartition(3, 0),
							dax.NewVersionedPartition(7, 0),
						},
					},
				},
				Version: 10,
			},
		}
		assert.Equal(t, exp, director.flush())

		// Remove a keyed table.
		assert.NoError(t, con.DropTable(ctx, tbl0.QualifiedID()))

		// Check directives.
		exp = []*dax.Directive{
			{
				Address: node0.Address,
				Method:  dax.DirectiveMethodDiff,
				Tables: []*dax.QualifiedTable{
					tbl1,
				},
				ComputeRoles: []dax.ComputeRole{},
				TranslateRoles: []dax.TranslateRole{
					{
						TableKey: tbl1.Key(),
						Partitions: dax.VersionedPartitions{
							dax.NewVersionedPartition(1, 0),
							dax.NewVersionedPartition(4, 0),
							dax.NewVersionedPartition(7, 0),
							dax.NewVersionedPartition(10, 0),
							dax.NewVersionedPartition(13, 0),
							dax.NewVersionedPartition(16, 0),
							dax.NewVersionedPartition(19, 0),
							dax.NewVersionedPartition(22, 0),
						},
					},
				},
				Version: 11,
			},
			{
				Address: node1.Address,
				Method:  dax.DirectiveMethodDiff,
				Tables: []*dax.QualifiedTable{
					tbl1,
				},
				ComputeRoles: []dax.ComputeRole{},
				TranslateRoles: []dax.TranslateRole{
					{
						TableKey: tbl1.Key(),
						Partitions: dax.VersionedPartitions{
							dax.NewVersionedPartition(2, 0),
							dax.NewVersionedPartition(5, 0),
							dax.NewVersionedPartition(8, 0),
							dax.NewVersionedPartition(11, 0),
							dax.NewVersionedPartition(14, 0),
							dax.NewVersionedPartition(17, 0),
							dax.NewVersionedPartition(20, 0),
							dax.NewVersionedPartition(23, 0),
						},
					},
				},
				Version: 12,
			},
			{
				Address: node2.Address,
				Method:  dax.DirectiveMethodDiff,
				Tables: []*dax.QualifiedTable{
					tbl1,
				},
				ComputeRoles: []dax.ComputeRole{},
				TranslateRoles: []dax.TranslateRole{
					{
						TableKey: tbl1.Key(),
						Partitions: dax.VersionedPartitions{
							dax.NewVersionedPartition(0, 0),
							dax.NewVersionedPartition(3, 0),
							dax.NewVersionedPartition(6, 0),
							dax.NewVersionedPartition(9, 0),
							dax.NewVersionedPartition(12, 0),
							dax.NewVersionedPartition(15, 0),
							dax.NewVersionedPartition(18, 0),
							dax.NewVersionedPartition(21, 0),
						},
					},
				},
				Version: 13,
			},
		}
		assert.Equal(t, exp, director.flush())

		// Remove a table which doesn't exist.
		err := con.DropTable(ctx, invalidQtid)
		if assert.Error(t, err) {
			assert.True(t, errors.Is(err, dax.ErrTableIDDoesNotExist))
		}

		// Add shards to a table which doesn't exist.
		err = con.AddShards(ctx, invalidQtid,
			dax.NewVersionedShard(1, 0),
			dax.NewVersionedShard(2, 0),
		)
		if assert.Error(t, err) {
			assert.True(t, errors.Is(err, dax.ErrTableIDDoesNotExist))
		}

		// Register an invalid node.
		nodeX := &dax.Node{
			Address: "",
		}
		err = con.RegisterNodes(ctx, nodeX)
		if assert.Error(t, err) {
			assert.True(t, errors.Is(err, controller.ErrCodeNodeKeyInvalid))
		}
	})

	t.Run("GetNodes", func(t *testing.T) {
		schemar, cleanup := daxtest.NewSchemar(t)
		defer cleanup()

		db := testbolt.MustOpenDB(t)
		db.InitializeBuckets(boltdb.NaiveBalancerBuckets...)
		defer func() {
			testbolt.MustCloseDB(t, db)
			testbolt.CleanupDB(t, db.Path())
		}()

		cfg := controller.Config{
			Schemar:           schemar,
			BoltDB:            db,
			StorageMethod:     "boltdb",
			ComputeBalancer:   boltdb.NewBalancer("compute", db, logger.StderrLogger),
			TranslateBalancer: boltdb.NewBalancer("translate", db, logger.StderrLogger),
		}
		con := controller.New(cfg)

		// Register two nodes.
		node0 := &dax.Node{
			Address: "10.0.0.1:80",
			RoleTypes: []dax.RoleType{
				dax.RoleTypeCompute,
				dax.RoleTypeTranslate,
			},
		}
		assert.NoError(t, con.RegisterNodes(ctx, node0))
		node1 := &dax.Node{
			Address: "10.0.0.1:81",
			RoleTypes: []dax.RoleType{
				dax.RoleTypeCompute,
				dax.RoleTypeTranslate,
			},
		}
		assert.NoError(t, con.RegisterNodes(ctx, node1))

		// Add a keyed table.
		tbl0 := daxtest.TestQualifiedTable(t, qual, "foo", 12, true)
		assert.NoError(t, schemar.CreateTable(ctx, tbl0))
		assert.NoError(t, con.CreateTable(ctx, tbl0))

		// Add shards.
		assert.NoError(t, con.AddShards(ctx, tbl0.QualifiedID(),
			dax.NewVersionedShard(0, 0),
			dax.NewVersionedShard(1, 0),
			dax.NewVersionedShard(2, 0),
			dax.NewVersionedShard(3, 0),
			dax.NewVersionedShard(11, 0),
			dax.NewVersionedShard(12, 0),
		))

		t.Run("ComputeRole", func(t *testing.T) {
			tests := []struct {
				role    dax.Role
				isWrite bool
				exp     []dax.AssignedNode
			}{
				{
					role: &dax.ComputeRole{
						TableKey: tbl0.Key(),
						Shards:   dax.NewVersionedShards(0, 1, 2, 3),
					},
					exp: []dax.AssignedNode{
						{
							Address: node0.Address,
							Role: &dax.ComputeRole{
								TableKey: tbl0.Key(),
								Shards: dax.VersionedShards{
									dax.NewVersionedShard(0, 0),
									dax.NewVersionedShard(2, 0),
								},
							},
						},
						{
							Address: node1.Address,
							Role: &dax.ComputeRole{
								TableKey: tbl0.Key(),
								Shards: dax.VersionedShards{
									dax.NewVersionedShard(1, 0),
									dax.NewVersionedShard(3, 0),
								},
							},
						},
					},
				},
				{
					role: &dax.ComputeRole{
						TableKey: tbl0.Key(),
						Shards:   dax.NewVersionedShards(1),
					},
					exp: []dax.AssignedNode{
						{
							Address: node1.Address,
							Role: &dax.ComputeRole{
								TableKey: tbl0.Key(),
								Shards: dax.VersionedShards{
									dax.NewVersionedShard(1, 0),
								},
							},
						},
					},
				},
				{
					// Add unassigned shards.
					role: &dax.ComputeRole{
						TableKey: tbl0.Key(),
						Shards:   dax.NewVersionedShards(1, 888, 889),
					},
					isWrite: true,
					exp: []dax.AssignedNode{
						{
							Address: node0.Address,
							Role: &dax.ComputeRole{
								TableKey: tbl0.Key(),
								Shards: dax.VersionedShards{
									dax.NewVersionedShard(888, 0),
								},
							},
						},
						{
							Address: node1.Address,
							Role: &dax.ComputeRole{
								TableKey: tbl0.Key(),
								Shards: dax.VersionedShards{
									dax.NewVersionedShard(1, 0),
									dax.NewVersionedShard(889, 0),
								},
							},
						},
					},
				},
				{
					// Ensure shards are not returned sorted as strings.
					role: &dax.ComputeRole{
						TableKey: tbl0.Key(),
						Shards:   dax.NewVersionedShards(2, 11),
					},
					exp: []dax.AssignedNode{
						{
							Address: node0.Address,
							Role: &dax.ComputeRole{
								TableKey: tbl0.Key(),
								Shards: dax.VersionedShards{
									dax.NewVersionedShard(2, 0),
									dax.NewVersionedShard(11, 0),
								},
							},
						},
					},
				},
			}
			for i, test := range tests {
				t.Run(fmt.Sprintf("test-%d", i), func(t *testing.T) {
					nodes, err := con.Nodes(ctx, test.role, test.isWrite)
					assert.NoError(t, err)
					assert.Equal(t, test.exp, nodes)
				})
			}
		})

		t.Run("TranslateRole", func(t *testing.T) {
			tests := []struct {
				role       dax.Role
				isWrite    bool
				exp        []dax.AssignedNode
				expErrCode errors.Code
			}{
				{
					role: &dax.TranslateRole{
						TableKey: tbl0.Key(),
						Partitions: dax.VersionedPartitions{
							dax.NewVersionedPartition(0, -1),
						},
					},
					isWrite: true,
					exp: []dax.AssignedNode{
						{
							Address: node0.Address,
							Role: &dax.TranslateRole{
								TableKey: tbl0.Key(),
								Partitions: dax.VersionedPartitions{
									dax.NewVersionedPartition(0, 0),
								},
							},
						},
					},
				},
				{
					role: &dax.TranslateRole{
						TableKey: tbl0.Key(),
						Partitions: dax.VersionedPartitions{
							dax.NewVersionedPartition(0, -1),
							dax.NewVersionedPartition(1, -1),
							dax.NewVersionedPartition(2, -1),
							dax.NewVersionedPartition(3, -1),
							dax.NewVersionedPartition(999, -1),
						},
					},
					isWrite: false,
					exp: []dax.AssignedNode{
						{
							Address: node0.Address,
							Role: &dax.TranslateRole{
								TableKey: tbl0.Key(),
								Partitions: dax.VersionedPartitions{
									dax.NewVersionedPartition(0, 0),
									dax.NewVersionedPartition(2, 0),
								},
							},
						},
						{
							Address: node1.Address,
							Role: &dax.TranslateRole{
								TableKey: tbl0.Key(),
								Partitions: dax.VersionedPartitions{
									dax.NewVersionedPartition(1, 0),
									dax.NewVersionedPartition(3, 0),
								},
							},
						},
					},
				},
				{
					role: &dax.TranslateRole{
						TableKey: tbl0.Key(),
						Partitions: dax.VersionedPartitions{
							dax.NewVersionedPartition(1, -1),
						},
					},
					isWrite: false,
					exp: []dax.AssignedNode{
						{
							Address: node1.Address,
							Role: &dax.TranslateRole{
								TableKey: tbl0.Key(),
								Partitions: dax.VersionedPartitions{
									dax.NewVersionedPartition(1, 0),
								},
							},
						},
					},
				},
				{
					// Ensure partitions are not returned sorted as strings.
					role: &dax.TranslateRole{
						TableKey: tbl0.Key(),
						Partitions: dax.VersionedPartitions{
							dax.NewVersionedPartition(2, -1),
							dax.NewVersionedPartition(10, -1),
						},
					},
					isWrite: false,
					exp: []dax.AssignedNode{
						{
							Address: node0.Address,
							Role: &dax.TranslateRole{
								TableKey: tbl0.Key(),
								Partitions: dax.VersionedPartitions{
									dax.NewVersionedPartition(2, 0),
									dax.NewVersionedPartition(10, 0),
								},
							},
						},
					},
				},
			}
			for i, test := range tests {
				t.Run(fmt.Sprintf("test-%d", i), func(t *testing.T) {
					nodes, err := con.Nodes(ctx, test.role, test.isWrite)

					if test.expErrCode != "" {
						assert.True(t, errors.Is(err, test.expErrCode))
					} else {
						assert.NoError(t, err)
						assert.Equal(t, test.exp, nodes)
					}
				})
			}
		})
	})
}

//////////////////////////////////////////////////////

// Ensure type implements interface.
var _ controller.Director = &testDirector{}

// testDirector is an implementation of the Director interface used for testing.
type testDirector struct {
	mu   sync.Mutex
	dirs []*dax.Directive
}

func newTestDirector() *testDirector {
	return &testDirector{}
}

func (d *testDirector) SendDirective(ctx context.Context, dir *dax.Directive) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.dirs = append(d.dirs, dir)
	return nil
}

func (d *testDirector) SendSnapshotShardDataRequest(ctx context.Context, req *dax.SnapshotShardDataRequest) error {
	return nil
}

func (d *testDirector) SendSnapshotTableKeysRequest(ctx context.Context, req *dax.SnapshotTableKeysRequest) error {
	return nil
}

func (d *testDirector) SendSnapshotFieldKeysRequest(ctx context.Context, req *dax.SnapshotFieldKeysRequest) error {
	return nil
}

// flush returns all the directives that have been captured through the Send()
// method and then resets the internal list.
func (d *testDirector) flush() []*dax.Directive {
	out := make([]*dax.Directive, len(d.dirs))
	copy(out, d.dirs)

	// Zero out the slice (but retain allocated memory).
	d.dirs = d.dirs[:0]

	// Since the directives can be received asyncronously, sort them here so
	// that we can more easily compare them in tests.
	sort.Sort(dax.Directives(out))

	return out
}
