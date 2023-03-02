package controller_test

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"testing"

	"github.com/featurebasedb/featurebase/v3/dax"
	directivedb "github.com/featurebasedb/featurebase/v3/dax/boltdb"
	"github.com/featurebasedb/featurebase/v3/dax/controller"
	balancerdb "github.com/featurebasedb/featurebase/v3/dax/controller/balancer/boltdb"
	schemardb "github.com/featurebasedb/featurebase/v3/dax/controller/schemar/boltdb"
	daxtest "github.com/featurebasedb/featurebase/v3/dax/test"
	testbolt "github.com/featurebasedb/featurebase/v3/dax/test/boltdb"
	"github.com/featurebasedb/featurebase/v3/errors"
	"github.com/featurebasedb/featurebase/v3/logger"
	"github.com/stretchr/testify/assert"
)

func TestController(t *testing.T) {
	ctx := context.Background()
	qdbid := dax.NewQualifiedDatabaseID("acme", "db1")

	t.Run("RegisterNode", func(t *testing.T) {
		director := newTestDirector()
		schemar, cleanup := daxtest.NewSchemar(t)
		defer cleanup()

		db := testbolt.MustOpenDB(t)
		db.InitializeBuckets(balancerdb.BalancerBuckets...)
		db.InitializeBuckets(schemardb.SchemarBuckets...)
		defer func() {
			testbolt.MustCloseDB(t, db)
			testbolt.CleanupDB(t, db.Path())
		}()

		cfg := controller.Config{}
		con := controller.New(cfg)
		con.Schemar = schemar
		con.Transactor = db
		con.Director = director

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
		db.InitializeBuckets(balancerdb.BalancerBuckets...)
		db.InitializeBuckets(schemardb.SchemarBuckets...)
		db.InitializeBuckets(directivedb.DirectiveBuckets...)
		defer func() {
			testbolt.MustCloseDB(t, db)
			testbolt.CleanupDB(t, db.Path())
		}()

		cfg := controller.Config{}
		con := controller.New(cfg)
		con.Schemar = schemar
		con.Balancer = balancerdb.NewBalancer(db, schemar, logger.StderrLogger)
		con.DirectiveVersion = directivedb.NewDirectiveVersion(db)
		con.Director = director
		con.Transactor = db

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

		// Add a qualified database.
		dbOptions := dax.DatabaseOptions{
			WorkersMin: 1,
			WorkersMax: 1,
		}
		qdb1 := daxtest.TestQualifiedDatabaseWithID(t, qdbid.OrganizationID, qdbid.DatabaseID, "dbname1", dbOptions)
		assert.NoError(t, con.CreateDatabase(ctx, qdb1))

		// tbls keeps the sorted list of tables used in tests
		var tbls dax.QualifiedTables

		// Add a non-keyed table.
		tbl0 := daxtest.TestQualifiedTable(t, qdbid, "foo", 0, false)
		assert.NoError(t, con.CreateTable(ctx, tbl0))

		tbls = append(tbls, tbl0)

		exp = []*dax.Directive{}
		assert.Equal(t, exp, director.flush())

		// Add a shard.
		addShards(t, ctx, con, tbl0.QualifiedID(), 0)

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
						Shards:   dax.NewShardNums(0),
					},
				},
				TranslateRoles: []dax.TranslateRole{},
				Version:        2,
			},
		}
		assert.Equal(t, exp, director.flush())

		// Set WorkersMin to 3 so we can used the added nodes that follow.
		{
			assert.NoError(t, con.SetDatabaseOption(ctx, qdb1.QualifiedID(), dax.DatabaseOptionWorkersMin, "3"))
		}

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
		addShards(t, ctx, con, tbl0.QualifiedID(), dax.NewShardNums(1, 2, 3, 5, 8)...)

		exp = []*dax.Directive{
			{
				Address: node1.Address,
				Method:  dax.DirectiveMethodDiff,
				Tables: []*dax.QualifiedTable{
					tbl0,
				},
				ComputeRoles: []dax.ComputeRole{
					{
						TableKey: tbl0.Key(),
						Shards:   dax.NewShardNums(1),
					},
				},
				TranslateRoles: []dax.TranslateRole{},
				Version:        5,
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
						Shards:   dax.NewShardNums(2),
					},
				},
				TranslateRoles: []dax.TranslateRole{},
				Version:        6,
			},
			{
				Address: node0.Address,
				Method:  dax.DirectiveMethodDiff,
				Tables: []*dax.QualifiedTable{
					tbl0,
				},
				ComputeRoles: []dax.ComputeRole{
					{
						TableKey: tbl0.Key(),
						Shards:   dax.NewShardNums(0, 3),
					},
				},
				TranslateRoles: []dax.TranslateRole{},
				Version:        7,
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
						Shards:   dax.NewShardNums(1, 5),
					},
				},
				TranslateRoles: []dax.TranslateRole{},
				Version:        8,
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
						Shards:   dax.NewShardNums(2, 8),
					},
				},
				TranslateRoles: []dax.TranslateRole{},
				Version:        9,
			},
		}
		assert.Equal(t, exp, director.flush())

		// Add another non-keyed table.
		tbl1 := daxtest.TestQualifiedTable(t, qdbid, "bar", 0, false)
		assert.NoError(t, con.CreateTable(ctx, tbl1))

		tbls = append(tbls, tbl1)
		sort.Sort(tbls)

		// Add more shards.
		addShards(t, ctx, con, tbl1.QualifiedID(), dax.NewShardNums(3, 5, 8, 13)...)

		exp = []*dax.Directive{
			{
				Address: node0.Address,
				Method:  dax.DirectiveMethodDiff,
				Tables: []*dax.QualifiedTable{
					tbls[0],
					tbls[1],
				},
				ComputeRoles: []dax.ComputeRole{
					{
						TableKey: tbls[0].Key(),
						Shards:   dax.NewShardNums(3),
					},
					{
						TableKey: tbls[1].Key(),
						Shards:   dax.NewShardNums(0, 3),
					},
				},
				TranslateRoles: []dax.TranslateRole{},
				Version:        10,
			},
			{
				Address: node1.Address,
				Method:  dax.DirectiveMethodDiff,
				Tables: []*dax.QualifiedTable{
					tbls[0],
					tbls[1],
				},
				ComputeRoles: []dax.ComputeRole{
					{
						TableKey: tbls[0].Key(),
						Shards:   dax.NewShardNums(5),
					},
					{
						TableKey: tbls[1].Key(),
						Shards:   dax.NewShardNums(1, 5),
					},
				},
				TranslateRoles: []dax.TranslateRole{},
				Version:        11,
			},
			{
				Address: node2.Address,
				Method:  dax.DirectiveMethodDiff,
				Tables: []*dax.QualifiedTable{
					tbls[0],
					tbls[1],
				},
				ComputeRoles: []dax.ComputeRole{
					{
						TableKey: tbls[0].Key(),
						Shards:   dax.NewShardNums(8),
					},
					{
						TableKey: tbls[1].Key(),
						Shards:   dax.NewShardNums(2, 8),
					},
				},
				TranslateRoles: []dax.TranslateRole{},
				Version:        12,
			},
			{
				Address: node0.Address,
				Method:  dax.DirectiveMethodDiff,
				Tables: []*dax.QualifiedTable{
					tbls[0],
					tbls[1],
				},
				ComputeRoles: []dax.ComputeRole{
					{
						TableKey: tbls[0].Key(),
						Shards:   dax.NewShardNums(3, 13),
					},
					{
						TableKey: tbls[1].Key(),
						Shards:   dax.NewShardNums(0, 3),
					},
				},
				TranslateRoles: []dax.TranslateRole{},
				Version:        13,
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
					tbls[0],
					tbls[1],
				},
				ComputeRoles: []dax.ComputeRole{
					{
						TableKey: tbls[0].Key(),
						Shards:   dax.NewShardNums(3, 13),
					},
					{
						TableKey: tbls[1].Key(),
						Shards:   dax.NewShardNums(0, 1, 3),
					},
				},
				TranslateRoles: []dax.TranslateRole{},
				Version:        14,
			},
			{
				Address: node2.Address,
				Method:  dax.DirectiveMethodDiff,
				Tables: []*dax.QualifiedTable{
					tbls[0],
					tbls[1],
				},
				ComputeRoles: []dax.ComputeRole{
					{
						TableKey: tbls[0].Key(),
						Shards:   dax.NewShardNums(5, 8),
					},
					{
						TableKey: tbls[1].Key(),
						Shards:   dax.NewShardNums(2, 5, 8),
					},
				},
				TranslateRoles: []dax.TranslateRole{},
				Version:        15,
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
					tbls[0],
					tbls[1],
				},
				ComputeRoles: []dax.ComputeRole{
					{
						TableKey: tbls[0].Key(),
						Shards:   dax.NewShardNums(3, 5, 8, 13),
					},
					{
						TableKey: tbls[1].Key(),
						Shards:   dax.NewShardNums(0, 1, 2, 3, 5, 8),
					},
				},
				TranslateRoles: []dax.TranslateRole{},
				Version:        16,
			},
		}
		assert.Equal(t, exp, director.flush())

		// Remove final node.
		assert.NoError(t, con.DeregisterNodes(ctx, node2.Address))

		exp = []*dax.Directive{}
		assert.Equal(t, exp, director.flush())

		// Set WorkersMin to 1 so we can add a single node and have it be used
		// (currently just adding 1 node won't satisfy the minimum of 3).
		{
			assert.NoError(t, con.SetDatabaseOption(ctx, qdb1.QualifiedID(), dax.DatabaseOptionWorkersMin, "1"))
		}

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
					tbls[0],
					tbls[1],
				},
				ComputeRoles: []dax.ComputeRole{
					{
						TableKey: tbls[0].Key(),
						Shards:   dax.NewShardNums(3, 5, 8, 13),
					},
					{
						TableKey: tbls[1].Key(),
						Shards:   dax.NewShardNums(0, 1, 2, 3, 5, 8),
					},
				},
				TranslateRoles: []dax.TranslateRole{},
				Version:        17,
			},
		}
		assert.Equal(t, exp, director.flush())

		// Remove shards.
		assert.NoError(t, con.RemoveShards(ctx, tbl0.QualifiedID(), dax.NewShardNums(2, 5)...))

		exp = []*dax.Directive{
			{
				Address: node3.Address,
				Method:  dax.DirectiveMethodDiff,
				Tables: []*dax.QualifiedTable{
					tbls[0],
					tbls[1],
				},
				ComputeRoles: []dax.ComputeRole{
					{
						TableKey: tbls[0].Key(),
						Shards:   dax.NewShardNums(3, 5, 8, 13),
					},
					{
						TableKey: tbls[1].Key(),
						Shards:   dax.NewShardNums(0, 1, 3, 8),
					},
				},
				TranslateRoles: []dax.TranslateRole{},
				Version:        18,
			},
		}
		assert.Equal(t, exp, director.flush())

		// Remove shards, one which does not exist.
		// Currently that doesn't result in an error, it simply no-ops on trying
		// to remove 99.
		assert.NoError(t, con.RemoveShards(ctx, tbl0.QualifiedID(), dax.NewShardNums(3, 99)...))

		exp = []*dax.Directive{
			{
				Address: node3.Address,
				Method:  dax.DirectiveMethodDiff,
				Tables: []*dax.QualifiedTable{
					tbls[0],
					tbls[1],
				},
				ComputeRoles: []dax.ComputeRole{
					{
						TableKey: tbls[0].Key(),
						Shards:   dax.NewShardNums(3, 5, 8, 13),
					},
					{
						TableKey: tbls[1].Key(),
						Shards:   dax.NewShardNums(0, 1, 8),
					},
				},
				TranslateRoles: []dax.TranslateRole{},
				Version:        19,
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
					tbls[0],
				},
				ComputeRoles: []dax.ComputeRole{
					{
						TableKey: tbls[0].Key(),
						Shards:   dax.NewShardNums(3, 5, 8, 13),
					},
				},
				TranslateRoles: []dax.TranslateRole{},
				Version:        20,
			},
		}
		assert.Equal(t, exp, director.flush())

		// Remove a node which doesn't exist.
		assert.NoError(t, con.DeregisterNodes(ctx, "invalidNode"))
	})

	t.Run("TranslateNodes", func(t *testing.T) {
		invalidQtid := dax.NewQualifiedTableID(
			dax.NewQualifiedDatabaseID("", ""),
			dax.TableID("invalidID"),
		)

		director := newTestDirector()
		schemar, cleanup := daxtest.NewSchemar(t)
		defer cleanup()

		db := testbolt.MustOpenDB(t)
		db.InitializeBuckets(balancerdb.BalancerBuckets...)
		db.InitializeBuckets(schemardb.SchemarBuckets...)
		db.InitializeBuckets(directivedb.DirectiveBuckets...)
		defer func() {
			testbolt.MustCloseDB(t, db)
			testbolt.CleanupDB(t, db.Path())
		}()

		cfg := controller.Config{}
		con := controller.New(cfg)
		con.Schemar = schemar
		con.Balancer = balancerdb.NewBalancer(db, schemar, logger.StderrLogger)
		con.DirectiveVersion = directivedb.NewDirectiveVersion(db)
		con.Transactor = db
		con.Director = director

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

		// Add a qualified database.
		dbOptions := dax.DatabaseOptions{
			WorkersMin: 1,
			WorkersMax: 1,
		}
		qdb1 := daxtest.TestQualifiedDatabaseWithID(t, qdbid.OrganizationID, qdbid.DatabaseID, "dbname1", dbOptions)
		assert.NoError(t, con.CreateDatabase(ctx, qdb1))

		// tbls keeps the sorted list of tables used in tests
		var tbls dax.QualifiedTables

		// Add a keyed table.
		tbl0 := daxtest.TestQualifiedTable(t, qdbid, "foo", 8, true)
		assert.NoError(t, con.CreateTable(ctx, tbl0))

		tbls = append(tbls, tbl0)

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
						TableKey:   tbl0.Key(),
						Partitions: dax.NewPartitionNums(0, 1, 2, 3, 4, 5, 6, 7),
					},
				},
				Version: 2,
			},
		}
		assert.Equal(t, exp, director.flush())

		// Set WorkersMin to 3 so we can used the two added nodes that follow.
		{
			assert.NoError(t, con.SetDatabaseOption(ctx, qdb1.QualifiedID(), dax.DatabaseOptionWorkersMin, "3"))
		}

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
						TableKey:   tbl0.Key(),
						Partitions: dax.NewPartitionNums(0, 1, 2),
					},
				},
				Version: 4,
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
						TableKey:   tbl0.Key(),
						Partitions: dax.NewPartitionNums(3, 5, 7),
					},
				},
				Version: 5,
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
						TableKey:   tbl0.Key(),
						Partitions: dax.NewPartitionNums(4, 6),
					},
				},
				Version: 6,
			},
		}
		assert.Equal(t, exp, director.flush())

		// Add another keyed table.
		// Make PartitionN double digit to ensure that partition ints aren't
		// sorted as strings. Also, it should be large enough to spill over
		// onto node0.
		tbl1 := daxtest.TestQualifiedTable(t, qdbid, "bar", 24, true)
		assert.NoError(t, con.CreateTable(ctx, tbl1))

		tbls = append(tbls, tbl1)
		sort.Sort(tbls)

		// Check directives.
		exp = []*dax.Directive{
			{
				Address: node0.Address,
				Method:  dax.DirectiveMethodDiff,
				Tables: []*dax.QualifiedTable{
					tbls[0],
					tbls[1],
				},
				ComputeRoles: []dax.ComputeRole{},
				TranslateRoles: []dax.TranslateRole{
					{
						TableKey:   tbls[0].Key(),
						Partitions: dax.NewPartitionNums(1, 4, 7, 10, 13, 16, 19, 22),
					},
					{
						TableKey:   tbls[1].Key(),
						Partitions: dax.NewPartitionNums(0, 1, 2),
					},
				},
				Version: 7,
			},
			{
				Address: node1.Address,
				Method:  dax.DirectiveMethodDiff,
				Tables: []*dax.QualifiedTable{
					tbls[0],
					tbls[1],
				},
				ComputeRoles: []dax.ComputeRole{},
				TranslateRoles: []dax.TranslateRole{
					{
						TableKey:   tbls[0].Key(),
						Partitions: dax.NewPartitionNums(2, 5, 8, 11, 14, 17, 20, 23),
					},
					{
						TableKey:   tbls[1].Key(),
						Partitions: dax.NewPartitionNums(3, 5, 7),
					},
				},
				Version: 8,
			},
			{
				Address: node2.Address,
				Method:  dax.DirectiveMethodDiff,
				Tables: []*dax.QualifiedTable{
					tbls[0],
					tbls[1],
				},
				ComputeRoles: []dax.ComputeRole{},
				TranslateRoles: []dax.TranslateRole{
					{
						TableKey:   tbls[0].Key(),
						Partitions: dax.NewPartitionNums(0, 3, 6, 9, 12, 15, 18, 21),
					},
					{
						TableKey:   tbls[1].Key(),
						Partitions: dax.NewPartitionNums(4, 6),
					},
				},
				Version: 9,
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
					tbls[0],
				},
				ComputeRoles: []dax.ComputeRole{},
				TranslateRoles: []dax.TranslateRole{
					{
						TableKey:   tbls[0].Key(),
						Partitions: dax.NewPartitionNums(1, 4, 7, 10, 13, 16, 19, 22),
					},
				},
				Version: 10,
			},
			{
				Address: node1.Address,
				Method:  dax.DirectiveMethodDiff,
				Tables: []*dax.QualifiedTable{
					tbls[0],
				},
				ComputeRoles: []dax.ComputeRole{},
				TranslateRoles: []dax.TranslateRole{
					{
						TableKey:   tbls[0].Key(),
						Partitions: dax.NewPartitionNums(2, 5, 8, 11, 14, 17, 20, 23),
					},
				},
				Version: 11,
			},
			{
				Address: node2.Address,
				Method:  dax.DirectiveMethodDiff,
				Tables: []*dax.QualifiedTable{
					tbls[0],
				},
				ComputeRoles: []dax.ComputeRole{},
				TranslateRoles: []dax.TranslateRole{
					{
						TableKey:   tbls[0].Key(),
						Partitions: dax.NewPartitionNums(0, 3, 6, 9, 12, 15, 18, 21),
					},
				},
				Version: 12,
			},
		}
		assert.Equal(t, exp, director.flush())

		// Remove a table which doesn't exist.
		err := con.DropTable(ctx, invalidQtid)
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
		db.InitializeBuckets(balancerdb.BalancerBuckets...)
		db.InitializeBuckets(schemardb.SchemarBuckets...)
		db.InitializeBuckets(directivedb.DirectiveBuckets...)
		defer func() {
			testbolt.MustCloseDB(t, db)
			testbolt.CleanupDB(t, db.Path())
		}()

		cfg := controller.Config{}
		con := controller.New(cfg)
		con.Schemar = schemar
		con.Balancer = balancerdb.NewBalancer(db, schemar, logger.StderrLogger)
		con.DirectiveVersion = directivedb.NewDirectiveVersion(db)
		con.Transactor = db

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

		// Add a qualified database.
		dbOptions := dax.DatabaseOptions{
			WorkersMin: 2,
			WorkersMax: 2,
		}
		qdb1 := daxtest.TestQualifiedDatabaseWithID(t, qdbid.OrganizationID, qdbid.DatabaseID, "dbname1", dbOptions)
		assert.NoError(t, con.CreateDatabase(ctx, qdb1))

		// Add a keyed table.
		tbl0 := daxtest.TestQualifiedTable(t, qdbid, "foo", 12, true)
		assert.NoError(t, con.CreateTable(ctx, tbl0))

		// Add shards.
		addShards(t, ctx, con, tbl0.QualifiedID(), 0, 1, 2, 3, 11, 12)

		t.Run("ComputeNodes", func(t *testing.T) {
			tests := []struct {
				role    *dax.ComputeRole
				isWrite bool
				exp     []dax.ComputeNode
			}{
				{
					role: &dax.ComputeRole{
						TableKey: tbl0.Key(),
						Shards:   dax.NewShardNums(0, 1, 2, 3),
					},
					exp: []dax.ComputeNode{
						{
							Address: node0.Address,
							Table:   tbl0.Key(),
							Shards:  dax.NewShardNums(0, 2),
						},
						{
							Address: node1.Address,
							Table:   tbl0.Key(),
							Shards:  dax.NewShardNums(1, 3),
						},
					},
				},
				{
					role: &dax.ComputeRole{
						TableKey: tbl0.Key(),
						Shards:   dax.NewShardNums(1),
					},
					exp: []dax.ComputeNode{
						{
							Address: node1.Address,
							Table:   tbl0.Key(),
							Shards:  dax.NewShardNums(1),
						},
					},
				},
				{
					// Add unassigned shards.
					role: &dax.ComputeRole{
						TableKey: tbl0.Key(),
						Shards:   dax.NewShardNums(1, 888, 889),
					},
					isWrite: true,
					exp: []dax.ComputeNode{
						{
							Address: node0.Address,
							Table:   tbl0.Key(),
							Shards:  dax.NewShardNums(888),
						},
						{
							Address: node1.Address,
							Table:   tbl0.Key(),
							Shards:  dax.NewShardNums(1, 889),
						},
					},
				},
				{
					// Ensure shards are not returned sorted as strings.
					role: &dax.ComputeRole{
						TableKey: tbl0.Key(),
						Shards:   dax.NewShardNums(2, 11),
					},
					exp: []dax.ComputeNode{
						{
							Address: node0.Address,
							Table:   tbl0.Key(),
							Shards:  dax.NewShardNums(2, 11),
						},
					},
				},
			}
			for i, test := range tests {
				t.Run(fmt.Sprintf("test-%d", i), func(t *testing.T) {
					if test.isWrite {
						addShards(t, ctx, con, test.role.TableKey.QualifiedTableID(), test.role.Shards...)
					}
					nodes, err := con.ComputeNodes(ctx, test.role.TableKey.QualifiedTableID(), test.role.Shards)
					assert.NoError(t, err)
					assert.Equal(t, test.exp, nodes)
				})
			}
		})

		t.Run("TranslateNodes", func(t *testing.T) {
			tests := []struct {
				role       *dax.TranslateRole
				isWrite    bool
				exp        []dax.TranslateNode
				expErrCode errors.Code
			}{
				{
					role: &dax.TranslateRole{
						TableKey:   tbl0.Key(),
						Partitions: dax.NewPartitionNums(0),
					},
					isWrite: true,
					exp: []dax.TranslateNode{
						{
							Address:    node0.Address,
							Table:      tbl0.Key(),
							Partitions: dax.NewPartitionNums(0),
						},
					},
				},
				{
					role: &dax.TranslateRole{
						TableKey:   tbl0.Key(),
						Partitions: dax.NewPartitionNums(0, 1, 2, 3, 999),
					},
					isWrite: false,
					exp: []dax.TranslateNode{
						{
							Address:    node0.Address,
							Table:      tbl0.Key(),
							Partitions: dax.NewPartitionNums(0, 2),
						},
						{
							Address:    node1.Address,
							Table:      tbl0.Key(),
							Partitions: dax.NewPartitionNums(1, 3),
						},
					},
					expErrCode: controller.ErrCodeUnassignedJobs,
				},
				{
					role: &dax.TranslateRole{
						TableKey:   tbl0.Key(),
						Partitions: dax.NewPartitionNums(1),
					},
					isWrite: false,
					exp: []dax.TranslateNode{
						{
							Address:    node1.Address,
							Table:      tbl0.Key(),
							Partitions: dax.NewPartitionNums(1),
						},
					},
				},
				{
					// Ensure partitions are not returned sorted as strings.
					role: &dax.TranslateRole{
						TableKey:   tbl0.Key(),
						Partitions: dax.NewPartitionNums(2, 10),
					},
					isWrite: false,
					exp: []dax.TranslateNode{
						{
							Address:    node0.Address,
							Table:      tbl0.Key(),
							Partitions: dax.NewPartitionNums(2, 10),
						},
					},
				},
			}
			for i, test := range tests {
				t.Run(fmt.Sprintf("test-%d", i), func(t *testing.T) {
					if test.isWrite {
						addPartitions(t, ctx, con, test.role.TableKey.QualifiedTableID(), test.role.Partitions...)
					}
					nodes, err := con.TranslateNodes(ctx, test.role.TableKey.QualifiedTableID(), test.role.Partitions)

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

func addShards(t *testing.T, ctx context.Context, con *controller.Controller, qtid dax.QualifiedTableID, shards ...dax.ShardNum) {
	t.Helper()
	for _, shard := range shards {
		if _, err := con.IngestShard(ctx, qtid, shard); err != nil {
			assert.NoError(t, err)
		}
	}
}

func addPartitions(t *testing.T, ctx context.Context, con *controller.Controller, qtid dax.QualifiedTableID, partitions ...dax.PartitionNum) {
	t.Helper()
	for _, parition := range partitions {
		if _, err := con.IngestPartition(ctx, qtid, parition); err != nil {
			assert.NoError(t, err)
		}
	}
}
