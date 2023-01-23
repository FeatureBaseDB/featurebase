package dax_test

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"testing"
	"time"

	featurebase "github.com/featurebasedb/featurebase/v3"
	"github.com/featurebasedb/featurebase/v3/dax"
	controllerclient "github.com/featurebasedb/featurebase/v3/dax/controller/client"
	queryerclient "github.com/featurebasedb/featurebase/v3/dax/queryer/client"
	"github.com/featurebasedb/featurebase/v3/dax/server"
	"github.com/featurebasedb/featurebase/v3/dax/server/test"
	"github.com/featurebasedb/featurebase/v3/logger"
	"github.com/featurebasedb/featurebase/v3/sql3/test/defs"
	goerrors "github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDAXIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	orgID := dax.OrganizationID("acme")
	dbID := dax.DatabaseID("db1")
	qdbid := dax.NewQualifiedDatabaseID(orgID, dbID)
	dbname := dax.DatabaseName("dbname1")
	qdb := &dax.QualifiedDatabase{
		OrganizationID: qdbid.OrganizationID,
		Database: dax.Database{
			ID:   qdbid.DatabaseID,
			Name: dbname,
			Options: dax.DatabaseOptions{
				WorkersMin: 1,
				WorkersMax: 1,
			},
		},
	}

	t.Run("ServiceStart", func(t *testing.T) {
		t.Run("AllServicesByDefault", func(t *testing.T) {
			// Run ManagedCommand with no options (just defaulting to one
			// instance of each service type).
			mc := test.MustRunManagedCommand(t)
			defer mc.Close()
		})

		t.Run("StartWithNoServices", func(t *testing.T) {
			// Start ManagedCommand with an empty, new config. This results in a
			// ServiceManager running with no active services. They are added
			// later, throughout the test.
			cfg := server.NewConfig()
			opt := server.OptCommandConfig(cfg)
			mc := test.MustRunManagedCommand(t, opt)
			defer mc.Close()

			svcmgr := mc.Manage()

			var controllerKey dax.ServiceKey
			var queryerKey dax.ServiceKey
			var computerKey0 dax.ServiceKey
			var computerKey1 dax.ServiceKey

			assert.False(t, mc.Healthy(controllerKey))
			assert.False(t, mc.Healthy(queryerKey))
			assert.False(t, mc.Healthy(computerKey0))
			assert.False(t, mc.Healthy(computerKey1))

			// Start Controller.
			controllerKey = mc.NewController(cfg.Controller.Config)
			assert.NoError(t, svcmgr.ControllerStart())
			assert.True(t, mc.Healthy(controllerKey))
			assert.False(t, mc.Healthy(queryerKey))
			assert.False(t, mc.Healthy(computerKey0))
			assert.False(t, mc.Healthy(computerKey1))

			// Start Queryer.
			queryerKey = mc.NewQueryer(cfg.Queryer.Config)
			assert.NoError(t, svcmgr.QueryerStart())
			assert.True(t, mc.Healthy(controllerKey))
			assert.True(t, mc.Healthy(queryerKey))
			assert.False(t, mc.Healthy(computerKey0))
			assert.False(t, mc.Healthy(computerKey1))

			// New and Start Computer 0.
			computerKey0 = mc.NewComputer()
			assert.NoError(t, svcmgr.ComputerStart(computerKey0))
			assert.True(t, mc.Healthy(controllerKey))
			assert.True(t, mc.Healthy(queryerKey))
			assert.True(t, mc.Healthy(computerKey0))
			assert.False(t, mc.Healthy(computerKey1))

			// New and Start Computer 1.
			computerKey1 = mc.NewComputer()
			assert.NoError(t, svcmgr.ComputerStart(computerKey1))
			assert.True(t, mc.Healthy(controllerKey))
			assert.True(t, mc.Healthy(queryerKey))
			assert.True(t, mc.Healthy(computerKey0))
			assert.True(t, mc.Healthy(computerKey1))

			// Stop Computer 1.
			assert.NoError(t, svcmgr.ComputerStop(computerKey0))
			assert.True(t, mc.Healthy(controllerKey))
			assert.True(t, mc.Healthy(queryerKey))
			assert.False(t, mc.Healthy(computerKey0))
			assert.True(t, mc.Healthy(computerKey1))
		})
	})

	t.Run("SQL", func(t *testing.T) {
		mc := test.MustRunManagedCommand(t)
		defer mc.Close()

		svcmgr := mc.Manage()

		// Set up Controller client.
		controllerClient := controllerclient.New(svcmgr.Controller.Address(), svcmgr.Logger)

		// Create database.
		qdb.Options.WorkersMin = 1
		qdb.Options.WorkersMax = 1
		assert.NoError(t, controllerClient.CreateDatabase(context.Background(), qdb))

		// skips is a list of tests which are currently not passing in dax. We
		// need to get these passing before alpha.
		skips := []string{
			"testinsert/test-5",             // error messages differ
			"percentile_test/test-6",        // related to TODO in orchestrator.executePercentile
			"alterTable/alterTableBadTable", // looks like table does not exist is a different error in DAX
			"top-tests/test-1",              // don't know why this is failing at all
			"delete_tests",
			"viewtests/drop-view", // drop view does a delete
			"viewtests/drop-view-if-exists-after-drop",
			"viewtests/select-view-after-drop",
		}

		doSkip := func(name string) bool {
			for i := range skips {
				if skips[i] == name {
					return true
				}
			}
			return false
		}

		// As long as we have "skips", remove them from the list of TableTests
		// passed into runTableTests. Once we fix all the skips, remove this and
		// just pass in `defs.TableTests` to runTableTests().
		tableTests := make([]defs.TableTest, 0)
		for i, test := range defs.TableTests {
			tt := defs.TableTest{
				// TODO(tlt): fill in all the TableTest names
				Table:    test.Table,
				SQLTests: make([]defs.SQLTest, 0),
				PQLTests: make([]defs.PQLTest, 0),
			}
			for j, sqltest := range test.SQLTests {
				if doSkip(test.Name(i)) {
					continue
				}
				if doSkip(test.Name(i) + "/" + sqltest.Name(j)) {
					continue
				}
				tt.SQLTests = append(tt.SQLTests, sqltest)
			}
			for j, pqltest := range test.PQLTests {
				if doSkip(test.Name(i)) {
					continue
				}
				if doSkip(test.Name(i) + "/" + pqltest.Name(j)) {
					continue
				}
				tt.PQLTests = append(tt.PQLTests, pqltest)
			}
			tableTests = append(tableTests, tt)
		}

		runTableTests(t,
			svcmgr.Queryer.Address(),
			basicTableTestConfig(qdbid, tableTests...)...,
		)
	})

	t.Run("StandardKeyedTable", func(t *testing.T) {
		mc := test.MustRunManagedCommand(t)
		defer mc.Close()

		svcmgr := mc.Manage()

		// Set up Controller client.
		controllerClient := controllerclient.New(svcmgr.Controller.Address(), svcmgr.Logger)

		// Create database.
		qdb.Options.WorkersMin = 1
		qdb.Options.WorkersMax = 1
		assert.NoError(t, controllerClient.CreateDatabase(context.Background(), qdb))

		runTableTests(t,
			mc.Manage().Queryer.Address(),
			basicTableTestConfig(qdbid, defs.Keyed)...,
		)
	})

	t.Run("Poller", func(t *testing.T) {
		cfg := test.DefaultConfig()
		cfg.Computer.N = 2
		opt := server.OptCommandConfig(cfg)
		mc := test.MustRunManagedCommand(t, opt)

		svcmgr := mc.Manage()

		// Set up Controller client.
		controllerClient := controllerclient.New(svcmgr.Controller.Address(), svcmgr.Logger)

		// Create database.
		qdb.Options.WorkersMin = 2
		qdb.Options.WorkersMax = 2
		assert.NoError(t, controllerClient.CreateDatabase(context.Background(), qdb))

		computers := svcmgr.Computers()
		computerKey0 := dax.ServiceKey(dax.ServicePrefixComputer + "0")
		computerKey1 := dax.ServiceKey(dax.ServicePrefixComputer + "1")

		// Ingest and query some data.
		runTableTests(t,
			svcmgr.Queryer.Address(),
			basicTableTestConfig(qdbid, defs.Keyed)...,
		)

		qtid, err := controllerClient.TableID(context.Background(), qdbid, dax.TableName(defs.Keyed.Name(0)))
		assert.NoError(t, err)

		// ensure partitions are covered
		partitions0 := dax.PartitionNums{0, 2, 4, 6, 8, 10}
		partitions1 := dax.PartitionNums{1, 3, 5, 7, 9, 11}
		allPartitions := append(partitions0, partitions1...)
		sort.Sort(allPartitions)

		nodes, err := controllerClient.TranslateNodes(context.Background(), qtid, allPartitions...)
		assert.NoError(t, err)
		if assert.Len(t, nodes, 2) {
			// computer0 (node0)
			assert.Equal(t, computers[computerKey0].Address(), nodes[0].Address)
			assert.Equal(t, partitions0, nodes[0].Partitions)
			// computer1 (node1)
			assert.Equal(t, computers[computerKey1].Address(), nodes[1].Address)
			assert.Equal(t, partitions1, nodes[1].Partitions)
		}

		// stop computer 0 (may need to sleep)
		svcmgr.ComputerStop(computerKey0)

		// Give the poller time to recognize the node is gone.
		// TODO: implement this without a sleep.
		time.Sleep(5 * time.Second)

		// ensure paritions are still covered
		nodes, err = controllerClient.TranslateNodes(context.Background(), qtid, append(partitions0, partitions1...)...)
		assert.NoError(t, err)
		if assert.Len(t, nodes, 1) {
			// computer1 (node0)
			assert.Equal(t, computers[computerKey1].Address(), nodes[0].Address)
			assert.Equal(t, allPartitions, nodes[0].Partitions)
		}
	})

	t.Run("Node_Recovery", func(t *testing.T) {
		mc := test.MustRunManagedCommand(t)
		defer mc.Close()

		svcmgr := mc.Manage()

		// Set up Controller client.
		controllerClient := controllerclient.New(svcmgr.Controller.Address(), svcmgr.Logger)

		// Create database.
		qdb.Options.WorkersMin = 1
		qdb.Options.WorkersMax = 1
		assert.NoError(t, controllerClient.CreateDatabase(context.Background(), qdb))

		computerKey0 := dax.ServiceKey(dax.ServicePrefixComputer + "0")

		// Ingest and query some data.
		t.Run("ingest and query some data", func(t *testing.T) {
			runTableTests(t,
				svcmgr.Queryer.Address(),
				basicTableTestConfig(qdbid, defs.Keyed)...,
			)
		})

		// stop computer 0 (may need to sleep)
		svcmgr.ComputerStop(computerKey0)
		assert.False(t, mc.Healthy(computerKey0))

		// Give the poller time to recognize the node is gone.
		// TODO: implement this without a sleep.
		time.Sleep(5 * time.Second)

		// New and Start Computer 1.
		computerKey1 := mc.NewComputer()
		assert.NoError(t, svcmgr.ComputerStart(computerKey1))
		assert.False(t, mc.Healthy(computerKey0))
		assert.True(t, mc.Healthy(computerKey1))
		mc.WaitForApplied(t, computerKey1, 60, time.Second)

		// Query the same data.
		t.Run("query the same data", func(t *testing.T) {
			runTableTests(t,
				svcmgr.Queryer.Address(),
				tableTestConfig{
					qdbid:      qdbid,
					test:       defs.Keyed,
					skipCreate: true,
					skipInsert: true,
					querySet:   0,
				},
			)
		})
	})

	t.Run("Node_Recovery_Snapshot", func(t *testing.T) {
		mc := test.MustRunManagedCommand(t)
		defer mc.Close()

		svcmgr := mc.Manage()

		// Set up Controller client.
		controllerClient := controllerclient.New(svcmgr.Controller.Address(), svcmgr.Logger)

		// Create database.
		qdb.Options.WorkersMin = 1
		qdb.Options.WorkersMax = 1
		assert.NoError(t, controllerClient.CreateDatabase(context.Background(), qdb))

		computerKey0 := dax.ServiceKey(dax.ServicePrefixComputer + "0")

		// Ingest and query some data.
		t.Run("ingest and query some data", func(t *testing.T) {
			runTableTests(t,
				svcmgr.Queryer.Address(),
				tableTestConfig{
					qdbid:     qdbid,
					test:      defs.Keyed,
					insertSet: 0,
				},
			)
		})

		// Snapshot table
		ctx := context.Background()
		qtid, err := controllerClient.TableID(ctx, qdbid, dax.TableName(defs.Keyed.Name(0)))
		assert.NoError(t, err)

		controllerClient.SnapshotTable(ctx, qtid)

		// Ingest more data.
		t.Run("ingest and query more data", func(t *testing.T) {
			runTableTests(t,
				svcmgr.Queryer.Address(),
				tableTestConfig{
					qdbid:      qdbid,
					test:       defs.Keyed,
					skipCreate: true,
					insertSet:  1,
					querySet:   1,
				},
			)
		})

		// stop computer 0 (may need to sleep)
		svcmgr.ComputerStop(computerKey0)
		assert.False(t, mc.Healthy(computerKey0))

		// Give the poller time to recognize the node is gone.
		// TODO: implement this without a sleep.
		time.Sleep(5 * time.Second)

		// New and Start Computer 1.
		computerKey1 := mc.NewComputer()
		assert.NoError(t, svcmgr.ComputerStart(computerKey1))
		assert.True(t, mc.Healthy(computerKey1))
		mc.WaitForApplied(t, computerKey1, 60, time.Second)

		// Query the same data.
		t.Run("query the same data", func(t *testing.T) {
			runTableTests(t,
				svcmgr.Queryer.Address(),
				tableTestConfig{
					qdbid:      qdbid,
					test:       defs.Keyed,
					skipCreate: true,
					skipInsert: true,
					querySet:   1,
				},
			)
		})
	})

	t.Run("Controller_Persistence", func(t *testing.T) {
		mc := test.MustRunManagedCommand(t)
		defer mc.Close()

		svcmgr := mc.Manage()

		// Set up Controller client.
		controllerClient := controllerclient.New(svcmgr.Controller.Address(), svcmgr.Logger)

		// Create database.
		qdb.Options.WorkersMin = 1
		qdb.Options.WorkersMax = 1
		assert.NoError(t, controllerClient.CreateDatabase(context.Background(), qdb))

		computerKey0 := dax.ServiceKey(dax.ServicePrefixComputer + "0")

		controllerKey := dax.ServiceKey(dax.ServicePrefixController)
		assert.True(t, mc.Healthy(controllerKey))

		// Ingest and query some data.
		runTableTests(t,
			svcmgr.Queryer.Address(),
			basicTableTestConfig(qdbid, defs.Keyed)...,
		)

		qtid, err := controllerClient.TableID(context.Background(), qdbid, dax.TableName(defs.Keyed.Name(0)))
		assert.NoError(t, err)

		// ensure partitions are covered
		partitions := dax.PartitionNums{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}

		nodes, err := controllerClient.TranslateNodes(context.Background(), qtid, partitions...)
		assert.NoError(t, err)
		if assert.Len(t, nodes, 1) {
			// computer0 (node0)
			assert.Equal(t, svcmgr.Computer(computerKey0).Address(), nodes[0].Address)
			assert.Equal(t, partitions, nodes[0].Partitions)
		}

		// Stop Controller.
		assert.NoError(t, svcmgr.ControllerStop())
		assert.False(t, mc.Healthy(controllerKey))

		// Start New Controller.
		controllerKey = mc.NewController(mc.Config.Controller.Config)
		assert.NoError(t, svcmgr.ControllerStart())
		assert.True(t, mc.Healthy(controllerKey))

		// ensure paritions are still covered
		nodes, err = controllerClient.TranslateNodes(context.Background(), qtid, partitions...)
		assert.NoError(t, err)
		if assert.Len(t, nodes, 1) {
			// computer0 (node0)
			assert.Equal(t, svcmgr.Computer(computerKey0).Address(), nodes[0].Address)
			assert.Equal(t, partitions, nodes[0].Partitions)
		}
	})

	t.Run("Computer_Restart", func(t *testing.T) {
		mc := test.MustRunManagedCommand(t)
		defer mc.Close()

		svcmgr := mc.Manage()

		// Set up Controller client.
		controllerClient := controllerclient.New(svcmgr.Controller.Address(), svcmgr.Logger)

		// Create database.
		qdb.Options.WorkersMin = 1
		qdb.Options.WorkersMax = 1
		assert.NoError(t, controllerClient.CreateDatabase(context.Background(), qdb))

		computerKey0 := dax.ServiceKey(dax.ServicePrefixComputer + "0")

		// Ingest and query some data.
		t.Run("ingest and query some data", func(t *testing.T) {
			runTableTests(t,
				svcmgr.Queryer.Address(),
				basicTableTestConfig(qdbid, defs.Keyed)...,
			)
		})

		t.Run("stop computer0", func(t *testing.T) {
			assert.NoError(t, svcmgr.ComputerStop(computerKey0))
			assert.False(t, mc.Healthy(computerKey0))
		})

		// Wait for the poller to remove the computer.
		time.Sleep(10 * time.Second)

		t.Run("restart computer0", func(t *testing.T) {
			assert.NoError(t, svcmgr.ComputerStart(computerKey0))
			assert.True(t, mc.Healthy(computerKey0))
			mc.WaitForApplied(t, computerKey0, 60, time.Second)
		})

		// Query the same data.
		t.Run("query the same data", func(t *testing.T) {
			runTableTests(t,
				svcmgr.Queryer.Address(),
				tableTestConfig{
					qdbid:      qdbid,
					test:       defs.Keyed,
					skipCreate: true,
					skipInsert: true,
				},
			)
		})
	})

	t.Run("Delete_Database", func(t *testing.T) {
		mc := test.MustRunManagedCommand(t)
		defer mc.Close()
		svcmgr := mc.Manage()

		ctx := context.Background()

		// Set up Controller client.
		controllerClient := controllerclient.New(svcmgr.Controller.Address(), svcmgr.Logger)

		// Create database.
		qdb.Options.WorkersMin = 1
		qdb.Options.WorkersMax = 1
		assert.NoError(t, controllerClient.CreateDatabase(context.Background(), qdb))

		// Create two tables with data. Query the data to ensure it exists.
		runTableTests(t,
			svcmgr.Queryer.Address(),
			basicTableTestConfig(qdbid, defs.Keyed, defs.Unkeyed)...,
		)

		// Make sure the database and tables exist.
		db, err := controllerClient.DatabaseByName(ctx, orgID, dbname)
		assert.NoError(t, err)
		assert.NotNil(t, db)

		tbl1, err := controllerClient.TableByName(ctx, qdbid, dax.TableName(defs.Keyed.Name(0)))
		assert.NoError(t, err)
		assert.NotNil(t, tbl1)

		tbl2, err := controllerClient.TableByName(ctx, qdbid, dax.TableName(defs.Unkeyed.Name(0)))
		assert.NoError(t, err)
		assert.NotNil(t, tbl2)

		// Drop the database
		assert.NoError(t, controllerClient.DropDatabase(ctx, qdbid))

		// Make sure the database and tables no longer exist.
		db, err = controllerClient.DatabaseByName(ctx, orgID, dbname)
		if assert.Error(t, err) {
			assert.Contains(t, err.Error(), "database name 'dbname1' does not exist")
			// TODO(tlt): replace the previous line with the following once we
			// have threaded error codes through the http calls.
			// assert.True(t, errors.Is(err, dax.ErrDatabaseNameDoesNotExist))
		}
		assert.Nil(t, db)

		tbl1, err = controllerClient.TableByName(ctx, qdbid, dax.TableName(defs.Keyed.Name(0)))
		if assert.Error(t, err) {
			assert.Contains(t, err.Error(), "table name 'keyed' does not exist")
			// TODO(tlt): replace the previous line with the following once we
			// have threaded error codes through the http calls.
			// assert.True(t, errors.Is(err, dax.ErrTableNameDoesNotExist))
		}
		assert.Nil(t, tbl1)

		tbl2, err = controllerClient.TableByName(ctx, qdbid, dax.TableName(defs.Unkeyed.Name(0)))
		if assert.Error(t, err) {
			assert.Contains(t, err.Error(), "table name 'unkeyed' does not exist")
			// TODO(tlt): replace the previous line with the following once we
			// have threaded error codes through the http calls.
			// assert.True(t, errors.Is(err, dax.ErrTableNameDoesNotExist))
		}
		assert.Nil(t, tbl2)
	})

	t.Run("Delete_Table", func(t *testing.T) {
		mc := test.MustRunManagedCommand(t)
		defer mc.Close()
		svcmgr := mc.Manage()

		// Set up Controller client.
		controllerClient := controllerclient.New(svcmgr.Controller.Address(), svcmgr.Logger)

		// Create database.
		qdb.Options.WorkersMin = 1
		qdb.Options.WorkersMax = 1
		assert.NoError(t, controllerClient.CreateDatabase(context.Background(), qdb))

		testconfigs := basicTableTestConfig(qdbid, defs.Keyed)
		for i := range testconfigs {
			testconfigs[i].skipQuery = true
		}
		runTableTests(t,
			svcmgr.Queryer.Address(),
			testconfigs...,
		)

		rootDir := mc.Config.Computer.Config.DataDir

		// Ensure the index and writelogger directories are empty.
		assert.False(t, dirIsEmpty(t, rootDir+"/computer0"))
		assert.False(t, dirIsEmpty(t, rootDir+"/computer0/indexes"))
		assert.False(t, dirIsEmpty(t, rootDir+"/controller"))
		assert.False(t, dirIsEmpty(t, rootDir+"/wl"))

		resp := runSQL(t, svcmgr.Queryer.Address(), testconfigs[0].qdbid, "drop table keyed")
		assert.Empty(t, resp.Error)

		// Ensure the index and writelogger directories are empty.
		assert.False(t, dirIsEmpty(t, rootDir+"/computer0"))
		assert.True(t, dirIsEmpty(t, rootDir+"/computer0/indexes"))
		assert.False(t, dirIsEmpty(t, rootDir+"/controller"))
		assert.True(t, dirIsEmpty(t, rootDir+"/wl"))
	})
}

func dirIsEmpty(t *testing.T, name string) bool {
	f, err := os.Open(name)
	assert.NoError(t, err)
	defer f.Close()

	_, err = f.Readdirnames(1) // Or f.Readdir(1)
	if err == io.EOF {
		return true
	}
	assert.NoError(t, err)

	return false
}

///////////////////////////////////////////////////

type tableTestConfig struct {
	qdbid      dax.QualifiedDatabaseID
	test       defs.TableTest
	skipCreate bool
	skipInsert bool
	skipQuery  bool
	insertSet  int
	querySet   int
}

func basicTableTestConfig(qdbid dax.QualifiedDatabaseID, tests ...defs.TableTest) []tableTestConfig {
	ret := make([]tableTestConfig, len(tests))

	for i := range tests {
		ret[i] = tableTestConfig{
			qdbid: qdbid,
			test:  tests[i],
		}
	}

	return ret
}

func runTableTests(t *testing.T, queryerAddr dax.Address, cfgs ...tableTestConfig) {
	emptyWireQueryResponse := &featurebase.WireQueryResponse{
		Schema: featurebase.WireQuerySchema{
			Fields: []*featurebase.WireQueryField{},
		},
		Data: [][]interface{}{},
	}

	for i, cfg := range cfgs {
		t.Run(cfg.test.Name(i), func(t *testing.T) {
			// Sometimes we want to run the tests against tables which have
			// already been create/populated. In that case, doCreate can be set
			// to false and the table creation step will be skipped.
			if !cfg.skipCreate {
				// Create a table.
				if cfg.test.HasTable() {
					resp := runSQL(t, queryerAddr, cfg.qdbid, cfg.test.CreateTable())
					assertResponseEqual(t, emptyWireQueryResponse, resp)
				}
			}

			if !cfg.skipInsert {
				// Populate table with data.
				if cfg.test.HasTable() && cfg.test.HasData() {
					resp := runSQL(t, queryerAddr, cfg.qdbid, cfg.test.InsertInto(t, cfg.insertSet))
					assertResponseEqual(t, emptyWireQueryResponse, resp)
				}
			}

			if cfg.skipQuery {
				return
			}

			for j, sqltest := range cfg.test.SQLTests {
				t.Run(sqltest.Name(j), func(t *testing.T) {
					for _, sql := range sqltest.SQLs {
						t.Run(fmt.Sprintf("sql-%s", sql), func(t *testing.T) {
							log.Printf("SQL: %s", sql)

							var expRows [][]interface{}
							if cfg.querySet == 0 {
								expRows = sqltest.ExpRows
							} else {
								if len(sqltest.ExpRowsPlus1) < cfg.querySet {
									log.Printf("ExpRows not provided for query set: %d", cfg.querySet)
									return
								}
								expRows = sqltest.ExpRowsPlus1[cfg.querySet-1]
							}

							resp := runSQL(t, queryerAddr, cfg.qdbid, sql)
							headers := resp.Schema.Fields
							rows := resp.Data
							var err error
							if resp.Error != "" {
								err = goerrors.New(resp.Error)
							}

							// Check expected error instead of results.
							if sqltest.ExpErr != "" {
								if assert.Error(t, err) {
									assert.Contains(t, err.Error(), sqltest.ExpErr)
								}
								return
							}
							require.NoError(t, err)

							// Check headers.
							assert.ElementsMatch(t, sqltest.ExpHdrs, headers)

							// make a map of column name to header index
							m := make(map[dax.FieldName]int)
							for i := range headers {
								m[dax.FieldName(headers[i].Name)] = i
							}

							// Put the expRows in the same column order as the headers returned
							// by the query.
							exp := make([][]interface{}, len(expRows))
							for i := range expRows {
								exp[i] = make([]interface{}, len(headers))
								for j := range sqltest.ExpHdrs {
									targetIdx := m[sqltest.ExpHdrs[j].Name]
									assert.GreaterOrEqual(t, len(expRows[i]), len(headers),
										"expected row set has fewer columns than returned headers")
									exp[i][targetIdx] = expRows[i][j]
								}
							}

							if sqltest.SortStringKeys {
								sortStringKeys(rows)
							}

							switch sqltest.Compare {
							case defs.CompareExactOrdered:
								assert.Equal(t, len(expRows), len(rows))
								assert.EqualValues(t, exp, rows)
							case defs.CompareExactUnordered:
								assert.Equal(t, len(expRows), len(rows))
								assert.ElementsMatch(t, exp, rows)
							case defs.CompareIncludedIn:
								assert.Equal(t, sqltest.ExpRowCount, len(rows))
								for _, row := range rows {
									assert.Contains(t, exp, row)
								}
							}
						})
					}
				})
			}
			for j, pqltest := range cfg.test.PQLTests {
				t.Run(pqltest.Name(j), func(t *testing.T) {
					for _, pql := range pqltest.PQLs {
						t.Run(fmt.Sprintf("pql-%s", pql), func(t *testing.T) {
							log.Printf("PQL: %s", pql)

							var expRows [][]interface{}
							if cfg.querySet == 0 {
								expRows = pqltest.ExpRows
							} else {
								if len(pqltest.ExpRowsPlus1) < cfg.querySet {
									log.Printf("ExpRows not provided for query set: %d", cfg.querySet)
									return
								}
								expRows = pqltest.ExpRowsPlus1[cfg.querySet-1]
							}

							resp := runPQL(t, queryerAddr, cfg.qdbid, pqltest.Table, pql)
							headers := resp.Schema.Fields
							rows := resp.Data
							var err error
							if resp.Error != "" {
								err = goerrors.New(resp.Error)
							}

							// Check expected error instead of results.
							if pqltest.ExpErr != "" {
								if assert.Error(t, err) {
									assert.Contains(t, err.Error(), pqltest.ExpErr)
								}
								return
							}

							require.NoError(t, err)

							// Check headers.
							assert.ElementsMatch(t, pqltest.ExpHdrs, headers)

							// make a map of column name to header index
							m := make(map[dax.FieldName]int)
							for i := range headers {
								m[dax.FieldName(headers[i].Name)] = i
							}

							// Put the expRows in the same column order as the headers returned
							// by the query.
							exp := make([][]interface{}, len(expRows))
							for i := range expRows {
								exp[i] = make([]interface{}, len(headers))
								for j := range pqltest.ExpHdrs {
									targetIdx := m[pqltest.ExpHdrs[j].Name]
									assert.GreaterOrEqual(t, len(expRows[i]), len(headers),
										"expected row set has fewer columns than returned headers")
									exp[i][targetIdx] = expRows[i][j]
								}
							}

							assert.Equal(t, len(expRows), len(rows))
							assert.EqualValues(t, exp, rows)
						})
					}
				})
			}
		})
	}
}

///////////////////////////////////////////////////

// assertResponseEqual is a test helper which does a custom comparison on two
// WireQueryResponses.
func assertResponseEqual(tb testing.TB, a, b *featurebase.WireQueryResponse) bool {
	tb.Helper()

	c := &wireResponseComparer{
		tb:  tb,
		exp: a,
		got: b,
	}
	return assert.Condition(tb, c.Equal)
}

type wireResponseComparer struct {
	tb  testing.TB
	exp *featurebase.WireQueryResponse
	got *featurebase.WireQueryResponse
}

func (c *wireResponseComparer) Equal() bool {
	// Since ExecutionTime will always differ between exp and got, and because
	// we don't care, we just make them equal and then do the comparison.
	c.exp.ExecutionTime = c.got.ExecutionTime
	return assert.Equal(c.tb, c.exp, c.got)
}

func runSQL(tb testing.TB, queryerAddr dax.Address, qdbid dax.QualifiedDatabaseID, sql string) *featurebase.WireQueryResponse {
	tb.Helper()

	client := queryerclient.New(queryerAddr, logger.StderrLogger)

	resp, err := client.QuerySQL(context.Background(), qdbid, sql)
	assert.NoError(tb, err)

	return resp
}

func runPQL(tb testing.TB, queryerAddr dax.Address, qdbid dax.QualifiedDatabaseID, table string, pql string) *featurebase.WireQueryResponse {
	tb.Helper()

	client := queryerclient.New(queryerAddr, logger.StderrLogger)

	resp, err := client.QueryPQL(context.Background(), qdbid, dax.TableName(table), pql)
	assert.NoError(tb, err)

	return resp
}

// sortStringKeys goes through an entire set of rows, and for any []string it
// finds, it orders the elements. This is obviously only useful in tests, and
// only in cases where we expect the elements to match, but we don't care what
// order they're in. It's basically the equivalent of assert.ElementsMatch(),
// but the way we use that on rows doesn't recurse down into the field values
// within each row.
// TODO(tlt): put this in sql test?
func sortStringKeys(in [][]interface{}) {
	for i := range in {
		for j := range in[i] {
			switch v := in[i][j].(type) {
			case []string:
				sort.Strings(v)
			}
		}
	}
}
