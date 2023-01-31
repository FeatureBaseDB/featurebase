package balancer_test

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/featurebasedb/featurebase/v3/dax"
	daxbolt "github.com/featurebasedb/featurebase/v3/dax/boltdb"
	"github.com/featurebasedb/featurebase/v3/dax/controller"
	"github.com/featurebasedb/featurebase/v3/dax/controller/balancer/boltdb"
	schemardb "github.com/featurebasedb/featurebase/v3/dax/controller/schemar/boltdb"
	daxtest "github.com/featurebasedb/featurebase/v3/dax/test"
	testbolt "github.com/featurebasedb/featurebase/v3/dax/test/boltdb"
	"github.com/featurebasedb/featurebase/v3/logger"
	"github.com/stretchr/testify/assert"
)

func newBoltBalancer(t *testing.T) (*daxbolt.DB, func()) {
	db := testbolt.MustOpenDB(t)
	assert.NoError(t, db.InitializeBuckets(boltdb.BalancerBuckets...))
	assert.NoError(t, db.InitializeBuckets(schemardb.SchemarBuckets...))

	return db, func() {
		testbolt.MustCloseDB(t, db)
		testbolt.CleanupDB(t, db.Path())
	}
}

type runner interface {
	run(tx dax.Transaction, bal controller.Balancer) ([]dax.WorkerDiff, error)
}

type addWorker struct {
	node *dax.Node
}

func (r *addWorker) run(tx dax.Transaction, bal controller.Balancer) ([]dax.WorkerDiff, error) {
	return bal.AddWorker(tx, r.node)
}

type removeWorker struct {
	addr dax.Address
}

func (r *removeWorker) run(tx dax.Transaction, bal controller.Balancer) ([]dax.WorkerDiff, error) {
	return bal.RemoveWorker(tx, r.addr)
}

type addJob struct {
	roleType dax.RoleType
	qtid     dax.QualifiedTableID
	job      dax.Job
}

func (r *addJob) run(tx dax.Transaction, bal controller.Balancer) ([]dax.WorkerDiff, error) {
	return bal.AddJobs(tx, r.roleType, r.qtid, r.job)
}

type removeJob struct {
	roleType dax.RoleType
	qtid     dax.QualifiedTableID
	job      dax.Job
}

func (r *removeJob) run(tx dax.Transaction, bal controller.Balancer) ([]dax.WorkerDiff, error) {
	return bal.RemoveJobs(tx, r.roleType, r.qtid, r.job)
}

type balanceDatabase struct {
	qdbid dax.QualifiedDatabaseID
}

func (r *balanceDatabase) run(tx dax.Transaction, bal controller.Balancer) ([]dax.WorkerDiff, error) {
	return bal.BalanceDatabase(tx, r.qdbid)
}

func TestBalancer(t *testing.T) {
	ctx := context.Background()
	orgID := dax.OrganizationID("acme")
	dbID := dax.DatabaseID("db1")
	dbName := dax.DatabaseName("db1name")
	tableID := dax.TableID("tbl1")
	qtid := dax.NewQualifiedTableID(dax.NewQualifiedDatabaseID(orgID, dbID), tableID)

	t.Run("SingleWorker", func(t *testing.T) {
		qdb := &dax.QualifiedDatabase{
			OrganizationID: orgID,
			Database: dax.Database{
				ID:   dbID,
				Name: dbName,
				Options: dax.DatabaseOptions{
					WorkersMin: 1,
					WorkersMax: 1,
				},
			},
		}

		schemar, cleanup := daxtest.NewSchemar(t)
		defer cleanup()

		db, cleanup := newBoltBalancer(t)
		defer cleanup()
		bal := boltdb.NewBalancer(db, schemar, logger.NewStandardLogger(os.Stderr))

		tx, err := db.BeginTx(ctx, true)
		assert.NoError(t, err)
		defer tx.Rollback()

		assert.NoError(t, schemar.CreateDatabase(tx, qdb))

		role := dax.RoleTypeCompute

		tests := []struct {
			runner   runner
			expDiff  []dax.WorkerDiff
			expState []dax.WorkerInfo
		}{
			{
				// Add job.
				runner: &addJob{
					roleType: role,
					qtid:     qtid,
					job:      "s2",
				},
				expDiff:  []dax.WorkerDiff{},
				expState: []dax.WorkerInfo{},
			},
			{
				// Add worker.
				runner: &addWorker{
					node: &dax.Node{
						Address:   "w1",
						RoleTypes: dax.RoleTypes{role},
					},
				},
				expDiff: []dax.WorkerDiff{
					{
						Address:     "w1",
						AddedJobs:   []dax.Job{"s2"},
						RemovedJobs: []dax.Job{},
					},
				},
				expState: []dax.WorkerInfo{
					{
						Address: "w1",
						Jobs:    []dax.Job{"s2"},
					},
				},
			},
			{
				// Add another job out of order.
				runner: &addJob{
					roleType: role,
					qtid:     qtid,
					job:      "s1",
				},
				expDiff: []dax.WorkerDiff{
					{
						Address:     "w1",
						AddedJobs:   []dax.Job{"s1"},
						RemovedJobs: []dax.Job{},
					},
				},
				expState: []dax.WorkerInfo{
					{
						Address: "w1",
						Jobs:    []dax.Job{"s1", "s2"},
					},
				},
			},
			{
				// Add another job.
				runner: &addJob{
					roleType: role,
					qtid:     qtid,
					job:      "s3",
				},
				expDiff: []dax.WorkerDiff{
					{
						Address:     "w1",
						AddedJobs:   []dax.Job{"s3"},
						RemovedJobs: []dax.Job{},
					},
				},
				expState: []dax.WorkerInfo{
					{
						Address: "w1",
						Jobs:    []dax.Job{"s1", "s2", "s3"},
					},
				},
			},
			{
				// Add a duplicate job.
				runner: &addJob{
					roleType: role,
					qtid:     qtid,
					job:      "s2",
				},
				expDiff: []dax.WorkerDiff{},
				expState: []dax.WorkerInfo{
					{
						Address: "w1",
						Jobs:    []dax.Job{"s1", "s2", "s3"},
					},
				},
			},
		}
		for i, test := range tests {
			t.Run(fmt.Sprintf("test-%d", i), func(t *testing.T) {
				diff, err := test.runner.run(tx, bal)
				assert.NoError(t, err)
				assert.Equal(t, test.expDiff, diff)

				state, err := bal.CurrentState(tx, role, qdb.QualifiedID())
				assert.NoError(t, err)
				assert.Equal(t, test.expState, state)
			})
		}

		assert.NoError(t, tx.Commit())
	})

	t.Run("MultipleWorkers", func(t *testing.T) {
		dbOptions := dax.DatabaseOptions{
			WorkersMin: 2,
			WorkersMax: 2,
		}

		qdb := &dax.QualifiedDatabase{
			OrganizationID: orgID,
			Database: dax.Database{
				ID:      dbID,
				Name:    dbName,
				Options: dbOptions,
			},
		}

		schemar, cleanup := daxtest.NewSchemar(t)
		defer cleanup()

		db, cleanup := newBoltBalancer(t)
		defer cleanup()
		bal := boltdb.NewBalancer(db, schemar, logger.NewStandardLogger(os.Stderr))

		tx, err := db.BeginTx(ctx, true)
		assert.NoError(t, err)
		defer tx.Rollback()

		assert.NoError(t, schemar.CreateDatabase(tx, qdb))

		role := dax.RoleTypeCompute

		type testPart struct {
			name     string
			runner   runner
			expDiff  []dax.WorkerDiff
			expState []dax.WorkerInfo
		}

		runTestPart := func(tp testPart) {
			t.Run(fmt.Sprintf("test-%s", tp.name), func(t *testing.T) {
				diff, err := tp.runner.run(tx, bal)
				assert.NoError(t, err)
				assert.Equal(t, tp.expDiff, diff)

				state, err := bal.CurrentState(tx, role, qdb.QualifiedID())
				assert.NoError(t, err)
				assert.Equal(t, tp.expState, state)
			})
		}

		runTestPart(testPart{
			name: "balance when empty",
			runner: &balanceDatabase{
				qdbid: qdb.QualifiedID(),
			},
			expDiff:  []dax.WorkerDiff{},
			expState: []dax.WorkerInfo{},
		})

		runTestPart(testPart{
			name: "add worker",
			runner: &addWorker{
				node: &dax.Node{
					Address:   "w2",
					RoleTypes: dax.RoleTypes{role},
				},
			},
			expDiff:  []dax.WorkerDiff{},
			expState: []dax.WorkerInfo{},
		})

		runTestPart(testPart{
			name: "add worker again",
			runner: &addWorker{
				node: &dax.Node{
					Address:   "w2",
					RoleTypes: dax.RoleTypes{role},
				},
			},
			expDiff:  []dax.WorkerDiff{},
			expState: []dax.WorkerInfo{},
		})

		runTestPart(testPart{
			name: "add a second worker",
			runner: &addWorker{
				node: &dax.Node{
					Address:   "w1",
					RoleTypes: dax.RoleTypes{role},
				},
			},
			expDiff:  []dax.WorkerDiff{},
			expState: []dax.WorkerInfo{},
		})

		runTestPart(testPart{
			name: "add job 2",
			runner: &addJob{
				roleType: role,
				qtid:     qtid,
				job:      "s2",
			},
			expDiff: []dax.WorkerDiff{
				{
					Address:     "w1",
					AddedJobs:   []dax.Job{"s2"},
					RemovedJobs: []dax.Job{},
				},
			},
			expState: []dax.WorkerInfo{
				{
					Address: "w1",
					Jobs:    []dax.Job{"s2"},
				},
				{
					Address: "w2",
					Jobs:    []dax.Job{},
				},
			},
		})

		runTestPart(testPart{
			name: "add job 3",
			runner: &addJob{
				roleType: role,
				qtid:     qtid,
				job:      "s3",
			},
			expDiff: []dax.WorkerDiff{
				{
					Address:     "w2",
					AddedJobs:   []dax.Job{"s3"},
					RemovedJobs: []dax.Job{},
				},
			},
			expState: []dax.WorkerInfo{
				{
					Address: "w1",
					Jobs:    []dax.Job{"s2"},
				},
				{
					Address: "w2",
					Jobs:    []dax.Job{"s3"},
				},
			},
		})

		runTestPart(testPart{
			name: "add job 1",
			runner: &addJob{
				roleType: role,
				qtid:     qtid,
				job:      "s1",
			},
			expDiff: []dax.WorkerDiff{
				{
					Address:     "w1",
					AddedJobs:   []dax.Job{"s1"},
					RemovedJobs: []dax.Job{},
				},
			},
			expState: []dax.WorkerInfo{
				{
					Address: "w1",
					Jobs:    []dax.Job{"s1", "s2"},
				},
				{
					Address: "w2",
					Jobs:    []dax.Job{"s3"},
				},
			},
		})

		// Update database options on schemar so min worker for db is 3. Here we
		// are updating the database option first, and then adding a worker to
		// satisfy those options.
		t.Run(fmt.Sprintf("test-%s", "set database options db min 3"), func(t *testing.T) {
			dbOptions.WorkersMin = 3
			dbOptions.WorkersMax = 3
			assert.NoError(t, schemar.SetDatabaseOptions(tx, qdb.QualifiedID(), dbOptions))
		})

		runTestPart(testPart{
			name: "add a third worker",
			runner: &addWorker{
				node: &dax.Node{
					Address:   "w0",
					RoleTypes: dax.RoleTypes{role},
				},
			},
			expDiff: []dax.WorkerDiff{
				{
					Address:     "w0",
					AddedJobs:   []dax.Job{"s2"},
					RemovedJobs: []dax.Job{},
				},
				{
					Address:     "w1",
					AddedJobs:   []dax.Job{},
					RemovedJobs: []dax.Job{"s2"},
				},
			},
			expState: []dax.WorkerInfo{
				{
					Address: "w0",
					Jobs:    []dax.Job{"s2"},
				},
				{
					Address: "w1",
					Jobs:    []dax.Job{"s1"},
				},
				{
					Address: "w2",
					Jobs:    []dax.Job{"s3"},
				},
			},
		})

		runTestPart(testPart{
			name: "add job 4",
			runner: &addJob{
				roleType: role,
				qtid:     qtid,
				job:      "s4",
			},
			expDiff: []dax.WorkerDiff{
				{
					Address:     "w0",
					AddedJobs:   []dax.Job{"s4"},
					RemovedJobs: []dax.Job{},
				},
			},
			expState: []dax.WorkerInfo{
				{
					Address: "w0",
					Jobs:    []dax.Job{"s2", "s4"},
				},
				{
					Address: "w1",
					Jobs:    []dax.Job{"s1"},
				},
				{
					Address: "w2",
					Jobs:    []dax.Job{"s3"},
				},
			},
		})

		runTestPart(testPart{
			name: "add job 5",
			runner: &addJob{
				roleType: role,
				qtid:     qtid,
				job:      "s5",
			},
			expDiff: []dax.WorkerDiff{
				{
					Address:     "w1",
					AddedJobs:   []dax.Job{"s5"},
					RemovedJobs: []dax.Job{},
				},
			},
			expState: []dax.WorkerInfo{
				{
					Address: "w0",
					Jobs:    []dax.Job{"s2", "s4"},
				},
				{
					Address: "w1",
					Jobs:    []dax.Job{"s1", "s5"},
				},
				{
					Address: "w2",
					Jobs:    []dax.Job{"s3"},
				},
			},
		})

		runTestPart(testPart{
			name: "add job 0",
			runner: &addJob{
				roleType: role,
				qtid:     qtid,
				job:      "s0",
			},
			expDiff: []dax.WorkerDiff{
				{
					Address:     "w2",
					AddedJobs:   []dax.Job{"s0"},
					RemovedJobs: []dax.Job{},
				},
			},
			expState: []dax.WorkerInfo{
				{
					Address: "w0",
					Jobs:    []dax.Job{"s2", "s4"},
				},
				{
					Address: "w1",
					Jobs:    []dax.Job{"s1", "s5"},
				},
				{
					Address: "w2",
					Jobs:    []dax.Job{"s0", "s3"},
				},
			},
		})

		runTestPart(testPart{
			name: "add job 6",
			runner: &addJob{
				roleType: role,
				qtid:     qtid,
				job:      "s6",
			},
			expDiff: []dax.WorkerDiff{
				{
					Address:     "w0",
					AddedJobs:   []dax.Job{"s6"},
					RemovedJobs: []dax.Job{},
				},
			},
			expState: []dax.WorkerInfo{
				{
					Address: "w0",
					Jobs:    []dax.Job{"s2", "s4", "s6"},
				},
				{
					Address: "w1",
					Jobs:    []dax.Job{"s1", "s5"},
				},
				{
					Address: "w2",
					Jobs:    []dax.Job{"s0", "s3"},
				},
			},
		})

		runTestPart(testPart{
			name: "add job 7",
			runner: &addJob{
				roleType: role,
				qtid:     qtid,
				job:      "s7",
			},
			expDiff: []dax.WorkerDiff{
				{
					Address:     "w1",
					AddedJobs:   []dax.Job{"s7"},
					RemovedJobs: []dax.Job{},
				},
			},
			expState: []dax.WorkerInfo{
				{
					Address: "w0",
					Jobs:    []dax.Job{"s2", "s4", "s6"},
				},
				{
					Address: "w1",
					Jobs:    []dax.Job{"s1", "s5", "s7"},
				},
				{
					Address: "w2",
					Jobs:    []dax.Job{"s0", "s3"},
				},
			},
		})

		//////////////////// Remove /////////////////////////

		runTestPart(testPart{
			name: "remove nonexistent worker",
			runner: &removeWorker{
				addr: "nonexistent",
			},
			expDiff: []dax.WorkerDiff{},
			expState: []dax.WorkerInfo{
				{
					Address: "w0",
					Jobs:    []dax.Job{"s2", "s4", "s6"},
				},
				{
					Address: "w1",
					Jobs:    []dax.Job{"s1", "s5", "s7"},
				},
				{
					Address: "w2",
					Jobs:    []dax.Job{"s0", "s3"},
				},
			},
		})

		runTestPart(testPart{
			name: "remove worker",
			runner: &removeWorker{
				addr: "w1",
			},
			expDiff: []dax.WorkerDiff{
				{
					Address:     "w0",
					AddedJobs:   []dax.Job{"s5"},
					RemovedJobs: []dax.Job{},
				},
				{
					Address:     "w1",
					AddedJobs:   []dax.Job{},
					RemovedJobs: []dax.Job{"s1", "s5", "s7"},
				},
				{
					Address:     "w2",
					AddedJobs:   []dax.Job{"s1", "s7"},
					RemovedJobs: []dax.Job{},
				},
			},
			expState: []dax.WorkerInfo{
				{
					Address: "w0",
					Jobs:    []dax.Job{"s2", "s4", "s5", "s6"},
				},
				{
					Address: "w2",
					Jobs:    []dax.Job{"s0", "s1", "s3", "s7"},
				},
			},
		})

		runTestPart(testPart{
			name: "remove active job",
			runner: &removeJob{
				roleType: role,
				qtid:     qtid,
				job:      "s0",
			},
			expDiff: []dax.WorkerDiff{
				{
					Address:     "w2",
					AddedJobs:   []dax.Job{},
					RemovedJobs: []dax.Job{"s0"},
				},
			},
			expState: []dax.WorkerInfo{
				{
					Address: "w0",
					Jobs:    []dax.Job{"s2", "s4", "s5", "s6"},
				},
				{
					Address: "w2",
					Jobs:    []dax.Job{"s1", "s3", "s7"},
				},
			},
		})

		// Set WorkersMin back to 2 so we can test the change to 3 again.
		t.Run(fmt.Sprintf("test-%s", "set database options db min back to 2"), func(t *testing.T) {
			dbOptions.WorkersMin = 2
			dbOptions.WorkersMax = 2
			assert.NoError(t, schemar.SetDatabaseOptions(tx, qdb.QualifiedID(), dbOptions))
		})

		runTestPart(testPart{
			name: "add a fourth worker",
			runner: &addWorker{
				node: &dax.Node{
					Address:   "w3",
					RoleTypes: dax.RoleTypes{role},
				},
			},
			expDiff: []dax.WorkerDiff{},
			expState: []dax.WorkerInfo{
				{
					Address: "w0",
					Jobs:    []dax.Job{"s2", "s4", "s5", "s6"},
				},
				{
					Address: "w2",
					Jobs:    []dax.Job{"s1", "s3", "s7"},
				},
			},
		})

		// Update database options on schemar so min worker for db is 4. Here we
		// have added a worker which will satisfy this option, and then updated
		// the option.
		t.Run(fmt.Sprintf("test-%s", "set database options db min back to 3"), func(t *testing.T) {
			dbOptions.WorkersMin = 3
			dbOptions.WorkersMax = 3
			assert.NoError(t, schemar.SetDatabaseOptions(tx, qdb.QualifiedID(), dbOptions))
		})

		// This implies that there is a condition where the database does not
		// get repaired to its workerMin: if workers per database has dropped
		// below its min, and there are no available workers to replace the
		// missing workers (and bring it back to min), then the database will
		// operate below min. If, then, a worker is added to the pool, we do not
		// currently have a process to automatically assign that worker to a
		// database under min. That happens with an explicit call to
		// assignMinWorkers or BalanceDatabase.
		runTestPart(testPart{
			name: "balance to include new third worker",
			runner: &balanceDatabase{
				qdbid: qdb.QualifiedID(),
			},
			expDiff: []dax.WorkerDiff{
				{
					Address:     "w0",
					AddedJobs:   []dax.Job{},
					RemovedJobs: []dax.Job{"s6"},
				},
				{
					Address:     "w2",
					AddedJobs:   []dax.Job{},
					RemovedJobs: []dax.Job{"s7"},
				},
				{
					Address:     "w3",
					AddedJobs:   []dax.Job{"s6", "s7"},
					RemovedJobs: []dax.Job{},
				},
			},
			expState: []dax.WorkerInfo{
				{
					Address: "w0",
					Jobs:    []dax.Job{"s2", "s4", "s5"},
				},
				{
					Address: "w2",
					Jobs:    []dax.Job{"s1", "s3"},
				},
				{
					Address: "w3",
					Jobs:    []dax.Job{"s6", "s7"},
				},
			},
		})

		assert.NoError(t, tx.Commit())
	})

	t.Run("WorkersForJobs", func(t *testing.T) {
		qdb := &dax.QualifiedDatabase{
			OrganizationID: orgID,
			Database: dax.Database{
				ID:   dbID,
				Name: dbName,
				Options: dax.DatabaseOptions{
					WorkersMin: 2,
					WorkersMax: 2,
				},
			},
		}
		qdbid := qdb.QualifiedID()

		schemar, scleanup := daxtest.NewSchemar(t)
		defer scleanup()

		role := dax.RoleTypeCompute

		db, cleanup := newBoltBalancer(t)
		defer cleanup()
		bal := boltdb.NewBalancer(db, schemar, logger.NewStandardLogger(os.Stderr))

		tx, err := db.BeginTx(ctx, true)
		assert.NoError(t, err)
		defer tx.Rollback()

		assert.NoError(t, schemar.CreateDatabase(tx, qdb))

		node1 := &dax.Node{
			Address:   "n1",
			RoleTypes: []dax.RoleType{role},
		}

		node2 := &dax.Node{
			Address:   "n2",
			RoleTypes: []dax.RoleType{role},
		}

		_, err = bal.AddWorker(tx, node1)
		assert.NoError(t, err)
		_, err = bal.AddWorker(tx, node2)
		assert.NoError(t, err)
		for i := 0; i < 12; i++ {
			_, err = bal.AddJobs(tx, role, qtid, dax.Job(fmt.Sprintf("s%d", i)))
			assert.NoError(t, err)
		}

		exp := dax.WorkerInfo{
			Address: "n1",
			Jobs:    []dax.Job{"s0", "s10", "s2", "s4", "s6", "s8"},
		}
		ws, err := bal.WorkerState(tx, role, "n1")
		assert.NoError(t, err)
		assert.Equal(t, exp, ws)

		exp = dax.WorkerInfo{
			Address: "n2",
			Jobs:    []dax.Job{"s1", "s11", "s3", "s5", "s7", "s9"},
		}
		ws, err = bal.WorkerState(tx, role, "n2")
		assert.NoError(t, err)
		assert.Equal(t, exp, ws)

		tests := []struct {
			jobs []dax.Job
			exp  []dax.WorkerInfo
		}{
			{
				jobs: []dax.Job{"s0"},
				exp: []dax.WorkerInfo{
					{Address: "n1", Jobs: []dax.Job{"s0"}},
				},
			},
			{
				jobs: []dax.Job{"s0", "s4"},
				exp: []dax.WorkerInfo{
					{Address: "n1", Jobs: []dax.Job{"s0", "s4"}},
				},
			},
			{
				jobs: []dax.Job{"s0", "s4", "s999"},
				exp: []dax.WorkerInfo{
					{Address: "n1", Jobs: []dax.Job{"s0", "s4"}},
				},
			},
			{
				jobs: []dax.Job{"s0", "s1"},
				exp: []dax.WorkerInfo{
					{Address: "n1", Jobs: []dax.Job{"s0"}},
					{Address: "n2", Jobs: []dax.Job{"s1"}},
				},
			},
			{
				jobs: []dax.Job{"s5", "s0", "s1", "s8"},
				exp: []dax.WorkerInfo{
					{Address: "n1", Jobs: []dax.Job{"s0", "s8"}},
					{Address: "n2", Jobs: []dax.Job{"s1", "s5"}},
				},
			},
		}
		for i, test := range tests {
			t.Run(fmt.Sprintf("test-%d", i), func(t *testing.T) {
				workers, err := bal.WorkersForJobs(tx, role, qdbid, test.jobs...)
				assert.NoError(t, err)
				assert.Equal(t, test.exp, workers)
			})
		}

		assert.NoError(t, tx.Commit())
	})

	t.Run("WorkersForTable", func(t *testing.T) {
		qdb := &dax.QualifiedDatabase{
			OrganizationID: orgID,
			Database: dax.Database{
				ID:   dbID,
				Name: dbName,
				Options: dax.DatabaseOptions{
					WorkersMin: 2,
					WorkersMax: 2,
				},
			},
		}

		qdbid := dax.NewQualifiedDatabaseID(orgID, dbID)

		// Table 1.
		tbl1 := &dax.Table{
			ID:   "id1",
			Name: "table1",
		}
		qtbl1 := dax.NewQualifiedTable(qdbid, tbl1)
		qtid1 := qtbl1.QualifiedID()

		// Table 2.
		tbl2 := &dax.Table{
			ID:   "id2",
			Name: "table2",
		}
		qtbl2 := dax.NewQualifiedTable(qdbid, tbl2)
		qtid2 := qtbl2.QualifiedID()

		schemar, scleanup := daxtest.NewSchemar(t)
		defer scleanup()

		role := dax.RoleTypeCompute

		db, cleanup := newBoltBalancer(t)
		defer cleanup()
		bal := boltdb.NewBalancer(db, schemar, logger.NewStandardLogger(os.Stderr))

		tx, err := db.BeginTx(ctx, true)
		assert.NoError(t, err)
		defer tx.Rollback()

		assert.NoError(t, schemar.CreateDatabase(tx, qdb))

		node1 := &dax.Node{
			Address:   "n1",
			RoleTypes: []dax.RoleType{role},
		}

		node2 := &dax.Node{
			Address:   "n2",
			RoleTypes: []dax.RoleType{role},
		}

		_, err = bal.AddWorker(tx, node1)
		assert.NoError(t, err)
		_, err = bal.AddWorker(tx, node2)
		assert.NoError(t, err)
		for i := 0; i < 12; i++ {
			job := fmt.Sprintf("%s|s%d", qtid1.Key(), i)
			_, err = bal.AddJobs(tx, role, qtid1, dax.Job(job))
			assert.NoError(t, err)
		}

		for i := 9; i < 16; i++ {
			job := fmt.Sprintf("%s|s%d", qtid2.Key(), i)
			_, err = bal.AddJobs(tx, role, qtid2, dax.Job(job))
			assert.NoError(t, err)
		}

		// fn1 and fn2 are just helper functions used to make the tests easier
		// to read.
		fn1 := func(s string) dax.Job {
			return dax.Job(fmt.Sprintf("tbl__acme__db1__id1|%s", s))
		}
		fn2 := func(s string) dax.Job {
			return dax.Job(fmt.Sprintf("tbl__acme__db1__id2|%s", s))
		}

		// Table 1
		workers, err := bal.WorkersForTable(tx, role, qtid1)
		assert.NoError(t, err)
		assert.ElementsMatch(t, []dax.WorkerInfo{
			{Address: "n1", Jobs: []dax.Job{fn1("s0"), fn1("s10"), fn1("s2"), fn1("s4"), fn1("s6"), fn1("s8")}},
			{Address: "n2", Jobs: []dax.Job{fn1("s1"), fn1("s11"), fn1("s3"), fn1("s5"), fn1("s7"), fn1("s9")}},
		}, workers)

		// Table 2
		workers, err = bal.WorkersForTable(tx, role, qtid2)
		assert.NoError(t, err)
		assert.ElementsMatch(t, []dax.WorkerInfo{
			{Address: "n1", Jobs: []dax.Job{fn2("s11"), fn2("s13"), fn2("s15"), fn2("s9")}},
			{Address: "n2", Jobs: []dax.Job{fn2("s10"), fn2("s12"), fn2("s14")}},
		}, workers)

		// No match
		qtid0 := dax.NewQualifiedTableID(qdbid, "bad")
		workers, err = bal.WorkersForTable(tx, role, qtid0)
		assert.NoError(t, err)
		assert.ElementsMatch(t, []dax.WorkerInfo{}, workers)

		assert.NoError(t, tx.Commit())
	})

	t.Run("Balance", func(t *testing.T) {
		dbOptions := dax.DatabaseOptions{
			WorkersMin: 2,
			WorkersMax: 2,
		}
		qdb := &dax.QualifiedDatabase{
			OrganizationID: orgID,
			Database: dax.Database{
				ID:      dbID,
				Name:    dbName,
				Options: dbOptions,
			},
		}

		qdbid := qdb.QualifiedID()

		schemar, scleanup := daxtest.NewSchemar(t)
		defer scleanup()

		role := dax.RoleTypeCompute

		db, cleanup := newBoltBalancer(t)
		defer cleanup()
		bal := boltdb.NewBalancer(db, schemar, logger.NewStandardLogger(os.Stderr))

		tx, err := db.BeginTx(ctx, true)
		assert.NoError(t, err)
		defer tx.Rollback()

		assert.NoError(t, schemar.CreateDatabase(tx, qdb))

		node1 := &dax.Node{
			Address:   "n1",
			RoleTypes: []dax.RoleType{role},
		}

		node2 := &dax.Node{
			Address:   "n2",
			RoleTypes: []dax.RoleType{role},
		}

		node3 := &dax.Node{
			Address:   "n3",
			RoleTypes: []dax.RoleType{role},
		}

		// Add two workers with some jobs evenly spread across them.
		_, err = bal.AddWorker(tx, node1)
		assert.NoError(t, err)
		_, err = bal.AddWorker(tx, node2)
		assert.NoError(t, err)
		for i := 0; i < 13; i++ {
			job := fmt.Sprintf("s%d", i)
			_, err = bal.AddJobs(tx, role, qtid, dax.Job(job))
			assert.NoError(t, err)
		}

		exp := dax.WorkerInfo{
			Address: "n1",
			Jobs:    []dax.Job{"s0", "s10", "s12", "s2", "s4", "s6", "s8"},
		}
		ws, err := bal.WorkerState(tx, role, "n1")
		assert.NoError(t, err)
		assert.Equal(t, exp, ws)

		exp = dax.WorkerInfo{
			Address: "n2",
			Jobs:    []dax.Job{"s1", "s11", "s3", "s5", "s7", "s9"},
		}
		ws, err = bal.WorkerState(tx, role, "n2")
		assert.NoError(t, err)
		assert.Equal(t, exp, ws)

		// Update database options on schemar so min worker for db is 3.
		t.Run(fmt.Sprintf("test-%s", "set database options db min 3"), func(t *testing.T) {
			dbOptions.WorkersMin = 3
			dbOptions.WorkersMax = 3
			assert.NoError(t, schemar.SetDatabaseOptions(tx, qdb.QualifiedID(), dbOptions))
		})

		// Now, add a worker and confirm that it has received some jobs.
		_, err = bal.AddWorker(tx, node3)
		assert.NoError(t, err)
		exp = dax.WorkerInfo{
			Address: "n3",
			Jobs:    []dax.Job{"s6", "s7", "s8", "s9"},
		}
		ws, err = bal.WorkerState(tx, role, "n3")
		assert.NoError(t, err)
		assert.Equal(t, exp, ws)

		// Finally, call Balance() and confirm that the appropriate jobs got are
		// as expected (actually, this is no longer needed here because we
		// automatically balance when we add workers, but calling it should
		// effectively be a no-op).
		_, err = bal.BalanceDatabase(tx, qdbid)
		assert.NoError(t, err)

		exp = dax.WorkerInfo{
			Address: "n1",
			Jobs:    []dax.Job{"s0", "s10", "s12", "s2", "s4"},
		}
		ws, err = bal.WorkerState(tx, role, "n1")
		assert.NoError(t, err)
		assert.Equal(t, exp, ws)

		exp = dax.WorkerInfo{
			Address: "n2",
			Jobs:    []dax.Job{"s1", "s11", "s3", "s5"},
		}
		ws, err = bal.WorkerState(tx, role, "n2")
		assert.NoError(t, err)
		assert.Equal(t, exp, ws)

		exp = dax.WorkerInfo{
			Address: "n3",
			Jobs:    []dax.Job{"s6", "s7", "s8", "s9"},
		}
		ws, err = bal.WorkerState(tx, role, "n3")
		assert.NoError(t, err)
		assert.Equal(t, exp, ws)

		assert.NoError(t, tx.Commit())
	})
}
