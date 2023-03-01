package balancer_test

import (
	"context"
	"testing"

	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/dax/controller/sqldb"
	"github.com/gobuffalo/pop/v6"
	"github.com/stretchr/testify/assert"
)

const (
	orgID     = "orgid"
	dbID      = "blah"
	dbName    = "nameofdb"
	role      = "compute"
	tableName = "tbl"
	tableID   = "tblid"
)

func TestWorkerJobService(t *testing.T) {
	// TODO: currently you must start w/ a clean test database
	// soda drop -e test; soda create -e test; soda migrate -e test
	conn, err := pop.Connect("test")
	assert.NoError(t, err, "connecting")

	trans := sqldb.Transactor{Connection: conn}

	tx, err := trans.BeginTx(context.Background(), true)
	assert.NoError(t, err, "getting transaction")

	defer func() {
		err := tx.Rollback()
		if err != nil {
			t.Logf("rolling back: %v", err)
		}
	}()

	// must have a database to do workerjob stuff
	schemar := &sqldb.Schemar{}
	err = schemar.CreateDatabase(tx,
		&dax.QualifiedDatabase{
			OrganizationID: orgID,
			Database:       dax.Database{ID: dbID, Name: dbName}})
	assert.NoError(t, err)

	wjSvc := &sqldb.WorkerJobService{}
	qdbid := dax.QualifiedDatabaseID{OrganizationID: orgID, DatabaseID: dbID}

	err = wjSvc.CreateWorker(tx, role, qdbid, nodeAddr)
	assert.NoError(t, err)

	// we create a qtid to prefix jobs so that we can then test the
	// "DeleteJobsForTable" method. Is it strange that a table is not
	// explicitly mentioned in the interface on the way in, but is in
	// the Delete method? Why yes, yes it is... thank you for asking. #TODO
	qtid := dax.QualifiedTableID{
		QualifiedDatabaseID: qdbid,
		Name:                tableName,
		ID:                  tableID,
	}
	job1 := dax.Job(qtid.Key() + "job1")
	job2 := dax.Job(qtid.Key() + "job2")

	err = wjSvc.CreateJobs(tx, role, qdbid, nodeAddr, job1, job2)
	assert.NoError(t, err)

	jobs, err := wjSvc.ListJobs(tx, role, qdbid, nodeAddr)
	assert.NoError(t, err)
	assert.ElementsMatch(t, dax.Jobs{job1, job2}, jobs)

	workerInfos, err := wjSvc.WorkersJobs(tx, role, qdbid)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(workerInfos))
	assert.ElementsMatch(t, []dax.Job{job1, job2}, workerInfos[0].Jobs)

	cnt, err := wjSvc.WorkerCount(tx, role, qdbid)
	assert.NoError(t, err)
	assert.Equal(t, 1, cnt)

	addrs, err := wjSvc.ListWorkers(tx, role, qdbid)
	assert.NoError(t, err)
	assert.ElementsMatch(t, dax.Addresses{nodeAddr}, addrs)

	job3 := dax.Job(qtid.Key() + "job3")
	err = wjSvc.CreateJobs(tx, role, qdbid, nodeAddr, job3)
	assert.NoError(t, err)

	jcs, err := wjSvc.JobCounts(tx, role, qdbid, nodeAddr)
	assert.NoError(t, err)
	assert.Equal(t, 3, jcs[nodeAddr])

	err = wjSvc.DeleteJob(tx, role, qdbid, nodeAddr, job3)
	assert.NoError(t, err)

	idiffs, err := wjSvc.DeleteJobsForTable(tx, role, qtid)
	assert.NoError(t, err)
	workerDiffs := idiffs.Output()
	assert.Equal(t, 1, len(workerDiffs))
	assert.EqualValues(t, nodeAddr, workerDiffs[0].Address)
	assert.Empty(t, workerDiffs[0].AddedJobs)
	assert.ElementsMatch(t, []dax.Job{job1, job2}, workerDiffs[0].RemovedJobs)

	jobs, err = wjSvc.ListJobs(tx, role, qdbid, nodeAddr)
	assert.NoError(t, err)
	assert.Empty(t, jobs)

	dk := wjSvc.DatabaseForWorker(tx, nodeAddr)
	assert.EqualValues(t, "db__orgid__blah", dk)

	err = wjSvc.DeleteWorker(tx, role, qdbid, nodeAddr)
	assert.NoError(t, err)

	addrs, err = wjSvc.ListWorkers(tx, role, qdbid)
	assert.NoError(t, err)
	assert.Empty(t, addrs)
}
