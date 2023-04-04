package balancer_test

import (
	"context"
	"testing"

	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/dax/controller/sqldb"
	"github.com/stretchr/testify/require"
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
	tx, err := SQLTransactor.BeginTx(context.Background(), true)
	require.NoError(t, err, "getting transaction")

	defer func() {
		err := tx.Rollback()
		if err != nil {
			t.Logf("rolling back: %v", err)
		}
	}()

	// must have a database to do workerjob stuff
	schemar := sqldb.NewSchemar(nil)
	err = schemar.CreateDatabase(tx,
		&dax.QualifiedDatabase{
			OrganizationID: orgID,
			Database:       dax.Database{ID: dbID, Name: dbName}})
	require.NoError(t, err)

	wjSvc := sqldb.NewWorkerJobService(nil)
	qdbid := dax.QualifiedDatabaseID{OrganizationID: orgID, DatabaseID: dbID}

	node := &dax.Node{
		Address:   nodeAddr,
		RoleTypes: []dax.RoleType{role},
	}

	// have to create a free worker before you can create a worker job worker
	workerReg := sqldb.NewWorkerRegistry(nil)
	err = workerReg.AddWorker(tx, node)
	require.NoError(t, err)

	err = wjSvc.CreateWorker(tx, role, qdbid, nodeAddr)
	require.NoError(t, err)

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
	job3 := dax.Job(qtid.Key() + "job3")

	fjSvc := sqldb.NewFreeJobService(nil)
	err = fjSvc.CreateJobs(tx, role, qdbid, job1, job2, job3)
	require.NoError(t, err)

	err = wjSvc.AssignWorkerToJobs(tx, role, qdbid, nodeAddr, job1, job2)
	require.NoError(t, err)

	jobs, err := wjSvc.ListJobs(tx, role, qdbid, nodeAddr)
	require.NoError(t, err)
	require.ElementsMatch(t, dax.Jobs{job1, job2}, jobs)

	workerInfos, err := wjSvc.WorkersJobs(tx, role, qdbid)
	require.NoError(t, err)
	require.Equal(t, 1, len(workerInfos))
	require.ElementsMatch(t, []dax.Job{job1, job2}, workerInfos[0].Jobs)

	cnt, err := wjSvc.WorkerCount(tx, role, qdbid)
	require.NoError(t, err)
	require.Equal(t, 1, cnt)

	addrs, err := wjSvc.ListWorkers(tx, role, qdbid)
	require.NoError(t, err)
	require.ElementsMatch(t, dax.Addresses{nodeAddr}, addrs)

	err = wjSvc.AssignWorkerToJobs(tx, role, qdbid, nodeAddr, job3)
	require.NoError(t, err)

	jcs, err := wjSvc.JobCounts(tx, role, qdbid, nodeAddr)
	require.NoError(t, err)
	require.Equal(t, 3, jcs[nodeAddr])

	err = wjSvc.DeleteJob(tx, role, qdbid, nodeAddr, job3)
	require.NoError(t, err)

	idiffs, err := wjSvc.DeleteJobsForTable(tx, role, qtid)
	require.NoError(t, err)
	workerDiffs := idiffs.Output()
	require.Equal(t, 1, len(workerDiffs))
	require.EqualValues(t, nodeAddr, workerDiffs[0].Address)
	require.Empty(t, workerDiffs[0].AddedJobs)
	require.ElementsMatch(t, []dax.Job{job1, job2}, workerDiffs[0].RemovedJobs)

	jobs, err = wjSvc.ListJobs(tx, role, qdbid, nodeAddr)
	require.NoError(t, err)
	require.Empty(t, jobs)

	dk := wjSvc.DatabaseForWorker(tx, nodeAddr)
	require.EqualValues(t, "db__orgid__blah", dk)

	err = wjSvc.ReleaseWorkers(tx, nodeAddr)
	require.NoError(t, err)

	addrs, err = wjSvc.ListWorkers(tx, role, qdbid)
	require.NoError(t, err)
	require.Empty(t, addrs)
}
