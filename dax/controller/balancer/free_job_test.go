package balancer_test

import (
	"context"
	"testing"

	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/dax/controller/sqldb"
	"github.com/stretchr/testify/require"
)

func TestFreeJobService(t *testing.T) {
	tx, err := SQLTransactor.BeginTx(context.Background(), true)
	require.NoError(t, err, "getting transaction")

	defer func() {
		err := tx.Rollback()
		if err != nil {
			t.Logf("rolling back: %v", err)
		}
	}()

	// must have a database to do job stuff
	schemar := sqldb.NewSchemar(nil)
	err = schemar.CreateDatabase(tx,
		&dax.QualifiedDatabase{
			OrganizationID: orgID,
			Database:       dax.Database{ID: dbID, Name: dbName}})
	require.NoError(t, err)

	fjSvc := sqldb.NewFreeJobService(nil)
	qdbid := dax.QualifiedDatabaseID{OrganizationID: orgID, DatabaseID: dbID}
	qtid := dax.QualifiedTableID{
		QualifiedDatabaseID: qdbid,
		Name:                tableName,
		ID:                  tableID,
	}
	job1 := dax.Job(qtid.Key() + "job1")
	job2 := dax.Job(qtid.Key() + "job2")
	job3 := dax.Job(qtid.Key() + "job3")

	node := &dax.Node{
		Address:   nodeAddr,
		RoleTypes: []dax.RoleType{role},
	}

	err = fjSvc.CreateJobs(tx, role, qdbid, job1, job2, job3)
	require.NoError(t, err)

	err = fjSvc.DeleteJob(tx, role, qdbid, job2)
	require.NoError(t, err)

	jobs, err := fjSvc.ListJobs(tx, role, qdbid)
	require.NoError(t, err)
	require.ElementsMatch(t, dax.Jobs{job1, job3}, jobs)

	workerReg := sqldb.NewWorkerRegistry(nil)
	err = workerReg.AddWorker(tx, node)
	require.NoError(t, err)

	wjSvc := sqldb.NewWorkerJobService(nil)
	err = wjSvc.CreateWorker(tx, role, qdbid, nodeAddr)
	require.NoError(t, err)

	err = wjSvc.AssignWorkerToJobs(tx, role, qdbid, nodeAddr, job1)
	require.NoError(t, err)

	jobs, err = fjSvc.ListJobs(tx, role, qdbid)
	require.NoError(t, err)
	require.ElementsMatch(t, dax.Jobs{job3}, jobs)

	err = fjSvc.MarkJobsAsFree(tx, role, qdbid, dax.Jobs{job1})
	require.NoError(t, err)

	jobs, err = fjSvc.ListJobs(tx, role, qdbid)
	require.NoError(t, err)
	require.ElementsMatch(t, dax.Jobs{job1, job3}, jobs)

	err = fjSvc.DeleteJobsForTable(tx, role, qtid)
	require.NoError(t, err)

	jobs, err = fjSvc.ListJobs(tx, role, qdbid)
	require.NoError(t, err)
	require.Empty(t, jobs)

}
