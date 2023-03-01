package balancer_test

import (
	"context"
	"testing"

	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/dax/controller/sqldb"
	"github.com/gobuffalo/pop/v6"
	"github.com/stretchr/testify/assert"
)

func TestFreeJobService(t *testing.T) {
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

	// must have a database to do job stuff
	schemar := &sqldb.Schemar{}
	err = schemar.CreateDatabase(tx,
		&dax.QualifiedDatabase{
			OrganizationID: orgID,
			Database:       dax.Database{ID: dbID, Name: dbName}})
	assert.NoError(t, err)

	fjSvc := &sqldb.FreeJobService{}
	qdbid := dax.QualifiedDatabaseID{OrganizationID: orgID, DatabaseID: dbID}
	qtid := dax.QualifiedTableID{
		QualifiedDatabaseID: qdbid,
		Name:                tableName,
		ID:                  tableID,
	}
	job1 := dax.Job(qtid.Key() + "job1")
	job2 := dax.Job(qtid.Key() + "job2")
	job3 := dax.Job(qtid.Key() + "job3")

	err = fjSvc.CreateJobs(tx, role, qdbid, job1, job2, job3)
	assert.NoError(t, err)

	err = fjSvc.DeleteJob(tx, role, qdbid, job2)
	assert.NoError(t, err)

	jobs, err := fjSvc.ListJobs(tx, role, qdbid)
	assert.NoError(t, err)
	assert.ElementsMatch(t, dax.Jobs{job1, job3}, jobs)

	wjSvc := &sqldb.WorkerJobService{}
	err = wjSvc.CreateWorker(tx, role, qdbid, nodeAddr)
	assert.NoError(t, err)

	err = wjSvc.CreateJobs(tx, role, qdbid, nodeAddr, job2)
	assert.NoError(t, err)

	jobs, err = fjSvc.ListJobs(tx, role, qdbid)
	assert.NoError(t, err)
	assert.ElementsMatch(t, dax.Jobs{job1, job3}, jobs)

	err = fjSvc.MergeJobs(tx, role, qdbid, dax.Jobs{job2})
	assert.NoError(t, err)

	jobs, err = fjSvc.ListJobs(tx, role, qdbid)
	assert.NoError(t, err)
	assert.ElementsMatch(t, dax.Jobs{job1, job2, job3}, jobs)

	err = fjSvc.DeleteJobsForTable(tx, role, qtid)
	assert.NoError(t, err)

	jobs, err = fjSvc.ListJobs(tx, role, qdbid)
	assert.NoError(t, err)
	assert.Empty(t, jobs)

}
