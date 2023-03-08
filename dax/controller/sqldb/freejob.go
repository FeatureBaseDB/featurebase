package sqldb

import (
	"fmt"

	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/dax/controller/balancer"
	"github.com/featurebasedb/featurebase/v3/dax/models"
	"github.com/featurebasedb/featurebase/v3/logger"
	"github.com/pkg/errors"
)

func NewFreeJobService(log logger.Logger) balancer.FreeJobService {
	if log == nil {
		log = logger.NopLogger
	}
	return &freeJobService{
		log: log,
	}
}

type freeJobService struct {
	log logger.Logger
}

func (fj *freeJobService) CreateJobs(tx dax.Transaction, roleType dax.RoleType, qdbid dax.QualifiedDatabaseID, job ...dax.Job) error {
	dt, ok := tx.(*DaxTransaction)
	if !ok {
		return dax.NewErrInvalidTransaction("*sqldb.DaxTransaction")
	}
	jobs := make(models.Jobs, len(job))
	for i, j := range job {
		jobs[i] = models.Job{
			Name:       j,
			Role:       roleType,
			DatabaseID: qdbid.DatabaseID,
		}
	}
	err := dt.C.Create(jobs)
	return errors.Wrap(err, "creating jobs")
}

func (fj *freeJobService) DeleteJob(tx dax.Transaction, roleType dax.RoleType, qdbid dax.QualifiedDatabaseID, job dax.Job) error {
	dt, ok := tx.(*DaxTransaction)
	if !ok {
		return dax.NewErrInvalidTransaction("*sqldb.DaxTransaction")
	}

	err := dt.C.RawQuery("DELETE from jobs where role = ? and database_id = ? and name = ? and worker_id is NULL", roleType, qdbid.DatabaseID, job).Exec()
	return errors.Wrap(err, "deleting")
}

func (fj *freeJobService) DeleteJobsForTable(tx dax.Transaction, roleType dax.RoleType, qtid dax.QualifiedTableID) error {
	dt, ok := tx.(*DaxTransaction)
	if !ok {
		return dax.NewErrInvalidTransaction("*sqldb.DaxTransaction")
	}

	err := dt.C.RawQuery("DELETE from jobs where role = ? and database_id = ? and name LIKE ? and worker_id is NULL",
		roleType, qtid.DatabaseID, fmt.Sprintf("%s%%", qtid.Key())).Exec()
	return errors.Wrap(err, "deleting")
}

func (fj *freeJobService) ListJobs(tx dax.Transaction, roleType dax.RoleType, qdbid dax.QualifiedDatabaseID) (dax.Jobs, error) {
	dt, ok := tx.(*DaxTransaction)
	if !ok {
		return nil, dax.NewErrInvalidTransaction("*sqldb.DaxTransaction")
	}

	jobs := make(models.Jobs, 0)
	err := dt.C.Where("role = ? and database_id = ? and worker_id is NULL", roleType, qdbid.DatabaseID).Order("name asc").All(&jobs)
	if err != nil {
		return nil, errors.Wrap(err, "querying for jobs")
	}

	djs := make(dax.Jobs, len(jobs))
	for i, job := range jobs {
		djs[i] = job.Name
	}
	return djs, nil
}

// MergeJobs - AFAICT this means "mark these jobs as free"
func (fj *freeJobService) MergeJobs(tx dax.Transaction, roleType dax.RoleType, qdbid dax.QualifiedDatabaseID, jobs dax.Jobs) error {
	dt, ok := tx.(*DaxTransaction)
	if !ok {
		return dax.NewErrInvalidTransaction("*sqldb.DaxTransaction")
	}

	err := dt.C.RawQuery("UPDATE jobs SET worker_id = NULL WHERE role = ? and database_id = ?", roleType, qdbid.DatabaseID).Exec()
	return errors.Wrap(err, "marking jobs free")
}
