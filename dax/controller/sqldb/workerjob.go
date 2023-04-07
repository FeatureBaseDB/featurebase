package sqldb

import (
	"fmt"

	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/dax/controller/balancer"
	"github.com/featurebasedb/featurebase/v3/dax/models"
	"github.com/featurebasedb/featurebase/v3/logger"
	"github.com/gofrs/uuid"
	"github.com/pkg/errors"
)

func NewWorkerJobService(log logger.Logger) balancer.WorkerJobService {
	if log == nil {
		log = logger.NopLogger
	}
	return &workerJobService{
		log: log,
	}
}

type workerJobService struct {
	log logger.Logger
}

// WorkersJobs returns all the workers for the database along with the jobs
// associated to each worker, even if the number of jobs is 0.
func (w *workerJobService) WorkersJobs(tx dax.Transaction, roleType dax.RoleType, qdbid dax.QualifiedDatabaseID) ([]dax.WorkerInfo, error) {
	dt, ok := tx.(*DaxTransaction)
	if !ok {
		return nil, dax.NewErrInvalidTransaction("*sqldb.DaxTransaction")
	}

	// First, get all workers for the database.
	workers := models.Workers{}
	q := dt.C.Q().Eager()
	q = q.InnerJoin("services", "services.id = workers.service_id")
	q = q.Where(fmt.Sprintf("services.role_%s = true and services.database_id = ?", roleType), qdbid.DatabaseID)
	err := q.Order("address asc").All(&workers)
	if err != nil {
		return nil, errors.Wrap(err, "getting workers")
	}

	// Then, get the jobs for each worker. Ideally, we would do this in a single
	// sql query, but it wasn't clear how to do an Eager() LeftJoin() where
	// there is a where clause condition on the right side of the join (in this
	// case, `jobs.role = ?`).
	ret := make([]dax.WorkerInfo, len(workers))
	for i, worker := range workers {
		ret[i].Address = worker.Address
		jobs, err := jobsForWorker(dt, &worker, roleType)
		if err != nil {
			return nil, errors.Wrap(err, "getting jobs for worker")
		}
		ret[i].Jobs = jobs
	}

	return ret, nil
}

func jobsForWorker(dt *DaxTransaction, worker *models.Worker, roleType dax.RoleType) ([]dax.Job, error) {
	jobs := models.Jobs{}
	if err := dt.C.Where("worker_id = ? and role = ?", worker.ID, roleType).Order("name asc").All(&jobs); err != nil {
		return nil, errors.Wrapf(err, "getting jobs for worker: %s", worker.ID)
	}
	ret := make([]dax.Job, len(jobs))
	for i := range jobs {
		ret[i] = jobs[i].Name
	}
	return ret, nil
}

func (w *workerJobService) WorkerCount(tx dax.Transaction, roleType dax.RoleType, qdbid dax.QualifiedDatabaseID) (int, error) {
	dt, ok := tx.(*DaxTransaction)
	if !ok {
		return 0, dax.NewErrInvalidTransaction("*sqldb.DaxTransaction")
	}
	var count int
	err := dt.C.RawQuery("SELECT count(*) from workers inner join services on workers.service_id = services.id where services.database_id = ?", qdbid.DatabaseID).First(&count)
	return count, errors.Wrap(err, "getting count")
}

func (w *workerJobService) ListWorkers(tx dax.Transaction, roleType dax.RoleType, qdbid dax.QualifiedDatabaseID) (dax.Addresses, error) {
	dt, ok := tx.(*DaxTransaction)
	if !ok {
		return nil, dax.NewErrInvalidTransaction("*sqldb.DaxTransaction")
	}

	workers := models.Workers{}
	sql := fmt.Sprintf("role_%s = true and database_id = ?", roleType)
	err := dt.C.Select("address").Where(sql, qdbid.DatabaseID).Order("address asc").All(&workers)
	if err != nil {
		return nil, errors.Wrap(err, "getting workers")
	}

	ret := make(dax.Addresses, len(workers))
	for i, wrkr := range workers {
		ret[i] = wrkr.Address
	}

	return ret, nil
}

func (w *workerJobService) AssignWorkerToJobs(tx dax.Transaction, roleType dax.RoleType, qdbid dax.QualifiedDatabaseID, addr dax.Address, job ...dax.Job) error {
	dt, ok := tx.(*DaxTransaction)
	if !ok {
		return dax.NewErrInvalidTransaction("*sqldb.DaxTransaction")
	}

	worker := &models.Worker{}
	sql := fmt.Sprintf("address = ? and role_%s = true", roleType)
	err := dt.C.Where(sql, addr).First(worker)
	if err != nil {
		return errors.Wrapf(err, "getting worker: (%s) %s", roleType, addr)
	}

	jobs := models.Jobs{}
	err = dt.C.RawQuery("UPDATE jobs SET worker_id = ? WHERE role = ? and name in (?) RETURNING jobs.ID, jobs.Name", worker.ID, roleType, job).All(&jobs)
	if err != nil {
		return errors.Wrap(err, "updating jobs")
	}

	// Assign jobs not in "jobs", and therefore didn't get updated by the
	// previous sql statement.
	toBeAssigned := jobsNotAssigned(job, jobs, roleType, worker, qdbid.DatabaseID)

	if err := dt.C.Create(toBeAssigned); err != nil {
		return errors.Wrap(err, "creating jobs")
	}

	return nil
}

func jobsNotAssigned(incomingJobs []dax.Job, assigned models.Jobs, roleType dax.RoleType, worker *models.Worker, dbid dax.DatabaseID) (toBeAssigned models.Jobs) {
outer:
	for _, incJob := range incomingJobs {
		for _, assignedJob := range assigned {
			if assignedJob.Name == incJob {
				continue outer
			}
		}
		toBeAssigned = append(toBeAssigned,
			models.Job{
				Name:       incJob,
				Role:       roleType,
				DatabaseID: dbid,
				Worker:     worker,
			},
		)
	}
	return toBeAssigned
}

func (w *workerJobService) DeleteJob(tx dax.Transaction, roleType dax.RoleType, qdbid dax.QualifiedDatabaseID, addr dax.Address, job dax.Job) error {
	dt, ok := tx.(*DaxTransaction)
	if !ok {
		return dax.NewErrInvalidTransaction("*sqldb.DaxTransaction")
	}

	worker := &models.Worker{}
	sql := fmt.Sprintf("role_%s = true and database_id = ? and address = ?", roleType)
	err := dt.C.Select("id").Where(sql, qdbid.DatabaseID, addr).First(worker)
	if err != nil {
		return errors.Wrap(err, "getting worker")
	}

	jerb := &models.Job{}
	dt.C.Select("id").Where("role = ? and worker_id = ? and name = ?", roleType, worker.ID, job).First(jerb)
	if err != nil {
		return errors.Wrap(err, "getting job")
	}

	err = dt.C.Destroy(jerb)
	if err != nil {
		return errors.Wrap(err, "destroying job")
	}

	return nil
}

func (w *workerJobService) DeleteJobsForTable(tx dax.Transaction, roleType dax.RoleType, qtid dax.QualifiedTableID) (balancer.InternalDiffs, error) {
	dt, ok := tx.(*DaxTransaction)
	if !ok {
		return nil, dax.NewErrInvalidTransaction("*sqldb.DaxTransaction")
	}

	results := []struct {
		ID      uuid.UUID   `db:"id"`
		Name    dax.Job     `db:"name"`
		Address dax.Address `db:"address"`
	}{}
	err := dt.C.RawQuery("select j.id, j.name, w.address from jobs j inner join workers w on j.worker_id = w.id where j.role = ? and j.database_id = ? and j.name LIKE ?", roleType, qtid.QualifiedDatabaseID.DatabaseID, fmt.Sprintf("%s%%", qtid.Key())).All(&results)
	if err != nil {
		return nil, errors.Wrap(err, "querying for jobs")
	}

	idiffs := make(balancer.InternalDiffs)
	ids := make([]uuid.UUID, 0, len(results))
	for _, job := range results {
		idiffs.Removed(job.Address, job.Name)
		ids = append(ids, job.ID)
	}

	if len(ids) > 0 {
		err = dt.C.RawQuery("DELETE FROM jobs WHERE id in (?)", ids).Exec()
	}

	return idiffs, errors.Wrap(err, "deleting jobs")
}

func (w *workerJobService) JobCounts(tx dax.Transaction, roleType dax.RoleType, qdbid dax.QualifiedDatabaseID, addrs ...dax.Address) (map[dax.Address]int, error) {
	dt, ok := tx.(*DaxTransaction)
	if !ok {
		return nil, dax.NewErrInvalidTransaction("*sqldb.DaxTransaction")
	}

	results := []struct {
		Address dax.Address `db:"address"`
		Count   int         `db:"count"`
	}{}
	var err error
	if len(addrs) == 0 {
		qstring := `select address, count(*) as count
                from workers w inner join jobs j on j.worker_id = w.id
                where w.database_id = ? and w.role_%s = true
				and j.role = ?
                group by w.address`
		sql := fmt.Sprintf(qstring, roleType)
		err = dt.C.RawQuery(sql, qdbid.DatabaseID, roleType).All(&results)
	} else {
		qstring := `select address, count(*) as count
                from workers w inner join jobs j on j.worker_id = w.id
                where w.database_id = ? and w.role_%s = true
				and j.role = ?
                and w.address in (?)
                group by w.address`
		sql := fmt.Sprintf(qstring, roleType)
		err = dt.C.RawQuery(sql, qdbid.DatabaseID, roleType, addrs).All(&results)
	}
	if err != nil {
		return nil, errors.Wrap(err, "querying for jobs")
	}
	ret := make(map[dax.Address]int)
	for _, res := range results {
		ret[res.Address] = res.Count
	}

	return ret, nil
}

func (w *workerJobService) ListJobs(tx dax.Transaction, roleType dax.RoleType, qdbid dax.QualifiedDatabaseID, addr dax.Address) (dax.Jobs, error) {
	dt, ok := tx.(*DaxTransaction)
	if !ok {
		return nil, dax.NewErrInvalidTransaction("*sqldb.DaxTransaction")
	}

	worker := &models.Worker{}
	sql := fmt.Sprintf("role_%s = true and database_id = ? and address = ?", roleType)
	err := dt.C.Where(sql, qdbid.DatabaseID, addr).First(worker)
	if isNoRowsError(err) {
		return nil, nil
	} else if err != nil {
		return nil, errors.Wrap(err, "getting worker")
	}

	return jobsForWorker(dt, worker, roleType)
}

func (w *workerJobService) DatabaseForWorker(tx dax.Transaction, addr dax.Address) dax.DatabaseKey {
	dt, ok := tx.(*DaxTransaction)
	if !ok {
		panic("wrong transaction type passed to sqldb DatabaseForWorker")
	}

	db := &models.Database{}
	err := dt.C.RawQuery("select d.ID, d.organization_id from databases d inner join workers w on d.id = w.database_id where w.address = ?", addr).First(db)
	if isNoRowsError(err) {
		return ""
	} else if err != nil {
		panic(err)
	}

	return dax.QualifiedDatabase{OrganizationID: dax.OrganizationID(db.OrganizationID), Database: dax.Database{ID: dax.DatabaseID(db.ID)}}.Key()
}
