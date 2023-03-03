package sqldb

import (
	"fmt"

	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/dax/controller/balancer"
	"github.com/featurebasedb/featurebase/v3/dax/models"
	"github.com/featurebasedb/featurebase/v3/logger"
	"github.com/gobuffalo/nulls"
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

func (w *workerJobService) WorkersJobs(tx dax.Transaction, roleType dax.RoleType, qdbid dax.QualifiedDatabaseID) ([]dax.WorkerInfo, error) {
	dt, ok := tx.(*DaxTransaction)
	if !ok {
		return nil, dax.NewErrInvalidTransaction("*sqldb.DaxTransaction")
	}

	workers := models.Workers{}
	err := dt.C.Eager().Where("role = ? and database_id = ?", roleType, qdbid.DatabaseID).All(&workers)
	if err != nil {
		return nil, errors.Wrap(err, "getting workers")
	}

	ret := make([]dax.WorkerInfo, len(workers))
	for i, worker := range workers {
		ret[i].Address = worker.Address
		ret[i].Jobs = make([]dax.Job, len(worker.Jobs))
		for j, job := range worker.Jobs {
			ret[i].Jobs[j] = job.Name
		}
	}

	return ret, nil
}

func (w *workerJobService) WorkerCount(tx dax.Transaction, roleType dax.RoleType, qdbid dax.QualifiedDatabaseID) (int, error) {
	dt, ok := tx.(*DaxTransaction)
	if !ok {
		return 0, dax.NewErrInvalidTransaction("*sqldb.DaxTransaction")
	}
	worker := &models.Worker{}
	cnt, err := dt.C.Where("role = ? and database_id = ?", roleType, qdbid.DatabaseID).Count(worker)
	return cnt, errors.Wrap(err, "getting count")
}

func (w *workerJobService) ListWorkers(tx dax.Transaction, roleType dax.RoleType, qdbid dax.QualifiedDatabaseID) (dax.Addresses, error) {
	dt, ok := tx.(*DaxTransaction)
	if !ok {
		return nil, dax.NewErrInvalidTransaction("*sqldb.DaxTransaction")
	}

	workers := models.Workers{}
	err := dt.C.Select("address").Where("role = ? and database_id = ?", roleType, qdbid.DatabaseID).All(&workers)
	if err != nil {
		return nil, errors.Wrap(err, "getting workers")
	}

	ret := make(dax.Addresses, len(workers))
	for i, wrkr := range workers {
		ret[i] = wrkr.Address
	}

	return ret, nil
}

func (w *workerJobService) CreateWorker(tx dax.Transaction, roleType dax.RoleType, qdbid dax.QualifiedDatabaseID, addr dax.Address) error {
	dt, ok := tx.(*DaxTransaction)
	if !ok {
		return dax.NewErrInvalidTransaction("*sqldb.DaxTransaction")
	}

	err := dt.C.RawQuery("UPDATE workers SET database_id = ? WHERE role = ? and address = ?", qdbid.DatabaseID, roleType, addr).Exec()

	return errors.Wrap(err, "associating worker to database")
}

func (w *workerJobService) DeleteWorker(tx dax.Transaction, roleType dax.RoleType, qdbid dax.QualifiedDatabaseID, addr dax.Address) error {
	dt, ok := tx.(*DaxTransaction)
	if !ok {
		return dax.NewErrInvalidTransaction("*sqldb.DaxTransaction")
	}

	worker := &models.Worker{}
	err := dt.C.Where("address = ? and role = ? and database_id = ?", addr, roleType, qdbid.DatabaseID).First(worker)
	if err != nil {
		return errors.Wrap(err, "getting worker")
	}
	err = dt.C.Destroy(worker)
	return errors.Wrap(err, "deleting worker")
}

func (w *workerJobService) CreateJobs(tx dax.Transaction, roleType dax.RoleType, qdbid dax.QualifiedDatabaseID, addr dax.Address, job ...dax.Job) error {
	dt, ok := tx.(*DaxTransaction)
	if !ok {
		return dax.NewErrInvalidTransaction("*sqldb.DaxTransaction")
	}

	worker := &models.Worker{}
	err := dt.C.Where("address = ? and role = ?", addr, roleType).First(worker)
	if err != nil {
		return errors.Wrap(err, "getting worker")
	}

	jobs := make(models.Jobs, len(job))
	for i, j := range job {
		jobs[i] = models.Job{
			Name:       j,
			Role:       roleType,
			DatabaseID: qdbid.DatabaseID,
			WorkerID:   nulls.NewUUID(worker.ID),
		}
	}

	err = dt.C.Create(jobs)

	return errors.Wrap(err, "creating jobs")
}
func (w *workerJobService) DeleteJob(tx dax.Transaction, roleType dax.RoleType, qdbid dax.QualifiedDatabaseID, addr dax.Address, job dax.Job) error {
	dt, ok := tx.(*DaxTransaction)
	if !ok {
		return dax.NewErrInvalidTransaction("*sqldb.DaxTransaction")
	}

	worker := &models.Worker{}
	err := dt.C.Select("id").Where("role = ? and database_id = ? and address = ?", roleType, qdbid.DatabaseID, addr).First(worker)
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

	err = dt.C.RawQuery("DELETE FROM jobs WHERE id in (?)", ids).Exec()

	return idiffs, errors.Wrap(err, "deleting jobs")
}

func (w *workerJobService) JobCounts(tx dax.Transaction, roleType dax.RoleType, qdbid dax.QualifiedDatabaseID, addr ...dax.Address) (map[dax.Address]int, error) {
	dt, ok := tx.(*DaxTransaction)
	if !ok {
		return nil, dax.NewErrInvalidTransaction("*sqldb.DaxTransaction")
	}

	results := []struct {
		Address dax.Address `db:"address"`
		Count   int         `db:"count"`
	}{}
	var err error
	if len(addr) == 0 {
		qstring := `select address, count(*) as count
                from workers w inner join jobs j on j.worker_id = w.id
                where w.database_id = ? and w.role = ?
                group by w.address`
		err = dt.C.RawQuery(qstring, qdbid.DatabaseID, roleType).All(&results)
	} else {
		qstring := `select address, count(*) as count
                from workers w inner join jobs j on j.worker_id = w.id
                where w.address in (?) and w.database_id = ? and w.role = ?
                group by w.address`
		err = dt.C.RawQuery(qstring, addr, qdbid.DatabaseID, roleType).All(&results)
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
	err := dt.C.Eager().Where("role = ? and database_id = ? and address = ?", roleType, qdbid.DatabaseID, addr).First(worker)
	if isNoRowsError(err) {
		return nil, nil
	} else if err != nil {
		return nil, errors.Wrap(err, "getting worker")
	}

	ret := make(dax.Jobs, len(worker.Jobs))
	for i, job := range worker.Jobs {
		ret[i] = job.Name
	}

	return ret, nil
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
