package sqldb

import (
	"fmt"

	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/dax/controller"
	"github.com/featurebasedb/featurebase/v3/dax/controller/balancer"
	"github.com/featurebasedb/featurebase/v3/dax/models"
	"github.com/featurebasedb/featurebase/v3/errors"
	"github.com/featurebasedb/featurebase/v3/logger"
	"github.com/gobuffalo/nulls"
	"github.com/gofrs/uuid"
)

var _ controller.Store = (*store)(nil)

func NewStore(log logger.Logger) *store {
	if log == nil {
		log = logger.NopLogger
	}
	return &store{
		log: log,
	}
}

type store struct {
	log logger.Logger
}

// AddWorker adds the given node to the workers table. For
// convenience, it returns the DatabaseID of the database
// with which the worker's WorkerService is associated. If the Service
// is not assigned to a database, it returns nil.
func (s *store) AddWorker(tx dax.Transaction, node *dax.Node) (*dax.DatabaseID, error) {
	dt, ok := tx.(*DaxTransaction)
	if !ok {
		return nil, dax.NewErrInvalidTransaction("*sqldb.DaxTransaction")
	}

	worker := &models.Worker{
		Address:   node.Address,
		ServiceID: node.ServiceID,
	}
	for _, roleType := range node.RoleTypes {
		if err := worker.SetRole(roleType); err != nil {
			return nil, errors.Wrapf(err, "setting role: %s", roleType)
		}
	}

	if err := dt.C.Create(worker); err != nil {
		return nil, errors.Wrapf(err, "putting worker into database")
	}

	workerSvc := &models.WorkerService{}
	if err := dt.C.Find(workerSvc, worker.ServiceID); err != nil {
		return nil, errors.Wrap(err, "getting worker service")
	}
	if !workerSvc.DatabaseID.Valid {
		return nil, nil
	}
	return (*dax.DatabaseID)(&workerSvc.DatabaseID.String), nil
}

// WorkerCount returns the number of workers with the given service
// ID.
func (s *store) WorkerCount(tx dax.Transaction, role dax.RoleType, svcID dax.WorkerServiceID) (int, error) {
	dt, ok := tx.(*DaxTransaction)
	if !ok {
		return 0, dax.NewErrInvalidTransaction("*sqldb.DaxTransaction")
	}

	cnt, err := dt.C.Where(fmt.Sprintf("service_id = ? AND role_%s = true", role), svcID).Count(&models.Worker{})
	if err != nil {
		return 0, errors.Wrapf(err, "counting workers for service '%s'", svcID)
	}
	return cnt, nil
}

// WorkerCount returns the number of workers with the given service
// ID.
func (s *store) WorkerCountDatabase(tx dax.Transaction, role dax.RoleType, dbid dax.DatabaseID) (int, error) {
	dt, ok := tx.(*DaxTransaction)
	if !ok {
		return 0, dax.NewErrInvalidTransaction("*sqldb.DaxTransaction")
	}

	sql := `SELECT
               count(w.id) as cnt
            FROM
               worker_services ws
               INNER JOIN databases d ON ws.database_id = d.id
               INNER JOIN workers w ON w.service_id = ws.id
            WHERE
               d.id = ?`

	var res struct {
		Count int `db:"cnt"`
	}
	if err := dt.C.RawQuery(sql, dbid).First(&res); err != nil {
		return 0, errors.Wrapf(err, "counting workers for database '%s'", dbid)
	}
	return res.Count, nil
}

func (s *store) ListFreeJobs(tx dax.Transaction, role dax.RoleType, dbid dax.DatabaseID) (dax.Jobs, error) {
	dt, ok := tx.(*DaxTransaction)
	if !ok {
		return nil, dax.NewErrInvalidTransaction("*sqldb.DaxTransaction")
	}

	jobs := make(models.Jobs, 0)
	err := dt.C.Where("role = ? and database_id = ? and worker_id is NULL", role, dbid).Order("name asc").All(&jobs)
	if err != nil {
		return nil, errors.Wrap(err, "querying for jobs")
	}

	djs := make(dax.Jobs, len(jobs))
	for i, job := range jobs {
		djs[i] = job.Name
	}
	return djs, nil
}

func (s *store) WorkerJobs(tx dax.Transaction, role dax.RoleType, addr dax.Address) (dax.WorkerInfo, error) {
	dt, ok := tx.(*DaxTransaction)
	if !ok {
		return dax.WorkerInfo{}, dax.NewErrInvalidTransaction("*sqldb.DaxTransaction")
	}

	sql := `SELECT
                jobs.name as name
                workers.id as id
            FROM
                jobs INNER JOIN workers on jobs.worker_id = worker.id
            WHERE
                workers.address = ?`

	results := []struct {
		Name dax.Job   `db:"name"`
		ID   uuid.UUID `db:"id"`
	}{}
	if err := dt.C.RawQuery(sql, addr).First(&results); err != nil {
		return dax.WorkerInfo{}, errors.Wrap(err, "querying for jobs")
	}

	if len(results) == 0 {
		return dax.WorkerInfo{Address: addr}, nil
	}
	wi := dax.WorkerInfo{
		Address: addr,
		ID:      dax.WorkerID(results[0].ID),
		Jobs:    make([]dax.Job, len(results)),
	}
	for i, res := range results {
		wi.Jobs[i] = res.Name
	}
	return wi, nil
}

func (s *store) WorkersJobs(tx dax.Transaction, role dax.RoleType, svcID dax.WorkerServiceID) ([]dax.WorkerInfo, error) {
	dt, ok := tx.(*DaxTransaction)
	if !ok {
		return nil, dax.NewErrInvalidTransaction("*sqldb.DaxTransaction")
	}

	results := []struct {
		Address  dax.Address  `db:"address"`
		WorkerID string       `db:"worker_id"`
		JobName  nulls.String `db:"job_name"`
	}{}

	sql := `SELECT
                workers.address as address
                workers.id as worker_id
                jobs.name as job_name
            FROM
                workers LEFT JOIN jobs on jobs.worker_id = workers.id
            WHERE
                jobs.role = ?
                workers.service_id = ?
            ORDER BY
                address ASC
                job_name ASC`

	err := dt.C.RawQuery(sql, role, svcID).All(&results)
	if err != nil {
		return nil, errors.Wrap(err, "querying")
	}

	infos := make([]dax.WorkerInfo, 0)
	if len(results) == 0 {
		return infos, nil
	}

	// convert flat results list to []dax.WorkerInfo
	addr := dax.Address("")
	infosIDX := -1
	for _, res := range results {
		if res.Address != addr {
			addr = res.Address
			infosIDX++
			infos = append(infos, dax.WorkerInfo{
				Address: addr,
				ID:      res.WorkerID,
			})
		}
		if res.JobName.Valid {
			infos[infosIDX].Jobs = append(infos[infosIDX].Jobs, dax.Job(res.JobName.String))
		}
	}
	return infos, nil
}

func (s *store) CreateFreeJobs(tx dax.Transaction, role dax.RoleType, dbid dax.DatabaseID, jobs ...dax.Job) error {
	dt, ok := tx.(*DaxTransaction)
	if !ok {
		return dax.NewErrInvalidTransaction("*sqldb.DaxTransaction")
	}

	modelJobs := make([]models.Job, len(jobs))
	for i, job := range jobs {
		modelJobs[i] = models.Job{
			Name:       job,
			Role:       role,
			DatabaseID: dbid,
		}
	}

	err := dt.C.Create(&modelJobs)
	return errors.Wrap(err, "creating jobs")
}

func (s *store) AssignWorkerToJobs(tx dax.Transaction, role dax.RoleType, dbid dax.DatabaseID, workerID dax.WorkerID, jobs ...dax.Job) error {
	dt, ok := tx.(*DaxTransaction)
	if !ok {
		return dax.NewErrInvalidTransaction("*sqldb.DaxTransaction")
	}

	modelJobs := models.Jobs{}
	sql := `UPDATE
                jobs
            SET
                worker_id = ?
            WHERE
                role = ? AND
                database_id = ? AND
                name in (?)
            RETURNING
                jobs.id,
                jobs.name`

	err := dt.C.RawQuery(sql, workerID, role, dbid, jobs).All(&modelJobs)
	if err != nil {
		return errors.Wrap(err, "updating jobs")
	}

	// Create any jobs not in the returned "modelJobs", which means they
	// didn't exist already and wouldn't have been updated by the
	// UPDATE statement.
	toBeAssigned := sjobsNotAssigned(jobs, modelJobs, role, uuid.UUID(workerID), dbid)

	if err := dt.C.Create(toBeAssigned); err != nil {
		return errors.Wrap(err, "creating jobs")
	}

	return nil
}

func (s *store) ListWorkers(tx dax.Transaction, role dax.RoleType, svcID dax.WorkerServiceID) (dax.Addresses, error) {
	dt, ok := tx.(*DaxTransaction)
	if !ok {
		return nil, dax.NewErrInvalidTransaction("*sqldb.DaxTransaction")
	}

	workers := models.Workers{}
	sql := fmt.Sprintf("role_%s = true and service_id = ?", role)
	err := dt.C.Select("address").Where(sql, svcID).Order("address asc").All(&workers)
	if err != nil {
		return nil, errors.Wrap(err, "getting workers")
	}

	ret := make(dax.Addresses, len(workers))
	for i, wrkr := range workers {
		ret[i] = wrkr.Address
	}

	return ret, nil
}

// TODO: rename once we delete jobsNotAssigned
func sjobsNotAssigned(incomingJobs []dax.Job, assigned models.Jobs, roleType dax.RoleType, workerUUID uuid.UUID, dbid dax.DatabaseID) (toBeAssigned models.Jobs) {
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
				WorkerID:   nulls.NewUUID(workerUUID),
			},
		)
	}
	return toBeAssigned
}

func (s *store) WorkerForAddress(tx dax.Transaction, addr dax.Address) (dax.Node, error) {
	dt, ok := tx.(*DaxTransaction)
	if !ok {
		return dax.Node{}, dax.NewErrInvalidTransaction("*sqldb.DaxTransaction")
	}

	result := struct {
		ID         dax.WorkerID        `db:"id"`
		Address    dax.Address         `db:"address"`
		ServiceID  dax.WorkerServiceID `db:"service_id"`
		DatabaseID nulls.String        `db:"database_id"`
	}{}
	sql := `SELECT
                w.id AS id,
                w.address AS address,
                w.service_id AS service_id,
                worker_services.database_id AS database_id
            FROM
               workers w INNER JOIN worker_services ON w.service_id = worker_services.id
            WHERE
               w.address = ?`
	err := dt.C.RawQuery(sql, addr).First(&result)
	if err != nil {
		return dax.Node{}, errors.Wrap(err, "getting workers")
	}

	var dbid *dax.DatabaseID
	if result.DatabaseID.Valid {
		tmp := dax.DatabaseID(result.DatabaseID.String)
		dbid = &tmp
	}

	return dax.Node{
		ID:         result.ID,
		Address:    result.Address,
		ServiceID:  result.ServiceID,
		DatabaseID: dbid,
		// RoleTypes:    []dax.RoleType{}, do we need??
	}, nil

}

func (s *store) WorkerService(tx dax.Transaction, dbid dax.DatabaseID) (dax.WorkerService, error) {
	dt, ok := tx.(*DaxTransaction)
	if !ok {
		return dax.WorkerService{}, dax.NewErrInvalidTransaction("*sqldb.DaxTransaction")
	}

	ws := &models.WorkerService{}
	if err := dt.C.Where("database_id = ?", dbid).First(ws); err != nil {
		return dax.WorkerService{}, errors.Wrapf(err, "finding worker service for database: '%s'", dbid)
	}
	return toDaxWorkerService(*ws), nil
}

func (s *store) RemoveWorker(tx dax.Transaction, id dax.WorkerID) error {
	dt, ok := tx.(*DaxTransaction)
	if !ok {
		return dax.NewErrInvalidTransaction("*sqldb.DaxTransaction")
	}

	err := dt.C.Destroy(&models.Worker{ID: uuid.UUID(id)})
	return errors.Wrap(err, "destroying")
}

func (s *store) DeleteJobsForTable(tx dax.Transaction, role dax.RoleType, qtid dax.QualifiedTableID) (controller.InternalDiffs, error) {
	dt, ok := tx.(*DaxTransaction)
	if !ok {
		return nil, dax.NewErrInvalidTransaction("*sqldb.DaxTransaction")
	}

	results := []struct {
		ID      uuid.UUID   `db:"id"`
		Name    dax.Job     `db:"name"`
		Address dax.Address `db:"address"`
	}{}
	err := dt.C.RawQuery("select j.id, j.name, w.address from jobs j inner join workers w on j.worker_id = w.id where j.role = ? and j.database_id = ? and j.name LIKE ?", role, qtid.QualifiedDatabaseID.DatabaseID, fmt.Sprintf("%s%%", qtid.Key())).All(&results)
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
