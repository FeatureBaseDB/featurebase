// Package balancer is an implementation of the controller.Balancer interface.
package balancer

import (
	"log"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/dax/controller"
	"github.com/featurebasedb/featurebase/v3/dax/controller/schemar"
	"github.com/featurebasedb/featurebase/v3/errors"
	"github.com/featurebasedb/featurebase/v3/logger"
)

// Ensure type implements interface.
var _ controller.Balancer = (*Balancer)(nil)

// Balancer is an implementation of the balancer.Balancer interface which
// isolates workers and jobs by database. It helps manage the relationships
// between workers and jobs. The logic it uses to balance jobs across workers is
// very simple; it bases everything off the number of workers and number of
// jobs. It does not take anything else (such as job size, worker capabilities,
// etc) into consideration.
type Balancer struct {
	// current represents the current state of worker/job assigments.
	current WorkerJobService

	workerRegistry controller.WorkerRegistry

	// freeJobs is the set of jobs which have yet to be assigned to a worker.
	// This could be because there are no available workers, or because a worker
	// has been removed and the jobs for which it was responsible have yet to be
	// reassigned.
	freeJobs FreeJobService

	freeWorkers FreeWorkerService

	schemar schemar.Schemar

	logger logger.Logger
}

// New returns a new instance of Balancer.
func New(wr controller.WorkerRegistry, fjs FreeJobService, wjs WorkerJobService, fws FreeWorkerService, schemar schemar.Schemar, logger logger.Logger) *Balancer {
	return &Balancer{
		current:        wjs,
		workerRegistry: wr,
		freeJobs:       fjs,
		freeWorkers:    fws,
		schemar:        schemar,
		logger:         logger,
	}
}

// AddWorker adds the given Node to the Balancer's available worker pool. Note
// that a node is used for ALL of the role types specified. In other words,
// specifying roleTypes = {compute, translate}, does not mean that the node can
// be used as either a compute worker or a translate worker. It means that it
// will be used as both.
func (b *Balancer) AddWorker(tx dax.Transaction, node *dax.Node) ([]dax.WorkerDiff, error) {
	b.logger.Debugf("AddWorker(%s)", node.Address)

	if err := b.workerRegistry.AddWorker(tx, node); err != nil {
		return nil, errors.Wrapf(err, "creating node on node service: %s", node.Address)
	}

	diffs := NewInternalDiffs()

	// Process the newly added workers.
	// TODO(tlt): this is a little heavy-handed. I'm sure we'll need to be more
	// intentional about knowing which databases need workers, as opposed to
	// this brute force loop over all databases every time.
	if diff, err := b.balance(tx); err != nil {
		return nil, errors.Wrapf(err, "balancing new worker: %s", node.Address)
	} else {
		diffs.Merge(diff)
	}

	return diffs.Output(), nil
}

func (b *Balancer) assignMinWorkers(tx dax.Transaction, roleType dax.RoleType, qdbid dax.QualifiedDatabaseID) (InternalDiffs, error) {
	b.logger.Debugf("assigning min workers for '%s', '%s'", roleType, qdbid)

	// Find out how many free workers we have.
	freeWorkers, err := b.freeWorkers.ListWorkers(tx, roleType)
	if err != nil {
		return nil, errors.Wrap(err, "getting free worker list")
	}
	freeWorkerCount := len(freeWorkers)

	// If there are no free workers, return early.
	if freeWorkerCount == 0 {
		b.logger.Debugf("No free workers for '%s'", roleType)
		return InternalDiffs{}, nil
	}

	// Get database and its minWorkerCount (Database.Options.WorkersMin). This
	// used to get all databases, but now this method is specific to a single
	// database. That's why we just get the one here.
	qdbs, err := b.schemar.Databases(tx, qdbid.OrganizationID, qdbid.DatabaseID)
	if err != nil {
		return nil, errors.Wrap(err, "getting database")
	}

	// Create a map[database]int where int is the number of workers required to
	// reach that database's minWorkerCount. This map will only contain database
	// which need more workers in order to reach their minimum.
	m := make(map[dax.QualifiedDatabaseID]int)

	for _, qdb := range qdbs {
		qdbid := qdb.QualifiedID()

		minWorkers := qdb.Options.WorkersMin
		if minWorkers == 0 {
			continue
		}

		// If the database doesn't have any jobs, there's no need to go any
		// further. In other words, we don't want to assign a worker to a
		// database until it has at least one job.
		if hasJobs, err := b.databaseHasJobs(tx, roleType, qdbid); err != nil {
			return nil, errors.Wrapf(err, "checking has jobs: (%s) %s", roleType, qdbid)
		} else if !hasJobs {
			continue
		}

		// Get the number of workers assigned to this database.
		workerCount, err := b.current.WorkerCount(tx, roleType, qdbid)
		if err != nil {
			return nil, errors.Wrapf(err, "getting worker count: (%s) %s", roleType, qdbid)
		}

		diff := minWorkers - workerCount

		// If we have more workers than the min required, or if we have the
		// exact number of workers , don't do anything for that database.
		if diff <= 0 {
			continue
		}

		m[qdbid] = diff
	}

	diffs := NewInternalDiffs()

	// Create an ordered slice of map keys so that tests are predictable.
	qdbids := make([]dax.QualifiedDatabaseID, 0, len(m))
	for qdbid := range m {
		qdbids = append(qdbids, qdbid)
	}
	sort.Sort(dax.QualifiedDatabaseIDs(qdbids))

	// For each database, if there are enough free workers to
	// satisfy its min, then pop that number of workers from the free list. If
	// not, continue to the next database until either reaching the end of the
	// database list or until there are no more free workers in the list,
	// whichever comes first.
	for _, qdbid := range qdbids {
		need := m[qdbid]

		if freeWorkerCount == 0 {
			break
		}

		if freeWorkerCount >= need {
			addrs, err := b.freeWorkers.PopWorkers(tx, roleType, need)
			if err != nil {
				return nil, errors.Wrapf(err, "popping free worker: (%s)", roleType)
			}

			if diff, err := b.addDatabaseWorkers(tx, roleType, qdbid, addrs...); err != nil {
				return nil, errors.Wrapf(err, "adding database workers: (%s) %s, %v", roleType, qdbid, addrs)
			} else {
				diffs.Merge(diff)
			}
		}
	}

	return diffs, nil
}

// addDatabaseWorkers adds workers from the free worker list to the pool of
// workers for a specific database.
func (b *Balancer) addDatabaseWorkers(tx dax.Transaction, roleType dax.RoleType, qdbid dax.QualifiedDatabaseID, addrs ...dax.Address) (InternalDiffs, error) {
	for _, addr := range addrs {
		if err := b.current.CreateWorker(tx, roleType, qdbid, addr); err != nil {
			return nil, errors.Wrap(err, "creating worker")
		}
	}

	// Process the freeJobs.
	return b.processFreeJobs(tx, roleType, qdbid)
}

// databaseHasJobs returns true if the database has at least one job.
func (b *Balancer) databaseHasJobs(tx dax.Transaction, roleType dax.RoleType, qdbid dax.QualifiedDatabaseID) (bool, error) {
	// Free jobs.
	if freeJobs, err := b.freeJobs.ListJobs(tx, roleType, qdbid); err != nil {
		return false, errors.Wrapf(err, "getting free jobs: (%s) %s", roleType, qdbid)
	} else if len(freeJobs) > 0 {
		return true, nil
	}

	// Assigned jobs.
	if wis, err := b.current.WorkersJobs(tx, roleType, qdbid); err != nil {
		return false, errors.Wrapf(err, "getting free jobs: (%s) %s", roleType, qdbid)
	} else {
		for _, wi := range wis {
			if len(wi.Jobs) > 0 {
				return true, nil
			}
		}
	}

	return false, nil
}

func (b *Balancer) RemoveWorker(tx dax.Transaction, addr dax.Address) ([]dax.WorkerDiff, error) {
	diffs := NewInternalDiffs()

	// See if the worker is assigned to a database. If it is, disassociate the
	// worker from all of its jobs for the database.
	dbkey := b.current.DatabaseForWorker(tx, addr)
	if dbkey != "" {
		qdbid := dbkey.QualifiedDatabaseID()
		for _, rt := range dax.AllRoleTypes {
			if diff, err := b.removeDatabaseWorker(tx, rt, qdbid, addr); err != nil {
				return nil, errors.Wrapf(err, "removing worker: (%s) %s", rt, addr)
			} else {
				diffs.Merge(diff)
			}
		}
	}

	// Remove the worker from the worker registry.
	if err := b.workerRegistry.RemoveWorker(tx, addr); err != nil {
		return nil, errors.Wrapf(err, "deleting node from node service: %s", addr)
	}

	if dbkey != "" {
		qdbid := dbkey.QualifiedDatabaseID()
		// Balance the affected database.
		if diff, err := b.balanceDatabase(tx, qdbid); err != nil {
			return nil, errors.Wrapf(err, "balancing database: %s", qdbid)
		} else {
			diffs.Merge(diff)
		}
	}

	return diffs.Output(), nil
}

// removeDatabaseWorker is used to remove a worker that has been associated with
// a database. The worker here is determined by address.
func (b *Balancer) removeDatabaseWorker(tx dax.Transaction, roleType dax.RoleType, qdbid dax.QualifiedDatabaseID, addr dax.Address) (InternalDiffs, error) {
	jobs, err := b.current.ListJobs(tx, roleType, qdbid, addr)
	if err != nil {
		return nil, errors.Wrap(err, "listing jobs")
	}

	// Before removing the worker, mark its jobs as free.
	if err := b.freeJobs.MarkJobsAsFree(tx, roleType, qdbid, jobs); err != nil {
		return nil, errors.Wrap(err, "marking jobs as free")
	}

	// Remove the worker.
	if err := b.current.DeleteWorker(tx, roleType, qdbid, addr); err != nil {
		return nil, errors.Wrap(err, "deleting worker")
	}

	// Even though this may not be useful to the caller (for example, in the
	// case where the worker has died and no longer exists), return the diffs
	// which represent the removal of jobs from the worker.
	diff := NewInternalDiffs()
	for _, job := range jobs {
		diff.Removed(addr, job)
	}

	return diff, nil
}

func (b *Balancer) ReleaseWorkers(tx dax.Transaction, addrs ...dax.Address) error {
	return errors.Wrap(b.current.ReleaseWorkers(tx, addrs...), "freeing workers")
}

func (b *Balancer) AddJobs(tx dax.Transaction, roleType dax.RoleType, qtid dax.QualifiedTableID, jobs ...dax.Job) ([]dax.WorkerDiff, error) {
	start := time.Now()
	defer func() {
		log.Printf("ELAPSED: Balancer.AddJob: %v", time.Since(start))
	}()

	switch len(jobs) {
	case 0:
		// No jobs so return early.
		b.logger.Debugf("%s: AddJobs (no jobs provided)", roleType)
		return []dax.WorkerDiff{}, nil
	case 1:
		b.logger.Debugf("%s: AddJobs (%s)", roleType, jobs[0])
	default:
		b.logger.Debugf("%s: AddJobs (%d)", roleType, len(jobs))
	}

	// TODO(tlt): we don't currently use "table" in this method; even though we
	// pass a table, we're still encoding the tableKey in the job. In theory, we
	// could exclude tableKey from the job coming into this method, and add it
	// here.
	qdbid := qtid.QualifiedDatabaseID

	diff, err := b.addJobs(tx, roleType, qdbid, jobs...)
	if err != nil {
		return nil, errors.Wrap(err, "adding job")
	}

	return diff.Output(), nil
}

func (b *Balancer) addJobs(tx dax.Transaction, roleType dax.RoleType, qdbid dax.QualifiedDatabaseID, jobs ...dax.Job) (InternalDiffs, error) {
	diffs := NewInternalDiffs()

	if len(jobs) == 0 {
		return diffs, nil
	}

	if cnt, err := b.current.WorkerCount(tx, roleType, qdbid); err != nil {
		return nil, errors.Wrap(err, "getting worker count")
	} else if cnt == 0 {
		if err := b.freeJobs.CreateJobs(tx, roleType, qdbid, jobs...); err != nil {
			return nil, errors.Wrap(err, "creating free job")
		}

		// Since we've just added free jobs to the database, try to add a worker
		// for the database. This case would happen because when a database is
		// first created, it is not assigned any workers. A database is not
		// assigned workers until it has at least one job (which this database
		// now has).
		if diff, err := b.balanceDatabaseForRole(tx, roleType, qdbid); err != nil {
			return nil, errors.Wrapf(err, "balancing database for role: (%s)", roleType)
		} else {
			diffs.Merge(diff)
		}

		// Now check, again, to see if the database has a worker.
		if cnt2, err := b.current.WorkerCount(tx, roleType, qdbid); err != nil {
			return nil, errors.Wrap(err, "getting worker count, again")
		} else if cnt2 == 0 {
			// TODO: we might want to inform the user that a job is in the free list
			// because there are no workers.
			return InternalDiffs{}, nil
		}
	}

	diff, err := b.addDatabaseJobs(tx, roleType, qdbid, jobs...)
	if err != nil {
		return nil, errors.Wrapf(err, "adding database jobs: (%s) %s", roleType, qdbid)
	}
	diffs.Merge(diff)

	return diffs, nil
}

// addDatabaseJobs adds the job for the provided database.
func (b *Balancer) addDatabaseJobs(tx dax.Transaction, roleType dax.RoleType, qdbid dax.QualifiedDatabaseID, jobs ...dax.Job) (InternalDiffs, error) {
	workerJobs, err := b.current.WorkersJobs(tx, roleType, qdbid)
	if err != nil {
		return nil, errors.Wrapf(err, "getting workers jobs: %s", roleType)
	}

	jset := dax.NewSet[dax.Job]()
	for _, workerInfo := range workerJobs {
		jset.Merge(dax.NewSet(workerInfo.Jobs...))
	}

	addrs := make(dax.Addresses, 0, len(workerJobs))
	jobCounts := make(map[dax.Address]int, 0)
	for _, v := range workerJobs {
		addrs = append(addrs, v.Address)
		jobCounts[v.Address] = len(v.Jobs)
	}

	diffs := NewInternalDiffs()

	jobsToCreate := make(map[dax.Address][]dax.Job)

	for _, job := range jobs {
		// Skip any job that already exists.
		if jset.Contains(job) {
			continue
		}

		// Find the worker with the fewest number of jobs and assign it this job.
		var lowCount int = math.MaxInt
		var lowWorker dax.Address

		// We loop over addrs here instead of jobCounts because jobCounts is a
		// map and it can return results in an unexpected order, which is a
		// problem for testing.
		for _, addr := range addrs {
			jobCount := jobCounts[addr]
			if jobCount < lowCount {
				lowCount = jobCount
				lowWorker = addr
			}
		}

		jobsToCreate[lowWorker] = append(jobsToCreate[lowWorker], job)
		jobCounts[lowWorker]++
	}

	for addr, jobs := range jobsToCreate {
		if err := b.current.AssignWorkerToJobs(tx, roleType, qdbid, addr, jobs...); err != nil {
			return nil, errors.Wrap(err, "creating job")
		}
		for _, job := range jobs {
			diffs.Added(addr, job)
		}
	}

	return diffs, nil
}

func (b *Balancer) RemoveJobs(tx dax.Transaction, roleType dax.RoleType, qtid dax.QualifiedTableID, jobs ...dax.Job) ([]dax.WorkerDiff, error) {
	qdbid := qtid.QualifiedDatabaseID

	// If no jobs are provided, remove all jobs for table.
	if len(jobs) == 0 {
		diffs, err := b.removeJobsForTable(tx, roleType, qtid)
		if err != nil {
			return nil, errors.Wrapf(err, "removing jobs for table: (%s) %s", roleType, qtid)
		}
		return diffs.Output(), nil
	}

	diffs := NewInternalDiffs()

	for _, job := range jobs {
		if diff, err := b.removeJob(tx, roleType, qdbid, job); err != nil {
			return nil, errors.Wrapf(err, "removing job: (%s) %s, %s", roleType, qdbid, job)
		} else {
			diffs.Merge(diff)
		}
	}

	return diffs.Output(), nil
}

func (b *Balancer) removeJobsForTable(tx dax.Transaction, roleType dax.RoleType, qtid dax.QualifiedTableID) (InternalDiffs, error) {
	idiffs, err := b.current.DeleteJobsForTable(tx, roleType, qtid)
	if err != nil {
		return nil, errors.Wrapf(err, "deleting jobs for table: (%s) %s", roleType, qtid)
	}
	if err := b.freeJobs.DeleteJobsForTable(tx, roleType, qtid); err != nil {
		return nil, errors.Wrapf(err, "deleting free jobs for table: (%s) %s", roleType, qtid)
	}
	return idiffs, nil
}

// Balance calls balanceDatabase on every database in the schemar.
func (b *Balancer) Balance(tx dax.Transaction) ([]dax.WorkerDiff, error) {
	diffs, err := b.balance(tx)
	if err != nil {
		return nil, errors.Wrapf(err, "balancing all")
	}

	return diffs.Output(), nil
}

func (b *Balancer) balance(tx dax.Transaction) (InternalDiffs, error) {
	qdbs, err := b.schemar.Databases(tx, "")
	if err != nil {
		return nil, errors.Wrapf(err, "getting all databases")
	}

	diffs := NewInternalDiffs()

	for _, qdb := range qdbs {
		qdbid := qdb.QualifiedID()
		if diff, err := b.balanceDatabase(tx, qdbid); err != nil {
			return nil, errors.Wrapf(err, "balancing database: %s", qdbid)
		} else {
			diffs.Merge(diff)
		}
	}

	return diffs, nil
}

func (b *Balancer) BalanceDatabase(tx dax.Transaction, qdbid dax.QualifiedDatabaseID) ([]dax.WorkerDiff, error) {
	diffs, err := b.balanceDatabase(tx, qdbid)
	if err != nil {
		return nil, errors.Wrapf(err, "balancing database: %s", qdbid)
	}

	return diffs.Output(), nil
}

func (b *Balancer) balanceDatabase(tx dax.Transaction, qdbid dax.QualifiedDatabaseID) (InternalDiffs, error) {
	diffs := NewInternalDiffs()

	for _, role := range dax.AllRoleTypes {
		diff, err := b.balanceDatabaseForRole(tx, role, qdbid)
		if err != nil {
			return nil, errors.Wrapf(err, "getting worker count: (%s) %s", role, qdbid)
		}
		diffs.Merge(diff)
	}

	return diffs, nil
}

func (b *Balancer) balanceDatabaseForRole(tx dax.Transaction, roleType dax.RoleType, qdbid dax.QualifiedDatabaseID) (InternalDiffs, error) {
	b.logger.Debugf("balancing database %s for role: %s\n", qdbid, roleType)
	diffs := NewInternalDiffs()

	// Before balancing, make sure the database has its minimum number of
	// workers satisfied.
	if diff, err := b.assignMinWorkers(tx, roleType, qdbid); err != nil {
		return nil, errors.Wrapf(err, "assigning min workers: (%s) %s", roleType, qdbid)
	} else {
		diffs.Merge(diff)
	}

	// If there are no workers, we can't properly balance.
	if cnt, err := b.current.WorkerCount(tx, roleType, qdbid); err != nil {
		return nil, errors.Wrapf(err, "getting worker count: (%s) %s", roleType, qdbid)
	} else if cnt == 0 {
		return InternalDiffs{}, nil
	}

	// Process the freeJobs.
	if diff, err := b.processFreeJobs(tx, roleType, qdbid); err != nil {
		return nil, errors.Wrapf(err, "processing free jobs: (%s) %s", roleType, qdbid)
	} else {
		diffs.Merge(diff)
	}

	// Balance the jobs among workers.
	diff, err := b.balanceDatabaseJobs(tx, roleType, qdbid, diffs)
	if err != nil {
		return nil, errors.Wrap(err, "balancing jobs")
	}

	return diff, nil
}

func (b *Balancer) CurrentState(tx dax.Transaction, roleType dax.RoleType, qdbid dax.QualifiedDatabaseID) ([]dax.WorkerInfo, error) {
	return b.current.WorkersJobs(tx, roleType, qdbid)
}

func (b *Balancer) WorkerState(tx dax.Transaction, roleType dax.RoleType, addr dax.Address) (dax.WorkerInfo, error) {
	info := dax.WorkerInfo{
		Address: addr,
	}

	dbkey := b.current.DatabaseForWorker(tx, addr)
	if dbkey == "" {
		return info, nil
	}
	qdbid := dbkey.QualifiedDatabaseID()

	jobs, err := b.current.ListJobs(tx, roleType, qdbid, addr)
	if err != nil {
		return dax.WorkerInfo{}, errors.Wrapf(err, "listing jobs: (%s) %s, %s", roleType, qdbid, addr)
	}
	info.Jobs = jobs

	return info, nil
}

func (b *Balancer) WorkersForJobs(tx dax.Transaction, roleType dax.RoleType, qdbid dax.QualifiedDatabaseID, jobs ...dax.Job) ([]dax.WorkerInfo, error) {
	out := make(map[dax.Address]dax.Set[dax.Job])

	workerJobs, err := b.current.WorkersJobs(tx, roleType, qdbid)
	if err != nil {
		return nil, errors.Wrapf(err, "getting worker jobs: (%s) %s", roleType, qdbid)
	}
	for _, workerInfo := range workerJobs {
		jset := dax.NewSet(workerInfo.Jobs...)

		matches := dax.NewSet[dax.Job]()
		for _, job := range jobs {
			if jset.Contains(job) {
				matches.Add(job)
			}
		}

		if len(matches) > 0 {
			out[workerInfo.Address] = matches
		}
	}

	workers := make([]dax.WorkerInfo, 0, len(out))

	for addr, jset := range out {
		workers = append(workers, dax.WorkerInfo{
			Address: addr,
			Jobs:    jset.Sorted(),
		})
	}

	sort.Sort(dax.WorkerInfos(workers))

	return workers, nil
}

func (b *Balancer) WorkersForTable(tx dax.Transaction, roleType dax.RoleType, qtid dax.QualifiedTableID) ([]dax.WorkerInfo, error) {
	out := make(map[dax.Address]dax.Set[dax.Job])

	qdbid := qtid.QualifiedDatabaseID

	prefix := string(qtid.Key())

	workerJobs, err := b.current.WorkersJobs(tx, roleType, qdbid)
	if err != nil {
		return nil, errors.Wrapf(err, "getting worker jobs: (%s) %s", roleType, qdbid)
	}
	for _, workerInfo := range workerJobs {

		matches := dax.NewSet[dax.Job]()
		for _, job := range workerInfo.Jobs {
			if strings.HasPrefix(string(job), prefix) {
				matches.Add(job)
			}
		}
		if len(matches) > 0 {
			out[workerInfo.Address] = matches
		}
	}

	workers := make([]dax.WorkerInfo, 0, len(out))

	for addr, jset := range out {
		workers = append(workers, dax.WorkerInfo{
			Address: addr,
			Jobs:    jset.Sorted(),
		})
	}

	sort.Sort(dax.WorkerInfos(workers))

	return workers, nil
}

func (b *Balancer) removeJob(tx dax.Transaction, roleType dax.RoleType, qdbid dax.QualifiedDatabaseID, job dax.Job) (InternalDiffs, error) {
	if addr, ok, err := b.workerForJob(tx, roleType, qdbid, job); err != nil {
		return nil, errors.Wrapf(err, "getting worker for job: %s", job)
	} else if ok {
		if err := b.current.DeleteJob(tx, roleType, qdbid, addr, job); err != nil {
			return nil, errors.Wrapf(err, "deleting job: (%s) %s, %s, %s", roleType, qdbid, addr, job)
		}

		diffs := NewInternalDiffs()
		diffs.Removed(addr, job)

		return diffs, nil
	}

	// Just in case the job is in the free list (and wasn't assigned to a
	// worker), remove it; there's no need to provide a diff. There should never
	// be a case where the same job is both in the free list and assigned to a
	// worker.
	if err := b.freeJobs.DeleteJob(tx, roleType, qdbid, job); err != nil {
		return nil, errors.Wrapf(err, "deleting free job: (%s) %s, %s", roleType, qdbid, job)
	}

	return InternalDiffs{}, nil
}

// balanceDatabaseJobs moves jobs among workers with the goal of having an equal
// number of jobs per worker. This method takes an `internalDiffs` as input for
// cases where some action has preceeded this call which also resulted in
// `internalDiffs`. Instead of having this method take a value, we could rely on
// the internalDiffs.merge() method, but we would need to modify that method to
// be smarter about the order in which it applies the add/remove operations.
// Until that's in place, we'll pass in a value here.
func (b *Balancer) balanceDatabaseJobs(tx dax.Transaction, roleType dax.RoleType, qdbid dax.QualifiedDatabaseID, diffs InternalDiffs) (InternalDiffs, error) {
	numWorkers, err := b.current.WorkerCount(tx, roleType, qdbid)
	if err != nil {
		return nil, errors.Wrapf(err, "getting worker count: (%s) %s", roleType, qdbid)
	}
	numJobs := 0
	if addrs, err := b.current.ListWorkers(tx, roleType, qdbid); err != nil {
		return nil, errors.Wrapf(err, "listing workers: (%s) %s", roleType, qdbid)
	} else {
		for _, addr := range addrs {
			jobCounts, err := b.current.JobCounts(tx, roleType, qdbid, addr)
			if err != nil {
				return nil, errors.Wrapf(err, "getting job count: (%s) %s, %s", roleType, qdbid, addr)
			}
			numJobs += jobCounts[addr]
		}
	}

	minJobsPerWorker := numJobs / numWorkers
	numWorkersAboveMin := numJobs % numWorkers

	// workerInfos is used now in order to guarantee a sort order.
	workerInfos, err := b.CurrentState(tx, roleType, qdbid)
	if err != nil {
		return nil, errors.Wrapf(err, "getting current state: (%s) %s", roleType, qdbid)
	}

	// Loop through each worker, and if the number of jobs for the worker
	// exceeds the target, then remove the job and add it back (which is
	// effectively how we rebalance a job).
	for i, workerInfo := range workerInfos {
		numTargetJobs := minJobsPerWorker
		if i < numWorkersAboveMin {
			numTargetJobs += 1
		}

		jobCounts, err := b.current.JobCounts(tx, roleType, qdbid, workerInfo.Address)
		if err != nil {
			return nil, errors.Wrapf(err, "getting job count: (%s) %s, %s", roleType, qdbid, workerInfo.Address)
		}
		numCurrentJobs := jobCounts[workerInfo.Address]

		// If we don't need to remove jobs from this worker, then just continue
		// on to the next worker.
		if numCurrentJobs <= numTargetJobs {
			continue
		}

		sortedJobs, err := b.current.ListJobs(tx, roleType, qdbid, workerInfo.Address)
		if err != nil {
			return nil, errors.Wrapf(err, "listing jobs: (%s) %s, %s", roleType, qdbid, workerInfo.Address)
		}

		// Remove the extra jobs from the end of the list, and add them back
		// again (which should place them on a worker with fewer jobs).
		for i := numCurrentJobs - 1; i >= numTargetJobs; i-- {
			if rj, err := b.removeJob(tx, roleType, qdbid, sortedJobs[i]); err != nil {
				return nil, errors.Wrapf(err, "removing job: %s", sortedJobs[i])
			} else {
				diffs.Merge(rj)
			}
			if aj, err := b.addJobs(tx, roleType, qdbid, sortedJobs[i]); err != nil {
				return nil, errors.Wrapf(err, "adding job: %s", sortedJobs[i])
			} else {
				diffs.Merge(aj)
			}
		}
	}

	return diffs, nil
}

// processFreeJobs assigns all jobs in the free list to a worker.
func (b *Balancer) processFreeJobs(tx dax.Transaction, roleType dax.RoleType, qdbid dax.QualifiedDatabaseID) (InternalDiffs, error) {
	diffs := NewInternalDiffs()
	jobs, err := b.freeJobs.ListJobs(tx, roleType, qdbid)
	if err != nil {
		return nil, errors.Wrapf(err, "listing free jobs: %s", roleType)
	}

	if aj, err := b.addDatabaseJobs(tx, roleType, qdbid, jobs...); err != nil {
		return nil, errors.Wrapf(err, "adding jobs: %s", jobs)
	} else {
		diffs.Merge(aj)
	}

	return diffs, nil
}

// workerForJob returns the worker currently assigned to the given job.
func (b *Balancer) workerForJob(tx dax.Transaction, roleType dax.RoleType, qdbid dax.QualifiedDatabaseID, job dax.Job) (dax.Address, bool, error) {
	workerJobs, err := b.current.WorkersJobs(tx, roleType, qdbid)
	if err != nil {
		return "", false, errors.Wrapf(err, "getting workers jobs: (%s) %s", roleType, qdbid)
	}
	for _, workerInfo := range workerJobs {
		jset := dax.NewSet(workerInfo.Jobs...)
		if jset.Contains(job) {
			return workerInfo.Address, true, nil
		}
	}
	return "", false, nil
}

func (b *Balancer) ReadNode(tx dax.Transaction, addr dax.Address) (*dax.Node, error) {
	return b.workerRegistry.Worker(tx, addr)
}

func (b *Balancer) Nodes(tx dax.Transaction) ([]*dax.Node, error) {
	return b.workerRegistry.Workers(tx)
}

type WorkerJobService interface {
	WorkersJobs(tx dax.Transaction, roleType dax.RoleType, qdbid dax.QualifiedDatabaseID) ([]dax.WorkerInfo, error)

	WorkerCount(tx dax.Transaction, roleType dax.RoleType, qdbid dax.QualifiedDatabaseID) (int, error)
	ListWorkers(tx dax.Transaction, roleType dax.RoleType, qdbid dax.QualifiedDatabaseID) (dax.Addresses, error)

	CreateWorker(tx dax.Transaction, roleType dax.RoleType, qdbid dax.QualifiedDatabaseID, addr dax.Address) error
	DeleteWorker(tx dax.Transaction, roleType dax.RoleType, qdbid dax.QualifiedDatabaseID, addr dax.Address) error
	ReleaseWorkers(tx dax.Transaction, addrs ...dax.Address) error

	AssignWorkerToJobs(tx dax.Transaction, roleType dax.RoleType, qdbid dax.QualifiedDatabaseID, addr dax.Address, job ...dax.Job) error
	DeleteJob(tx dax.Transaction, roleType dax.RoleType, qdbid dax.QualifiedDatabaseID, addr dax.Address, job dax.Job) error
	DeleteJobsForTable(tx dax.Transaction, roleType dax.RoleType, qtid dax.QualifiedTableID) (InternalDiffs, error)
	JobCounts(tx dax.Transaction, roleType dax.RoleType, qdbid dax.QualifiedDatabaseID, addr ...dax.Address) (map[dax.Address]int, error)
	ListJobs(tx dax.Transaction, roleType dax.RoleType, qdbid dax.QualifiedDatabaseID, addr dax.Address) (dax.Jobs, error)

	DatabaseForWorker(tx dax.Transaction, addr dax.Address) dax.DatabaseKey
}

type FreeJobService interface {
	CreateJobs(tx dax.Transaction, roleType dax.RoleType, qdbid dax.QualifiedDatabaseID, job ...dax.Job) error
	DeleteJob(tx dax.Transaction, roleType dax.RoleType, qdbid dax.QualifiedDatabaseID, job dax.Job) error
	DeleteJobsForTable(tx dax.Transaction, roleType dax.RoleType, qtid dax.QualifiedTableID) error
	ListJobs(tx dax.Transaction, roleType dax.RoleType, qdbid dax.QualifiedDatabaseID) (dax.Jobs, error)
	MarkJobsAsFree(tx dax.Transaction, roleType dax.RoleType, qdbid dax.QualifiedDatabaseID, jobs dax.Jobs) error
}

type FreeWorkerService interface {
	PopWorkers(tx dax.Transaction, roleType dax.RoleType, num int) ([]dax.Address, error)
	ListWorkers(tx dax.Transaction, roleType dax.RoleType) (dax.Addresses, error)
}
