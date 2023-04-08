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
	"github.com/featurebasedb/featurebase/v3/errors"
	"github.com/featurebasedb/featurebase/v3/logger"
)

// Ensure type implements interface.
var _ controller.Balancer = (*Balancer)(nil)

// TODO: continue figuring out which things we removed need to come back and implement them, based on what's not implemented on the balancer interface

// Balancer is an implementation of the balancer.Balancer interface which
// isolates workers and jobs by database. It helps manage the relationships
// between workers and jobs. The logic it uses to balance jobs across workers is
// very simple; it bases everything off the number of workers and number of
// jobs. It does not take anything else (such as job size, worker capabilities,
// etc) into consideration.
type Balancer struct {
	store controller.Store

	logger logger.Logger
}

// New returns a new instance of Balancer.
func New(store controller.Store, logger logger.Logger) *Balancer {
	return &Balancer{
		store: store,

		logger: logger,
	}
}

// AddWorker adds the given Node to the Balancer's available worker pool. Note
// that a node is used for ALL of the role types specified. In other words,
// specifying roleTypes = {compute, translate}, does not mean that the node can
// be used as either a compute worker or a translate worker. It means that it
// will be used as both.
func (b *Balancer) AddWorker(tx dax.Transaction, node *dax.Node) ([]dax.WorkerDiff, error) {
	b.logger.Debugf("AddWorker(%s)", node.Address)

	qdbidp, err := b.store.AddWorker(tx, node)
	if err != nil {
		return nil, errors.Wrapf(err, "creating node on node service: %s", node.Address)
	}

	diffs, err := b.balanceDatabase(tx, *qdbidp)
	if err != nil {
		return nil, errors.Wrap(err, "balancing db")
	}
	return diffs.Output(), nil
}

// RemoveWorker removes a worker from the system. If the worker is currently
// assigned to a database and has jobs, it will be removed and its jobs will
// be either transferred to other workers or placed on the free job list.
func (b *Balancer) RemoveWorker(tx dax.Transaction, addr dax.Address) ([]dax.WorkerDiff, error) {
	diffs := controller.NewInternalDiffs()

	worker, err := b.store.WorkerForAddress(tx, addr)
	if err != nil {
		return nil, errors.Wrap(err, "getting worker for address")
	}

	// drop from workers table. This *should* also remove all associations of jobs to this worker.
	if err := b.store.RemoveWorker(tx, worker.ID); err != nil {
		return nil, errors.Wrap(err, "removing worker")
	}

	if worker.DatabaseID != nil {
		// Balance the affected database.
		if diff, err := b.balanceDatabase(tx, *worker.DatabaseID); err != nil {
			return nil, errors.Wrapf(err, "balancing database: %s", worker.DatabaseID)
		} else {
			diffs.Merge(diff)
		}
	}

	return diffs.Output(), nil
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
	dbid := qtid.QualifiedDatabaseID.DatabaseID

	diff, err := b.addJobs(tx, roleType, dbid, jobs...)
	if err != nil {
		return nil, errors.Wrap(err, "adding job")
	}

	return diff.Output(), nil
}

func (b *Balancer) addJobs(tx dax.Transaction, roleType dax.RoleType, dbid dax.DatabaseID, jobs ...dax.Job) (controller.InternalDiffs, error) {
	diffs := controller.NewInternalDiffs()

	if len(jobs) == 0 {
		return diffs, nil
	}

	if cnt, err := b.store.WorkerCount(tx, roleType, dbid); err != nil {
		return nil, errors.Wrap(err, "getting worker count")
	} else if cnt == 0 {
		if err := b.store.CreateFreeJobs(tx, roleType, dbid, jobs...); err != nil {
			return nil, errors.Wrap(err, "creating free jobs")
		}
	}

	diff, err := b.addDatabaseJobs(tx, roleType, dbid, jobs...)
	if err != nil {
		return nil, errors.Wrapf(err, "adding database jobs: (%s) %s", roleType, dbid)
	}
	diffs.Merge(diff)

	return diffs, nil
}

// addDatabaseJobs adds the job for the provided database. TODO, bad doc
func (b *Balancer) addDatabaseJobs(tx dax.Transaction, roleType dax.RoleType, dbid dax.DatabaseID, jobs ...dax.Job) (controller.InternalDiffs, error) {
	workerJobs, err := b.store.WorkersJobs(tx, roleType, dbid)
	if err != nil {
		return nil, errors.Wrapf(err, "getting workers jobs: %s", roleType)
	}

	jset := dax.NewSet[dax.Job]()
	addrToID := make(map[dax.Address]dax.WorkerID)
	for _, workerInfo := range workerJobs {
		addrToID[workerInfo.Address] = workerInfo.ID
		jset.Merge(dax.NewSet(workerInfo.Jobs...))
	}

	addrs := make(dax.Addresses, 0, len(workerJobs))
	jobCounts := make(map[dax.Address]int, 0)
	for _, v := range workerJobs {
		addrs = append(addrs, v.Address)
		jobCounts[v.Address] = len(v.Jobs)
	}

	diffs := controller.NewInternalDiffs()

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
		if err := b.store.AssignWorkerToJobs(tx, roleType, dbid, addrToID[addr], jobs...); err != nil {
			return nil, errors.Wrap(err, "creating job")
		}
		for _, job := range jobs {
			diffs.Added(addr, job)
		}
	}

	return diffs, nil
}

func (b *Balancer) CurrentState(tx dax.Transaction, roleType dax.RoleType, qdbid dax.QualifiedDatabaseID) ([]dax.WorkerInfo, error) {
	dbid := qdbid.DatabaseID
	return b.store.WorkersJobs(tx, roleType, dbid)
}

func (b *Balancer) WorkerState(tx dax.Transaction, roleType dax.RoleType, addr dax.Address) (dax.WorkerInfo, error) {
	return b.store.WorkerJobs(tx, roleType, addr)
}

func (b *Balancer) RemoveJobs(tx dax.Transaction, roleType dax.RoleType, qtid dax.QualifiedTableID) ([]dax.WorkerDiff, error) {
	diffs, err := b.store.DeleteJobsForTable(tx, roleType, qtid)
	if err != nil {
		return nil, errors.Wrap(err, "deleting jobs from storage")
	}
	return diffs.Output(), nil
}

// TODO can probably simplify this with better SQL
func (b *Balancer) WorkersForJobs(tx dax.Transaction, roleType dax.RoleType, qdbid dax.QualifiedDatabaseID, jobs ...dax.Job) ([]dax.WorkerInfo, error) {
	out := make(map[dax.Address]dax.Set[dax.Job])

	workerJobs, err := b.store.WorkersJobs(tx, roleType, qdbid.DatabaseID)
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

// TODO can probably simplify this with better SQL
func (b *Balancer) WorkersForTable(tx dax.Transaction, roleType dax.RoleType, qtid dax.QualifiedTableID) ([]dax.WorkerInfo, error) {
	out := make(map[dax.Address]dax.Set[dax.Job])

	dbid := qtid.QualifiedDatabaseID.DatabaseID

	prefix := string(qtid.Key())

	workerJobs, err := b.store.WorkersJobs(tx, roleType, dbid)
	if err != nil {
		return nil, errors.Wrapf(err, "getting worker jobs: (%s) %s", roleType, dbid)
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

func (b *Balancer) BalanceDatabase(tx dax.Transaction, qdbid dax.QualifiedDatabaseID) ([]dax.WorkerDiff, error) {
	diffs, err := b.balanceDatabase(tx, qdbid.DatabaseID)
	if err != nil {
		return nil, errors.Wrap(err, "balancing")
	}
	return diffs.Output(), nil
}

func (b *Balancer) balanceDatabase(tx dax.Transaction, dbid dax.DatabaseID) (controller.InternalDiffs, error) {
	diffs := controller.NewInternalDiffs()

	for _, role := range dax.AllRoleTypes {
		diff, err := b.balanceDatabaseForRole(tx, role, dbid)
		if err != nil {
			return nil, errors.Wrapf(err, "getting worker count: (%s) %s", role, dbid)
		}
		diffs.Merge(diff)
	}

	return diffs, nil
}

func (b *Balancer) balanceDatabaseForRole(tx dax.Transaction, roleType dax.RoleType, dbid dax.DatabaseID) (controller.InternalDiffs, error) {
	b.logger.Debugf("balancing database %s for role: %s\n", dbid, roleType)
	diffs := controller.NewInternalDiffs()

	// If there are no workers, we can't properly balance.
	if cnt, err := b.store.WorkerCount(tx, roleType, dbid); err != nil {
		return nil, errors.Wrapf(err, "getting worker count: (%s) %s", roleType, dbid)
	} else if cnt == 0 {
		return controller.InternalDiffs{}, errors.Errorf("unexpected? no workers for database %v (TODO: this used to silently return nil)", dbid)
	}

	// Process the freeJobs.
	if diff, err := b.processFreeJobs(tx, roleType, dbid); err != nil {
		return nil, errors.Wrapf(err, "processing free jobs: (%s) %s", roleType, dbid)
	} else {
		diffs.Merge(diff)
	}

	// Balance the jobs among workers.
	diff, err := b.balanceDatabaseJobs(tx, roleType, dbid, diffs)
	if err != nil {
		return nil, errors.Wrap(err, "balancing jobs")
	}

	return diff, nil
}

// balanceDatabaseJobs moves jobs among workers with the goal of having an equal
// number of jobs per worker. This method takes an `internalDiffs` as input for
// cases where some action has preceeded this call which also resulted in
// `internalDiffs`. Instead of having this method take a value, we could rely on
// the internalDiffs.merge() method, but we would need to modify that method to
// be smarter about the order in which it applies the add/remove operations.
// Until that's in place, we'll pass in a value here.
func (b *Balancer) balanceDatabaseJobs(tx dax.Transaction, roleType dax.RoleType, dbid dax.DatabaseID, diffs controller.InternalDiffs) (controller.InternalDiffs, error) {
	workerInfos, err := b.store.WorkersJobs(tx, roleType, dbid)
	if err != nil {
		return nil, errors.Wrapf(err, "getting current state: (%s) %s", roleType, dbid)
	}

	numWorkers := len(workerInfos)
	if numWorkers == 0 {
		return nil, errors.Errorf("can't balance a database ID %v with no workers", dbid)
	}
	numJobs := 0
	for _, info := range workerInfos {
		numJobs += len(info.Jobs)
	}

	minJobsPerWorker := numJobs / numWorkers
	numWorkersAboveMin := numJobs % numWorkers

	removedJobs := make(dax.Jobs, 0)
	addedJobs := make(map[dax.WorkerID]dax.Jobs)

	// remove jobs loop
	//
	// Find all the workers that have more jobs than the target and
	// add those jobs to the list of removedJobs.
	for i, workerInfo := range workerInfos {
		numTargetJobs := minJobsPerWorker
		if i < numWorkersAboveMin {
			numTargetJobs += 1
		}

		numCurrentJobs := len(workerInfo.Jobs)
		// If we don't need to remove jobs from this worker, then just continue
		// on to the next worker.
		if numCurrentJobs <= numTargetJobs {
			continue
		}

		for i := numTargetJobs; i < numCurrentJobs; i++ {
			diffs.Removed(workerInfo.Address, workerInfo.Jobs[i])
		}
		removedJobs = append(removedJobs, workerInfo.Jobs[numTargetJobs:]...)
	}

	// add jobs loop
	//
	// Find all the workers that have fewer jobs than the target, and
	// add jobs to them from the removedJobs list. This should balance
	// perfectly such that removedJobs is empty at the end of this
	// loop.
	for i, workerInfo := range workerInfos {
		numTargetJobs := minJobsPerWorker
		if i < numWorkersAboveMin {
			numTargetJobs += 1
		}

		numCurrentJobs := len(workerInfo.Jobs)
		// If current jobs is already at the target, we don't need to
		// add jobs to this worker, so continue.
		if numCurrentJobs >= numTargetJobs {
			continue
		}

		for i := 0; i < numTargetJobs-numCurrentJobs; i++ {
			diffs.Added(workerInfo.Address, removedJobs[0])
			addedJobs[workerInfo.ID] = append(addedJobs[workerInfo.ID], removedJobs[0])
			removedJobs = removedJobs[1:]
		}
	}
	if len(removedJobs) != 0 {
		panic("everything is terrible")
	}

	for wid, jobs := range addedJobs {
		err := b.store.AssignWorkerToJobs(tx, roleType, dbid, wid, jobs...)
		if err != nil {
			return nil, errors.Wrap(err, "assigning worker to jobs in DB")
		}
	}

	return diffs, nil
}

// processFreeJobs assigns all jobs in the free list to a worker.
func (b *Balancer) processFreeJobs(tx dax.Transaction, roleType dax.RoleType, dbid dax.DatabaseID) (controller.InternalDiffs, error) {
	diffs := controller.NewInternalDiffs()
	jobs, err := b.store.ListFreeJobs(tx, roleType, dbid)
	if err != nil {
		return nil, errors.Wrapf(err, "listing free jobs: %s", roleType)
	}

	if aj, err := b.addDatabaseJobs(tx, roleType, dbid, jobs...); err != nil {
		return nil, errors.Wrapf(err, "adding jobs: %s", jobs)
	} else {
		diffs.Merge(aj)
	}

	return diffs, nil
}

func (b *Balancer) ReadNode(tx dax.Transaction, addr dax.Address) (*dax.Node, error) {
	return b.store.Worker(tx, addr)
}
func (b *Balancer) Nodes(tx dax.Transaction) ([]*dax.Node, error) {
	return b.store.Workers(tx)
}

func (b *Balancer) CreateWorkerServiceProvider(tx dax.Transaction, sp dax.WorkerServiceProvider) error {
	return b.store.CreateWorkerServiceProvider(tx, sp)
}

func (b *Balancer) CreateWorkerService(tx dax.Transaction, srv dax.WorkerService) error {
	return b.store.CreateWorkerService(tx, srv)
}

func (b *Balancer) WorkerServiceProviders(tx dax.Transaction /*, future optional filters */) (dax.WorkerServiceProviders, error) {
	return b.store.WorkerServiceProviders(tx)
}

func (b *Balancer) AssignFreeServiceToDatabase(tx dax.Transaction, wspID dax.WorkerServiceProviderID, qdb *dax.QualifiedDatabase) (*dax.WorkerService, error) {
	return b.store.AssignFreeServiceToDatabase(tx, wspID, qdb)
}

func (b *Balancer) WorkerServices(tx dax.Transaction, wsp dax.WorkerServiceProviderID) (dax.WorkerServices, error) {
	return b.store.WorkerServices(tx, wsp)
}

type WorkerJobService interface {
	WorkersJobs(tx dax.Transaction, roleType dax.RoleType, qdbid dax.QualifiedDatabaseID) ([]dax.WorkerInfo, error)

	WorkerCount(tx dax.Transaction, roleType dax.RoleType, qdbid dax.QualifiedDatabaseID) (int, error)
	ListWorkers(tx dax.Transaction, roleType dax.RoleType, qdbid dax.QualifiedDatabaseID) (dax.Addresses, error)

	AssignWorkerToJobs(tx dax.Transaction, roleType dax.RoleType, qdbid dax.QualifiedDatabaseID, addr dax.Address, job ...dax.Job) error
	DeleteJob(tx dax.Transaction, roleType dax.RoleType, qdbid dax.QualifiedDatabaseID, addr dax.Address, job dax.Job) error
	DeleteJobsForTable(tx dax.Transaction, roleType dax.RoleType, qtid dax.QualifiedTableID) (controller.InternalDiffs, error)
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

type WorkerServiceProviderService interface {
	CreateWorkerServiceProvider(tx dax.Transaction, sp dax.WorkerServiceProvider) error
	CreateWorkerService(tx dax.Transaction, srv dax.WorkerService) error
	WorkerServiceProviders(tx dax.Transaction /*, future optional filters */) (dax.WorkerServiceProviders, error)
	WorkerServices(tx dax.Transaction, wspID dax.WorkerServiceProviderID) (dax.WorkerServices, error)
	AssignFreeServiceToDatabase(tx dax.Transaction, wspID dax.WorkerServiceProviderID, qdb *dax.QualifiedDatabase) (*dax.WorkerService, error)
}
