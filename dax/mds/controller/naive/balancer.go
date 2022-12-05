// Package naive contains a naive implementation of the Balancer interface.
package naive

import (
	"context"
	"fmt"
	"log"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/molecula/featurebase/v3/dax"
	"github.com/molecula/featurebase/v3/dax/mds/controller"
	"github.com/molecula/featurebase/v3/errors"
	"github.com/molecula/featurebase/v3/logger"
)

// Ensure type implements interface.
var _ controller.Balancer = (*Balancer)(nil)

// Balancer is a naive implementation of the controller.Balancer interface. It
// helps manage the relationships between workers and jobs. The logic it uses to
// balance jobs across workers is very simple; it bases everything off the
// number of workers and number of jobs. It does not take anything else (such as
// job size, worker capabilities, etc) into consideration.
type Balancer struct {
	mu sync.RWMutex

	// name is used in logging to help identify the balancer responsible for the
	// log.
	name string

	// current represents the current state of worker/job assigments.
	current WorkerJobService

	// freeJobs is the set of jobs which have yet to be assigned to a worker.
	// This could be because there are no available workers, or because a worker
	// has been removed and the jobs for which it was responsible have yet to be
	// reassigned.
	freeJobs FreeJobService

	logger logger.Logger
}

type WorkerJobService interface {
	WorkersJobs(ctx context.Context, balancerName string) ([]dax.WorkerInfo, error)

	WorkerCount(ctx context.Context, balancerName string) (int, error)
	ListWorkers(ctx context.Context, balancerName string) (dax.Workers, error)
	WorkerExists(ctx context.Context, balancerName string, worker dax.Worker) (bool, error)
	CreateWorker(ctx context.Context, balancerName string, worker dax.Worker) error
	DeleteWorker(ctx context.Context, balancerName string, worker dax.Worker) error

	CreateJobs(ctx context.Context, balancerName string, worker dax.Worker, job ...dax.Job) error
	DeleteJob(ctx context.Context, balancerName string, worker dax.Worker, job dax.Job) error
	JobCounts(ctx context.Context, balancerName string, worker ...dax.Worker) (map[dax.Worker]int, error)
	ListJobs(ctx context.Context, balancerName string, worker dax.Worker) (dax.Jobs, error)
}

type FreeJobService interface {
	CreateFreeJobs(ctx context.Context, balancerName string, job ...dax.Job) error
	DeleteFreeJob(ctx context.Context, balancerName string, job dax.Job) error
	ListFreeJobs(ctx context.Context, balancerName string) (dax.Jobs, error)
	MergeFreeJobs(ctx context.Context, balancerName string, jobs dax.Jobs) error
}

// New returns a new instance of Balancer.
func New(name string, fjs FreeJobService, wjs WorkerJobService, logger logger.Logger) *Balancer {
	return &Balancer{
		name:     name,
		current:  wjs,
		freeJobs: fjs,
		logger:   logger,
	}
}

// AddWorker adds a worker to the Balancer's worker pool. This may cause the
// Balancer to assign existing jobs that are currently in the free list to the
// worker. Also, the worker will immediately be available for assignments of new
// jobs.
func (b *Balancer) AddWorker(ctx context.Context, worker fmt.Stringer) ([]dax.WorkerDiff, error) {
	b.logger.Debugf("%s: AddWorker(%s)", b.name, worker.String())
	b.mu.Lock()
	defer b.mu.Unlock()

	diff, err := b.addWorker(ctx, dax.Worker(worker.String()))
	if err != nil {
		return nil, errors.Wrap(err, "adding worker")
	}

	return diff.output(), nil
}

func (b *Balancer) addWorker(ctx context.Context, worker dax.Worker) (internalDiffs, error) {
	// If this worker already exists, don't do anything.
	if exists, err := b.current.WorkerExists(ctx, b.name, worker); err != nil {
		return nil, errors.Wrap(err, "checking if worker exists")
	} else if exists {
		return internalDiffs{}, nil
	}

	if err := b.current.CreateWorker(ctx, b.name, worker); err != nil {
		return nil, errors.Wrap(err, "creating worker")
	}

	// Process the freeJobs.
	return b.processFreeJobs(ctx)
}

// ReplaceWorker is meant to avoid the job re-assignment caused by performing a
// RemoveWorker followed by an AddWorker. In this case, it does both in one step
// so that it's more likely that the jobs will just get transferred directly
// over. NOT IMPLEMENTED YET.
// func (b *Balancer) ReplaceWorker(fromWorker string, toWorker string) []WorkerDiff {
// 	b.mu.Lock()
// 	defer b.mu.Unlock()

// 	return []WorkerDiff{}
// }

// RemoveWorker removes a worker from the worker pool and moves any of its
// currently assigned jobs to the free list. If the intention is to remove a
// worker and reassign its jobs to other workers, then RemoveWorker() should be
// followed by Balance().
func (b *Balancer) RemoveWorker(ctx context.Context, worker fmt.Stringer) ([]dax.WorkerDiff, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	diff, err := b.removeWorker(ctx, dax.Worker(worker.String()))
	if err != nil {
		return nil, errors.Wrap(err, "removing worker")
	}

	return diff.output(), nil
}

func (b *Balancer) removeWorker(ctx context.Context, worker dax.Worker) (internalDiffs, error) {
	// If this worker doesn't exist, don't do anything else.
	if exists, err := b.current.WorkerExists(ctx, b.name, worker); err != nil {
		return nil, errors.Wrap(err, "checking if worker exists")
	} else if !exists {
		return internalDiffs{}, nil
	}

	jobs, err := b.current.ListJobs(ctx, b.name, worker)
	if err != nil {
		return nil, errors.Wrap(err, "listing jobs")
	}

	// Before removing the worker, mark its jobs as free.
	if err := b.freeJobs.MergeFreeJobs(ctx, b.name, jobs); err != nil {
		return nil, errors.Wrap(err, "merging free jobs")
	}

	// Remove the worker.
	if err := b.current.DeleteWorker(ctx, b.name, worker); err != nil {
		return nil, errors.Wrap(err, "deleting worker")
	}

	// Even though this may not be useful to the caller (for example, in the
	// case where the worker has died and no longer exists), return the diffs
	// which represent the removal of jobs from the worker.
	diff := newInternalDiffs()
	for _, job := range jobs {
		diff.removed(worker, job)
	}

	return diff, nil
}

// AddJobs adds one or more jobs to an existing worker. If there are no existing
// workers, the jobs are placed into the free list and will be assigned to a
// worker once one becomes available.
func (b *Balancer) AddJobs(ctx context.Context, jobs ...fmt.Stringer) ([]dax.WorkerDiff, error) {
	start := time.Now()
	defer func() {
		log.Printf("ELAPSED: Balancer.AddJob: %v", time.Since(start))
	}()

	jobsToAdd := make([]dax.Job, 0, len(jobs))
	for _, job := range jobs {
		jobsToAdd = append(jobsToAdd, dax.Job(job.String()))
	}

	if len(jobsToAdd) == 1 {
		b.logger.Debugf("%s: AddJobs (%s)", b.name, jobsToAdd[0])
	} else {
		b.logger.Debugf("%s: AddJobs (%d)", b.name, len(jobsToAdd))
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	diff, err := b.addJobs(ctx, jobsToAdd...)
	if err != nil {
		return nil, errors.Wrap(err, "adding job")
	}

	return diff.output(), nil
}

func (b *Balancer) addJobs(ctx context.Context, jobs ...dax.Job) (internalDiffs, error) {
	if cnt, err := b.current.WorkerCount(ctx, b.name); err != nil {
		return nil, errors.Wrap(err, "getting worker count")
	} else if cnt == 0 {
		if err := b.freeJobs.CreateFreeJobs(ctx, b.name, jobs...); err != nil {
			return nil, errors.Wrap(err, "creating free job")
		}
		// TODO: we might want to inform the user that a job is in the free list
		// because there are no workers.
		return internalDiffs{}, nil
	}

	workerJobs, err := b.current.WorkersJobs(ctx, b.name)
	if err != nil {
		return nil, errors.Wrapf(err, "getting workers jobs: %s", b.name)
	}
	jset := dax.NewSet[dax.Job]()
	for _, workerInfo := range workerJobs {
		jset.Merge(dax.NewSet(workerInfo.Jobs...))
	}

	workerIDs := make(dax.Workers, 0, len(workerJobs))
	jobCounts := make(map[dax.Worker]int, 0)
	for _, v := range workerJobs {
		workerIDs = append(workerIDs, v.ID)
		jobCounts[v.ID] = len(v.Jobs)
	}

	diffs := newInternalDiffs()

	jobsToCreate := make(map[dax.Worker][]dax.Job)

	for _, job := range jobs {
		// Skip any job that already exists.
		if jset.Contains(job) {
			continue
		}

		// Find the worker with the fewest number of jobs and assign it this job.
		var lowCount int = math.MaxInt
		var lowWorker dax.Worker

		// We loop over workerIDs here instead of jobCounts because jobCounts is
		// a map and it can return results in an unexpected order, which is a
		// problem for testing.
		for _, worker := range workerIDs {
			jobCount := jobCounts[worker]
			if jobCount < lowCount {
				lowCount = jobCount
				lowWorker = worker
			}
		}

		jobsToCreate[lowWorker] = append(jobsToCreate[lowWorker], job)
		jobCounts[lowWorker]++
	}

	for worker, jobs := range jobsToCreate {
		if err := b.current.CreateJobs(ctx, b.name, worker, jobs...); err != nil {
			return nil, errors.Wrap(err, "creating job")
		}
		for _, job := range jobs {
			diffs.added(worker, job)
		}
	}

	return diffs, nil
}

// RemoveJob removes a job from the worker to which is was assigned. If the job
// is not currently assigned to a worker, but it is in the free list, then it
// will be removed from the free list.
func (b *Balancer) RemoveJob(ctx context.Context, job fmt.Stringer) ([]dax.WorkerDiff, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	diff, err := b.removeJob(ctx, dax.Job(job.String()))
	if err != nil {
		return nil, errors.Wrapf(err, "removing job: %s", job)
	}

	return diff.output(), nil
}

func (b *Balancer) removeJob(ctx context.Context, job dax.Job) (internalDiffs, error) {
	if worker, ok, err := b.workerForJob(ctx, job); err != nil {
		return nil, errors.Wrapf(err, "getting worker for job: %s", job)
	} else if ok {
		if err := b.current.DeleteJob(ctx, b.name, worker, job); err != nil {
			return nil, errors.Wrapf(err, "deleting job: %s", job)
		}

		diffs := newInternalDiffs()
		diffs.removed(worker, job)

		return diffs, nil
	}

	// Just in case the job is in the free list (and wasn't assigned to a
	// worker), remove it; there's no need to provide a diff. There should never
	// be a case where the same job is both in the free list and assigned to a
	// worker.
	if err := b.freeJobs.DeleteFreeJob(ctx, b.name, job); err != nil {
		return nil, errors.Wrapf(err, "deleting free job: %s", job)
	}

	return internalDiffs{}, nil
}

// Balance ensures that all jobs are being handled by a worker by assigning jobs
// in the free list to workers, and by moving job assignments around in order to
// balance the load on workers.
func (b *Balancer) Balance(ctx context.Context) ([]dax.WorkerDiff, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// If there are no workers, we can't properly balance.
	if cnt, err := b.current.WorkerCount(ctx, b.name); err != nil {
		return nil, errors.Wrapf(err, "getting worker count: %s", b.name)
	} else if cnt == 0 {
		return []dax.WorkerDiff{}, nil
	}

	// Process the freeJobs.
	diffs, err := b.processFreeJobs(ctx)
	if err != nil {
		return nil, errors.Wrapf(err, "processing free jobs: %s", b.name)
	}

	// Balance the jobs among workers.
	diff, err := b.balance(ctx, diffs)
	if err != nil {
		return nil, errors.Wrap(err, "balancing jobs")
	}

	return diff.output(), nil
}

// balance moves jobs among workers with the goal of having an equal number of
// jobs per worker. This method takes an `internalDiffs` as input for cases
// where some action has preceeded this call which also resulted in
// `internalDiffs`. Instead of having this method take a value, we could rely on
// the internalDiffs.merge() method, but we would need to modify that method to
// be smarter about the order in which it applies the add/remove operations.
// Until that's in place, we'll pass in a value here.
func (b *Balancer) balance(ctx context.Context, diffs internalDiffs) (internalDiffs, error) {
	numWorkers, err := b.current.WorkerCount(ctx, b.name)
	if err != nil {
		return nil, errors.Wrapf(err, "getting worker count: %s", b.name)
	}
	numJobs := 0
	if workers, err := b.current.ListWorkers(ctx, b.name); err != nil {
		return nil, errors.Wrapf(err, "listing workers: %s", b.name)
	} else {
		for _, worker := range workers {
			jobCounts, err := b.current.JobCounts(ctx, b.name, worker)
			if err != nil {
				return nil, errors.Wrapf(err, "getting job count: %s", worker)
			}
			numJobs += jobCounts[worker]
		}
	}

	minJobsPerWorker := numJobs / numWorkers
	numWorkersAboveMin := numJobs % numWorkers

	// sortedWorkerInfos is used now in order to guarantee a sort order.
	sortedWorkerInfos, err := b.currentState(ctx, true)
	if err != nil {
		return nil, errors.Wrapf(err, "getting current state: %s", b.name)
	}

	// Loop through each worker, and if the number of jobs for the worker
	// exceeds the target, then remove the job and add it back (which is
	// effectively how we rebalance a job).
	for i, workerInfo := range sortedWorkerInfos {
		numTargetJobs := minJobsPerWorker
		if i < numWorkersAboveMin {
			numTargetJobs += 1
		}

		jobCounts, err := b.current.JobCounts(ctx, b.name, workerInfo.ID)
		if err != nil {
			return nil, errors.Wrapf(err, "getting job count: %s", workerInfo.ID)
		}
		numCurrentJobs := jobCounts[workerInfo.ID]

		// If we don't need to remove jobs from this worker, then just continue
		// on to the next worker.
		if numCurrentJobs <= numTargetJobs {
			continue
		}

		sortedJobs, err := b.current.ListJobs(ctx, b.name, workerInfo.ID)
		if err != nil {
			return nil, errors.Wrapf(err, "listing jobs: %s", workerInfo.ID)
		}

		// Remove the extra jobs from the end of the list, and add them back
		// again (which should place them on a worker with fewer jobs).
		for i := numCurrentJobs - 1; i >= numTargetJobs; i-- {
			if rj, err := b.removeJob(ctx, sortedJobs[i]); err != nil {
				return nil, errors.Wrapf(err, "removing job: %s", sortedJobs[i])
			} else {
				diffs.merge(rj)
			}
			if aj, err := b.addJobs(ctx, sortedJobs[i]); err != nil {
				return nil, errors.Wrapf(err, "adding job: %s", sortedJobs[i])
			} else {
				diffs.merge(aj)
			}
		}
	}

	return diffs, nil
}

// CurrentState returns the current state of worker and job assignments. Note
// that there could be unassigned jobs which are not captured in this output.
// Calling Balance() would force any unassigned jobs to be assigned (assuming
// there is at least one worker), and the output would then reflect that.
func (b *Balancer) CurrentState(ctx context.Context) ([]dax.WorkerInfo, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.currentState(ctx, true)
}

func (b *Balancer) currentState(ctx context.Context, sorted bool) ([]dax.WorkerInfo, error) {
	return b.current.WorkersJobs(ctx, b.name)
}

// WorkerState returns the current state of job assignments for a given worker.
func (b *Balancer) WorkerState(ctx context.Context, worker dax.Worker) (dax.WorkerInfo, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.workerState(ctx, worker)
}

func (b *Balancer) workerState(ctx context.Context, worker dax.Worker) (dax.WorkerInfo, error) {
	if exists, err := b.current.WorkerExists(ctx, b.name, worker); err != nil {
		return dax.WorkerInfo{}, errors.Wrapf(err, "checking worker exists: %s", worker)
	} else if !exists {
		return dax.WorkerInfo{
			ID: dax.Worker(worker),
		}, nil
	}

	jobs, err := b.current.ListJobs(ctx, b.name, worker)
	if err != nil {
		return dax.WorkerInfo{}, errors.Wrapf(err, "listing jobs: %s", worker)
	}

	return dax.WorkerInfo{
		ID:   dax.Worker(worker),
		Jobs: jobs,
	}, nil
}

// WorkersForJobs returns the list of workers for the given jobs. If a given job
// is not currently assigned to a worker, it will be ignored.
func (b *Balancer) WorkersForJobs(ctx context.Context, jobs []dax.Job) ([]dax.WorkerInfo, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.workersForJobs(ctx, jobs)
}

func (b *Balancer) workersForJobs(ctx context.Context, jobs []dax.Job) ([]dax.WorkerInfo, error) {
	out := make(map[dax.Worker]dax.Set[dax.Job])

	workerJobs, err := b.current.WorkersJobs(ctx, b.name)
	if err != nil {
		return nil, errors.Wrapf(err, "getting worker jobs: %s", b.name)
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
			out[workerInfo.ID] = matches
		}
	}

	workers := make([]dax.WorkerInfo, len(out))

	i := 0
	for w, jset := range out {
		workers[i] = dax.WorkerInfo{
			ID:   dax.Worker(w),
			Jobs: jset.Sorted(),
		}
		i++
	}

	sort.Sort(dax.WorkerInfos(workers))

	return workers, nil
}

func (b *Balancer) WorkersForJobPrefix(ctx context.Context, prefix string) ([]dax.WorkerInfo, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	jobs, err := b.freeJobs.ListFreeJobs(ctx, b.name)
	if err != nil {
		return nil, errors.Wrap(err, "listing free jobs")
	}
	for _, job := range jobs {
		if strings.HasPrefix(string(job), prefix) {
			return nil, errors.Errorf("found free job '%s' matching prefix '%s'", job, prefix)
		}
	}

	workerJobs, err := b.current.WorkersJobs(ctx, b.name)
	if err != nil {
		return nil, errors.Wrapf(err, "getting worker jobs: %s", b.name)
	}

	result := make([]dax.WorkerInfo, 0)
	for _, workerInfo := range workerJobs {
		matchedJobs := make([]dax.Job, 0)
		for _, job := range workerInfo.Jobs {
			if strings.HasPrefix(string(job), prefix) {
				matchedJobs = append(matchedJobs, job)
			}
		}
		if len(matchedJobs) > 0 {
			result = append(result, dax.WorkerInfo{
				ID:   workerInfo.ID,
				Jobs: matchedJobs,
			})
		}
	}
	return result, nil

}

// processFreeJobs assigns all jobs in the free list to a worker.
func (b *Balancer) processFreeJobs(ctx context.Context) (internalDiffs, error) {
	diffs := newInternalDiffs()
	jobs, err := b.freeJobs.ListFreeJobs(ctx, b.name)
	if err != nil {
		return nil, errors.Wrapf(err, "listing free jobs: %s", b.name)
	}
	for _, job := range jobs {
		if aj, err := b.addJobs(ctx, job); err != nil {
			return nil, errors.Wrapf(err, "adding job: %s", job)
		} else {
			diffs.merge(aj)
		}
		if err := b.freeJobs.DeleteFreeJob(ctx, b.name, job); err != nil {
			return nil, errors.Wrapf(err, "deleting free job: %s", job)
		}
	}
	return diffs, nil
}

// workerForJob returns the worker currently assigned to the given job.
func (b *Balancer) workerForJob(ctx context.Context, job dax.Job) (dax.Worker, bool, error) {
	workerJobs, err := b.current.WorkersJobs(ctx, b.name)
	if err != nil {
		return "", false, errors.Wrapf(err, "getting workers jobs: %s", b.name)
	}
	for _, workerInfo := range workerJobs {
		jset := dax.NewSet(workerInfo.Jobs...)
		if jset.Contains(job) {
			return workerInfo.ID, true, nil
		}
	}
	return "", false, nil
}
