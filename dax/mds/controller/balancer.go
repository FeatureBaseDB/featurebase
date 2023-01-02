package controller

import (
	"context"
	"fmt"

	"github.com/molecula/featurebase/v3/dax"
)

type Balancer interface {
	AddWorker(ctx context.Context, worker fmt.Stringer) ([]dax.WorkerDiff, error)
	RemoveWorker(ctx context.Context, worker fmt.Stringer) ([]dax.WorkerDiff, error)
	AddJobs(ctx context.Context, job ...fmt.Stringer) ([]dax.WorkerDiff, error)
	RemoveJob(ctx context.Context, job fmt.Stringer) ([]dax.WorkerDiff, error)
	Balance(ctx context.Context) ([]dax.WorkerDiff, error)
	CurrentState(ctx context.Context) ([]dax.WorkerInfo, error)
	WorkerState(ctx context.Context, worker dax.Worker) (dax.WorkerInfo, error)
	WorkersForJobs(ctx context.Context, jobs []dax.Job) ([]dax.WorkerInfo, error)

	// WorkersForJobPrefix returns all workers and their job
	// assignments which start with `prefix` for all jobs that start
	// with `prefix`. If there are free jobs that start with `prefix`
	// an error is returned.
	//
	// The motivating use case is getting all workers for a particular
	// table so we can execute a query that will hit every shard in a
	// table. If there are jobs representing shards in that table
	// which are not assigned to any worker, that means the query
	// would return incomplete data, so we want to error.
	WorkersForJobPrefix(ctx context.Context, prefix string) ([]dax.WorkerInfo, error)

	// RemoveJobs is for e.g. when dropping a table remove all jobs
	// associated with that table without needing to look up in
	// advance which shards or partitions are actually present.
	RemoveJobs(ctx context.Context, prefix string) ([]dax.WorkerDiff, error)
}

// Ensure type implements interface.
var _ Balancer = (*NopBalancer)(nil)

// NopBalancer is a no-op implementation of the Balancer interface.
type NopBalancer struct{}

func NewNopBalancer() *NopBalancer {
	return &NopBalancer{}
}

func (b *NopBalancer) AddWorker(ctx context.Context, worker fmt.Stringer) ([]dax.WorkerDiff, error) {
	return []dax.WorkerDiff{}, nil
}
func (b *NopBalancer) RemoveWorker(ctx context.Context, worker fmt.Stringer) ([]dax.WorkerDiff, error) {
	return []dax.WorkerDiff{}, nil
}
func (b *NopBalancer) AddJobs(ctx context.Context, job ...fmt.Stringer) ([]dax.WorkerDiff, error) {
	return []dax.WorkerDiff{}, nil
}
func (b *NopBalancer) RemoveJob(ctx context.Context, job fmt.Stringer) ([]dax.WorkerDiff, error) {
	return []dax.WorkerDiff{}, nil
}
func (b *NopBalancer) Balance(ctx context.Context) ([]dax.WorkerDiff, error) {
	return []dax.WorkerDiff{}, nil
}
func (b *NopBalancer) CurrentState(ctx context.Context) ([]dax.WorkerInfo, error) {
	return []dax.WorkerInfo{}, nil
}
func (b *NopBalancer) WorkerState(ctx context.Context, worker dax.Worker) (dax.WorkerInfo, error) {
	return dax.WorkerInfo{}, nil
}
func (b *NopBalancer) WorkersForJobs(ctx context.Context, jobs []dax.Job) ([]dax.WorkerInfo, error) {
	return []dax.WorkerInfo{}, nil
}
func (b *NopBalancer) WorkersForJobPrefix(ctx context.Context, prefix string) ([]dax.WorkerInfo, error) {
	return []dax.WorkerInfo{}, nil
}

func (b *NopBalancer) RemoveJobs(ctx context.Context, prefix string) ([]dax.WorkerDiff, error) {
	return nil, nil
}
