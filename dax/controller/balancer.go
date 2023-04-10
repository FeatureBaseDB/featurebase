package controller

import (
	"github.com/featurebasedb/featurebase/v3/dax"
)

type Balancer interface {
	// AddWorker adds a worker to the global pool of available workers.
	AddWorker(tx dax.Transaction, node *dax.Node) ([]dax.WorkerDiff, error)

	// RemoveWorker removes a worker from the system. If the worker is currently
	// assigned to a database and has jobs, it will be removed and its jobs will
	// be either transferred to other workers or placed on the free job list.
	RemoveWorker(tx dax.Transaction, addr dax.Address) ([]dax.WorkerDiff, error)

	// ReleaseWorkers dissociates the given workers from a database.
	ReleaseWorkers(tx dax.Transaction, addrs ...dax.Address) error

	// AddJobs adds new jobs for the given database.
	AddJobs(tx dax.Transaction, roleType dax.RoleType, qtid dax.QualifiedTableID, jobs ...dax.Job) ([]dax.WorkerDiff, error)

	// RemoveJobs removes jobs for the given database.
	RemoveJobs(tx dax.Transaction, roleType dax.RoleType, qtid dax.QualifiedTableID, jobs ...dax.Job) ([]dax.WorkerDiff, error)

	// BalanceDatabase forces a database balance. TODO(tlt): currently this is
	// only used in tests, so perhaps we can get rid of it.
	BalanceDatabase(tx dax.Transaction, qdbid dax.QualifiedDatabaseID) ([]dax.WorkerDiff, error)

	// CurrentState returns the workers and jobs currently active for the given
	// database.
	CurrentState(tx dax.Transaction, roleType dax.RoleType, qdbid dax.QualifiedDatabaseID) ([]dax.WorkerInfo, error)

	// WorkerState returns the jobs currently active for the given worker.
	WorkerState(tx dax.Transaction, roleType dax.RoleType, addr dax.Address) (dax.WorkerInfo, error)

	// WorkersForJobs returns the workers and jobs currently responsible for the
	// given jobs.
	WorkersForJobs(tx dax.Transaction, roleType dax.RoleType, qdbid dax.QualifiedDatabaseID, jobs ...dax.Job) ([]dax.WorkerInfo, error)

	// WorkersForTable returns the workers responsible for any job related to
	// the given table.
	WorkersForTable(tx dax.Transaction, roleType dax.RoleType, qtid dax.QualifiedTableID) ([]dax.WorkerInfo, error)

	// ReadNode returns the node for the given address.
	ReadNode(tx dax.Transaction, addr dax.Address) (*dax.Node, error)

	// Nodes returns all nodes known by the Balancer.
	Nodes(tx dax.Transaction) ([]*dax.Node, error)

	// WorkerCount returns the number of workers for the given database.
	WorkerCount(tx dax.Transaction, qdbid dax.QualifiedDatabaseID) (int, error)
}

// Ensure type implements interface.
var _ Balancer = (*NopBalancer)(nil)

// NopBalancer is a no-op implementation of the Balancer interface.
type NopBalancer struct{}

func NewNopBalancer() *NopBalancer {
	return &NopBalancer{}
}

func (b *NopBalancer) AddWorker(tx dax.Transaction, node *dax.Node) ([]dax.WorkerDiff, error) {
	return []dax.WorkerDiff{}, nil
}
func (b *NopBalancer) RemoveWorker(tx dax.Transaction, addr dax.Address) ([]dax.WorkerDiff, error) {
	return []dax.WorkerDiff{}, nil
}
func (b *NopBalancer) ReleaseWorkers(tx dax.Transaction, addrs ...dax.Address) error {
	return nil
}
func (b *NopBalancer) AddJobs(tx dax.Transaction, roleType dax.RoleType, qtid dax.QualifiedTableID, jobs ...dax.Job) ([]dax.WorkerDiff, error) {
	return []dax.WorkerDiff{}, nil
}
func (b *NopBalancer) RemoveJobs(tx dax.Transaction, roleType dax.RoleType, qtid dax.QualifiedTableID, jobs ...dax.Job) ([]dax.WorkerDiff, error) {
	return []dax.WorkerDiff{}, nil
}
func (b *NopBalancer) BalanceDatabase(tx dax.Transaction, qdbid dax.QualifiedDatabaseID) ([]dax.WorkerDiff, error) {
	return []dax.WorkerDiff{}, nil
}
func (b *NopBalancer) CurrentState(tx dax.Transaction, roleType dax.RoleType, qdbid dax.QualifiedDatabaseID) ([]dax.WorkerInfo, error) {
	return []dax.WorkerInfo{}, nil
}
func (b *NopBalancer) WorkerState(tx dax.Transaction, roleType dax.RoleType, addr dax.Address) (dax.WorkerInfo, error) {
	return dax.WorkerInfo{}, nil
}
func (b *NopBalancer) WorkersForJobs(tx dax.Transaction, roleType dax.RoleType, qdbid dax.QualifiedDatabaseID, jobs ...dax.Job) ([]dax.WorkerInfo, error) {
	return []dax.WorkerInfo{}, nil
}
func (b *NopBalancer) WorkersForTable(tx dax.Transaction, roleType dax.RoleType, qtid dax.QualifiedTableID) ([]dax.WorkerInfo, error) {
	return []dax.WorkerInfo{}, nil
}
func (b *NopBalancer) ReadNode(tx dax.Transaction, addr dax.Address) (*dax.Node, error) {
	return nil, nil
}
func (b *NopBalancer) Nodes(tx dax.Transaction) ([]*dax.Node, error) {
	return []*dax.Node{}, nil
}
func (b *NopBalancer) WorkerCount(tx dax.Transaction, qdbid dax.QualifiedDatabaseID) (int, error) {
	return 0, nil
}
