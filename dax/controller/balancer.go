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

	// AddJobs adds new jobs for the given database.
	AddJobs(tx dax.Transaction, roleType dax.RoleType, qtid dax.QualifiedTableID, jobs ...dax.Job) ([]dax.WorkerDiff, error)

	// RemoveJobs removes jobs for the given database.
	RemoveJobs(tx dax.Transaction, roleType dax.RoleType, qtid dax.QualifiedTableID, jobs ...dax.Job) ([]dax.WorkerDiff, error)

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

	// CreateWorkerServiceProvider adds a WorkerServiceProvider (WSP) to
	// storage. Any WorkerService which registers must have a
	// WorkerServiceProviderID that matches an existing WSP, and the
	// Controller will ask for which WSPs are available when asked to
	// create a database. It will then ask one of the WSPs for a
	// WorkerService to assign to the database.
	CreateWorkerServiceProvider(tx dax.Transaction, sp dax.WorkerServiceProvider) error

	// CreateWorkerService adds a WorkerService to storage. Generally
	// a WSP can create WorkerServices (which can create Workers)
	// before they are requested. Because the workers will register
	// themselves as soon as they come up, and they must be associated
	// with a WorkerService, the WSP registers all Services with the
	// Controller which stores the knowledge of their existence by
	// calling this method.
	CreateWorkerService(tx dax.Transaction, srv dax.WorkerService) error

	WorkerServiceProviders(tx dax.Transaction /*, future optional filters */) (dax.WorkerServiceProviders, error)

	AssignFreeServiceToDatabase(tx dax.Transaction, wspID dax.WorkerServiceProviderID, qdb *dax.QualifiedDatabase) (*dax.WorkerService, error)

	// WorkerServices returns all worker services which came from the
	// WorkerServiceProvider with the given ID. If that ID is empty,
	// then all WorkerServices are returned.
	WorkerServices(tx dax.Transaction, wspID dax.WorkerServiceProviderID) (dax.WorkerServices, error)
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

func (b *NopBalancer) CreateWorkerServiceProvider(tx dax.Transaction, sp dax.WorkerServiceProvider) error {
	return nil
}
func (b *NopBalancer) CreateWorkerService(tx dax.Transaction, srv dax.WorkerService) error {
	return nil
}

func (b *NopBalancer) WorkerServices(tx dax.Transaction, wspID dax.WorkerServiceProviderID) (dax.WorkerServices, error) {
	return nil, nil
}

func (b *NopBalancer) WorkerServiceProviders(tx dax.Transaction /*, future optional filters */) (dax.WorkerServiceProviders, error) {
	return nil, nil
}

func (b *NopBalancer) AssignFreeServiceToDatabase(tx dax.Transaction, wspID dax.WorkerServiceProviderID, qdb *dax.QualifiedDatabase) (*dax.WorkerService, error) {
	return nil, nil
}
