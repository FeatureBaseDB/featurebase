package controller

import (
	"github.com/featurebasedb/featurebase/v3/dax"
)

// Store wraps up (and will replace??) all the controller interfaces
// for accessing storage. Since all the storage is in one database
// under the hood and is interrelated, breaking it into separate
// "services" is confusing the heck outta me.
type Store interface {
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

	// WorkerServices returns all worker services which came from the
	// WorkerServiceProvider with the given ID. If that ID is empty,
	// then all WorkerServices are returned.
	WorkerServices(tx dax.Transaction, wspID dax.WorkerServiceProviderID) (dax.WorkerServices, error)

	WorkerService(tx dax.Transaction, dbid dax.DatabaseID) (dax.WorkerService, error)

	AddWorker(tx dax.Transaction, node *dax.Node) (*dax.DatabaseID, error)
	WorkerCount(tx dax.Transaction, role dax.RoleType, svcID dax.WorkerServiceID) (int, error)
	WorkerCountDatabase(tx dax.Transaction, role dax.RoleType, dbid dax.DatabaseID) (int, error)
	ListFreeJobs(tx dax.Transaction, role dax.RoleType, dbid dax.DatabaseID) (dax.Jobs, error)
	WorkersJobs(tx dax.Transaction, role dax.RoleType, svcID dax.WorkerServiceID) ([]dax.WorkerInfo, error)
	AssignWorkerToJobs(tx dax.Transaction, role dax.RoleType, dbid dax.DatabaseID, workerID string, jobs ...dax.Job) error
	ListWorkers(tx dax.Transaction, role dax.RoleType, svcID dax.WorkerServiceID) (dax.Addresses, error)

	AssignFreeServiceToDatabase(tx dax.Transaction, wspID dax.WorkerServiceProviderID, qdb *dax.QualifiedDatabase) (*dax.WorkerService, error)

	WorkerForAddress(tx dax.Transaction, addr dax.Address) (dax.Node, error)
	RemoveWorker(tx dax.Transaction, id dax.WorkerID) error

	CreateFreeJobs(tx dax.Transaction, role dax.RoleType, dbid dax.DatabaseID, jobs ...dax.Job) error
	DeleteJobsForTable(tx dax.Transaction, role dax.RoleType, qtid dax.QualifiedTableID) (InternalDiffs, error)
	WorkerJobs(tx dax.Transaction, role dax.RoleType, addr dax.Address) (dax.WorkerInfo, error)
}
