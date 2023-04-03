package dax

import "context"

type Controller interface {
	Noder
	Schemar

	// RegisterWorkerServiceProvider makes the controller aware of a
	// new WorkerServiceProvider.
	RegisterWorkerServiceProvider(ctx context.Context, sp WorkerServiceProvider) (WorkerServices, error)

	// RegisterWorkerService makes the controller aware of a new
	// WorkerService, so that when workers of that service register
	// themselves, the controller will have an entity to associate
	// them with, which will ultimately correspond to what Database
	// those workers get jobs for.
	RegisterWorkerService(ctx context.Context, srv WorkerService) error
}

// Ensure type implements interface.
var _ Noder = &nopController{}
var _ Schemar = &nopController{}

// nopController is a no-op implementation of the Controller interface.
type nopController struct {
	nopNoder
	NopSchemar
}

func NewNopController() *nopController {
	return &nopController{}
}

func (n *nopController) RegisterWorkerServiceProvider(ctx context.Context, sp WorkerServiceProvider) (WorkerServices, error) {
	return nil, nil
}

func (n *nopController) RegisterWorkerService(ctx context.Context, srv WorkerService) error {
	return nil
}
