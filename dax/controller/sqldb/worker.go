package sqldb

import (
	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/dax/controller"
	"github.com/featurebasedb/featurebase/v3/dax/models"
	"github.com/featurebasedb/featurebase/v3/logger"
	"github.com/pkg/errors"
)

var _ controller.WorkerRegistry = (*workerRegistry)(nil)

func NewWorkerRegistry(log logger.Logger) *workerRegistry {
	if log == nil {
		log = logger.NopLogger
	}
	return &workerRegistry{
		log: log,
	}
}

type workerRegistry struct {
	log logger.Logger
}

func (w *workerRegistry) AddWorker(tx dax.Transaction, node *dax.Node) error {
	dt, ok := tx.(*DaxTransaction)
	if !ok {
		return dax.NewErrInvalidTransaction("*sqldb.DaxTransaction")
	}

	workers := models.Workers{}

	// Determine if a worker for this address already exists. We use `All()`
	// here instead of `First()` because `First()` returns an error if there's
	// no match.
	if err := dt.C.Where("address = ?", node.Address).All(&workers); err != nil {
		return errors.Wrapf(err, "getting workers by address: %s", node.Address)
	}

	switch len(workers) {
	case 0:
		// Continue on to create.
	case 1:
		// Since a worker for this address already exists, just update it and
		// return.
		worker := workers[0]
		for _, roleType := range node.RoleTypes {
			if err := worker.SetRole(roleType); err != nil {
				return errors.Wrapf(err, "setting role: %s", roleType)
			}
		}
		return dt.C.Update(worker)
	default:
		return errors.Errorf("found more than one worker for address: %s", node.Address)
	}

	worker := &models.Worker{
		Address: node.Address,
	}
	for _, roleType := range node.RoleTypes {
		if err := worker.SetRole(roleType); err != nil {
			return errors.Wrapf(err, "setting role: %s", roleType)
		}
	}

	return dt.C.Create(worker)
}

func (w *workerRegistry) Worker(tx dax.Transaction, addr dax.Address) (*dax.Node, error) {
	dt, ok := tx.(*DaxTransaction)
	if !ok {
		return nil, dax.NewErrInvalidTransaction("*sqldb.DaxTransaction")
	}

	worker := &models.Worker{}
	err := dt.C.Eager().Where("address = ?", addr).First(worker)
	if err != nil {
		return nil, errors.Wrapf(err, "getting worker: %s", addr)
	}

	return &dax.Node{
		Address:   worker.Address,
		RoleTypes: workerRoleTypes(worker),
	}, nil
}

func (w *workerRegistry) RemoveWorker(tx dax.Transaction, addr dax.Address) error {
	dt, ok := tx.(*DaxTransaction)
	if !ok {
		return dax.NewErrInvalidTransaction("*sqldb.DaxTransaction")
	}

	worker := &models.Worker{}
	err := dt.C.Eager().Where("address = ?", addr).First(worker)
	if isNoRowsError(err) {
		return nil
	} else if err != nil {
		return errors.Wrapf(err, "finding worker: %s", addr)
	}

	err = dt.C.Destroy(worker)
	return errors.Wrap(err, "destroying worker")
}

func (w *workerRegistry) Workers(tx dax.Transaction) ([]*dax.Node, error) {
	dt, ok := tx.(*DaxTransaction)
	if !ok {
		return nil, dax.NewErrInvalidTransaction("*sqldb.DaxTransaction")
	}

	workers := []*models.Worker{}
	dt.C.Eager().Order("address asc").All(&workers)

	ret := make([]*dax.Node, len(workers))
	for i, worker := range workers {
		ret[i] = &dax.Node{
			Address:   worker.Address,
			RoleTypes: workerRoleTypes(worker),
		}
	}

	return ret, nil
}

func workerRoleTypes(worker *models.Worker) []dax.RoleType {
	roleTypes := make([]dax.RoleType, 0)
	if worker.RoleCompute {
		roleTypes = append(roleTypes, dax.RoleTypeCompute)
	}
	if worker.RoleTranslate {
		roleTypes = append(roleTypes, dax.RoleTypeTranslate)
	}
	if worker.RoleQuery {
		roleTypes = append(roleTypes, dax.RoleTypeQuery)
	}

	return roleTypes
}
