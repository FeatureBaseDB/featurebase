package sqldb

import (
	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/dax/controller/balancer"
	"github.com/featurebasedb/featurebase/v3/dax/models"
	"github.com/featurebasedb/featurebase/v3/logger"
	"github.com/pkg/errors"
)

func NewFreeWorkerService(log logger.Logger) balancer.FreeWorkerService {
	if log == nil {
		log = logger.NopLogger
	}
	return &freeWorkerService{
		log: log,
	}
}

type freeWorkerService struct {
	log logger.Logger
}

func (fw *freeWorkerService) AddWorkers(tx dax.Transaction, roleType dax.RoleType, addrs ...dax.Address) error {
	dt, ok := tx.(*DaxTransaction)
	if !ok {
		return dax.NewErrInvalidTransaction("*sqldb.DaxTransaction")
	}

	workers := make(models.Workers, len(addrs))
	for i, addr := range addrs {
		workers[i] = models.Worker{
			Address: addr,
			Role:    roleType,
		}
	}

	err := dt.C.Create(workers)
	return errors.Wrap(err, "creating workers")
}

func (fw *freeWorkerService) RemoveWorker(tx dax.Transaction, roleType dax.RoleType, addr dax.Address) error {
	dt, ok := tx.(*DaxTransaction)
	if !ok {
		return dax.NewErrInvalidTransaction("*sqldb.DaxTransaction")
	}

	err := dt.C.RawQuery("DELETE from workers where database_id is null and role = ? and address = ?", roleType, addr).Exec()
	return errors.Wrap(err, "deleting")
}

func (fw *freeWorkerService) PopWorkers(tx dax.Transaction, roleType dax.RoleType, num int) ([]dax.Address, error) {
	dt, ok := tx.(*DaxTransaction)
	if !ok {
		return nil, dax.NewErrInvalidTransaction("*sqldb.DaxTransaction")
	}

	results := make([]struct {
		Address dax.Address `db:"address"`
	}, 0, num)
	err := dt.C.RawQuery("select address from workers where role = ? and database_id is NULL limit ?", roleType, num).All(&results)
	if err != nil {
		return nil, errors.Wrap(err, "querying")
	}
	if len(results) < num {
		return nil, errors.Errorf("not enough free workers to get: wanted %d, have: %d", num, len(results))
	}

	ret := make([]dax.Address, num)
	for i, res := range results {
		ret[i] = res.Address
	}

	return ret, nil
}

func (fw *freeWorkerService) ListWorkers(tx dax.Transaction, roleType dax.RoleType) (dax.Addresses, error) {
	dt, ok := tx.(*DaxTransaction)
	if !ok {
		return nil, dax.NewErrInvalidTransaction("*sqldb.DaxTransaction")
	}

	workers := make(models.Workers, 0)
	err := dt.C.Select("address").Where("role = ? and database_id is NULL", roleType).All(&workers)
	if err != nil {
		return nil, errors.Wrap(err, "querying for workers")
	}

	ret := make(dax.Addresses, len(workers))
	for i, w := range workers {
		ret[i] = w.Address
	}

	return ret, nil

}
