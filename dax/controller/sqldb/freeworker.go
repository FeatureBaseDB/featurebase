package sqldb

import (
	"fmt"

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

func (fw *freeWorkerService) PopWorkers(tx dax.Transaction, roleType dax.RoleType, num int) ([]dax.Address, error) {
	dt, ok := tx.(*DaxTransaction)
	if !ok {
		return nil, dax.NewErrInvalidTransaction("*sqldb.DaxTransaction")
	}

	results := make([]struct {
		Address dax.Address `db:"address"`
	}, 0, num)
	sel := fmt.Sprintf("select address from workers where role_%s = true and database_id is NULL limit ?", roleType)
	err := dt.C.RawQuery(sel, num).All(&results)
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

func (fw *freeWorkerService) ListWorkers(tx dax.Transaction, qdbid dax.QualifiedDatabaseID, roleType dax.RoleType) (dax.Addresses, error) {
	dt, ok := tx.(*DaxTransaction)
	if !ok {
		return nil, dax.NewErrInvalidTransaction("*sqldb.DaxTransaction")
	}

	workers := make(models.Workers, 0)
	where := fmt.Sprintf("role_%s = true and database_id is NULL", roleType)
	err := dt.C.Select("address").Where(where).Order("address asc").All(&workers)
	if err != nil {
		return nil, errors.Wrap(err, "querying for free workers")
	}

	ret := make(dax.Addresses, len(workers))
	for i, w := range workers {
		ret[i] = w.Address
	}

	return ret, nil

}
