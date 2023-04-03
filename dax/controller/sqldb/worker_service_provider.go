package sqldb

import (
	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/dax/models"
	"github.com/featurebasedb/featurebase/v3/errors"
	"github.com/featurebasedb/featurebase/v3/logger"
	"github.com/gobuffalo/nulls"
)

func NewWorkerServiceProviderService(log logger.Logger) *workerServiceProviderService {
	if log == nil {
		log = logger.NopLogger
	}
	return &workerServiceProviderService{
		log: log,
	}
}

type workerServiceProviderService struct {
	log logger.Logger
}

func (w *workerServiceProviderService) CreateWorkerServiceProvider(tx dax.Transaction, sp dax.WorkerServiceProvider) error {
	dt, ok := tx.(*DaxTransaction)
	if !ok {
		return dax.NewErrInvalidTransaction("*sqldb.DaxTransaction")
	}

	mod := &models.WorkerServiceProvider{
		ID:            string(sp.ID),
		Address:       sp.Address,
		RoleCompute:   sp.Roles.Contains(dax.RoleTypeCompute),
		RoleTranslate: sp.Roles.Contains(dax.RoleTypeTranslate),
		Description:   sp.Description,
	}

	err := dt.C.Create(mod)
	return errors.Wrap(err, "inserting to DB")
}

func (w *workerServiceProviderService) CreateWorkerService(tx dax.Transaction, srv dax.WorkerService) error {
	dt, ok := tx.(*DaxTransaction)
	if !ok {
		return dax.NewErrInvalidTransaction("*sqldb.DaxTransaction")
	}

	var dbID nulls.String
	if srv.DatabaseID != "" {
		dbID = nulls.NewString(string(srv.DatabaseID))
	}

	mod := &models.WorkerService{
		ID:                      string(srv.ID),
		WorkerServiceProviderID: string(srv.WorkerServiceProviderID),
		DatabaseID:              dbID,
		RoleCompute:             srv.Roles.Contains(dax.RoleTypeCompute),
		RoleTranslate:           srv.Roles.Contains(dax.RoleTypeTranslate),
		WorkersMin:              srv.WorkersMin,
		WorkersMax:              srv.WorkersMax,
	}

	err := dt.C.Create(mod)
	return errors.Wrap(err, "inserting to DB")
}

func (w *workerServiceProviderService) WorkerServiceProviders(tx dax.Transaction /*, future optional filters */) (dax.WorkerServiceProviders, error) {
	dt, ok := tx.(*DaxTransaction)
	if !ok {
		return nil, dax.NewErrInvalidTransaction("*sqldb.DaxTransaction")
	}

	wsps := make([]models.WorkerServiceProvider, 0, 1)
	err := dt.C.All(&wsps)

	ret := make(dax.WorkerServiceProviders, len(wsps))
	for i, wsp := range wsps {
		ret[i] = toDaxWSP(wsp)
	}

	return ret, errors.Wrap(err, "querying for all worker service providers")
}

func toDaxWSP(wsp models.WorkerServiceProvider) dax.WorkerServiceProvider {
	roles := []dax.RoleType{}
	if wsp.RoleCompute {
		roles = append(roles, dax.RoleTypeCompute)
	}
	if wsp.RoleTranslate {
		roles = append(roles, dax.RoleTypeTranslate)
	}
	return dax.WorkerServiceProvider{
		ID:          dax.WorkerServiceProviderID(wsp.ID),
		Roles:       roles,
		Address:     wsp.Address,
		Description: wsp.Description,
	}
}

// AssignFreeServiceToDatabase finds a WorkerService with the given service provider ID and
func (w *workerServiceProviderService) AssignFreeServiceToDatabase(tx dax.Transaction, wspID dax.WorkerServiceProviderID, qdb *dax.QualifiedDatabase) (*dax.WorkerService, error) {
	dt, ok := tx.(*DaxTransaction)
	if !ok {
		return nil, dax.NewErrInvalidTransaction("*sqldb.DaxTransaction")
	}

	ws := models.WorkerService{}
	err := dt.C.Where("database_id = NULL and worker_service_provider_id = ?", wspID).First(&ws)
	if err != nil {
		return nil, errors.Wrap(err, "querying for free worker_service")
	}

	ws.DatabaseID = nulls.NewString(string(qdb.Database.ID))
	ws.WorkersMin = qdb.Database.Options.WorkersMin
	ws.WorkersMax = qdb.Database.Options.WorkersMax
	if err := dt.C.Update(ws); err != nil {
		return nil, errors.Wrap(err, "updating worker service")
	}

	daxWS := toDaxWorkerService(ws)
	return &daxWS, nil

}

func (w *workerServiceProviderService) WorkerServices(tx dax.Transaction, wspID dax.WorkerServiceProviderID) (dax.WorkerServices, error) {
	dt, ok := tx.(*DaxTransaction)
	if !ok {
		return nil, dax.NewErrInvalidTransaction("*sqldb.DaxTransaction")
	}

	query := dt.C.Q()
	workerServices := make([]models.WorkerService, 0)
	if wspID != "" {
		query = query.Where("worker_service_provider_id = ?", wspID)
	}

	if err := query.All(&workerServices); err != nil {
		return nil, errors.Wrapf(err, "getting Worker Services for ID '%s'", wspID)
	}

	return toDaxWorkerServices(workerServices), nil

}

func toDaxWorkerServices(wss []models.WorkerService) dax.WorkerServices {
	dws := make(dax.WorkerServices, len(wss))
	for i, wsm := range wss {
		dws[i] = toDaxWorkerService(wsm)
	}
	return dws
}

func toDaxWorkerService(ws models.WorkerService) dax.WorkerService {
	roles := []dax.RoleType{}
	if ws.RoleCompute {
		roles = append(roles, dax.RoleTypeCompute)
	}
	if ws.RoleTranslate {
		roles = append(roles, dax.RoleTypeTranslate)
	}
	dbID := ""
	if byts, _ := ws.DatabaseID.MarshalJSON(); string(byts) != "null" {
		dbID = string(byts)
	}
	return dax.WorkerService{
		ID:                      dax.WorkerServiceID(ws.ID),
		Roles:                   roles,
		WorkerServiceProviderID: dax.WorkerServiceProviderID(ws.WorkerServiceProviderID),
		DatabaseID:              dax.DatabaseID(dbID),
		WorkersMin:              ws.WorkersMin,
		WorkersMax:              ws.WorkersMax,
	}
}
