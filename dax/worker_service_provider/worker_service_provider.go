package worker_service_provider

import (
	"context"
	"fmt"
	"sync"

	"github.com/featurebasedb/featurebase/v3/dax"
	computersvc "github.com/featurebasedb/featurebase/v3/dax/computer/service"
	controllerclient "github.com/featurebasedb/featurebase/v3/dax/controller/client"
	"github.com/featurebasedb/featurebase/v3/errors"
	"github.com/featurebasedb/featurebase/v3/logger"
	fbnet "github.com/featurebasedb/featurebase/v3/net"
)

func NewConfig() Config {
	return Config{
		ID:     "default",
		Roles:  []string{"compute", "translate"},
		Logger: logger.StderrLogger,
	}
}

type Config struct {
	ID                string        `toml:"id"`
	Address           *fbnet.URI    `toml:"-"`
	ControllerAddress string        `toml:"controller-address"`
	Roles             []string      `toml:"roles"`
	Logger            logger.Logger `toml:"-"`
}

func New(svcmgr *dax.ServiceManager, cfg Config) *WSP {
	if cfg.Logger == nil {
		cfg.Logger = logger.StderrLogger
	}
	if cfg.Address == nil {
		panic("can't new up a WSP without an Address")
	}
	if cfg.ID == "" {
		panic("can't new up a WSP without and ID")
	}
	if len(cfg.Roles) == 0 {
		panic("can't new up a WSP without roles")
	}
	if cfg.ControllerAddress == "" {
		panic("can't new up WSP without specifying a controller address")
	}
	roles, err := dax.RoleTypesFromStrings(cfg.Roles)
	if err != nil {
		panic(errors.Wrap(err, "validating roles"))
	}

	return &WSP{
		id:         dax.WorkerServiceProviderID(cfg.ID),
		cfg:        &cfg,
		roles:      roles,
		controller: controllerclient.New(dax.Address(cfg.ControllerAddress), cfg.Logger),
		svcmgr:     svcmgr,
		services:   make(map[dax.WorkerServiceID]*workerService),
		logger:     cfg.Logger,
	}
}

type WSP struct {
	cfg            *Config
	computerConfig computersvc.CommandConfig

	id         dax.WorkerServiceProviderID
	controller dax.Controller

	svcmgr *dax.ServiceManager

	roles dax.RoleTypes

	mu sync.Mutex
	// svcNum is a monotonically increasing integer used to assign a
	// unique service ID to each service... it is not the number of
	// services which can be gotten by len(wsp.services).
	svcNum int
	// workerNum is a monotonically increasing integer used by service
	// manager to assign a unique key to each worker. It is *not* the
	// total number of active workers.
	workerNum int
	services  map[dax.WorkerServiceID]*workerService

	logger logger.Logger
}

func (w *WSP) Start() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// register with controller
	svcs, err := w.controller.RegisterWorkerServiceProvider(context.Background(), dax.WorkerServiceProvider{
		ID:          w.id,
		Roles:       w.roles,
		Address:     dax.Address(w.cfg.Address.Normalize()),
		Description: "",
	})
	if err != nil {
		return errors.Wrap(err, "registering")
	}
	if len(svcs) > 0 {
		return errors.Errorf("wasn't expecting svcs, but got %d", len(svcs))
	}

	// start a service and add it to services
	if err := w.addService(); err != nil {
		return errors.Wrap(err, "adding initial worker service")
	}

	return nil
}

// addService creates a new empty Service and starts a worker.
func (w *WSP) addService() error {
	cfg := w.computerConfig
	cfg.WorkerServiceID = dax.WorkerServiceID(fmt.Sprintf("ServiceID-%d", w.svcNum))

	// create service
	workerSvc := &workerService{
		WorkerService: dax.WorkerService{
			ID:                      cfg.WorkerServiceID,
			Roles:                   w.roles,
			WorkerServiceProviderID: w.id,
			DatabaseID:              "",
			WorkersMin:              1,
			WorkersMax:              1,
		},
		keys: make([]string, 1),
	}
	w.services[workerSvc.ID] = workerSvc
	// register service

	if err := w.controller.RegisterWorkerService(context.Background(), workerSvc.WorkerService); err != nil {
		return errors.Wrap(err, "registering worker service")
	}

	// create worker in service
	key := w.svcmgr.AddComputer(
		computersvc.New(dax.Address(w.cfg.Address.HostPort()), cfg, w.logger), w.workerNum)

	// track workers on service
	workerSvc.keys[0] = string(key)

	w.svcNum++
	w.workerNum++

	return nil
}

func (w *WSP) ClaimService(ctx context.Context, svc dax.WorkerService) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// check that svc is in services, then update it
	mysvc, ok := w.services[svc.ID]
	if !ok {
		return errors.Errorf("can't claim/update service with ID %s because it doesn't exist", svc.ID)
	}
	if svc.DatabaseID == "" {
		return errors.Errorf("service must have a database ID in order to be claimed/updated: %+v", svc)
	}
	if mysvc.DatabaseID != "" && mysvc.DatabaseID != svc.DatabaseID {
		return errors.Errorf("can't claim svc ID %s, svc is not free and databases don't match", svc.ID)
	}
	isClaim := mysvc.DatabaseID == ""

	mysvc.WorkerService = svc
	w.services[svc.ID] = mysvc

	// claiming a service might update min/max workers, so we scale.
	err := w.scale(mysvc)
	if err != nil {
		return errors.Wrap(err, "scaling service")
	}

	// start a new service so we have a free one in reserve
	if isClaim {
		err := w.addService()
		if err != nil {
			return errors.Wrap(err, "adding service")
		}
	}

	return nil
}

// scale must be called with lock held
func (w *WSP) scale(mysvc *workerService) error {
	// scale up
	for mysvc.WorkersMin > len(mysvc.keys) {
		w.addWorker(mysvc)
	}
	// scale down
	for mysvc.WorkersMin < len(mysvc.keys) {
		err := w.removeWorker(mysvc)
		if err != nil {
			return errors.Wrap(err, "while scaling down")
		}
	}
	return nil
}

func (w *WSP) addWorker(mysvc *workerService) {
	// create worker in service
	cfg := w.computerConfig
	cfg.WorkerServiceID = dax.WorkerServiceID(fmt.Sprintf("ServiceID-%d", w.svcNum))
	key := w.svcmgr.AddComputer(
		computersvc.New(dax.Address(w.cfg.Address.HostPort()), cfg, w.logger), w.workerNum)

	// track workers on service
	mysvc.keys = append(mysvc.keys, string(key))

	w.workerNum++
}

func (w *WSP) removeWorker(mysvc *workerService) error {
	if len(mysvc.keys) == 0 {
		return errors.Errorf("asked to remove a worker from empty service: %+v", mysvc)
	}

	key := dax.ServiceKey(mysvc.keys[len(mysvc.keys)-1])

	if err := w.svcmgr.ComputerStop(key); err != nil {
		return errors.Wrapf(err, "stopping computer '%s'", key)
	}

	if ok := w.svcmgr.RemoveComputer(key); !ok {
		return errors.Errorf("computer not found by RemoveComputer after stopping??? key: %s", key)
	}

	mysvc.keys = mysvc.keys[:len(mysvc.keys)-1]

	return nil
}

func (w *WSP) UpdateService(ctx context.Context, svc dax.WorkerService) error {
	return w.ClaimService(ctx, svc)
}

func (w *WSP) DropService(ctx context.Context, svc dax.WorkerService) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// check that svc is in services
	mysvc, ok := w.services[svc.ID]
	if !ok {
		return errors.Errorf("can't drop service with ID %s because it doesn't exist", svc.ID)
	}

	// stop it
	mysvc.WorkersMin = 0
	mysvc.WorkersMax = 0
	err := w.scale(mysvc)
	if err != nil {
		return errors.Wrapf(err, "scaling down service %s to drop it", svc.ID)
	}

	// remove it
	delete(w.services, svc.ID)

	return nil
}

func (w *WSP) SetController(controller dax.Controller) error {
	w.controller = controller
	return nil
}

func (w *WSP) Logger() logger.Logger {
	return w.logger
}

type workerService struct {
	dax.WorkerService
	keys []string
}
