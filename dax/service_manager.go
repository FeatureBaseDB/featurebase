package dax

import (
	"fmt"
	"net/http"
	"sync"

	"github.com/gorilla/mux"
	"github.com/molecula/featurebase/v3/errors"
	"github.com/molecula/featurebase/v3/logger"
)

// ServiceKey is a unique key used to identify one service managed by the
// ServiceManager. These typically align with the ServicePrefix* values.
type ServiceKey string

// ServiceManager manages the various services running in process. It is used to
// do things like start/stop services, and it dynamically builds the http router
// depending on the state of all services.
type ServiceManager struct {
	mu sync.RWMutex

	// MDS
	MDS        MDSService
	mdsStarted bool

	// Queryer
	Queryer        QueryerService
	queryerStarted bool

	// Computers
	computerID int
	computers  map[ServiceKey]*computerServiceState

	drouter *dynamicRouter

	Logger logger.Logger
}

type computerServiceState struct {
	service ComputerService
	started bool
}

// NewServiceManager returns a new ServiceManager with default values.
func NewServiceManager() *ServiceManager {
	return &ServiceManager{
		computers: map[ServiceKey]*computerServiceState{},
		drouter:   &dynamicRouter{},
		Logger:    logger.NopLogger,
	}
}

// HTTPHandler returns the current http.Handler for ServiceManager based on the
// state of its services.
func (s *ServiceManager) HTTPHandler() http.Handler {
	s.mu.RLock()
	defer s.mu.RUnlock()
	s.resetRouter()
	return s.drouter
}

// StartAll starts all services which have been added to ServiceManager.
func (s *ServiceManager) StartAll() error {
	// MDS
	if err := s.MDSStart(); err != nil {
		return errors.Wrap(err, "starting mds")
	}

	// Queryer
	if err := s.QueryerStart(); err != nil {
		return errors.Wrap(err, "starting queryer")
	}

	// Computer(s)
	for key := range s.computers {
		if err := s.ComputerStart(key); err != nil {
			return errors.Wrapf(err, "starting computer (%s)", key)
		}
	}

	return nil
}

func (s *ServiceManager) StopAll() error {
	for key := range s.computers {
		if err := s.ComputerStop(key); err != nil {
			s.Logger.Printf("stopping computer %s: %v", key, err)
		}
	}
	if err := s.QueryerStop(); err != nil {
		s.Logger.Printf("stopping queryer: %v", err)
	}
	return s.MDSStop()
}

// MDSStart starts the MDS service.
func (s *ServiceManager) MDSStart() error {
	if s.MDS == nil {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.mdsStarted {
		return nil
	}

	s.mdsStarted = true
	s.resetRouter()

	if err := s.MDS.Start(); err != nil {
		s.mdsStarted = false
		return errors.Wrap(err, "starting mds")
	}

	return nil
}

// MDSStop stops the MDS service.
func (s *ServiceManager) MDSStop() error {
	if s.MDS == nil {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.mdsStarted {
		return nil
	}

	s.mdsStarted = false
	s.resetRouter()

	if err := s.MDS.Stop(); err != nil {
		s.mdsStarted = true
		return errors.Wrap(err, "stopping controller")
	}

	return nil
}

// QueryerStart starts the Queryer service.
func (s *ServiceManager) QueryerStart() error {
	if s.Queryer == nil {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.queryerStarted {
		return nil
	}

	s.queryerStarted = true
	s.resetRouter()

	if err := s.Queryer.Start(); err != nil {
		s.queryerStarted = false
		return errors.Wrap(err, "starting queryer")
	}

	return nil
}

// QueryerStop stops the Queryer service.
func (s *ServiceManager) QueryerStop() error {
	if s.Queryer == nil {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.queryerStarted {
		return nil
	}

	s.queryerStarted = false
	s.resetRouter()

	if err := s.Queryer.Stop(); err != nil {
		s.queryerStarted = true
		return errors.Wrap(err, "stopping queryer")
	}

	return nil
}

// Computer returns the ComputerService specified by the provided key.
func (s *ServiceManager) Computer(key ServiceKey) ComputerService {
	serviceState, ok := s.computers[key]
	if !ok {
		return nil
	}
	return serviceState.service
}

// ComputerStart starts the Computer service specified by the provided key.
func (s *ServiceManager) ComputerStart(key ServiceKey) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	serviceState, ok := s.computers[key]
	if !ok {
		return errors.Errorf("computer to be started does not exist: %s", key)
	}

	if serviceState.started {
		return nil
	}

	serviceState.started = true

	if err := serviceState.service.Start(); err != nil {
		serviceState.started = false
		return errors.Wrapf(err, "starting computer (%s)", key)
	}

	// resetRouter is called *after* service.Start() for computer (but not other
	// service types) because currently, the handler returned by
	// server.Command.HTTPHandler() doesn't get initialized until startup. A
	// task for the future will be to tease out the computer http routes so that
	// they're available prior to startup.
	s.resetRouter()

	return nil
}

// ComputerStop stops the Computer service specified by the provided key.
func (s *ServiceManager) ComputerStop(key ServiceKey) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	serviceState, ok := s.computers[key]
	if !ok {
		return errors.Errorf("computer to be stopped does not exist: %s", key)
	}

	if !serviceState.started {
		return nil
	}

	serviceState.started = false
	s.resetRouter()

	if err := serviceState.service.Stop(); err != nil {
		serviceState.started = true
		return errors.Wrapf(err, "stopping computer (%s)", key)
	}

	return nil
}

// Computers returns a map (keyed by ServiceKey) of all computers registered
// with ServiceManager.
func (s *ServiceManager) Computers() map[ServiceKey]ComputerService {
	s.mu.RLock()
	defer s.mu.RUnlock()
	m := make(map[ServiceKey]ComputerService)

	for k, v := range s.computers {
		m[k] = v.service
	}

	return m
}

// AddComputer adds the provided ComputerService to ServiceManager. It assigns
// the service a unique ServiceKey.
func (s *ServiceManager) AddComputer(cs ComputerService) ServiceKey {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := ServiceKey(fmt.Sprintf("%s%d", ServicePrefixComputer, s.computerID))
	s.computers[key] = &computerServiceState{
		service: cs,
	}
	cs.SetKey(key)

	s.computerID++

	return key
}

// RemoveComputer removes the ComputerService specified by the provided key.
func (s *ServiceManager) RemoveComputer(key ServiceKey) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.computers[key]; ok {
		delete(s.computers, key)
		s.resetRouter()
		return true
	}

	return false
}

func getHealth(w http.ResponseWriter, req *http.Request) {
	w.WriteHeader(http.StatusOK)
}

// Must be called with at least a read lock held (because that's required of buildRouter).
func (s *ServiceManager) resetRouter() {
	s.drouter.Swap(s.buildRouter())
}

// Must be called with at least a read lock held?
func (s *ServiceManager) buildRouter() *mux.Router {
	router := mux.NewRouter()
	router.HandleFunc("/health", getHealth).Methods("GET").Name("GetHealth")

	// MDS.
	if s.MDS != nil && s.mdsStarted {
		pre := "/" + ServicePrefixMDS
		router.PathPrefix(pre + "/").Handler(
			http.StripPrefix(pre, s.MDS.HTTPHandler()))
	}

	// Computers.
	for k, serviceState := range s.computers {
		// Skip any computer which have not been started.
		if !serviceState.started {
			continue
		}

		pre := "/" + string(k)
		router.PathPrefix(pre + "/").Handler(
			http.StripPrefix(pre, serviceState.service.HTTPHandler()))
	}

	// Queryer.
	if s.Queryer != nil {
		pre := "/" + ServicePrefixQueryer
		router.PathPrefix(pre + "/").Handler(
			http.StripPrefix(pre, s.Queryer.HTTPHandler()))
	}

	return router
}

//////////////////////////////////////////

// Service is an interface implemented by any service which is part of
// ServiceManager.
type Service interface {
	Start() error
	Stop() error
	Address() Address
	HTTPHandler() http.Handler
}

// MultiService is a service type which can have multiple instances within
// ServicesManager.
type MultiService interface {
	Service

	SetKey(ServiceKey)
	Key() ServiceKey
}

type MDSService interface {
	Service
}

type ComputerService interface {
	MultiService

	SetMDS(Address) error
}

type QueryerService interface {
	Service

	SetMDS(Address) error
}

//////////////////////////////////////////

// dynamicRouter is used to dynamically swap out http routers as service states
// withing ServiceManager change.
type dynamicRouter struct {
	mu     sync.RWMutex
	router *mux.Router
}

func (dr *dynamicRouter) Swap(new *mux.Router) {
	dr.mu.Lock()
	defer dr.mu.Unlock()

	dr.router = new
}

func (dr *dynamicRouter) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	dr.mu.RLock()
	router := dr.router
	dr.mu.RUnlock()

	router.ServeHTTP(w, r)
}
