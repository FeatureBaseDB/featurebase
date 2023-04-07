package service

import (
	"net/http"
	"os"

	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/dax/controller"
	controllerhttp "github.com/featurebasedb/featurebase/v3/dax/controller/http"
	"github.com/featurebasedb/featurebase/v3/dax/controller/sqldb"
	wsphttp "github.com/featurebasedb/featurebase/v3/dax/worker_service_provider/http"
	"github.com/featurebasedb/featurebase/v3/errors"
	"github.com/featurebasedb/featurebase/v3/logger"
	fbnet "github.com/featurebasedb/featurebase/v3/net"
)

// Ensure type implements interface.
var _ dax.Service = (*controllerService)(nil)

type controllerService struct {
	uri        *fbnet.URI
	controller *controller.Controller

	logger logger.Logger
}

func New(uri *fbnet.URI, cfg controller.Config) *controllerService {
	// Set up logger.
	var logr logger.Logger = logger.StderrLogger
	if cfg.Logger != nil {
		logr = cfg.Logger.WithPrefix("Controller: ")
	}

	controller := controller.New(cfg)
	controller.WSPGetter = wsphttp.NewClient
	controllerSvc := &controllerService{
		uri:        uri,
		controller: controller,
		logger:     logr,
	}

	// Storage methods.
	switch cfg.StorageMethod {
	case "sqldb":
		controller.Schemar = sqldb.NewSchemar(logr)
		controller.Balancer = sqldb.NewBalancer(logr)
		controller.DirectiveVersion = sqldb.NewDirectiveVersion(logr)

		transactor, err := sqldb.NewTransactor(cfg.SQLDB, logr)
		if err != nil {
			logr.Printf("setting up new transactor: %v", err)
			os.Exit(1)
		}
		controller.Transactor = transactor
	default:
		logr.Printf("storagemethod %s not supported, only 'sqldb' is currently accepted.", cfg.StorageMethod)
		os.Exit(1)
	}

	if cfg.Director != nil {
		controller.Director = cfg.Director
	}

	return controllerSvc
}

func (m *controllerService) Start() error {
	// Start controller service.
	if err := m.controller.Start(); err != nil {
		return errors.Wrap(err, "starting controller")
	}
	return nil
}

func (m *controllerService) Stop() error {
	err := m.controller.Stop()
	if err != nil {
		m.logger.Warnf("error stopping controller: %v", err)
	}

	return err
}

func (m *controllerService) Address() dax.Address {
	return dax.Address(m.uri.HostPort() + "/" + dax.ServicePrefixController)
}

func (m *controllerService) HTTPHandler() http.Handler {
	return controllerhttp.Handler(m.controller)
}
