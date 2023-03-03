package service

import (
	"net/http"
	"os"

	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/dax/boltdb"
	"github.com/featurebasedb/featurebase/v3/dax/controller"
	balancerboltdb "github.com/featurebasedb/featurebase/v3/dax/controller/balancer/boltdb"
	controllerhttp "github.com/featurebasedb/featurebase/v3/dax/controller/http"
	schemarboltdb "github.com/featurebasedb/featurebase/v3/dax/controller/schemar/boltdb"
	"github.com/featurebasedb/featurebase/v3/dax/controller/sqldb"
	"github.com/featurebasedb/featurebase/v3/errors"
	"github.com/featurebasedb/featurebase/v3/logger"
	fbnet "github.com/featurebasedb/featurebase/v3/net"
	"github.com/gobuffalo/pop/v6"
)

// Ensure type implements interface.
var _ dax.Service = (*controllerService)(nil)

type controllerService struct {
	uri        *fbnet.URI
	controller *controller.Controller

	// Because we stopped using a storage method interface, and always use bolt,
	// we need to be sure to close the boltDBs that are created in controller.New()
	// whenever controller.Stop() is called. These are pointers to that DB so we can
	// close it.
	boltDB *boltdb.DB

	logger logger.Logger
}

func New(uri *fbnet.URI, cfg controller.Config) *controllerService {
	// Set up logger.
	var logr logger.Logger = logger.StderrLogger
	if cfg.Logger != nil {
		logr = cfg.Logger.WithPrefix("Controller: ")
	}

	if cfg.DataDir == "" {
		dir, err := os.MkdirTemp("", "controller_*")
		if err != nil {
			logr.Printf("Making temp dir for Controller storage: %v", err)
			os.Exit(1)
		}
		cfg.DataDir = dir
		logr.Warnf("no DataDir given (like '/path/to/directory'); using temp dir at '%s'", cfg.DataDir)
	}

	controller := controller.New(cfg)
	controllerSvc := &controllerService{
		uri:        uri,
		controller: controller,
		logger:     logr,
	}

	// Storage methods.
	switch cfg.StorageMethod {
	case "boltdb":
		buckets := append(schemarboltdb.SchemarBuckets, balancerboltdb.BalancerBuckets...)
		controllerDB, err := boltdb.NewSvcBolt(cfg.DataDir, "controller", buckets...)
		if err != nil {
			logr.Printf(errors.Wrap(err, "creating controller bolt").Error())
			os.Exit(1)
		}
		// Directive version.
		if err := controllerDB.InitializeBuckets(boltdb.DirectiveBuckets...); err != nil {
			logr.Panicf("initializing directive buckets: %v", err)
		}
		controller.Schemar = schemarboltdb.NewSchemar(controllerDB, logr)
		controller.Balancer = balancerboltdb.NewBalancer(controllerDB, controller.Schemar, logr)
		directiveVersion := boltdb.NewDirectiveVersion(controllerDB)
		controller.DirectiveVersion = directiveVersion

		controller.Transactor = controllerDB

		controllerSvc.boltDB = controllerDB

		// transactor = new transactor
	case "sqldb":
		controller.Schemar = sqldb.NewSchemar(logr)
		controller.Balancer = sqldb.NewBalancer(logr)
		controller.DirectiveVersion = sqldb.NewDirectiveVersion(logr)
		conn, err := pop.Connect("development")
		if err != nil {
			logr.Printf("Connecting to development database: %v", err)
			os.Exit(1)
		}
		controller.Transactor = sqldb.Transactor{Connection: conn}
	default:
		logr.Printf("storagemethod %s not supported, try 'boltdb' or 'sqldb'", cfg.StorageMethod)
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

	if m.boltDB != nil {
		m.boltDB.Close()
	}

	return err
}

func (m *controllerService) Address() dax.Address {
	return dax.Address(m.uri.HostPort() + "/" + dax.ServicePrefixController)
}

func (m *controllerService) HTTPHandler() http.Handler {
	return controllerhttp.Handler(m.controller)
}
