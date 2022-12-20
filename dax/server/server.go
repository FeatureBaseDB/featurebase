// Copyright 2021 Molecula Corp. All rights reserved.
//
// Package server contains the `pilosa server` subcommand which runs Pilosa
// itself. The purpose of this package is to define an easily tested Command
// object which handles interpreting configuration and setting up all the
// objects that Pilosa needs.

package server

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"io"
	"log"
	"math/rand"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"

	featurebase "github.com/molecula/featurebase/v3"
	"github.com/molecula/featurebase/v3/dax"
	computersvc "github.com/molecula/featurebase/v3/dax/computer/service"
	daxhttp "github.com/molecula/featurebase/v3/dax/http"
	"github.com/molecula/featurebase/v3/dax/mds"
	controllerhttp "github.com/molecula/featurebase/v3/dax/mds/controller/http"
	mdssvc "github.com/molecula/featurebase/v3/dax/mds/service"
	"github.com/molecula/featurebase/v3/dax/queryer"
	queryersvc "github.com/molecula/featurebase/v3/dax/queryer/service"
	"github.com/molecula/featurebase/v3/errors"
	"github.com/molecula/featurebase/v3/logger"
	fbnet "github.com/molecula/featurebase/v3/net"
)

// Command represents the state of the dax server command.
type Command struct {
	// Server *pilosa.Server

	// Configuration.
	Config *Config

	Handler featurebase.HandlerI

	// done will be closed when Command.Close() is called
	done chan struct{}

	// Standard input/output
	stderr io.Writer

	ln           net.Listener
	listenURI    *fbnet.URI
	advertiseURI *fbnet.URI
	tlsConfig    *tls.Config

	logger    logger.Logger
	logOutput io.Writer

	svcmgr *dax.ServiceManager
}

type CommandOption func(c *Command) error

func OptCommandConfig(config *Config) CommandOption {
	return func(c *Command) error {
		defer c.Config.MustValidate()
		c.Config = config
		return nil
	}
}

// OptCommandServiceManager allows the ability to pass in a ServiceManage that
// has been initialized outside of the Command. This is useful for testing where
// we want to controll the service manager during a test run.
func OptCommandServiceManager(svcmgr *dax.ServiceManager) CommandOption {
	return func(c *Command) error {
		c.svcmgr = svcmgr
		return nil
	}
}

// NewCommand returns a new instance of Command.
func NewCommand(stderr io.Writer, opts ...CommandOption) *Command {
	c := &Command{
		Config: NewConfig(),

		stderr: stderr,

		done: make(chan struct{}),

		svcmgr: dax.NewServiceManager(),
	}

	for _, opt := range opts {
		err := opt(c)
		if err != nil {
			panic(err)
			// TODO: Return error instead of panic?
		}
	}

	return c
}

// Start starts the DAX server.
func (m *Command) Start() (err error) {
	// Seed random number generator
	seed := m.Config.Seed
	if seed == 0 {
		seed = time.Now().UTC().UnixNano()
	}
	log.Printf("Random seed: %d", seed)
	rand.Seed(seed)

	if err := m.setupServer(); err != nil {
		return errors.Wrap(err, "setting up server")
	}

	// Serve HTTP.
	go func() {
		if err := m.Handler.Serve(); err != nil {
			m.logger.Errorf("handler serve error (dax): %v", err)
		}
	}()
	m.logger.Printf("listening as %s\n", m.listenURI)

	if err := m.setupServices(); err != nil {
		return errors.Wrap(err, "setting up services")
	}

	if err := m.svcmgr.StartAll(); err != nil {
		return errors.Wrap(err, "starting all services")
	}

	return nil
}

// Wait waits for the server to be closed or interrupted.
func (m *Command) Wait() error {
	// First SIGKILL causes server to shut down gracefully.
	c := make(chan os.Signal, 2)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	select {
	case sig := <-c:
		m.logger.Infof("received signal '%s', gracefully shutting down...\n", sig.String())

		// Second signal causes a hard shutdown.
		go func() { <-c; os.Exit(1) }()
		return errors.Wrap(m.Close(), "closing command")
	case <-m.done:
		m.logger.Infof("server closed externally")
		return nil
	}
}

// Close shuts down the server.
func (m *Command) Close() error {
	select {
	case <-m.done:
		return nil
	default:
		eg := errgroup.Group{}
		eg.Go(m.svcmgr.StopAll)
		err := eg.Wait()
		//_ = testhook.Closed(pilosa.NewAuditor(), m, nil)
		close(m.done)

		return errors.Wrap(err, "closing everything")
	}
}

// URI returns the advertise URI at which the command can be reached.
func (m *Command) URI() *fbnet.URI {
	return m.advertiseURI
}

// Address returns the advertise address at which the command can be reached.
func (m *Command) Address() dax.Address {
	return dax.Address(m.advertiseURI.Normalize())
}

// // ParseConfig parses s into a Config.
// func ParseConfig(s string) (Config, error) {
// 	var c Config
// 	err := toml.Unmarshal([]byte(s), &c)
// 	return c, err
// }

// // expandDirName was copied from pilosa/server.go.
// // TODO: consider centralizing this if we need this across packages.
// func expandDirName(path string) (string, error) {
// 	prefix := "~" + string(filepath.Separator)
// 	if strings.HasPrefix(path, prefix) {
// 		HomeDir := os.Getenv("HOME")
// 		if HomeDir == "" {
// 			return "", errors.New("data directory not specified and no home dir available")
// 		}
// 		return filepath.Join(HomeDir, strings.TrimPrefix(path, prefix)), nil
// 	}
// 	return path, nil
// }

// setupServer uses the configuration to set up this server.
func (m *Command) setupServer() error {
	// Set up logger.
	if err := m.setupLogger(); err != nil {
		return errors.Wrap(err, "setting up logger")
	}

	if m.svcmgr != nil {
		m.svcmgr.Logger = m.logger
	}

	conf, err := json.MarshalIndent(m.Config, "", "\t")
	if err != nil {
		return errors.Wrap(err, "marshalling config")
	}
	m.logger.Printf("Config: %s", conf)

	// validateAddrs sets the appropriate values for Bind and Advertise
	// based on the inputs. It is not responsible for applying defaults, although
	// it does provide a non-zero port (10101) in the case where no port is specified.
	// The alternative would be to use port 0, which would choose a random port, but
	// currently that's not what we want.
	if err := m.Config.validateAddrs(context.Background()); err != nil {
		return errors.Wrap(err, "validating addresses")
	}

	uri, err := featurebase.AddressWithDefaults(m.Config.Bind)
	if err != nil {
		return errors.Wrap(err, "processing bind address")
	}

	m.ln, err = getListener(*uri, m.tlsConfig)
	if err != nil {
		return errors.Wrap(err, "getting listener")
	}

	// If port is 0, get auto-allocated port from listener
	if uri.Port == 0 {
		uri.SetPort(uint16(m.ln.Addr().(*net.TCPAddr).Port))
	}

	// Save listenURI for later reference.
	m.listenURI = uri

	//m.Config.FeatureBase.Config.Listener = ln

	// Get advertise address as uri.
	m.advertiseURI, err = featurebase.AddressWithDefaults(m.Config.Advertise)
	if err != nil {
		return errors.Wrap(err, "processing advertise address")
	}
	if m.advertiseURI.Port == 0 {
		m.advertiseURI.SetPort(uri.Port)
	}

	handlerOpts := []daxhttp.HandlerOption{
		daxhttp.OptHandlerBind(m.Config.Bind),
		daxhttp.OptHandlerListener(m.ln, m.advertiseURI.String()),
		daxhttp.OptHandlerLogger(m.logger),
	}

	drouter := m.svcmgr.HTTPHandler()

	// Set up Handler based on which services are running in process.
	m.Handler, err = daxhttp.NewHandler(drouter, handlerOpts...)
	if err != nil {
		return errors.Wrap(err, "new handler")
	}

	return nil
}

// setupServices uses the configuration to set up the configured services.
func (m *Command) setupServices() error {
	// Set up MDS.
	if m.Config.MDS.Run {
		mdsCfg := mds.Config{
			RegistrationBatchTimeout: m.Config.MDS.Config.RegistrationBatchTimeout,
			StorageMethod:            m.Config.MDS.Config.StorageMethod,
			DataDir:                  m.Config.MDS.Config.DataDir,
			SnappingTurtleTimeout:    m.Config.MDS.Config.SnappingTurtleTimeout,
			Logger:                   m.logger,
			Director: controllerhttp.NewDirector(
				controllerhttp.DirectorConfig{
					DirectivePath:       "directive",
					SnapshotRequestPath: "snapshot",
					Logger:              m.logger,
				}),
		}

		m.svcmgr.MDS = mdssvc.New(m.advertiseURI, mds.New(mdsCfg))
		if err := m.svcmgr.MDSStart(); err != nil {
			return errors.Wrap(err, "starting mds service")
		}
	}

	// Set up Queryer.
	if m.Config.Queryer.Run {
		qryrCfg := queryer.Config{
			Logger: m.logger,
		}

		m.svcmgr.Queryer = queryersvc.New(m.advertiseURI, queryer.New(qryrCfg), m.logger)

		var mdsAddr dax.Address
		if m.Config.Queryer.Config.MDSAddress != "" {
			mdsAddr = dax.Address(m.Config.Queryer.Config.MDSAddress)
		} else if m.svcmgr.MDS != nil {
			mdsAddr = m.svcmgr.MDS.Address()
		} else {
			return errors.Errorf("queryer requires MDS")
		}

		// Set MDS
		if err := m.svcmgr.Queryer.SetMDS(mdsAddr); err != nil {
			return errors.Wrap(err, "setting mds")
		}

		// Start queryer.
		if err := m.svcmgr.QueryerStart(); err != nil {
			return errors.Wrap(err, "starting queryer service")
		}
	}

	// rootDataDir holds the initial value in Config.DataDir. Because we change
	// that value for every computer instance, we need to know what it started
	// out as. A better solution might be to make a copy of Computer.Config on
	// every iteration and create the new Command based on the copy (which can
	// have a unique DataDir).
	rootDataDir := m.Config.Computer.Config.DataDir

	// Set up Computer.
	if m.Config.Computer.Run {
		n := m.Config.Computer.N
		if n == 0 {
			n = 1
		}

		for i := 0; i < n; i++ {
			m.logger.Printf("Set up computer (%d)", i)
			cfg := computersvc.CommandConfig{
				WriteLoggerRun:    m.Config.WriteLogger.Run,
				WriteLoggerConfig: m.Config.WriteLogger.Config,
				SnapshotterRun:    m.Config.Snapshotter.Run,
				SnapshotterConfig: m.Config.Snapshotter.Config,
				ComputerConfig:    m.Config.Computer.Config,

				Listener:    m.ln,
				RootDataDir: rootDataDir,

				Stderr: m.stderr,
				Logger: m.logger,
			}

			if cfg.ComputerConfig.MDSAddress == "" && m.svcmgr.MDS != nil {
				cfg.ComputerConfig.MDSAddress = string(m.svcmgr.MDS.Address())
			}

			// Add new computer service.
			_ = m.svcmgr.AddComputer(
				computersvc.New(dax.Address(m.advertiseURI.HostPort()), cfg, m.logger))
		}
	}

	return nil
}

// setupLogger sets up the logger based on the configuration.
func (m *Command) setupLogger() error {
	var f *logger.FileWriter
	var err error
	if m.Config.LogPath == "" {
		m.logOutput = os.Stderr
	} else {
		f, err = logger.NewFileWriter(m.Config.LogPath)
		if err != nil {
			return errors.Wrap(err, "opening file")
		}
		m.logOutput = f
	}
	if m.Config.Verbose {
		m.logger = logger.NewVerboseLogger(m.logOutput)
	} else {
		m.logger = logger.NewStandardLogger(m.logOutput)
	}
	if m.Config.LogPath != "" {
		sighup := make(chan os.Signal, 1)
		signal.Notify(sighup, syscall.SIGHUP)
		go func() {
			for {
				// duplicate stderr onto log file
				err := m.dup(int(f.Fd()), int(os.Stderr.Fd()))
				if err != nil {
					m.logger.Errorf("syscall dup: %s\n", err.Error())
				}

				// reopen log file on SIGHUP
				<-sighup
				err = f.Reopen()
				if err != nil {
					m.logger.Infof("reopen: %s\n", err.Error())
				}
			}
		}()
	}
	return nil
}

// getListener gets a net.Listener based on the config.
func getListener(uri fbnet.URI, tlsconf *tls.Config) (ln net.Listener, err error) {
	// If bind URI has the https scheme, enable TLS
	if uri.Scheme == "https" && tlsconf != nil {
		ln, err = tls.Listen("tcp", uri.HostPort(), tlsconf)
		if err != nil {
			return nil, errors.Wrap(err, "tls.Listener")
		}
	} else if uri.Scheme == "http" {
		// Open HTTP listener to determine port (if specified as :0).
		ln, err = net.Listen("tcp", uri.HostPort())
		if err != nil {
			return nil, errors.Wrap(err, "net.Listen")
		}
	} else {
		return nil, errors.Errorf("unsupported scheme: %s", uri.Scheme)
	}

	return ln, nil
}
