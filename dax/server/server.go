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
	"math/rand"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"

	featurebase "github.com/molecula/featurebase/v3"
	"github.com/molecula/featurebase/v3/dax"
	daxhttp "github.com/molecula/featurebase/v3/dax/http"
	"github.com/molecula/featurebase/v3/dax/mds"
	mdsclient "github.com/molecula/featurebase/v3/dax/mds/client"
	controlleralpha "github.com/molecula/featurebase/v3/dax/mds/controller/alpha"
	controllerhttp "github.com/molecula/featurebase/v3/dax/mds/controller/http"
	"github.com/molecula/featurebase/v3/dax/queryer"
	queryeralpha "github.com/molecula/featurebase/v3/dax/queryer/alpha"
	"github.com/molecula/featurebase/v3/dax/snapshotter"
	snapshotterclient "github.com/molecula/featurebase/v3/dax/snapshotter/client"
	"github.com/molecula/featurebase/v3/dax/writelogger"
	writeloggerclient "github.com/molecula/featurebase/v3/dax/writelogger/client"
	"github.com/molecula/featurebase/v3/errors"
	"github.com/molecula/featurebase/v3/logger"
	fbnet "github.com/molecula/featurebase/v3/net"
	featurebaseserver "github.com/molecula/featurebase/v3/server"
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
	*featurebase.CmdIO

	ln           net.Listener
	listenURI    *fbnet.URI
	advertiseURI *fbnet.URI
	tlsConfig    *tls.Config

	// registerFns is a list of functions to call once the service is up and
	// running. This is typically used to register the service with MDS.
	registerFns []registerFn

	// checkInFn is a function to call periodically in order to check-in with a
	// monitorinig service such as MDS.
	checkInFn checkInFn

	logger    logger.Logger
	logOutput io.Writer
}

type registerFn func() error
type checkInFn func() error

type CommandOption func(c *Command) error

func OptCommandConfig(config *Config) CommandOption {
	return func(c *Command) error {
		defer c.Config.MustValidate()
		if c.Config != nil {
			// c.Config.Etcd = config.Etcd
			// c.Config.Auth = config.Auth
			// c.Config.TLS = config.TLS
			// c.Config.Controller = config.Controller
			// c.Config.WriteLogger = config.WriteLogger
			return nil
		}
		c.Config = config
		return nil
	}
}

// NewCommand returns a new instance of Command.
func NewCommand(stdin io.Reader, stdout, stderr io.Writer, opts ...CommandOption) *Command {
	c := &Command{
		Config: NewConfig(),

		CmdIO: featurebase.NewCmdIO(stdin, stdout, stderr),

		registerFns: make([]registerFn, 0),

		done: make(chan struct{}),
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
	rand.Seed(time.Now().UTC().UnixNano())

	if err := m.setupServer(); err != nil {
		return errors.Wrap(err, "setting up server")
	}

	// // Initialize server.
	// if err = m.Server.Open(); err != nil {
	// 	return errors.Wrap(err, "opening server")
	// }

	// Serve HTTP.
	go func() {
		if err := m.Handler.Serve(); err != nil {
			m.logger.Errorf("handler serve error (dax): %v", err)
		}
	}()
	m.logger.Printf("listening as %s\n", m.listenURI)

	// Register the service(s) by calling any registerFn they have implemented.
	for i := range m.registerFns {
		if err := m.registerFns[i](); err != nil {
			return errors.Wrap(err, "calling register function")
		}
	}

	// Start the "check-in" background process which periodically checks in with
	// MDS.
	go m.checkIn()

	return nil
}

// checkIn calls the CheckIn function set on m.checkInFn every interval period.
// If the interval period is 0, the check-in is disabled.
func (m *Command) checkIn() {
	interval := m.Config.Computer.Config.CheckInInterval

	if interval == 0 || m.checkInFn == nil {
		return
	}

	for {
		select {
		case <-m.done:
			return
		case <-time.After(interval):
			m.logger.Debugf("node check-in in last %s, address: %s", interval, m.Config.Advertise)
			if err := m.checkInFn(); err != nil {
				m.logger.Errorf("checking in node: %s, %v", m.Config.Advertise, err)
			}
		}
	}
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
		//eg.Go(m.Server.Close)

		err := eg.Wait()
		//_ = testhook.Closed(pilosa.NewAuditor(), m, nil)
		close(m.done)

		return errors.Wrap(err, "closing everything")
	}
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

	// Set up WriteLogger.
	var wlSvc *writelogger.WriteLogger
	if m.Config.WriteLogger.Run {
		wlSvc = writelogger.New(writelogger.Config{
			DataDir: m.Config.WriteLogger.Config.DataDir,
			Logger:  m.logger,
		})
		handlerOpts = append(handlerOpts, daxhttp.OptHandlerWriteLogger(wlSvc))
	}

	// Set up Snapshotter.
	var ssSvc *snapshotter.Snapshotter
	if m.Config.Snapshotter.Run {
		ssSvc = snapshotter.New(snapshotter.Config{
			DataDir: m.Config.Snapshotter.Config.DataDir,
			Logger:  m.logger,
		})
		handlerOpts = append(handlerOpts, daxhttp.OptHandlerSnapshotter(ssSvc))
	}

	// Set up MDS.
	var mdsSvc *mds.MDS

	// alphaDirector is used in the case where both the `mds` and `computer`
	// services are running in the same process. It maintains the mapping
	// between computer address and its API.
	alphaDirector := controlleralpha.NewDirector()

	alphaRouter := queryeralpha.NewRouter()

	if m.Config.MDS.Run {
		mdsSvcCfg := mds.Config{
			RegistrationBatchTimeout: m.Config.MDS.Config.RegistrationBatchTimeout,
			StorageMethod:            m.Config.StorageMethod,
			StorageDSN:               m.Config.StorageDSN,
			Logger:                   m.logger,
		}

		// If the computer service is being run locally (in process) with MDS,
		// then we want to use an implementation of the controller.Director
		// interface which calls the interface methods *directly* on the compute
		// node service (as opposed to going over http).
		if m.Config.Computer.Run {
			mdsSvcCfg.Director = alphaDirector
		} else {
			mdsSvcCfg.Director = controllerhttp.NewDirector(
				controllerhttp.DirectorConfig{
					DirectivePath:       dax.ServicePrefixComputer + "/directive",
					SnapshotRequestPath: dax.ServicePrefixComputer + "/snapshot",
					Logger:              m.logger,
				})
		}

		mdsSvc = mds.New(mdsSvcCfg)
		handlerOpts = append(handlerOpts, daxhttp.OptHandlerMDS(mdsSvc))

		// Start mds services.
		if err := mdsSvc.Run(); err != nil {
			return errors.Wrap(err, "running mds")
		}
	}

	// Set up Queryer.
	if m.Config.Queryer.Run {
		qryrSvcCfg := queryer.Config{
			Logger: m.logger,
		}

		var qryrSvcMDS queryer.MDS
		var mdsRunning bool

		// This intentionally gives precedence to an MDSAddress over an MDS
		// sub-service running in the same process.
		if m.Config.Queryer.Config.MDSAddress != "" {
			qryrSvcMDS = mdsclient.New(dax.Address(m.Config.Queryer.Config.MDSAddress))
		} else if m.Config.MDS.Run {
			qryrSvcMDS = mdsSvc
			mdsRunning = true
		} else {
			return errors.Errorf("queryer can't run without MDS")
		}
		qryrSvcCfg.MDS = qryrSvcMDS

		// If the computer service is being run locally (in process) with MDS,
		// then we want to use an importer which bypasses http requests and
		// instead calls the respective services directly.
		if mdsRunning && m.Config.Computer.Run {
			qryrSvcCfg.Router = alphaRouter
		}

		qryrSvc := queryer.New(qryrSvcCfg)
		handlerOpts = append(handlerOpts, daxhttp.OptHandlerQueryer(qryrSvc))
	}

	// Set up Computer.
	if m.Config.Computer.Run {
		// Set the FeatureBase.Config values based on the top-level Config
		// values.
		m.Config.Computer.Config.Listener = m.ln
		m.Config.Computer.Config.Bind = uri.HostPort()
		m.Config.Computer.Config.Advertise = m.advertiseURI.HostPort()

		var mdsImpl featurebase.MDS
		if m.Config.Computer.Config.MDSAddress != "" {
			mdsImpl = mdsclient.New(dax.Address(m.Config.Computer.Config.MDSAddress))
		} else if mdsSvc != nil {
			mdsImpl = mdsSvc
		} else {
			return errors.Errorf("computer requires MDS")
		}

		var writeLoggerImpl featurebase.WriteLogger
		if m.Config.Computer.Config.WriteLogger != "" {
			writeLoggerImpl = writeloggerclient.New(dax.Address(m.Config.Computer.Config.WriteLogger))
		} else if wlSvc != nil {
			writeLoggerImpl = wlSvc
		} else {
			m.logger.Warnf("No writelogger configured, dynamic scaling will not function properly.")
		}

		var snapshotterImpl featurebase.Snapshotter
		if m.Config.Computer.Config.Snapshotter != "" {
			snapshotterImpl = snapshotterclient.New(dax.Address(m.Config.Computer.Config.Snapshotter))
		} else if ssSvc != nil {
			snapshotterImpl = ssSvc
		} else {
			m.logger.Warnf("No snapshotter configured.")
		}

		fbcmd := featurebaseserver.NewCommand(m.CmdIO.Stdin, m.CmdIO.Stdout, m.CmdIO.Stderr,
			featurebaseserver.OptCommandSetConfig(&m.Config.Computer.Config),
			featurebaseserver.OptCommandServerOptions(
				featurebase.OptServerIsComputeNode(true),
				featurebase.OptServerLogger(m.logger),
			),
			featurebaseserver.OptCommandInjections(featurebaseserver.Injections{
				MDS:           mdsImpl,
				WriteLogger:   writeLoggerImpl,
				Snapshotter:   snapshotterImpl,
				IsComputeNode: true,
			}),
		)

		// Register the API with the local Director.
		if err := alphaDirector.AddCmd(dax.Address(m.advertiseURI.HostPort()), fbcmd); err != nil {
			return errors.Wrap(err, "adding cmd to director")
		}

		// Register the API with the local Router.
		if err := alphaRouter.AddCmd(dax.Address(m.advertiseURI.HostPort()), fbcmd); err != nil {
			return errors.Wrap(err, "adding cmd to router")
		}

		if err := fbcmd.StartNoServe(); err != nil {
			return errors.Wrap(err, "start featurebase command")
		}

		// Add the cmd.Register function to the list of functions to call after
		// setup.
		m.registerFns = append(m.registerFns, fbcmd.Register)
		m.checkInFn = fbcmd.CheckIn

		handlerOpts = append(handlerOpts, daxhttp.OptHandlerComputer(fbcmd.HTTPHandler()))
	}

	// Set up Handler based on which services are running in process.
	m.Handler, err = daxhttp.NewHandler(handlerOpts...)
	if err != nil {
		return errors.Wrap(err, "new handler")
	}

	return nil
}

// setupLogger sets up the logger based on the configuration.
func (m *Command) setupLogger() error {
	var f *logger.FileWriter
	var err error
	if m.Config.LogPath == "" {
		m.logOutput = m.Stderr
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
