package service

import (
	"context"
	"io"
	"net"
	"net/http"

	featurebase "github.com/molecula/featurebase/v3"
	"github.com/molecula/featurebase/v3/dax"
	"github.com/molecula/featurebase/v3/dax/computer"
	mdsclient "github.com/molecula/featurebase/v3/dax/mds/client"
	"github.com/molecula/featurebase/v3/dax/snapshotter"
	snapshotterclient "github.com/molecula/featurebase/v3/dax/snapshotter/client"
	"github.com/molecula/featurebase/v3/dax/writelogger"
	writeloggerclient "github.com/molecula/featurebase/v3/dax/writelogger/client"
	"github.com/molecula/featurebase/v3/errors"
	"github.com/molecula/featurebase/v3/logger"
	fbserver "github.com/molecula/featurebase/v3/server"
)

// Ensure type implements interface.
var _ dax.ComputerService = (*computerService)(nil)

type computerService struct {
	addr     dax.Address
	cfg      CommandConfig
	key      dax.ServiceKey
	computer *fbserver.Command
	logger   logger.Logger
}

func New(addr dax.Address, cfg CommandConfig, logger logger.Logger) *computerService {
	cfg.ComputerConfig.Advertise = addr.HostPort()

	return &computerService{
		addr:   addr,
		cfg:    cfg,
		logger: logger,
	}
}

func (c *computerService) Start() error {
	// We initialize the fbserver.Command at start (as opposed to in New()
	// above) because we want to inject (via cfg.Name) the ServiceKey into the
	// *fbserver.Command returned by newCommand() so that it can be used
	// internally. In the future, when we have the pilosa package moved down to
	// the computer package, we should be able to clean this up so that things
	// happen in a reasonable order like we do with the other service types.
	if c.computer == nil {
		c.cfg.Name = string(c.Key())
		c.computer = newCommand(c.addr, c.cfg)

		if c.cfg.ComputerConfig.MDSAddress != "" {
			mdsAddr := dax.Address(c.cfg.ComputerConfig.MDSAddress)
			// Set mds (registrar) on computer.
			if err := c.SetMDS(mdsAddr); err != nil {
				return errors.Wrapf(err, "setting mds service on computer: %s, %v", c.cfg.Name, err)
			}
		}
	}

	if err := c.computer.StartNoServe(c.Address()); err != nil {
		return errors.Wrap(err, "starting featurebase command")
	}

	if c.computer.Registrar != nil {
		node := &dax.Node{
			Address: c.Address(),
			RoleTypes: []dax.RoleType{
				dax.RoleTypeCompute,
				dax.RoleTypeTranslate,
			},
		}

		if err := c.computer.Registrar.RegisterNode(context.TODO(), node); err != nil {
			return errors.Wrapf(err, "registering computer: %s", c.Address())
		}
	}
	return nil
}

func (c *computerService) Stop() error {
	return c.computer.Close()
}

func (c *computerService) Key() dax.ServiceKey {
	return c.key
}

func (c *computerService) SetKey(key dax.ServiceKey) {
	c.key = key
}

func (c *computerService) Address() dax.Address {
	return dax.Address(c.addr.HostPort() + "/" + string(c.key))
}

func (c *computerService) HTTPHandler() http.Handler {
	return c.computer.HTTPHandler()
}

func (c *computerService) SetMDS(addr dax.Address) error {
	c.computer.Registrar = mdsclient.New(addr, c.logger)
	return nil
}

type CommandConfig struct {
	// Name is used to distinguish between locally running commands.
	// For example, it's appended to DataDir so that each cmd has a
	// separate data directory for its holder.
	Name string

	WriteLoggerRun    bool
	WriteLoggerConfig writelogger.Config
	SnapshotterRun    bool
	SnapshotterConfig snapshotter.Config
	ComputerConfig    fbserver.Config

	Listener    net.Listener
	RootDataDir string

	Stderr io.Writer
	Logger logger.Logger
}

func newCommand(addr dax.Address, cfg CommandConfig) *fbserver.Command {
	// Set up WriteLogger.
	// TODO(tlt): since WriteLogger is no longer a separate service (but
	// rather just a directory path) its configuration could be moved under
	// computer, and then get rid of WriteLogger.Run. This would become "if
	// DataDir != ''". Let's do this after we get rid of the dax integration
	// tests which start up separate writelogger and snapshotter containers.
	var wlSvc *writelogger.WriteLogger
	if cfg.WriteLoggerRun {
		wlSvc = writelogger.New(writelogger.Config{
			DataDir: cfg.WriteLoggerConfig.DataDir,
			Logger:  cfg.Logger,
		})
	}

	// Set up Snapshotter.
	var ssSvc *snapshotter.Snapshotter
	if cfg.SnapshotterRun {
		ssSvc = snapshotter.New(snapshotter.Config{
			DataDir: cfg.SnapshotterConfig.DataDir,
			Logger:  cfg.Logger,
		})
	}

	// Set the FeatureBase.Config values based on the top-level Config
	// values.
	cfg.ComputerConfig.Listener = &nopListener{}
	cfg.ComputerConfig.Advertise = addr.HostPort()
	cfg.ComputerConfig.GRPCListener = &nopListener{}
	cfg.ComputerConfig.DataDir = cfg.RootDataDir + "/" + cfg.Name

	var writeLoggerImpl computer.WriteLogger
	if cfg.ComputerConfig.WriteLogger != "" {
		writeLoggerImpl = writeloggerclient.New(dax.Address(cfg.ComputerConfig.WriteLogger))
	} else if wlSvc != nil {
		writeLoggerImpl = wlSvc
	} else {
		cfg.Logger.Warnf("No writelogger configured, dynamic scaling will not function properly.")
	}

	var snapshotterImpl computer.Snapshotter
	if cfg.ComputerConfig.Snapshotter != "" {
		snapshotterImpl = snapshotterclient.New(dax.Address(cfg.ComputerConfig.Snapshotter))
	} else if ssSvc != nil {
		snapshotterImpl = ssSvc
	} else {
		cfg.Logger.Warnf("No snapshotter configured.")
	}

	fbcmd := fbserver.NewCommand(cfg.Stderr,
		fbserver.OptCommandSetConfig(&cfg.ComputerConfig),
		fbserver.OptCommandServerOptions(
			featurebase.OptServerIsComputeNode(true),
			featurebase.OptServerLogger(cfg.Logger),
		),
		fbserver.OptCommandInjections(fbserver.Injections{
			WriteLogger:   writeLoggerImpl,
			Snapshotter:   snapshotterImpl,
			IsComputeNode: true,
		}),
	)

	return fbcmd
}

type nopListener struct{}

func (n *nopListener) Accept() (net.Conn, error) { return nil, nil }
func (n *nopListener) Close() error              { return nil }
func (n *nopListener) Addr() net.Addr            { return nil }
