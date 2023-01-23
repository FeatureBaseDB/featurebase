package service

import (
	"context"
	"io"
	"net"
	"net/http"
	"strings"

	featurebase "github.com/featurebasedb/featurebase/v3"
	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/dax/computer"
	controllerclient "github.com/featurebasedb/featurebase/v3/dax/controller/client"
	"github.com/featurebasedb/featurebase/v3/dax/snapshotter"
	"github.com/featurebasedb/featurebase/v3/dax/writelogger"
	"github.com/featurebasedb/featurebase/v3/errors"
	"github.com/featurebasedb/featurebase/v3/logger"
	fbserver "github.com/featurebasedb/featurebase/v3/server"
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
		if cmd, err := newCommand(c.addr, c.cfg); err != nil {
			return errors.Wrapf(err, "getting new command for computer config: %s", c.cfg.Name)
		} else {
			c.computer = cmd
		}

		if c.cfg.ComputerConfig.ControllerAddress != "" {
			controllerAddr := dax.Address(c.cfg.ComputerConfig.ControllerAddress)
			// Set Controller (registrar) on computer.
			if err := c.SetController(controllerAddr); err != nil {
				return errors.Wrapf(err, "setting controller service on computer: %s", c.cfg.Name)
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

func (c *computerService) SetController(addr dax.Address) error {
	c.computer.Registrar = controllerclient.New(addr, c.logger)
	return nil
}

type CommandConfig struct {
	// Name is used to distinguish between locally running commands.
	// For example, it's appended to DataDir so that each cmd has a
	// separate data directory for its holder.
	Name string

	ComputerConfig fbserver.Config

	Listener    net.Listener
	RootDataDir string

	Stderr io.Writer
	Logger logger.Logger
}

// serviceOffValue is a reserved term used to explicitly disable a service. When
// used, the service will be set to use its no-op implementation. For the user,
// this is case-insensitive.
var serviceOffValue = "off"

func newCommand(addr dax.Address, cfg CommandConfig) (*fbserver.Command, error) {
	// Set up Writelogger.
	var wlSvc computer.WritelogService
	wlDirToCompare := strings.TrimSpace(strings.ToLower(cfg.ComputerConfig.WriteloggerDir))
	switch wlDirToCompare {
	case "":
		return nil, errors.New(errors.ErrUncoded, "no writelogger directory configured")
	case serviceOffValue:
		wlSvc = computer.NewNopWritelogService()
		cfg.Logger.Warnf("No writelogger configured, dynamic scaling will not function properly.")
	default:
		wlSvc = writelogger.New(cfg.ComputerConfig.WriteloggerDir, cfg.Logger)
	}

	// Set up Snapshotter.
	var ssSvc computer.SnapshotService
	ssDirToCompare := strings.TrimSpace(strings.ToLower(cfg.ComputerConfig.SnapshotterDir))
	switch ssDirToCompare {
	case "":
		return nil, errors.New(errors.ErrUncoded, "no snapshotter directory configured")
	case serviceOffValue:
		ssSvc = computer.NewNopSnapshotterService()
		cfg.Logger.Warnf("No snapshotter configured, dynamic scaling will not function properly.")
	default:
		ssSvc = snapshotter.New(cfg.ComputerConfig.SnapshotterDir, cfg.Logger)
	}

	// Set the FeatureBase.Config values based on the top-level Config
	// values.
	cfg.ComputerConfig.Listener = &nopListener{}
	cfg.ComputerConfig.Advertise = addr.HostPort()
	cfg.ComputerConfig.GRPCListener = &nopListener{}
	cfg.ComputerConfig.DataDir = cfg.RootDataDir + "/" + cfg.Name

	fbcmd := fbserver.NewCommand(cfg.Stderr,
		fbserver.OptCommandSetConfig(&cfg.ComputerConfig),
		fbserver.OptCommandServerOptions(
			featurebase.OptServerIsComputeNode(true),
			featurebase.OptServerLogger(cfg.Logger),
		),
		fbserver.OptCommandInjections(fbserver.Injections{
			Writelogger:   wlSvc,
			Snapshotter:   ssSvc,
			IsComputeNode: true,
		}),
	)

	return fbcmd, nil
}

type nopListener struct{}

func (n *nopListener) Accept() (net.Conn, error) { return nil, nil }
func (n *nopListener) Close() error              { return nil }
func (n *nopListener) Addr() net.Addr            { return nil }
