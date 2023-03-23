// Copyright 2021 Molecula Corp. All rights reserved.
package test

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/featurebasedb/featurebase/v3/dax"
	computersvc "github.com/featurebasedb/featurebase/v3/dax/computer/service"
	"github.com/featurebasedb/featurebase/v3/dax/controller"
	controllersvc "github.com/featurebasedb/featurebase/v3/dax/controller/service"
	"github.com/featurebasedb/featurebase/v3/dax/controller/sqldb"
	"github.com/featurebasedb/featurebase/v3/dax/queryer"
	queryersvc "github.com/featurebasedb/featurebase/v3/dax/queryer/service"
	"github.com/featurebasedb/featurebase/v3/dax/server"
	"github.com/featurebasedb/featurebase/v3/errors"
	"github.com/featurebasedb/featurebase/v3/logger"
	fbtest "github.com/featurebasedb/featurebase/v3/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ManagedCommand represents a test wrapper for server.Command.
type ManagedCommand struct {
	*server.Command

	svcmgr *dax.ServiceManager

	// Hang on to the Transactor so we can use it to drop the database upon
	// closing the ManagedCommand.
	trans sqldb.Transactor

	started bool
}

// Manage returns the ServiceManager for the ManagedCommand.
func (mc *ManagedCommand) Manage() *dax.ServiceManager {
	return mc.svcmgr
}

// Address returns the advertise address at which the command can be reached.
func (mc *ManagedCommand) Address() dax.Address {
	uri := mc.URI()
	return dax.Address(uri.String())
}

// Start starts the embedded command.
func (mc *ManagedCommand) Start() error {
	if mc.started {
		return nil
	}

	if err := mc.Command.Start(); err != nil {
		return errors.Wrap(err, "starting command")
	}
	mc.started = true

	return nil
}

// Close closes the embedded  command.
func (mc *ManagedCommand) Close() error {
	if err := mc.Command.Close(); err != nil {
		return errors.Wrap(err, "closing command")
	}

	// Drop the database upon closing.
	// return sqldb.DropDatabase(mc.trans)

	return nil
}

// NewController adds a new ControllerService to the ManagedCommands ServiceManager.
func (mc *ManagedCommand) NewController(cfg controller.Config) dax.ServiceKey {
	uri := mc.URI()
	cfg.Logger = mc.svcmgr.Logger
	mc.svcmgr.Controller = controllersvc.New(uri, cfg)

	return dax.ServicePrefixController
}

// NewQueryer adds a new QueryerService to the ManagedCommands ServiceManager.
func (mc *ManagedCommand) NewQueryer(cfg queryer.Config) dax.ServiceKey {
	uri := mc.URI()
	logger := mc.svcmgr.Logger
	cfg.Logger = logger
	mc.svcmgr.Queryer = queryersvc.New(uri, queryer.New(cfg), logger)

	var controllerAddr dax.Address
	if cfg.ControllerAddress != "" {
		controllerAddr = dax.Address(cfg.ControllerAddress + "/" + dax.ServicePrefixController)
	} else if mc.svcmgr.Controller != nil {
		controllerAddr = mc.svcmgr.Controller.Address()
	}

	// Set Controller
	if err := mc.svcmgr.Queryer.SetController(controllerAddr); err != nil {
		logger.Panicf(errors.Wrap(err, "setting controller").Error())
	}

	return dax.ServicePrefixQueryer
}

// NewComputer adds a new ComputerService to the ManagedCommands ServiceManager.
func (mc *ManagedCommand) NewComputer() dax.ServiceKey {
	cfg := computersvc.CommandConfig{
		ComputerConfig: mc.Config.Computer.Config,

		RootDataDir: mc.Config.Computer.Config.DataDir,

		Stderr: os.Stderr,
		Logger: mc.svcmgr.Logger,
	}

	cfg.ComputerConfig.ControllerAddress = mc.svcmgr.Controller.Address().String()

	// Add new computer service.
	return mc.svcmgr.AddComputer(
		computersvc.New(mc.Address(), cfg, cfg.Logger))
}

// Healthy returns true if the provided service's /health endpoint returns 200
// OK. This means that the service has been added to the ServiceManager and
// started, and that its http handler has been dynamically added.
func (mc *ManagedCommand) Healthy(key dax.ServiceKey) bool {
	if key == "" {
		return false
	}

	addr := mc.Address()
	url := fmt.Sprintf("%s/%s/health", addr.WithScheme("http"), key)
	log.Println("HEALTH URL:", url)
	res, err := http.Get(url)
	if err != nil {
		return false
	} else if res.StatusCode != http.StatusOK {
		return false
	}

	return true
}

// WaitForApplied is a test helper function which retries a computer's
// /directive endpoint a specified number of times, along with a sleep time in
// between tries, until the computer returns applied=true.
func (mc *ManagedCommand) WaitForApplied(t *testing.T, key dax.ServiceKey, n int, sleep time.Duration) {
	t.Helper()

	addr := mc.Address()
	url := fmt.Sprintf("%s/%s/directive", addr.WithScheme("http"), key)
	log.Println("WAIT URL:", url)

	for i := 0; i < n; i++ {
		resp, err := http.Get(url)
		assert.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode)

		body := resp.Body
		defer body.Close()

		got := struct {
			Applied bool `json:"applied"`
		}{}
		assert.NoError(t, json.NewDecoder(body).Decode(&got))

		if got.Applied {
			return
		}

		t.Logf("Wait (%d/%d): url: %s (sleep: %s)\n", i, n, url, sleep.String())
		if i < n-1 {
			time.Sleep(sleep)
		}
	}

	// Getting to here means the directive endpoint never returned successfully,
	// so we need to stop the test.
	t.Fatal("WaitForApplied timed out")
}

// NewManagedCommand returns a new instance of Command.
func NewManagedCommand(tb fbtest.DirCleaner, opts ...server.CommandOption) *ManagedCommand {
	path := tb.TempDir()

	svcmgr := dax.NewServiceManager()
	opts = append(opts, server.OptCommandServiceManager(svcmgr))

	mc := &ManagedCommand{}
	output := io.Discard
	if testing.Verbose() {
		output = os.Stderr
	}
	mc.Command = server.NewCommand(output, opts...)
	mc.svcmgr = svcmgr

	mc.Config.Bind = "http://localhost:0"
	mc.Config.Computer.Config.DataDir = path
	mc.Config.Computer.Config.WriteloggerDir = path + "/wl"
	mc.Config.Controller.Config.WriteloggerDir = path + "/wl"
	mc.Config.Computer.Config.SnapshotterDir = path + "/sn"
	mc.Config.Controller.Config.SnapshotterDir = path + "/sn"

	var err error
	testconf := sqldb.GetTestConfig()
	mc.trans, err = sqldb.NewTransactor(testconf, logger.StderrLogger)
	if err != nil {
		tb.Fatalf("getting new transactor: %v", err)
	}

	return mc
}

// DefaultConfig includes a single instance of each service type.
func DefaultConfig() *server.Config {
	cfg := server.NewConfig()
	cfg.Verbose = true
	cfg.Controller.Run = true
	cfg.Controller.Config.StorageMethod = "sqldb"
	cfg.Controller.Config.RegistrationBatchTimeout = 0
	cfg.Controller.Config.SQLDB = sqldb.GetTestConfig()
	cfg.Queryer.Run = true
	cfg.Computer.Run = true
	cfg.Computer.N = 1
	return cfg
}

// MustRunManagedCommand starts an in-process set of Services based on the
// provided configuration. If no configuration is provided, it will use the
// DefaultConfig which consists of one instance of each service type.
func MustRunManagedCommand(tb testing.TB, opts ...server.CommandOption) *ManagedCommand {
	// If no opts are passed, use the default configuration which includes a
	// single instance of each service type. This is really just meant to keep
	// test code a bit cleaner when it's not necessary to have a custom service
	// configuration.
	var basic bool
	if len(opts) == 0 {
		opts = []server.CommandOption{
			server.OptCommandConfig(DefaultConfig()),
		}
		basic = true
	}

	mc := NewManagedCommand(tb, opts...)

	// Start the Transactor.
	require.NoError(tb, mc.trans.Start())

	// The integration tests reuse the same database every time, but
	// truncate all the tables *before* the tests run (rather than
	// after). This has the advantage that if the tests fail partway
	// through, you can inspect the state of the database for
	// debugging purposes.
	err := mc.trans.TruncateAll()
	if err != nil {
		tb.Fatalf("truncating DB: %v", err)
	}

	// The migrations contain an insert, but since we just truncated everything we need to redo that insert.
	err = mc.trans.RawQuery("INSERT INTO directive_versions (id, version, created_at, updated_at) VALUES (1, 1, '1970-01-01T00:00', '1970-01-01T00:00')").Exec()
	if err != nil {
		tb.Fatalf("reinserting directive_version record after truncation: %v", err)
	}

	err = mc.trans.Close()
	if err != nil {
		tb.Fatalf("Closing conn after truncating all tables: %v", err)
	}

	if err := mc.Start(); err != nil {
		tb.Fatalf("starting managed command: %v", err)
	}

	if basic {
		assert.True(tb, mc.Healthy(dax.ServicePrefixController))
		assert.True(tb, mc.Healthy(dax.ServicePrefixQueryer))
		assert.True(tb, mc.Healthy(dax.ServicePrefixComputer+"0"))
	}

	return mc
}
