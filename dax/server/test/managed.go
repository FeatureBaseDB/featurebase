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
	"github.com/featurebasedb/featurebase/v3/dax/mds"
	mdssvc "github.com/featurebasedb/featurebase/v3/dax/mds/service"
	"github.com/featurebasedb/featurebase/v3/dax/queryer"
	queryersvc "github.com/featurebasedb/featurebase/v3/dax/queryer/service"
	"github.com/featurebasedb/featurebase/v3/dax/server"
	"github.com/featurebasedb/featurebase/v3/errors"
	fbtest "github.com/featurebasedb/featurebase/v3/test"
	"github.com/stretchr/testify/assert"
)

// ManagedCommand represents a test wrapper for server.Command.
type ManagedCommand struct {
	*server.Command

	svcmgr *dax.ServiceManager

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
	return mc.Command.Close()
}

// NewMDS adds a new MDSService to the ManagedCommands ServiceManager.
func (mc *ManagedCommand) NewMDS(cfg mds.Config) dax.ServiceKey {
	uri := mc.URI()
	cfg.Logger = mc.svcmgr.Logger
	mc.svcmgr.MDS = mdssvc.New(uri, mds.New(cfg))

	return dax.ServicePrefixMDS
}

// NewQueryer adds a new QueryerService to the ManagedCommands ServiceManager.
func (mc *ManagedCommand) NewQueryer(cfg queryer.Config) dax.ServiceKey {
	uri := mc.URI()
	logger := mc.svcmgr.Logger
	cfg.Logger = logger
	mc.svcmgr.Queryer = queryersvc.New(uri, queryer.New(cfg), logger)

	var mdsAddr dax.Address
	if cfg.MDSAddress != "" {
		mdsAddr = dax.Address(cfg.MDSAddress + "/" + dax.ServicePrefixMDS)
	} else if mc.svcmgr.MDS != nil {
		mdsAddr = mc.svcmgr.MDS.Address()
	}

	// Set MDS
	if err := mc.svcmgr.Queryer.SetMDS(mdsAddr); err != nil {
		logger.Panicf(errors.Wrap(err, "setting mds").Error())
	}

	return dax.ServicePrefixQueryer
}

// NewComputer adds a new ComputerService to the ManagedCommands ServiceManager.
func (mc *ManagedCommand) NewComputer() dax.ServiceKey {
	cfg := computersvc.CommandConfig{
		WriteLoggerRun:    mc.Config.WriteLogger.Run,
		WriteLoggerConfig: mc.Config.WriteLogger.Config,
		SnapshotterRun:    mc.Config.Snapshotter.Run,
		SnapshotterConfig: mc.Config.Snapshotter.Config,
		ComputerConfig:    mc.Config.Computer.Config,

		RootDataDir: mc.Config.Computer.Config.DataDir,

		Stderr: os.Stderr,
		Logger: mc.svcmgr.Logger,
	}

	cfg.ComputerConfig.MDSAddress = mc.svcmgr.MDS.Address().String()

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
	mc.Config.MDS.Config.DataDir = path + "/mds"
	mc.Config.Computer.Config.DataDir = path
	mc.Config.WriteLogger.Config.DataDir = path + "/wl"
	mc.Config.Snapshotter.Config.DataDir = path + "/sn"

	return mc
}

// DefaultConfig includes a single instance of each service type.
func DefaultConfig() *server.Config {
	cfg := server.NewConfig()
	cfg.Verbose = true
	cfg.MDS.Run = true
	cfg.MDS.Config.RegistrationBatchTimeout = 0
	cfg.Queryer.Run = true
	cfg.Computer.Run = true
	cfg.Computer.N = 1
	cfg.WriteLogger.Run = true
	cfg.Snapshotter.Run = true
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

	if err := mc.Start(); err != nil {
		tb.Fatalf("starting managed command: %v", err)
	}

	if basic {
		assert.True(tb, mc.Healthy(dax.ServicePrefixMDS))
		assert.True(tb, mc.Healthy(dax.ServicePrefixQueryer))
		assert.True(tb, mc.Healthy(dax.ServicePrefixComputer+"0"))
	}

	return mc
}
