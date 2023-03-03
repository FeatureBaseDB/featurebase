package controller

import (
	"time"

	"github.com/featurebasedb/featurebase/v3/logger"
)

type NewBalancerFn func(string, logger.Logger) Balancer

type Config struct {
	Director Director

	// Poller
	PollInterval time.Duration `toml:"poll-interval"`

	// Storage
	StorageMethod     string `toml:"storage-method"`
	StorageEnv        string `toml:"storage-env"`
	StorageConfigFile string `toml:"storage-config-file"`
	DataDir           string `toml:"-"`

	SnapshotterDir string `toml:"snapshotter-dir"`
	WriteloggerDir string `toml:"writelogger-dir"`

	// RegistrationBatchTimeout is the time that the controller will
	// wait after a node registers itself to see if any more nodes
	// will register before sending out directives to all nodes which
	// have been registered.
	RegistrationBatchTimeout time.Duration

	// SnappingTurtleTimeout is the period on which the automatic
	// snapshotting routine will run. If performing all the snapshots
	// takes longer than this amount of time, snapshotting will run
	// continuously. If it finishes before the timeout, it will wait
	// until the timeout expires to start another round of snapshots.
	SnappingTurtleTimeout time.Duration

	Logger logger.Logger `toml:"-"`
}
