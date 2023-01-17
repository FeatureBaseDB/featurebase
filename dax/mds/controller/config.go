package controller

import (
	"time"

	"github.com/molecula/featurebase/v3/dax/boltdb"
	"github.com/molecula/featurebase/v3/dax/mds/schemar"
	"github.com/molecula/featurebase/v3/logger"
)

type NewBalancerFn func(string, logger.Logger) Balancer

type Config struct {
	Director Director
	Schemar  schemar.Schemar

	Balancer Balancer

	StorageMethod string
	BoltDB        *boltdb.DB

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

	Logger logger.Logger
}
