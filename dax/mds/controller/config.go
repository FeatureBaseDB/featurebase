package controller

import (
	"time"

	"github.com/featurebasedb/featurebase/v3/dax/boltdb"
	"github.com/featurebasedb/featurebase/v3/dax/mds/schemar"
	"github.com/featurebasedb/featurebase/v3/logger"
)

type NewBalancerFn func(string, logger.Logger) Balancer

type Config struct {
	Director          Director
	Schemar           schemar.Schemar
	ComputeBalancer   Balancer
	TranslateBalancer Balancer

	StorageMethod string
	BoltDB        *boltdb.DB

	// RegistrationBatchTimeout is the time that the controller will
	// wait after a node registers itself to see if any more nodes
	// will register before sending out directives to all nodes which
	// have been registered.
	RegistrationBatchTimeout time.Duration

	Logger logger.Logger
}
