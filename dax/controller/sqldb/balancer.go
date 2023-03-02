package sqldb

import (
	"github.com/featurebasedb/featurebase/v3/dax/controller/balancer"
	"github.com/featurebasedb/featurebase/v3/logger"
)

// NewBalancer returns a new instance of controller.Balancer.
func NewBalancer(logger logger.Logger) *balancer.Balancer {
	schemar := &Schemar{}
	fjs := &FreeJobService{}
	wjs := &WorkerJobService{}
	fws := &FreeWorkerService{}
	ns := &NodeService{}

	return balancer.New(ns, fjs, wjs, fws, schemar, logger)
}
