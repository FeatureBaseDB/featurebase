package sqldb

import (
	"github.com/featurebasedb/featurebase/v3/dax/controller/balancer"
	"github.com/featurebasedb/featurebase/v3/logger"
)

// NewBalancer returns a new instance of controller.Balancer.
func NewBalancer(logger logger.Logger) *balancer.Balancer {
	schemar := NewSchemar(logger)
	fjs := NewFreeJobService(logger)
	wjs := NewWorkerJobService(logger)
	fws := NewFreeWorkerService(logger)
	ns := NewNodeService(logger)

	return balancer.New(ns, fjs, wjs, fws, schemar, logger)
}
