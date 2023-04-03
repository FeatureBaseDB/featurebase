package sqldb

import (
	"github.com/featurebasedb/featurebase/v3/dax/controller/balancer"
	"github.com/featurebasedb/featurebase/v3/logger"
)

// NewBalancer returns a new instance of controller.Balancer.
func NewBalancer(log logger.Logger) *balancer.Balancer {
	schemar := NewSchemar(log)
	fjs := NewFreeJobService(log)
	wjs := NewWorkerJobService(log)
	fws := NewFreeWorkerService(log)
	ns := NewWorkerRegistry(log)
	wsp := NewWorkerServiceProviderService(log)

	return balancer.New(ns, fjs, wjs, fws, schemar, wsp, log)
}
