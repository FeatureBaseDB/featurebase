package sqldb

import (
	"github.com/featurebasedb/featurebase/v3/dax/controller/balancer"
	"github.com/featurebasedb/featurebase/v3/logger"
)

// NewBalancer returns a new instance of controller.Balancer.
func NewBalancer(log logger.Logger) *balancer.Balancer {
	store := NewStore(log)

	return balancer.New(store, log)
}
