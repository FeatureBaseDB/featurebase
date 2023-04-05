package poller

import (
	"time"

	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/logger"
)

type Config struct {
	AddressManager dax.AddressManager
	WorkerRegistry dax.WorkerRegistry
	NodePoller     NodePoller
	PollInterval   time.Duration
	Logger         logger.Logger
}
