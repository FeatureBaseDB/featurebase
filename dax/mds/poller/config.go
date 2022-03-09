package poller

import (
	"time"

	"github.com/molecula/featurebase/v3/dax"
	"github.com/molecula/featurebase/v3/logger"
)

type Config struct {
	AddressManager dax.AddressManager
	NodePoller     NodePoller
	PollInterval   time.Duration
	Logger         logger.Logger
}
