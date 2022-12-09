package queryer

import (
	"github.com/featurebasedb/featurebase/v3/logger"
)

// Config defines the configuration parameters for Queryer. At the moment, it's
// being used to serve two different purposes. This first is to provide the
// config parameters for the toml (i.e. human-friendly) file used at server
// startup. The second is as the Config for the Queryer type. If this gets more
// complex, it might make sense to split this into two different config structs.
// We initially did that with something called "Injections", but that separation
// was a bit premature.
type Config struct {
	MDSAddress string        `toml:"mds-address"`
	Logger     logger.Logger `toml:"-"`
}
