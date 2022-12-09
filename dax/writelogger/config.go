package writelogger

import "github.com/featurebasedb/featurebase/v3/logger"

type Config struct {
	DataDir string        `toml:"data-dir"`
	Logger  logger.Logger `toml:"-"`
}
