package writelogger

import "github.com/molecula/featurebase/v3/logger"

type Config struct {
	DataDir string        `toml:"data-dir"`
	Logger  logger.Logger `toml:"-"`
}
