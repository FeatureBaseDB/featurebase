package server

import (
	"os"
	"syscall"

	"github.com/pilosa/pilosa/logger"
	"github.com/pkg/errors"
)

// setupLogger sets up the logger based on the configuration.
func (m *Command) setupLogger() error {
	if m.Config.LogPath == "" {
		m.logOutput = m.Stderr
	} else {
		f, err := os.OpenFile(m.Config.LogPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
		if err != nil {
			return errors.Wrap(err, "opening file")
		}
		m.logOutput = f
		err = syscall.Dup3(int(f.Fd()), int(os.Stderr.Fd()), 0)
		if err != nil {
			return errors.Wrap(err, "dup2ing stderr onto logfile")
		}
	}

	if m.Config.Verbose {
		m.logger = logger.NewVerboseLogger(m.logOutput)
	} else {
		m.logger = logger.NewStandardLogger(m.logOutput)
	}
	return nil
}
