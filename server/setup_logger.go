// Copyright 2017 Pilosa Corp.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// +build !arm64

package server

import (
	"os"
	"syscall"

	"github.com/pilosa/pilosa/v2/logger"
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
		err = syscall.Dup2(int(f.Fd()), int(os.Stderr.Fd()))
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
