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

package pilosa

import (
	"io/ioutil"
	"log"
)

// Ensure nopLogger implements interface.
var _ Logger = &nopLogger{}

// Logger represents an interface for a shared logger.
type Logger interface {
	Printf(format string, v ...interface{})
	Debugf(format string, v ...interface{})
}

func init() {
	NopLogger = &nopLogger{
		logger: log.New(ioutil.Discard, "", log.LstdFlags),
	}
}

// NopLogger represents a Logger that doesn't do anything.
var NopLogger Logger

type nopLogger struct {
	logger *log.Logger
}

// Printf is a no-op implementation of the Logger Printf method.
func (n *nopLogger) Printf(format string, v ...interface{}) {}

// Debugf is a no-op implementation of the Logger Debugf method.
func (n *nopLogger) Debugf(format string, v ...interface{}) {}

// StandardLogger is a basic implementation of pilosa.Logger based on log.Logger.
type StandardLogger struct {
	logger *log.Logger
}

func NewStandardLogger(logger *log.Logger) *StandardLogger {
	return &StandardLogger{
		logger: logger,
	}
}

// Printf is a no-op implementation of the Logger Printf method.
func (s *StandardLogger) Printf(format string, v ...interface{}) {
	s.logger.Printf(format, v...)
}

// Debugf is a no-op implementation of the Logger Debugf method.
func (s *StandardLogger) Debugf(format string, v ...interface{}) {
	// TODO: implement this
}
