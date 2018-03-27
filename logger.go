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
	"io"
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
	NopLogger = &nopLogger{}
}

// NopLogger represents a Logger that doesn't do anything.
var NopLogger Logger

type nopLogger struct{}

// Printf is a no-op implementation of the Logger Printf method.
func (n *nopLogger) Printf(format string, v ...interface{}) {}

// Debugf is a no-op implementation of the Logger Debugf method.
func (n *nopLogger) Debugf(format string, v ...interface{}) {}

// StandardLogger is a basic implementation of pilosa.Logger based on log.Logger.
type StandardLogger struct {
	logger *log.Logger
}

func NewStandardLogger(w io.Writer) *StandardLogger {
	return &StandardLogger{
		logger: log.New(w, "", log.LstdFlags),
	}
}

func (s *StandardLogger) Printf(format string, v ...interface{}) {
	s.logger.Printf(format, v...)
}

func (s *StandardLogger) Debugf(format string, v ...interface{}) {}

func (s *StandardLogger) Logger() *log.Logger {
	return s.logger
}

// VerboseLogger is an implementation of pilosa.Logger which includes debug messages.
type VerboseLogger struct {
	logger *log.Logger
}

func NewVerboseLogger(w io.Writer) *VerboseLogger {
	return &VerboseLogger{
		logger: log.New(w, "", log.LstdFlags),
	}
}

func (vb *VerboseLogger) Printf(format string, v ...interface{}) {
	vb.logger.Printf(format, v...)
}

func (vb *VerboseLogger) Debugf(format string, v ...interface{}) {
	vb.logger.Printf(format, v...)
}

func (vb *VerboseLogger) Logger() *log.Logger {
	return vb.logger
}
