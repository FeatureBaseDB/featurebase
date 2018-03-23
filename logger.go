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
	"os"
)

// Ensure nopLogger implements interface.
var _ Logger = &nopLogger{}

// Logger represents an interface for a shared logger.
type Logger interface {
	Printf(format string, v ...interface{})
	Debugf(format string, v ...interface{})
	Close() error
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

// Close is a no-op implementation of the Logger Close method.
func (n *nopLogger) Close() error { return nil }

// StandardLogger is a basic implementation of pilosa.Logger based on log.Logger.
type StandardLogger struct {
	logger *log.Logger
	f      *os.File
}

func NewStandardLogger(path string, defaultWriter io.Writer) (*StandardLogger, error) {
	var lw io.Writer
	var err error
	var f *os.File

	if path == "" {
		lw = defaultWriter
	} else {
		f, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
		if err != nil {
			return nil, err
		}
		lw = f
	}

	return &StandardLogger{
		logger: log.New(lw, "", log.LstdFlags),
		f:      f,
	}, nil
}

func (s *StandardLogger) Printf(format string, v ...interface{}) {
	s.logger.Printf(format, v...)
}

func (s *StandardLogger) Debugf(format string, v ...interface{}) {}

func (s *StandardLogger) Close() error {
	if s.f == nil {
		return nil
	}
	return s.f.Close()
}

func (s *StandardLogger) Logger() *log.Logger {
	return s.logger
}

// VerboseLogger is an implementation of pilosa.Logger which includes debug messages.
type VerboseLogger struct {
	logger *log.Logger
	f      *os.File
}

func NewVerboseLogger(path string, defaultWriter io.Writer) (*VerboseLogger, error) {
	var lw io.Writer
	var err error
	var f *os.File

	if path == "" {
		lw = defaultWriter
	} else {
		f, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
		if err != nil {
			return nil, err
		}
		lw = f
	}

	return &VerboseLogger{
		logger: log.New(lw, "", log.LstdFlags),
		f:      f,
	}, nil
}

func (vb *VerboseLogger) Printf(format string, v ...interface{}) {
	vb.logger.Printf(format, v...)
}

func (vb *VerboseLogger) Debugf(format string, v ...interface{}) {
	vb.logger.Printf(format, v...)
}

func (vb *VerboseLogger) Close() error {
	if vb.f == nil {
		return nil
	}
	return vb.f.Close()
}

func (vb *VerboseLogger) Logger() *log.Logger {
	return vb.logger
}
