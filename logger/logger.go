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

package logger

import (
	"fmt"
	"io"
	"log"
	"time"
)

const RFC3339UsecTz0 = "2006-01-02T15:04:05.000000Z07:00"

// Ensure nopLogger implements interface.
var _ Logger = &nopLogger{}

// Logger represents an interface for a shared logger.
type Logger interface {
	Printf(format string, v ...interface{})
	Debugf(format string, v ...interface{})
}

// NopLogger represents a Logger that doesn't do anything.
var NopLogger Logger = &nopLogger{}

type nopLogger struct{}

// Printf is a no-op implementation of the Logger Printf method.
func (n *nopLogger) Printf(format string, v ...interface{}) {}

// Debugf is a no-op implementation of the Logger Debugf method.
func (n *nopLogger) Debugf(format string, v ...interface{}) {}

// standardLogger is a basic implementation of Logger based on log.Logger.
type standardLogger struct {
	logger *log.Logger
}

// write in UTC with constant width and microsecond resolution.
type formatLog struct {
	w io.Writer
}

func (fl formatLog) Write(bytes []byte) (int, error) {
	return fmt.Fprintf(fl.w, "%v %v", time.Now().UTC().Format(RFC3339UsecTz0), string(bytes))
}

func NewStandardLogger(w io.Writer) *standardLogger {
	logger := log.New(w, "", 0)
	logger.SetOutput(formatLog{w: w})
	return &standardLogger{
		logger: logger,
	}
}

func (s *standardLogger) Printf(format string, v ...interface{}) {
	s.logger.Printf(format, v...)
}

func (s *standardLogger) Debugf(format string, v ...interface{}) {}

func (s *standardLogger) Logger() *log.Logger {
	return s.logger
}

// verboseLogger is an implementation of Logger which includes debug messages.
type verboseLogger struct {
	logger *log.Logger
}

func NewVerboseLogger(w io.Writer) *verboseLogger {
	logger := log.New(w, "", 0)
	logger.SetOutput(formatLog{w: w})
	return &verboseLogger{
		logger: logger,
	}
}

func (vb *verboseLogger) Printf(format string, v ...interface{}) {
	vb.logger.Printf(format, v...)
}

func (vb *verboseLogger) Debugf(format string, v ...interface{}) {
	vb.logger.Printf(format, v...)
}

func (vb *verboseLogger) Logger() *log.Logger {
	return vb.logger
}

// CaptureLogger is a logger that stores all the print and debug messages
// it sees, useful for testing.
type CaptureLogger struct {
	Prints []string
	Debugs []string
}

// NewCaptureLogger yields a CaptureLogger.
func NewCaptureLogger() *CaptureLogger {
	return &CaptureLogger{}
}

// Printf formats a message and appends it to Prints.
func (cl *CaptureLogger) Printf(format string, v ...interface{}) {
	cl.Prints = append(cl.Prints, fmt.Sprintf(format, v...))
}

// Debugf formats a message and appends it to Debugs.
func (cl *CaptureLogger) Debugf(format string, v ...interface{}) {
	cl.Debugs = append(cl.Debugs, fmt.Sprintf(format, v...))
}

// Logfer is a thing that has only a Logf() method, like for instance,
// testing.T or testing.B.
type Logfer interface {
	Logf(format string, v ...interface{})
}

// LogfLogger is a logger that wraps something that has a Logf interface
// and makes it act like our logger.
type LogfLogger struct {
	wrapped Logfer
}

func (ll *LogfLogger) Printf(format string, v ...interface{}) {
	ll.wrapped.Logf(format, v...)
}

func (ll *LogfLogger) Debugf(format string, v ...interface{}) {
	ll.wrapped.Logf(format, v...)
}

func NewLogfLogger(l Logfer) *LogfLogger {
	return &LogfLogger{wrapped: l}
}
