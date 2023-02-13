// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package logger

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"time"

	"github.com/featurebasedb/featurebase/v3/monitor"
)

const RFC3339UsecTz0 = "2006-01-02T15:04:05.000000Z07:00"

// Ensure nopLogger implements interface.
var _ Logger = &nopLogger{}

// Logger represents an interface for a shared logger.
type Logger interface {
	Printf(format string, v ...interface{}) // backward compatibility
	Debugf(format string, v ...interface{})
	Infof(format string, v ...interface{})
	Warnf(format string, v ...interface{})
	Errorf(format string, v ...interface{})
	Panicf(format string, v ...interface{})
	// WithPrefix returns a new Logger with the same configuration as
	// this one, but all logs will have the given prefix.
	WithPrefix(prefix string) Logger
}

const (
	LevelPanic = iota
	LevelError
	LevelWarn
	LevelInfo
	LevelDebug
)

func LevelPrefix(level int) string {
	return [...]string{"PANIC: ", "ERROR: ", "WARN:  ", "INFO:  ", "DEBUG: "}[level]
}

var StderrLogger = NewStandardLogger(os.Stderr)

// NopLogger represents a Logger that doesn't do anything.
var NopLogger Logger = &nopLogger{}

type nopLogger struct{}

// Printf is a no-op implementation of the Logger Printf method.
func (n *nopLogger) Printf(format string, v ...interface{}) {}

// Debugf is a no-op implementation of the Logger Debugf method.
func (n *nopLogger) Debugf(format string, v ...interface{}) {}

// Infof is a no-op implementation of the Logger Infof method.
func (n *nopLogger) Infof(format string, v ...interface{}) {}

// Warnf is a no-op implementation of the Logger Warnf method.
func (n *nopLogger) Warnf(format string, v ...interface{}) {}

// Errorf is a no-op implementation of the Logger Errorf method.
func (n *nopLogger) Errorf(format string, v ...interface{}) {}

// Panicf is a no-op implementation of the Logger Panicf method.
func (n *nopLogger) Panicf(format string, v ...interface{}) {}

func (n *nopLogger) WithPrefix(prefix string) Logger {
	return n
}

// standardLogger is a basic implementation of Logger based on log.Logger.
type standardLogger struct {
	logger    *log.Logger
	verbosity int
	prefix    string
	w         io.Writer
}

// write in UTC with constant width and microsecond resolution.
type formatLog struct {
	w io.Writer
}

func (fl formatLog) Write(bytes []byte) (int, error) {
	return fmt.Fprintf(fl.w, "%v %v", time.Now().UTC().Format(RFC3339UsecTz0), string(bytes))
}

func newStandardLogger(w io.Writer, verbosity int, prefix string) *standardLogger {
	logger := log.New(w, prefix, 0)
	logger.SetOutput(formatLog{w: w})
	return &standardLogger{
		logger:    logger,
		verbosity: verbosity,
		prefix:    prefix,
		w:         w,
	}
}

func NewStandardLogger(w io.Writer) *standardLogger {
	return newStandardLogger(w, LevelInfo, "")
}

func NewVerboseLogger(w io.Writer) *standardLogger {
	return newStandardLogger(w, LevelDebug, "")
}

func (s *standardLogger) printf(level int, format string, v ...interface{}) {
	if level > s.verbosity {
		return
	}
	if monitor.IsOn() {
		// intercepts the log message and sends it to the monitor
		monitor.CaptureException(level, format, v...)
	}
	s.logger.Printf(LevelPrefix(level)+format, v...)
}

func (s *standardLogger) Printf(format string, v ...interface{}) {
	s.printf(LevelInfo, format, v...)
}

func (s *standardLogger) Debugf(format string, v ...interface{}) {
	s.printf(LevelDebug, format, v...)
}

func (s *standardLogger) Infof(format string, v ...interface{}) {
	s.printf(LevelInfo, format, v...)
}

func (s *standardLogger) Warnf(format string, v ...interface{}) {
	s.printf(LevelWarn, format, v...)
}

func (s *standardLogger) Errorf(format string, v ...interface{}) {
	s.printf(LevelError, format, v...)
}

func (s *standardLogger) Panicf(format string, v ...interface{}) {
	s.printf(LevelPanic, format, v...)
}

func (s *standardLogger) Logger() *log.Logger {
	return s.logger
}

func (s *standardLogger) WithPrefix(prefix string) Logger {
	return newStandardLogger(s.w, s.verbosity, prefix)
}

// Logfer is a thing that has only a Logf() method, like for instance,
// testing.T or testing.B.
type Logfer interface {
	Logf(format string, v ...interface{})
}

// LogfLogger is a test logger that wraps something that has a Logf interface
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

func (ll *LogfLogger) Infof(format string, v ...interface{}) {
	ll.wrapped.Logf(format, v...)
}

func (ll *LogfLogger) Warnf(format string, v ...interface{}) {
	ll.wrapped.Logf(format, v...)
}

func (ll *LogfLogger) Errorf(format string, v ...interface{}) {
	ll.wrapped.Logf(format, v...)
}

func (ll *LogfLogger) Panicf(format string, v ...interface{}) {
	ll.wrapped.Logf(format, v...)
}

// WithPrefix does nothing for LogfLogger because I'm lazy.
func (ll *LogfLogger) WithPrefix(prefix string) Logger {
	return ll
}

func NewLogfLogger(l Logfer) *LogfLogger {
	return &LogfLogger{wrapped: l}
}

// bufferLogger represents a test Logger that holds log messages
// in a buffer for review.
type bufferLogger struct {
	buf *bytes.Buffer
	mu  sync.Mutex
}

// NewBufferLogger returns a new instance of bufferLogger.
func NewBufferLogger() *bufferLogger {
	return &bufferLogger{
		buf: &bytes.Buffer{},
	}
}

func (b *bufferLogger) Printf(format string, v ...interface{}) {
	b.mu.Lock()
	defer b.mu.Unlock()
	s := fmt.Sprintf(format, v...)
	_, err := b.buf.WriteString(s)
	if err != nil {
		panic(err)
	}
}

func (b *bufferLogger) Debugf(format string, v ...interface{}) {}
func (b *bufferLogger) Infof(format string, v ...interface{}) {
	b.Printf(LevelPrefix(1)+format, v...)
}
func (b *bufferLogger) Warnf(format string, v ...interface{}) {
	b.Printf(LevelPrefix(2)+format, v...)
}
func (b *bufferLogger) Errorf(format string, v ...interface{}) {
	b.Printf(LevelPrefix(3)+format, v...)
}
func (b *bufferLogger) Panicf(format string, v ...interface{}) {
	b.Printf(LevelPrefix(4)+format, v...)
}

// WithPrefix does nothing for bufferLogger because I'm lazy.
func (b *bufferLogger) WithPrefix(prefix string) Logger {
	return b
}

func (b *bufferLogger) ReadAll() ([]byte, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return io.ReadAll(b.buf)
}
