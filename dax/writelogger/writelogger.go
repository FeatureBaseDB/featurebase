// Package writelogger provides the writelogger structs.
package writelogger

import (
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"sync"

	"github.com/featurebasedb/featurebase/v3/errors"
	"github.com/featurebasedb/featurebase/v3/logger"
)

type WriteLogger struct {
	mu sync.RWMutex

	dataDir  string
	logFiles map[string]*os.File

	logger logger.Logger
}

func New(cfg Config) *WriteLogger {
	return &WriteLogger{
		dataDir:  cfg.DataDir,
		logFiles: make(map[string]*os.File),
		logger:   logger.NopLogger,
	}
}

// SetLogger sets the logger used for logging messages. Note, this is not the
// same "logger" that the WriteLogger represents, which logs data writes.
func (w *WriteLogger) SetLogger(l logger.Logger) {
	w.logger = l
}

func (w *WriteLogger) AppendMessage(bucket string, key string, version int, message []byte) error {
	fKey := fullKey(bucket, key, version)
	logFile, err := w.logFileByKey(fKey)
	if err != nil {
		return errors.Wrapf(err, "getting log file by key: %s", fKey)
	}

	logFile.Write(append(message, "\n"...))
	logFile.Sync()

	return nil
}

func (w *WriteLogger) LogReader(bucket string, key string, version int) (io.Reader, io.Closer, error) {
	_, filePath := w.paths(fullKey(bucket, key, version))

	f, err := os.Open(filePath)
	if err != nil {
		if e, ok := err.(*fs.PathError); ok {
			return nil, nil, e
		}
		return nil, nil, err
	}
	w.logger.Debugf("WriteLogger LogReader file: %s", f.Name())

	return f, f, nil
}

func (w *WriteLogger) DeleteLog(bucket string, key string, version int) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	fullKey := fullKey(bucket, key, version)

	f, ok := w.logFiles[fullKey]
	if !ok {
		return nil
	}

	// Close the log file.
	if err := f.Close(); err != nil {
		return errors.Wrap(err, "closing log file")
	}

	// Remove the log file.
	return os.Remove(f.Name())
}

// paths takes a key and returns the full file path (including the root data
// directory) as well as the full directory path (i.e. the file path without the
// file portion).
func (w *WriteLogger) paths(key string) (string, string) {
	filePath := path.Join(w.dataDir, key)
	dirPath, _ := path.Split(filePath)
	return dirPath, filePath
}

// logFileByKey returns a pointer to the file specified by key. If the file does
// not exist, the file is created (along with any directories in which the file
// is nested).
func (w *WriteLogger) logFileByKey(key string) (*os.File, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if f, ok := w.logFiles[key]; ok {
		return f, nil
	}

	dirPath, filePath := w.paths(key)

	// make directories
	if err := os.MkdirAll(dirPath, 0777); err != nil {
		return nil, errors.Wrapf(err, "making direcory: %s", dirPath)
	}

	// open log file
	f, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, errors.Wrapf(err, "opening file: %s", filePath)
	}
	w.logFiles[key] = f

	return f, nil
}

// fullKey returns the full file key including the bucket and version.
func fullKey(bucket string, key string, version int) string {
	return path.Join(bucket, key, fmt.Sprintf("%d", version))
}
