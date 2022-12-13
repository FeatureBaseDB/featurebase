// Package writelogger provides the writelogger structs.
package writelogger

import (
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"strconv"
	"sync"
	"syscall"

	"github.com/molecula/featurebase/v3/dax/computer"
	"github.com/molecula/featurebase/v3/errors"
	"github.com/molecula/featurebase/v3/logger"
)

type WriteLogger struct {
	dataDir string

	mu        sync.RWMutex
	logFiles  map[string]*os.File
	lockFiles map[string]*os.File

	logger logger.Logger
}

func New(cfg Config) *WriteLogger {
	return &WriteLogger{
		dataDir:   cfg.DataDir,
		logFiles:  make(map[string]*os.File),
		lockFiles: make(map[string]*os.File),
		logger:    logger.NopLogger,
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

	_, err = logFile.Write(append(message, "\n"...))
	if err != nil {
		return errors.Wrapf(err, "writing to log file %s", logFile.Name())
	}
	err = logFile.Sync()
	return errors.Wrapf(err, "syncing log file %s", logFile.Name())
}

func (w *WriteLogger) List(bucket, key string) ([]computer.WriteLogInfo, error) {
	dirpath := path.Join(w.dataDir, bucket, key)

	entries, err := os.ReadDir(dirpath)
	if err != nil {
		if pe, ok := err.(*os.PathError); ok && pe.Err == syscall.ENOENT {
			return nil, nil
		}
		return nil, errors.Wrap(err, "reading directory")
	}

	wLogs := make([]computer.WriteLogInfo, len(entries))
	for i, entry := range entries {
		version, err := strconv.ParseInt(entry.Name(), 10, 64)
		if err != nil {
			return nil, errors.Wrapf(err, "writelog filename '%s' could not be parsed to version number", entry.Name())
		}
		wLogs[i] = computer.WriteLogInfo{
			Version: int(version),
		}
	}
	return wLogs, nil
}

func (w *WriteLogger) LogReader(bucket, key string, version int) (io.ReadCloser, error) {
	return w.LogReaderFrom(bucket, key, version, 0)
}

func (w *WriteLogger) LogReaderFrom(bucket string, key string, version int, offset int) (io.ReadCloser, error) {
	_, filePath := w.paths(fullKey(bucket, key, version))

	f, err := os.Open(filePath)
	if err != nil {
		if e, ok := err.(*fs.PathError); ok {
			return nil, e
		}
		return nil, err
	}
	if offset > 0 {
		f.Seek(int64(offset), io.SeekStart)
	}
	w.logger.Debugf("WriteLogger LogReader file: %s", f.Name())

	return f, nil
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

func (w *WriteLogger) lockFile(bucket, key string) (string, string) {
	lockFile := path.Join(w.dataDir, bucket, fmt.Sprintf("_lock_%s", key))
	return path.Dir(lockFile), lockFile
}

func (w *WriteLogger) Lock(bucket, key string) error {
	lockDir, lockFile := w.lockFile(bucket, key)

	if err := os.MkdirAll(lockDir, 0777); err != nil {
		return errors.Wrapf(err, "lock dir %s", lockDir)
	}
	f, err := os.OpenFile(lockFile, os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		return errors.Wrapf(err, "opening lock file: %s", lockFile)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.lockFiles[lockFile] = f
	// fd, err = syscall.Open(lockFile, syscall.O_RDWR|syscall.O_CREAT, 0644)
	// if err != nil {
	// 	return 0, errors.Wrapf(err, "syscall opening %s", lockFile)
	// }
	// err = syscall.FcntlFlock(uintptr(fd), syscall.F_SETLK, &syscall.Flock_t{
	// 	Type: syscall.F_WRLCK,
	// })
	return errors.Wrap(err, "locking")

}

func (w *WriteLogger) Unlock(bucket, key string) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	// TODO(jaffee) since the file isn't guaranteed to be removed if
	// the process is killed, we should actually use flock instead of
	// EXCL file creation. Problem with that is it makes testing
	// tricky because file handles from the same process are able to
	// acquire the flock simultaneously. Headache.
	_, lockFile := w.lockFile(bucket, key)
	f, ok := w.lockFiles[lockFile]
	if !ok {
		return errors.New(errors.ErrUncoded, "couldn't find file to unlock")
	}
	f.Close()
	err := os.Remove(lockFile)
	delete(w.lockFiles, lockFile)

	// defer func() {
	// 	err := syscall.Close(fd)
	// 	if err != nil {
	// 		w.logger.Printf("error closing lockfile %s", lockFile)
	// 	}
	// }()
	// err := syscall.FcntlFlock(uintptr(fd), syscall.F_SETLK, &syscall.Flock_t{
	// 	Type: syscall.F_UNLCK,
	// })
	return errors.Wrap(err, "closing lock file")
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
