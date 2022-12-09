// Package snapshotter provides the core snapshotter structs.
package snapshotter

import (
	"bytes"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"sync"

	"github.com/featurebasedb/featurebase/v3/errors"
	"github.com/featurebasedb/featurebase/v3/logger"
)

type Snapshotter struct {
	mu sync.RWMutex

	dataDir string

	logger logger.Logger
}

func New(cfg Config) *Snapshotter {
	return &Snapshotter{
		dataDir: cfg.DataDir,
		logger:  logger.NopLogger,
	}
}

// SetLogger sets the logger used for logging messages.
func (s *Snapshotter) SetLogger(l logger.Logger) {
	s.logger = l
}

func (s *Snapshotter) Write(bucket string, key string, version int, rc io.ReadCloser) error {
	fKey := fullKey(bucket, key, version)
	snapshotFile, err := s.snapshotFileByKey(fKey)
	if err != nil {
		return errors.Wrapf(err, "shapshotting file by key: %s", fKey)
	}
	defer snapshotFile.Close()

	defer rc.Close()
	if _, err := snapshotFile.ReadFrom(rc); err != nil {
		return errors.Wrap(err, "reading from shapshot file")
	}

	return snapshotFile.Sync()
}

func (s *Snapshotter) Read(bucket string, key string, version int) (io.ReadCloser, error) {
	_, filePath := s.paths(fullKey(bucket, key, version))
	f, err := os.Open(filePath)
	if err != nil {
		if e, ok := err.(*fs.PathError); ok {
			return nil, e
		}
		return nil, errors.Wrapf(err, "reading snapshot file: %s", filePath)
	}

	return f, nil
}

// WriteTo is exactly the same as Write, except that it takes an io.WriteTo
// instead of an io.ReadCloser. This needs to be cleaned up so that we're only
// using one or the other.
func (s *Snapshotter) WriteTo(bucket string, key string, version int, wrTo io.WriterTo) error {
	buf := &bytes.Buffer{}
	if _, err := wrTo.WriteTo(buf); err != nil {
		return errors.Wrap(err, "writing to buffer")
	}
	return s.Write(bucket, key, version, io.NopCloser(buf))
}

// paths takes a key and returns the full file path (including the root data
// directory) as well as the full directory path (i.e. the file path without the
// file portion).
func (s *Snapshotter) paths(key string) (string, string) {
	filePath := path.Join(s.dataDir, key)
	dirPath, _ := path.Split(filePath)
	return dirPath, filePath
}

// snapshotFileByKey returns a pointer to the file specified by key. If the file
// does not exist, the file is created (along with any directories in which the
// file is nested).
func (s *Snapshotter) snapshotFileByKey(key string) (*os.File, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	dirPath, filePath := s.paths(key)

	// make directories
	if err := os.MkdirAll(dirPath, 0777); err != nil {
		return nil, errors.Wrapf(err, "making directory: %s", dirPath)
	}

	// open snapshot file
	f, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, errors.Wrapf(err, "opening shapshot file: %s", filePath)
	}

	return f, nil
}

// fullKey returns the full file key including the bucket and version.
func fullKey(bucket string, key string, version int) string {
	return path.Join(bucket, key, fmt.Sprintf("%d", version))
}
