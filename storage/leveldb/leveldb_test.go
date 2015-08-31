package leveldb_test

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/umbel/pilosa"
	"github.com/umbel/pilosa/storage/leveldb"
	"github.com/umbel/pilosa/util"
)

func init() {
	util.SetupStatsd()
}

func TestStorage_Fetch(t *testing.T) {
	s := MustOpenStorage()
	defer s.Close()
}

// Storage represents a test wrapper for leveldb.Storage.
type Storage struct {
	*leveldb.Storage
}

// NewStorage returns a new instance of Storage with a temporary path.
func NewStorage() *Storage {
	// Create temporary path.
	f, err := ioutil.TempFile("", "pilosa-leveldb-")
	if err != nil {
		panic(err)
	}
	f.Close()
	os.Remove(f.Name())

	return &Storage{leveldb.NewStorage(pilosa.StorageOptions{
		LevelDBPath: f.Name(),
	})}
}

// MustOpenStorage returns a new, opened instance of Storage.
func MustOpenStorage() *Storage {
	s := NewStorage()
	if err := s.Open(); err != nil {
		panic(err)
	}
	return s
}

// Close closes the storage and removes the underlying data file.
func (s *Storage) Close() error {
	defer os.Remove(s.Path())
	return s.Storage.Close()
}
