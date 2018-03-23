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

package test

import (
	"io/ioutil"
	"os"
	"runtime"
	"sync"
	"testing"

	"github.com/pilosa/pilosa"
	"github.com/pilosa/pilosa/boltdb"
)

// AttrStore represents a test wrapper for pilosa.AttrStore.
type AttrStore struct {
	pilosa.AttrStore
}

// NewAttrStore returns a new instance of AttrStore.
func NewAttrStore(string) pilosa.AttrStore {
	f, err := ioutil.TempFile("", "pilosa-attr-")
	if err != nil {
		panic(err)
	}
	f.Close()
	os.Remove(f.Name())

	return &AttrStore{boltdb.NewAttrStore(f.Name())}
}

func BenchmarkAttrStore_Duplicate(b *testing.B) {
	s := MustOpenAttrStore()
	defer s.Close()

	// Set attributes.
	const n = 5
	for i := 0; i < n; i++ {
		if err := s.SetAttrs(uint64(i), map[string]interface{}{"A": 100, "B": "foo", "C": true, "D": 100.2}); err != nil {
			b.Fatal(err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()

	// Update attributes with an existing subset.
	cpuN := runtime.GOMAXPROCS(0)
	var wg sync.WaitGroup
	for i := 0; i < cpuN; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < b.N/cpuN; j++ {
				if err := s.SetAttrs(uint64(j%n), map[string]interface{}{"A": int64(100), "B": "foo", "D": 100.2}); err != nil {
					b.Fatal(err)
				}
			}
		}()
	}
	wg.Wait()
}

// MustOpenAttrStore returns a new, opened attribute store at a temporary path. Panic on error.
func MustOpenAttrStore() pilosa.AttrStore {
	s := NewAttrStore("")
	if err := s.Open(); err != nil {
		panic(err)
	}
	return s
}

// Close closes the database and removes the underlying data.
func (s *AttrStore) Close() error {
	defer os.RemoveAll(s.Path())
	return s.AttrStore.Close()
}
