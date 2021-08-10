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

// Package syswrap wraps syscalls (just mmap right now) in order to impose a
// global in-process limit on the maximum number of active mmaps.
package syswrap

import (
	"sync"
	"sync/atomic"
	"syscall"
	"strings"

	"github.com/pkg/errors"
)

var mapCount uint64

var ErrMaxMapCountReached = errors.New("maximum map count reached")

// maxMapCount default to slightly less than the typical
// default on Linux (65K). We want to leave some
// overhead for (e.g.) the Go runtime.
var maxMapCount uint64 = 60000
var mu sync.RWMutex

// SetMaxMapCount sets the maximum map count, and returns the previous maximum.
func SetMaxMapCount(max uint64) uint64 {
	mu.Lock()
	prev := maxMapCount
	maxMapCount = max
	mu.Unlock()
	return prev
}

// Mmap increments the global map count, and then calls syscall.Mmap. It
// decrements the map count and returns an error if the count was over the
// limit. If syscall.Mmap returns an error it also decrements the count.
func Mmap(fd int, offset int64, length int, prot int, flags int) (data []byte, err error) {
	mu.RLock()
	defer mu.RUnlock()
	if newCount := atomic.AddUint64(&mapCount, 1); newCount > maxMapCount {
		atomic.AddUint64(&mapCount, ^uint64(0)) // decrement
		return nil, ErrMaxMapCountReached
	}
	data, err = syscall.Mmap(fd, offset, length, prot, flags)
	if err != nil {
		atomic.AddUint64(&mapCount, ^uint64(0)) // decrement
		if strings.Contains(err.Error(), "cannot allocate memory") {
			err = errors.New("mmap 'cannot allocate memory' — please see the troubleshooting how-to in the FeatureBase docs.")
		}
	}
	return data, err
}

// Munmap calls sycall.Munmap, and then decrements the global map count if there
// was no error.
func Munmap(b []byte) (err error) {
	err = syscall.Munmap(b)
	if err == nil {
		atomic.AddUint64(&mapCount, ^uint64(0)) // decrement
	}
	return err
}
