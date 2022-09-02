// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
// Package syswrap wraps syscalls (just mmap right now) in order to impose a
// global in-process limit on the maximum number of active mmaps.
package syswrap

import (
	"strings"
	"sync"
	"sync/atomic"
	"syscall"

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
