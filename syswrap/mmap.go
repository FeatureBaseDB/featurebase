// Package syswrap wraps syscalls (just mmap right now) in order to impose a
// global in-process limit on the maximum number of active mmaps.
package syswrap

import (
	"sync/atomic"
	"syscall"

	"github.com/pkg/errors"
)

var mapCount uint64

var ErrMaxMapCountReached = errors.New("maximum map count reached")

// MaxMapCount default to slightly less than the typical
// default on Linux (65K). We want to leave some
// overhead for (e.g.) the Go runtime.
var MaxMapCount uint64 = 60000

// Mmap increments the global map count, and then calls syscall.Mmap. It
// decrements the map count and returns an error if the count was over the
// limit. If syscall.Mmap returns an error it also decrements the count.
func Mmap(fd int, offset int64, length int, prot int, flags int) (data []byte, err error) {
	if newCount := atomic.AddUint64(&mapCount, 1); newCount > MaxMapCount {
		atomic.AddUint64(&mapCount, ^uint64(0)) // decrement
		return nil, ErrMaxMapCountReached
	}
	data, err = syscall.Mmap(fd, offset, length, prot, flags)
	if err != nil {
		atomic.AddUint64(&mapCount, ^uint64(0)) // decrement
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
