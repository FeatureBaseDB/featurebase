// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package syswrap

import (
	"os"
	"sync"
	"sync/atomic"
)

var fileCount uint64

// maxFileCount is the soft limit on the number of open files. syswrap.OpenFile
// will warn when this limit is passed.
var maxFileCount uint64 = 500000
var fileMu sync.RWMutex

func SetMaxFileCount(max uint64) uint64 {
	fileMu.Lock()
	prev := maxFileCount
	maxFileCount = max
	fileMu.Unlock()
	return prev
}

// OpenFile passes the arguments along to os.OpenFile while incrementing a
// counter. If the counter is above the maximum, it returns mustClose true to
// signal the calling function that it should not keep the file open
// indefinitely. Files opened with this function should be closed by
// syswrap.CloseFile.
func OpenFile(name string, flag int, perm os.FileMode) (file *os.File, mustClose bool, err error) {
	file, err = os.OpenFile(name, flag, perm)
	fileMu.RLock()
	defer fileMu.RUnlock()
	if newCount := atomic.AddUint64(&fileCount, 1); newCount > maxFileCount {
		mustClose = true
	}
	return file, mustClose, err
}

// CloseFile decrements the global count of open files and closes the file.
func CloseFile(f *os.File) error {
	atomic.AddUint64(&fileCount, ^uint64(0)) // decrement
	return f.Close()
}
