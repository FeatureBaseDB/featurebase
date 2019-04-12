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

func SetMaxFileCount(max uint64) {
	fileMu.Lock()
	maxFileCount = max
	fileMu.Unlock()
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
