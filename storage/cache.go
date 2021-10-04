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
package storage

import (
	"sync/atomic"
)

// if enableRowCache, then we must not return mmap-ed memory
// directly, but only a copy.
var enableRowcache int64 = 1

// SetRowCacheOn should only be called in NewHolder before
// all other reads.
func SetRowCacheOn(on bool) {
	if on {
		atomic.StoreInt64(&enableRowcache, 1)
	} else {
		atomic.StoreInt64(&enableRowcache, 0)
	}
}

func RowCacheEnabled() bool {
	return atomic.LoadInt64(&enableRowcache) == 1
}
