// Copyright 2021 Molecula Corp. All rights reserved.
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
