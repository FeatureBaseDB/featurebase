// +build darwin

package main

import (
	"syscall"
	"time"
)

func CTimeNano(stat *syscall.Stat_t) int64 {
	ts := stat.Ctimespec
	time.Unix(int64(ts.Sec), int64(ts.Nsec))
	return int64(ts.Sec)*1e9 + int64(ts.Nsec)
}
