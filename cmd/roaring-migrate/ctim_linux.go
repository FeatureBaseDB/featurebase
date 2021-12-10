//go:build linux
// +build linux

package main

import (
	"syscall"
)

func CTimeNano(stat *syscall.Stat_t) int64 {
	return stat.Ctim.Nano()
}
