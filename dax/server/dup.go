// Copyright 2022 Molecula Corp. All rights reserved.
//go:build darwin || (linux && !arm64)
// +build darwin linux,!arm64

package server

import (
	"syscall"
)

// dup is an alias for syscall.Dup2 on darwin-amd64, darwin-arm64,
// linux-amd64, linux-arm or syscall.Dup3 on linux-arm64
func (m *Command) dup(oldfd int, newfd int) error {
	return syscall.Dup2(oldfd, newfd)
}
