// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0

//go:build linux && arm64
// +build linux,arm64

package idk

import (
	"syscall"
)

// dup is an alias for syscall.Dup2 on darwin-amd64, darwin-arm64,
// linux-amd64, linux-arm or syscall.Dup3 on linux-arm64
func dup(oldfd int, newfd int) error {
	return syscall.Dup3(oldfd, newfd, 0)
}
