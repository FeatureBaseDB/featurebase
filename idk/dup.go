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

//go:build darwin || (linux && !arm64)
// +build darwin linux,!arm64

package idk

import (
	"syscall"
)

// dup is an alias for syscall.Dup2 on darwin-amd64, darwin-arm64,
// linux-amd64, linux-arm or syscall.Dup3 on linux-arm64
func dup(oldfd int, newfd int) error {
	return syscall.Dup2(oldfd, newfd)
}
