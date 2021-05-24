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

// +build linux,arm64

package server

import (
	"syscall"
)

// dup is an alias for syscall.Dup2 on most platforms or syscall.Dup3 on ARM
func (m *Command) dup(oldfd int, newfd int) error {
	return syscall.Dup3(oldfd, newfd, 0)
}
