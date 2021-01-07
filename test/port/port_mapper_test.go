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

package port_test

import (
	"fmt"
	"net"
	"testing"

	"github.com/pilosa/pilosa/v2/test/port"
)

func TestPortsAreUnique(t *testing.T) {

	local := port.NewGlobalPortMapper(port.BlockOfPortsSize)

	oracle := make(map[int]bool)

	for i := 0; i < port.BlockOfPortsSize; i++ {
		port := local.MustGetPort()
		if oracle[port] {
			panic(fmt.Sprintf("port %v was already issued!", port))
		}
		oracle[port] = true
	}
}

func TestPortsAreUsable(t *testing.T) {

	local := port.NewGlobalPortMapper(port.BlockOfPortsSize)

	oracle := make(map[int]bool)

	for i := 0; i < port.BlockOfPortsSize; i++ {
		port := local.MustGetPort()
		if oracle[port] {
			panic(fmt.Sprintf("port %v was already issued!", port))
		}
		lsn, err := net.Listen("tcp", fmt.Sprintf(":%v", port))
		if err != nil {
			panic(err)
		}
		oracle[port] = true
		lsn.Close()
	}
}
