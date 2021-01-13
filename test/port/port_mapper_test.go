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
	"log"
	"net"
	"testing"

	"github.com/pilosa/pilosa/v2/test/port"
)

func TestPortsAreUnique(t *testing.T) {
	portmap := make(map[int]struct{})
	port.GetPorts(func(ports []int) error {
		for _, p := range ports {
			log.Println("PORTTT", p)
			if _, exists := portmap[p]; exists {
				panic(fmt.Sprintf("port %v was already issued!", p))
			}
			portmap[p] = struct{}{}
		}

		return nil
	}, 2000, 3)
}

func TestPortsAreUsable(t *testing.T) {
	portmap := make(map[int]struct{})
	port.GetPorts(func(ports []int) error {
		for _, p := range ports {
			if _, exists := portmap[p]; exists {
				panic(fmt.Sprintf("port %v was already issued!", p))
			}

			lsn, err := net.Listen("tcp", fmt.Sprintf(":%v", p))
			if err != nil {
				panic(err)
			}

			portmap[p] = struct{}{}
			lsn.Close()
		}

		return nil
	}, 2000, 3)

}
