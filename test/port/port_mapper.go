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

package port

import (
	"fmt"
	"log"
	"net"
	"syscall"
)

func ColonZeroString(port int) string {
	return fmt.Sprintf(":%d", port)
}

func GetPort(wrapper func(int) error, retries int) error {
	f := func(ports []int) error { return wrapper(ports[0]) }
	return GetPorts(f, 1, retries)
}

func GetPorts(wrapper func([]int) error, requestedPorts, retries int) error {
	for i := 0; i < retries; i++ {
		// get all requested ports
		listeners := make([]net.Listener, requestedPorts)
		ports := make([]int, requestedPorts)
		for i := 0; i < requestedPorts; i++ {
			l, err := net.Listen("tcp", ":0")
			if err != nil {
				log.Println("[port_mapper] error getting a free port", err)
				return GetPorts(wrapper, requestedPorts, retries-1)
			}

			ports[i] = l.Addr().(*net.TCPAddr).Port
			listeners[i] = l
		}
		for _, l := range listeners {
			if err := l.Close(); err != nil {
				log.Println("[port_mapper] error closing the listener", err)
			}
		}
		// send to wrapper and check output error
		err := wrapper(ports)
		if err == syscall.EADDRINUSE {
			log.Println("[port_mapper] address already in use error calling the wrapper", err)
			// only retry on address already in use error
			continue
		}

		return err
	}

	return nil
}
