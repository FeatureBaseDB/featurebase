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
	"net"
	"sync"
	"syscall"
)

const BlockOfPortsSize = 2000

// GlobalPortMap avoids many races and port conflicts when setting
// up ports for test clusters. Used for tests only.
var GlobalPortMap *globalPortMapper
var GlobalPortMapMu sync.Mutex

func init() {
	RaiseUlimitNofiles()
	GlobalPortMap = NewGlobalPortMapper(BlockOfPortsSize)
}

func MustGetPort() int {
	port := GlobalPortMap.MustGetPort()
	return port
}
func ColonZeroString() string {
	return fmt.Sprintf(":%d", MustGetPort())
}

// globalPortMapper maintains a pool of available ports by
// holding them open until GetPort() is called.
type globalPortMapper struct {
	numPorts   int
	availPorts []net.Listener
}

// newGlobalPortMapper initalizes a globalPortMapper with n ports.
func NewGlobalPortMapper(n int) (pm *globalPortMapper) {
	GlobalPortMapMu.Lock()
	defer GlobalPortMapMu.Unlock()

	pm = &globalPortMapper{
		numPorts: n,
	}
	pm.allocateAtTop()
	return
}

var _ = (&globalPortMapper{}).allocate // happy linter

func (pm *globalPortMapper) allocate() {
	println("888888 allocate ports called")
	pm.availPorts = make([]net.Listener, pm.numPorts)
	i := 0
	for i < pm.numPorts {
		lsn, err := net.Listen("tcp", ":0")
		if err != nil {
			panic(err)
		}
		// must be available to UDP too!
		addr := lsn.Addr()
		port := addr.(*net.TCPAddr).Port
		udpConn, err := net.ListenUDP("udp4", &net.UDPAddr{
			IP:   net.IP{}, // listen on all non-multicast addresses...
			Port: port,
		})
		if err != nil {
			fmt.Printf("UDP port %v was available on tcp but not udp: %v\n", port, err)
			lsn.Close()
		} else {
			_ = udpConn.Close()
			if lsn == nil {
				panic("lsn should never be nil")
			}
			pm.availPorts[i] = lsn
			i++
			//println("------ bulk reservation: port mapping reserves port ", lsn.Addr().(*net.TCPAddr).Port)
		}
	}
}

func (pm *globalPortMapper) allocateAtTop() {
	println("888888 allocateAtTop ports called")
	pm.availPorts = make([]net.Listener, pm.numPorts)
	i := 0

	for next := 65000; i < pm.numPorts && next > 1000; next-- {
		lsn, err := net.Listen("tcp", fmt.Sprintf(":%d", next))
		if err != nil {
			//fmt.Printf("next=%v, err = %v\n", next+1, err)
			continue
		}
		//println("next was avail: ", next+1)

		// must be available to UDP too!
		addr := lsn.Addr()
		port := addr.(*net.TCPAddr).Port
		udpConn, err := net.ListenUDP("udp4", &net.UDPAddr{
			IP:   net.IP{}, // listen on all non-multicast addresses...
			Port: port,
		})
		if err != nil {
			fmt.Printf("UDP port %v was available on tcp but not udp: %v\n", port, err)
			lsn.Close()
		} else {
			_ = udpConn.Close()
			if lsn == nil {
				panic("lsn should never be nil")
			}
			pm.availPorts[i] = lsn
			i++
			//println("------ bulk reservation: port mapping reserves port ", lsn.Addr().(*net.TCPAddr).Port)
		}
	}
}

func (pm *globalPortMapper) GetPort() (port int, err error) {
	GlobalPortMapMu.Lock()
	defer GlobalPortMapMu.Unlock()

	i := len(pm.availPorts)
	if i < 1 {
		panic(fmt.Sprintf("ran out of ports, allocate more up front for these tests. had BlockOfPortsSize=%v", BlockOfPortsSize))
	}

	lsn := pm.availPorts[i-1]
	addr := lsn.Addr()
	port = addr.(*net.TCPAddr).Port

	println("port mapping gives out port ", port)
	lsn.Close()
	pm.availPorts = pm.availPorts[:i-1]

	// verify that it IS usable again
	lsn, err = net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		panic(err)
	}
	lsn.Close()

	return port, nil
}

func (pm *globalPortMapper) MustGetPort() int {
	port, err := pm.GetPort()
	if err != nil {
		panic(err)
	}
	//fmt.Printf("port %v allocated at stack:\n'%v'", port, string(debug.Stack()))
	return port
}

// RaiseUlimitNofiles raises the number of open file handles
// to at least 3000. This allows us to reserve 2000 open
// ports for the etcd tests that need to know their ports
// up front and not have them re-used quickly (since
// a socket might be still in TIME_WAIT closing state if
// the server closes first).
func RaiseUlimitNofiles() {
	var rLimit syscall.Rlimit
	err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rLimit)
	if err != nil {
		panic(fmt.Sprintf("Error Getting Rlimit '%v'", err))
	}

	if rLimit.Cur < 6000 {
		rLimit.Cur = 6000
		err = syscall.Setrlimit(syscall.RLIMIT_NOFILE, &rLimit)
		if err != nil {
			fmt.Println("Error Setting Rlimit ", err)
		}
	}
	fmt.Printf("RaiseUlimitNofiles is now %v\n", rLimit.Cur)
}
