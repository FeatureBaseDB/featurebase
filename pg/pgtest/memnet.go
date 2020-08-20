// Copyright 2020 Pilosa Corp.
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

package pgtest

import (
	"errors"
	"net"
	"sync"
)

// errListenerClosed is an error returned when the listener is closed.
var errListenerClosed = errors.New("listener closed")

type inMemoryListener struct {
	ch     chan net.Conn
	closed chan struct{}
	once   sync.Once
}

func (l *inMemoryListener) Accept() (net.Conn, error) {
	select {
	case <-l.closed:
		return nil, errListenerClosed
	default:
	}
	select {
	case conn := <-l.ch:
		return conn, nil
	case <-l.closed:
		return nil, errListenerClosed
	}
}

func (l *inMemoryListener) Close() error {
	l.once.Do(func() { close(l.closed) })

	return nil
}

type memAddr struct{}

func (a memAddr) Network() string { return "memory" }
func (a memAddr) String() string  { return "memory" }

func (l *inMemoryListener) Addr() net.Addr {
	return memAddr{}
}

func (l *inMemoryListener) Dial() (net.Conn, error) {
	serverConn, clientConn := net.Pipe()
	select {
	case l.ch <- serverConn:
		return clientConn, nil
	case <-l.closed:
		serverConn.Close()
		clientConn.Close()
		return nil, errListenerClosed
	}
}
