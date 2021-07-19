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
	"context"
	"fmt"
	"net"
	"testing"

	"github.com/molecula/featurebase/v2/pg"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

// ShutdownFunc is a function to use to shut down a test fixture.
// This function will send a shutdown signal and then wait for completion.
type ShutdownFunc func() error

// Finish invokes the shutdown function and fails the test if an error occurs.
func (f ShutdownFunc) Finish(tb testing.TB, name string) {
	err := f()
	if err != nil {
		tb.Errorf("failed to shut down %s: %v", name, err)
	}
}

// ServeListener serves postgres wire protocol on a listener.
func ServeListener(listener net.Listener, server *pg.Server) (net.Addr, ShutdownFunc, error) {
	laddr := listener.Addr()

	ctx, cancel := context.WithCancel(context.Background())
	var eg errgroup.Group
	eg.Go(func() error { return server.Serve(ctx, listener) })

	return laddr,
		func() error {
			cancel()
			return eg.Wait()
		},
		nil
}

// ServeTCP creates a TCP listener and serves postgres wire protocol on it.
func ServeTCP(addr string, server *pg.Server) (net.Addr, ShutdownFunc, error) {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, nil, errors.Wrap(err, "listening on TCP")
	}
	return ServeListener(listener, server)
}

// ServeTLSListener sets up TLS on the server and invokes ServeListener.
func ServeTLSListener(listener net.Listener, server *pg.Server) (net.Addr, ShutdownFunc, error) {
	err := SetupTLS(server)
	if err != nil {
		return nil, nil, errors.Wrap(err, "server TLS setup failed")
	}

	var tries int = 5
	var netAddr net.Addr
	var shutdown ShutdownFunc

	for i := 0; i < tries; i++ {
		if i > 0 {
			fmt.Printf("--- try serving TLS again: %d\n", i)
		}
		if netAddr, shutdown, err = ServeListener(listener, server); err == nil {
			break
		}
	}
	return netAddr, shutdown, err
}

// ServeTLS sets up TLS on the server and invokes ServeTCP.
func ServeTLS(addr string, server *pg.Server) (net.Addr, ShutdownFunc, error) {
	err := SetupTLS(server)
	if err != nil {
		return nil, nil, errors.Wrap(err, "server TLS setup failed")
	}

	var tries int = 5
	var netAddr net.Addr
	var shutdown ShutdownFunc

	for i := 0; i < tries; i++ {
		if i > 0 {
			fmt.Printf("--- try serving TLS again: %d\n", i)
		}
		if netAddr, shutdown, err = ServeTCP(addr, server); err == nil {
			break
		}
	}
	return netAddr, shutdown, err
}

// ConnectFunc is a function to connect to a server.
type ConnectFunc func() (net.Conn, error)

// ServeMem serves postgres on in-memory connections.
// TLS does not work here, as it relies on the OS to buffer and discard data.
func ServeMem(server *pg.Server) (ConnectFunc, ShutdownFunc, error) {
	listener := &inMemoryListener{
		ch:     make(chan net.Conn),
		closed: make(chan struct{}),
	}

	ctx, cancel := context.WithCancel(context.Background())
	var eg errgroup.Group
	eg.Go(func() error { return server.Serve(ctx, listener) })

	return listener.Dial,
		func() error {
			cancel()
			return eg.Wait()
		},
		nil
}
