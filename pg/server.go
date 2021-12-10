// Copyright 2021 Molecula Corp. All rights reserved.
package pg

import (
	"context"
	"crypto/tls"
	"net"
	"sync"
	"time"

	"github.com/molecula/featurebase/v2/logger"
)

// Server is a postgres wire protocol server.
type Server struct {
	// QueryHandler will be used to serve query requests.
	QueryHandler QueryHandler

	// TypeEngine is the type engine to use to result columns in query requests.
	TypeEngine TypeEngine

	// TLSConfig is the TLS configuration to use to serve postgres TLS connections.
	TLSConfig *tls.Config

	// StartupTimeout is the timeout to use for connection startup.
	// If a connection fails to set up a protocol before this completes, it will be terminated.
	StartupTimeout time.Duration

	// ReadTimeout is the timeout to apply for active reads (reads during the lifetime of a command).
	// This timeout does not apply to an idling connection.
	ReadTimeout time.Duration

	// WriteTimeout is the timeout to apply to network writes.
	WriteTimeout time.Duration

	// MaxStartupSize is the maximum size of the startup packet (in bytes).
	// This defaults to 2^31-1 bytes, which is the maximum size allowed by the protocol.
	MaxStartupSize uint32

	// ConnectionLimit is the maximum number of connections to allow at once.
	ConnectionLimit uint16

	// Logger is the logger to use for error conditions and state changes.
	Logger logger.Logger

	// CancellationManager is the cancellation manager to use.
	// If this is not set, no cancellations will be applied.
	CancellationManager CancellationManager
	lookerChannel       chan struct{}
	mu                  sync.Mutex
	portals             []*Portal
}

// ServeConn serves a single connection.
func (s *Server) ServeConn(ctx context.Context, conn net.Conn) error {
	return s.handle(ctx, conn)
}

// Serve accepts postgres connections from a listener and processes them.
// If the context is cancelled, this will stop accepting requests and wait until all connections have terminated.
// No error will be returned if terminated by context cancellation.
// This will close the connection for the caller.
func (s *Server) Serve(ctx context.Context, l net.Listener) (err error) {
	// Ignore errors triggered by a shutdown.
	// Also propogate any error from terminating the listener.
	var cerr error
	defer func(ctx context.Context) {
		if ctx.Err() == context.Canceled {
			err = cerr
		}
	}(ctx)
	// TODO (twg) added for looker
	s.lookerChannel = make(chan struct{})

	// Wait for the listener to be closed and all connections to shut down.
	var wg sync.WaitGroup
	defer wg.Wait()

	// Wrap the context to propogate a shutdown to the listeners and connection handlers.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Start a goroutine to shut down the listener when the context is canceled.
	wg.Add(1)
	go func() {
		defer wg.Done()

		<-ctx.Done()
		cerr = l.Close()
	}()

	// Set up a semaphore for the connection limit.
	var limit chan struct{}
	done := ctx.Done()
	if s.ConnectionLimit != 0 {
		limit = make(chan struct{}, s.ConnectionLimit)
	}

	for {
		if limit != nil {
			// Wait for connection limit.
			if len(limit) == cap(limit) {
				s.Logger.Warnf("postgres connection limit reached")
			}
			select {
			case limit <- struct{}{}:
			case <-done:
				return nil
			}
		}

		// Accept a connection.
		conn, err := l.Accept()
		if err != nil {
			return err
		}

		// Handle the connection in another goroutine.
		wg.Add(1)
		go func() {
			defer wg.Done()
			if limit != nil {
				// Restore connection limit when done.
				defer func() { <-limit }()
			}
			err := s.handle(ctx, conn)
			if err != nil {
				s.Logger.Errorf("postgres connection terminated with error: %v", err)
			}
		}()
	}
}
