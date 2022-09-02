// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package pg

import (
	"context"
	"encoding/binary"
	"io"
	"sync"

	"github.com/pkg/errors"
)

// ErrCancelledMissingConnection is an error triggered by cancelling a connection that does not exist.
var ErrCancelledMissingConnection = errors.New("cancelled connection does not exist")

// CancellationToken is a value used to identify a backend for cancellation.
type CancellationToken struct {
	PID, Key int32
}

// CancellationManager manages postgres connection cancellation.
type CancellationManager interface {
	// Token acquires a new cancellation token.
	// The returned channel is sent to every time the connection is cancelled.
	// The connection may be cancelled an unlimited number of times.
	Token() (<-chan struct{}, context.CancelFunc, CancellationToken, error)

	// Cancel sends a cancellation notification to the connection with the associated token.
	// If the token is not associated with a connection, this returns ErrCancelledMissingConnection.
	Cancel(CancellationToken) error
}

// NewLocalCancellationManager creates an in-memory CancellationManager using randomly generated tokens.
// The provided reader is expected to be secure (e.g. crypto/rand.Reader).
func NewLocalCancellationManager(rand io.Reader) CancellationManager {
	return &localCancellationManager{
		rand:        rand,
		connections: make(map[CancellationToken]chan<- struct{}),
	}
}

type localCancellationManager struct {
	mu          sync.RWMutex
	rand        io.Reader
	connections map[CancellationToken]chan<- struct{}
}

func (c *localCancellationManager) Token() (<-chan struct{}, context.CancelFunc, CancellationToken, error) {
	notify := make(chan struct{}, 1)

gen:
	token, err := c.generateToken()
	if err != nil {
		return nil, nil, CancellationToken{}, err
	}
	cancel := c.registerToken(token, notify)
	if cancel == nil {
		goto gen
	}

	return notify, cancel, token, nil
}

func (c *localCancellationManager) generateToken() (CancellationToken, error) {
	var data [8]byte
	for {
		var n int
		for n < 8 {
			nn, err := c.rand.Read(data[n:])
			if err != nil {
				return CancellationToken{}, errors.Wrap(err, "generating a cancellation token")
			}
			n += nn
		}

		pid := int32(binary.LittleEndian.Uint32(data[:4]))
		if pid < 0 {
			continue
		}
		key := int32(binary.LittleEndian.Uint32(data[4:]))
		if key < 0 {
			continue
		}

		return CancellationToken{PID: pid, Key: key}, nil
	}
}

func (c *localCancellationManager) registerToken(token CancellationToken, notify chan<- struct{}) context.CancelFunc {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, collision := c.connections[token]; collision {
		return nil
	}

	c.connections[token] = notify

	return func() {
		c.mu.Lock()
		defer c.mu.Unlock()

		delete(c.connections, token)

		close(notify)
	}
}

func (c *localCancellationManager) Cancel(token CancellationToken) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	ch := c.connections[token]
	if ch == nil {
		return ErrCancelledMissingConnection
	}

	select {
	case ch <- struct{}{}:
	default:
	}

	return nil
}
