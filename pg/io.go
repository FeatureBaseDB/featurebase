// Copyright 2021 Molecula Corp. All rights reserved.
package pg

import (
	"net"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/pkg/errors"
)

// timeoutWriter wraps a connection and implements io.Writer with a timeout for each write.
type timeoutWriter struct {
	conn    net.Conn
	timeout time.Duration
}

func (w *timeoutWriter) Write(data []byte) (int, error) {
	err := w.conn.SetWriteDeadline(time.Now().Add(w.timeout))
	if err != nil {
		return 0, err
	}
	return w.conn.Write(data)
}

// errPreempted is an error used to indicate preemption of an idle connection.
var errPreempted = errors.New("preempted during idle")

// idleState is an atomic state value used to track a preemptible connection.
type idleState uint32

const (
	idleStateActive idleState = iota
	idleStateIdle
	idleStatePreempted
	idleStatePendingPreemption
)

func (s *idleState) load() idleState {
	return idleState(atomic.LoadUint32((*uint32)(unsafe.Pointer(s))))
}

func (s *idleState) cas(old, new idleState) bool {
	return atomic.CompareAndSwapUint32((*uint32)(unsafe.Pointer(s)), uint32(old), uint32(new))
}

// idleReader is an io.Reader implementation on a preemptible network connection.
// The connection has 2 modes: "idle" and "active".
// While in idle mode, the connection has no timeout but can be preempted.
// While in active mode, the connection may have a read timeout but cannot be immediately preempted.
// When a read completes in idle mode, the connection returns to active mode.
// If the connection is preempted in active mode, the preemption will be deferred until the connection returns to idle mode.
// This also provides a read timeout.
type idleReader struct {
	conn      net.Conn
	timeout   time.Duration
	state     idleState
	preemptMu sync.Mutex
}

// setIdle pushes the reader into idle mode.
// If a preemption is pending, it will be delivered on the next call to Read.
func (r *idleReader) setIdle() error {
	// Clear the read deadline.
	err := r.conn.SetReadDeadline(time.Time{})
	if err != nil {
		return errors.Wrap(err, "failed to clear deadline")
	}

	for {
		// Transition to idle mode.
		state := r.state.load()
		var target idleState
		switch state {
		case idleStateActive:
			// active -> idle
			target = idleStateIdle
		case idleStatePendingPreemption:
			// pending preemption -> preempted
			// Switching to idle mode activates the preemption.
			target = idleStatePreempted
		default:
			panic("inconsistent state")
		}

		if r.state.cas(state, target) {
			return nil
		}
	}
}

// preempt the reader.
// If the reader is not currently idle, the preemption will be delivered next time the connection enters idle mode.
// This does not wait until the preemption error is delivered.
func (r *idleReader) preempt() error {
	r.preemptMu.Lock()
	defer r.preemptMu.Unlock()
	for {
		state := r.state.load()
		var target idleState
		switch state {
		case idleStateActive:
			// active -> pending preemption
			target = idleStatePendingPreemption
		case idleStateIdle:
			// idle -> preempted
			target = idleStatePreempted
		case idleStatePendingPreemption, idleStatePreempted:
			// A preemption has already been delivered.
			return nil
		default:
			panic("inconsistent state")
		}

		ok := r.state.cas(state, target)
		if ok && target == idleStatePreempted {
			// We have entered preemption mode.
			// Preempt the current read on the connection.
			return r.conn.SetReadDeadline(time.Now())
		}
	}
}

// Read from the connection.
func (r *idleReader) Read(data []byte) (int, error) {
	var needsDeadlineReset bool
	state := r.state.load()
	switch state {
	case idleStateActive, idleStatePendingPreemption:
		// Connection is active.
		// There is no need to worry about preemption.
		if r.timeout != 0 {
			// Apply a read timeout.
			err := r.conn.SetReadDeadline(time.Now().Add(r.timeout))
			if err != nil {
				return 0, err
			}
		}
		return r.conn.Read(data)

	case idleStateIdle:
		// Read, and handle preemption.
		n, err := r.conn.Read(data)
		if err != nil {
			// Check if the error was caused by preemption.
			state = r.state.load()
			switch {
			case state == idleStatePreempted && n != 0:
				// Some data was read before the preemption was delivered.
				// Re-activate and discard the error.

				// Synchronize against the preempter.
				// This is necessary to ensure that the cancellation deadline is cleared.
				r.preemptMu.Lock()
				defer r.preemptMu.Unlock()

				// Re-activate the connection.
				// No CAS loop is necessary since we are synchronized against preempters.
				r.state = idleStatePendingPreemption

				// The deadline may need to reset since the preempter may have changed it.
				needsDeadlineReset = true

			case state == idleStatePreempted:
				// The read was preempted.
				return 0, errPreempted

			case state != idleStateIdle:
				// No other states make sense here.
				panic("inconsistent state")

			default:
				// No preemption was involved.
				// It is just a regular network error.
				return n, err
			}
		} else {
			// The read went through.

			// Exit from idle mode.
			// Ideally, transition to active mode.
			// However, a preemption may trigger while this is running.
			for state == idleStateIdle {
				if r.state.cas(idleStateIdle, idleStateActive) {
					state = idleStateActive
					break
				}

				state = r.state.load()
			}

			switch state {
			case idleStateActive:
				// The connection was reactivated normally.

			case idleStatePreempted:
				// The connection was preempted after the read completed.
				// Defer the preemption and complete successfully.

				// Synchronize against the preempter.
				// This is necessary to ensure that the cancellation deadline is cleared.
				r.preemptMu.Lock()
				defer r.preemptMu.Unlock()

				// Re-activate the connection.
				// No CAS loop is necessary since we are synchronized against preempters.
				r.state = idleStatePendingPreemption

				// The deadline may need to reset since the preempter may have changed it.
				needsDeadlineReset = true

			default:
				panic("inconsistent state")
			}
		}

		if needsDeadlineReset && r.timeout == 0 {
			// Clear the deadline.
			err := r.conn.SetReadDeadline(time.Time{})
			if err != nil {
				return n, err
			}
		}

		return n, nil

	case idleStatePreempted:
		// The connection is preempted.
		return 0, errPreempted

	default:
		panic("inconsistent state")
	}
}
