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

package pg

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/molecula/featurebase/v2/pg/message"
	"github.com/pkg/errors"
)

// Protocol is a Postgres protocol version.
type Protocol uint32

const (
	// ProtocolPostgres30 is version 3.0 of the Postgres wire protocol.
	ProtocolPostgres30 Protocol = (3 << 16)

	// ProtocolCancel is the protocol used for query cancellation.
	ProtocolCancel Protocol = (1234 << 16) | 5678

	// ProtocolSSL is the protocol used for SSL upgrades.
	ProtocolSSL Protocol = (1234 << 16) | 5679

	// ProtocolSupported is the main protocol version supported by this package.
	ProtocolSupported Protocol = ProtocolPostgres30
)

// Major returns the major revision of the protocol.
func (p Protocol) Major() uint16 {
	return uint16(p >> 16)
}

// Minor returns the minor revision of the protocol.
func (p Protocol) Minor() uint16 {
	return uint16(p)
}

func (p Protocol) String() string {
	switch p {
	case ProtocolCancel:
		return "cancel"
	case ProtocolSSL:
		return "SSL"
	}

	return fmt.Sprintf("v%d.%d", p.Major(), p.Minor())
}

// handle reads the startup packet and dispatches an appropriate protocol handler for the connection.
func (s *Server) handle(ctx context.Context, conn net.Conn) (err error) {
	var hasTLS bool

	defer func() {
		cerr := conn.Close()
		if cerr != nil && err == nil {
			if hasTLS {
				if nerr, ok := cerr.(net.Error); ok && nerr.Timeout() {
					// TLS does this sometimes.
					return
				}
			}
			err = errors.Wrap(cerr, "closing connection")
		}
	}()

	if tcpconn, ok := conn.(*net.TCPConn); ok {
		// Postgres does not have any real mechanism for confirming that a connection is still alive.
		// Without this, a connection that breaks while idle would live indefinitely.
		// With a TCP keepalive, this should return an error after approximately 2 hours (depending on OS configuration).
		err := tcpconn.SetKeepAlive(true)
		if err != nil {
			return errors.Wrap(err, "enabling TCP keepalive")
		}
	}

	var startupDeadline time.Time
	if s.StartupTimeout > 0 {
		// Set deadline for processing the startup.
		startupDeadline = time.Now().Add(s.StartupTimeout)
		err = conn.SetDeadline(startupDeadline)
		if err != nil {
			return errors.Wrap(err, "setting deadline on protocol startup")
		}
	}

startup:
	// Read startup packet.
	var buf [4]byte
	_, err = io.ReadFull(conn, buf[:])
	if err != nil {
		return errors.Wrap(err, "reading startup message length")
	}
	size := binary.BigEndian.Uint32(buf[:])
	if size < 4 {
		return errors.Errorf("invalid startup packet length: %d bytes", size)
	}
	maxLen := s.MaxStartupSize
	if maxLen == 0 {
		maxLen = 1024 * 1024
	}
	if size > maxLen {
		return errors.Errorf("oversized startup frame of %d bytes (max: %d bytes)", size, maxLen)
	}
	data := make([]byte, size-4)
	_, err = io.ReadFull(conn, data)
	if err != nil {
		return errors.Wrap(err, "reading startup packet")
	}

	// Extract protocol ID.
	if len(data) < 4 {
		return errors.Errorf("startup packet is too small for protocol ID: %d bytes", len(data))
	}
	proto := Protocol(binary.BigEndian.Uint32(data))
	data = data[4:]

	if proto == ProtocolSSL {
		if s.TLSConfig != nil {
			// Upgrade the connection to TLS and renegotiate on the tunneled connection.
			_, err = conn.Write([]byte{'S'})
			if err != nil {
				return errors.Wrap(err, "sending SSL support confirmation")
			}
			conn = tls.Server(conn, s.TLSConfig)
			if s.StartupTimeout > 0 {
				err := conn.SetDeadline(startupDeadline)
				if err != nil {
					return errors.Wrap(err, "transferring startup deadline to TLS connection")
				}
			}
			hasTLS = true
			goto startup
		}

		// Inform the client that SSL is not available and try again.
		s.Logger.Debugf("client at %s requested a secure postgres connection but TLS is not configured", conn.RemoteAddr())
		_, err = conn.Write([]byte{'N'})
		if err != nil {
			return errors.Wrap(err, "sending SSL unsupported notification")
		}
		goto startup
	}

	if s.TLSConfig != nil && !hasTLS {
		// Reject the unsecured connection.
		return errors.Errorf("client at %s attempted to initiate an unsecured postgres conenction", conn.RemoteAddr())
	}

	switch proto {
	case ProtocolCancel:
		// Handle cancellation.
		return s.handleCancel(ctx, conn, data)
	default:
		// Handle regular postgres.
		return s.handleStandard(ctx, proto, conn, data)
	}
}

// parseParams parses a parameter list from a startup packet.
func parseParams(data []byte) (map[string]string, error) {
	params := make(map[string]string)
	for {
		idx := bytes.IndexByte(data, 0)
		switch idx {
		case 0:
			return params, nil
		case -1:
			return nil, errors.New("malformed startup parameter list")
		}

		key := string(data[:idx])
		data = data[idx+1:]

		idx = bytes.IndexByte(data, 0)
		if idx == -1 {
			return nil, errors.New("malformed startup parameter list")
		}
		val := string(data[:idx])
		data = data[idx+1:]

		params[key] = val
	}
}

// handleCancel handles cancel request connections.
func (s *Server) handleCancel(ctx context.Context, conn net.Conn, data []byte) error {
	if len(data) != 8 {
		return errors.New("malformed cancellation packet")
	}

	if s.CancellationManager == nil {
		return errors.New("cancellation is not configured")
	}

	pid := int32(binary.BigEndian.Uint32(data[:4]))
	key := int32(binary.BigEndian.Uint32(data[4:]))

	err := s.CancellationManager.Cancel(CancellationToken{PID: pid, Key: key})
	switch err {
	case nil:
	case ErrCancelledMissingConnection:
		// This is usually not a real error (race condition in the protocol).
		// This can happen if a client cancels a request and shuts down.
		s.Logger.Debugf("client at %v sent a mismatched cancellation token (is a load balancer misconfigured?)", conn.RemoteAddr())
	default:
		return err
	}

	return nil
}

// handleStandard handles a connection in the standard postgres wire protocol.
// The client is responsible for closing the connection when this finishes.
func (s *Server) handleStandard(ctx context.Context, proto Protocol, conn net.Conn, data []byte) error {
	// Wait for helper goroutines to finish.
	var wg sync.WaitGroup
	defer wg.Wait()

	// Set up context.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Check the major version.
	if proto.Major() != ProtocolSupported.Major() {
		return errors.Errorf("unsupported protocol %v", proto)
	}

	// Parse the parameters bundled in the startup packet.
	params, err := parseParams(data)
	if err != nil {
		return errors.Wrap(err, "parsing parameters")
	}

	if user, ok := params["user"]; ok {
		// Log the connection.
		s.Logger.Debugf("new postgres connection from user %q at %v", user, conn.RemoteAddr())
	} else {
		// We do not use this much yet, but the wire protocol says that it is required.
		return errors.New("missing username")
	}

	// Set up message input and output.

	// Set up a reader that will preempt the connection when the context is canceled.
	ir := idleReader{
		conn:    conn,
		timeout: s.ReadTimeout,
	}
	wg.Add(1)
	go func() {
		defer wg.Done()

		<-ctx.Done()

		ir.preempt() //nolint:errcheck
	}()

	// Clear the startup deadline.
	err = conn.SetDeadline(time.Time{})
	if err != nil {
		return err
	}

	// Set up a message reader with buffering.
	rbuf := bufio.NewReader(&ir)
	r := message.NewWireReader(rbuf)

	// Set up a writer on the connection.
	var ww io.Writer = conn
	if s.WriteTimeout != 0 {
		// Apply the write timeout.
		ww = &timeoutWriter{
			conn:    conn,
			timeout: s.WriteTimeout,
		}
	}

	// Set up a message writer with buffering.
	w := message.NewWireWriter(bufio.NewWriter(ww))

	var encoder message.Encoder
	if proto.Minor() > ProtocolSupported.Minor() {
		// Negotiate the version down.
		s.Logger.Debugf("client requested unsupported protocol version %v; attempting to downgrade to %v", proto, ProtocolSupported)
		msg, err := encoder.NegotiateProtocolVersion(int32(ProtocolSupported.Minor()))
		if err != nil {
			return errors.Wrap(err, "negotiating version")
		}
		err = w.WriteMessage(msg)
		if err != nil {
			return errors.Wrap(err, "negotiating version")
		}
	}

	// TODO: real auth
	err = w.WriteMessage(message.AuthenticationOK)
	if err != nil {
		return errors.Wrap(err, "sending authentication confirmation")
	}

	var cancelNotify <-chan struct{}
	if s.CancellationManager != nil {
		notify, cancel, token, err := s.CancellationManager.Token()
		if err != nil {
			return errors.Wrap(err, "setting up cancellation")
		}
		defer cancel()

		msg, err := encoder.BackendKeyData(token.PID, token.Key)
		if err != nil {
			return errors.Wrap(err, "encoding cancellation key data")
		}
		err = w.WriteMessage(msg)
		if err != nil {
			return errors.Wrap(err, "sending cancellation key data")
		}
		cancelNotify = notify
	}

	var queryReady bool
	for {
		if !queryReady {
			// Indicate that we are ready for a query.
			// TODO: provide a valid transaction state.
			msg, err := encoder.ReadyForQuery(message.TransactionStatusActive)
			if err != nil {
				return errors.Wrap(err, "sending query ready status")
			}
			err = w.WriteMessage(msg)
			if err != nil {
				return errors.Wrap(err, "sending query ready status")
			}

			// Flush the write buffer so that the client can respond.
			err = w.Flush()
			if err != nil {
				return errors.Wrap(err, "flushing status")
			}

			if rbuf.Buffered() == 0 {
				// Put the connection into idle mode.
				err = ir.setIdle()
				if err != nil {
					return errors.Wrap(err, "setting idle mode")
				}
			} else {
				// If the client follows the spec, then it should not have sent anything more.
				// However, it seems that no clients completely follow the spec, so we shouldn't rely on anything that isn't entirely straightforward.
				s.Logger.Debugf("postgres client sent additional data without waiting for completion")
			}
		}

		// Read the next packet.
		msg, err := r.ReadMessage()
		if err != nil {
			if err == errPreempted {
				// The server is shutting down.
				return errors.Wrap(s.handleShutdown(
					conn, w, &encoder,

					message.NoticeField{
						Type: message.NoticeFieldSeverity,
						Data: "ERROR",
					},
					message.NoticeField{
						Type: message.NoticeFieldMessage,
						Data: "server shutting down",
					},
					message.NoticeField{
						Type: message.NoticeFieldHint,
						Data: "This is normal. This message is sent when a server is shutting down and terminating its connections.",
					},
				), "processing connection shutdown")
			}

			return err
		}

		switch msg.Type {
		case message.TypeTermination:
			// We are done.
			return w.Flush()

		case message.TypeSimpleQuery:
			// Execute a simple query.

			queryReady = false

			// Parse the query message (a null-terminated string).
			query := SimpleQuery(strings.TrimSuffix(string(msg.Data), "\x00"))

			// Execute the query.
			err := s.handleQuery(w, query, cancelNotify)
			if err != nil {
				return err
			}

		default:
			// The message is not supported yet.
			// Send an error.
			s.Logger.Errorf("unrecognized postgres packet %v", msg)
			msg, err = encoder.Error(
				message.NoticeField{
					Type: message.NoticeFieldSeverity,
					Data: "ERROR",
				},
				message.NoticeField{
					Type: message.NoticeFieldMessage,
					Data: fmt.Sprintf("unrecognized message type %q", msg.Type),
				},
				message.NoticeField{
					Type: message.NoticeFieldDetail,
					Data: "message body:" + hex.Dump(msg.Data),
				},
			)
			if err != nil {
				return errors.Wrap(err, "sending unrecognized message error")
			}
			err = w.WriteMessage(msg)
			if err != nil {
				return errors.Wrap(err, "sending unrecognized message error")
			}
			err = w.Flush()
			if err != nil {
				return errors.Wrap(err, "sending unrecognized message error")
			}
		}
	}
}

// handleQuery processes a single query on a connection.
func (s *Server) handleQuery(w message.Writer, query Query, cancelNotify <-chan struct{}) error {
	// Configure cancellation.
	// This is not the connection context, since we want the request to finish safely before connection shutdown.
	ctx := context.Background()
	if cancelNotify != nil {
		defer func() {
			// Flush any cancel notifications.
			// This works on a best-effort basis.
			// It is still entirely possible that the cancel notification may be delivered to the next request.
			// Regardless of what we do, we either get false positives or false negatives.
			// This code chooses false positives.
			for len(cancelNotify) > 0 {
				<-cancelNotify
			}
		}()

		var wg sync.WaitGroup
		defer wg.Add(1)

		var cancel context.CancelFunc
		ctx, cancel = context.WithCancel(ctx)
		defer cancel()

		wg.Add(1)
		go func() {
			defer wg.Done()

			select {
			case <-ctx.Done():
			case <-cancelNotify:
				cancel()
			}
		}()
	}

	// Set up a result writer.
	// SELECT is used as a default tag, which seems to be handled decently by clients.
	// The encoder is intentionally not re-used because its buffer may be huge.
	qwriter := &queryResultWriter{
		w:   w,
		te:  s.TypeEngine,
		tag: "SELECT",
	}

	// Dispatch the query handler.
	qerr := s.QueryHandler.HandleQuery(ctx, qwriter, query)
	if qerr != nil {
		// There was an error in processing the query.
		// Send the error back to the client and keep going.
		s.Logger.Debugf("failed to execute query %q: %v", query, qerr)
		msg, err := qwriter.enc.GoError(qerr)
		if err != nil {
			return errors.Wrap(err, "failed to send query error to client")
		}
		err = w.WriteMessage(msg)
		if err != nil {
			return errors.Wrap(err, "failed to send query error to client")
		}
	} else {
		if !qwriter.wroteHeaders {
			// The handler did not write headers.
			// Write back an empty set of headers.
			err := qwriter.WriteHeader()
			if err != nil {
				return errors.Wrap(err, "sending empty column headers")
			}
		}
		// The query completed normally.
		// Notify the client of completion.
		msg, err := qwriter.enc.CommandComplete(qwriter.tag)
		if err != nil {
			return errors.Wrap(err, "sending command completion notification")
		}
		err = w.WriteMessage(msg)
		if err != nil {
			return errors.Wrap(err, "sending command completion notification")
		}
	}

	// The data will be flushed after we write back the "ready for query" state.
	return nil
}

func (s *Server) handleShutdown(conn net.Conn, w message.Writer, encoder *message.Encoder, notice ...message.NoticeField) error {
	var wg sync.WaitGroup
	defer wg.Wait()

	// Try to send a message to the client before closing the connection.
	msg, err := encoder.Error(notice...)
	if err != nil {
		return errors.Wrap(err, "generating shutdown notification")
	}

	if s.WriteTimeout == 0 {
		// The client is likely to not listen for incoming messages.
		// Force a write timeout to ensure that this terminates.
		err := conn.SetWriteDeadline(time.Now().Add(time.Second))
		if err != nil {
			return errors.Wrap(err, "setting shutdown write deadline")
		}
	}

	// The client may be waiting on a write, so we need to drain the incoming data stream.
	err = conn.SetReadDeadline(time.Time{})
	if err != nil {
		return errors.Wrap(err, "clearing read deadline for shutdown")
	}
	defer conn.SetReadDeadline(time.Now()) //nolint:errcheck
	wg.Add(1)
	go func() {
		defer wg.Done()

		io.Copy(ioutil.Discard, conn) //nolint:errcheck
	}()

	// Attempt to send the shutdown notification.
	// This will fail under many scenarios, as the client is not necessarily reading.
	err = w.WriteMessage(msg)
	if err != nil {
		return nil
	}
	w.Flush()

	return nil
}
