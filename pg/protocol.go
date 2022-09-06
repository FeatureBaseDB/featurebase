// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
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
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/featurebasedb/featurebase/v3/pg/message"
	"github.com/featurebasedb/featurebase/v3/sql"
	"github.com/pkg/errors"
	"vitess.io/vitess/go/vt/sqlparser"
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

	// PgServerVersion is the latest version of postgres that we claim to support.
	PgServerVersion = "13.0.0"
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
func (s *Server) SendParameterStatus(w *message.WireWriter, param, value string, encoder *message.Encoder) error {
	msg, err := encoder.ParameterStatus(param, value)
	if err != nil {
		return err
	}
	err = w.WriteMessage(msg)
	if err != nil {
		return errors.Wrap(err, "sending parameter status")
	}
	return nil
}

type Result struct {
}
type PgType byte

// Constants used to indicated query interception
// Only pgPassOn is allowed to be processed in Featurebase query handling
const (
	pgPassOn         PgType = 'x'
	pgBackendPid     PgType = 'a'
	pgVersion        PgType = 'b'
	pgCountType      PgType = 'c'
	pgQueryTime      PgType = 'd'
	pgTerminate      PgType = 'e'
	pgEmpty          PgType = 'f'
	pgSetApplication PgType = 'g'
	pgSelect1        PgType = 'h'
	pgSchema         PgType = 'i'
	pgBegin          PgType = 'j'
	pgTypeLen        PgType = 'k'
)

type Portal struct {
	Name         string
	Writer       *message.WireWriter
	commands     []message.Message
	Encoder      *message.Encoder
	mapper       *sql.Mapper
	sql          string
	pgspecial    PgType
	pid          int32
	queryStart   time.Time
	server       *Server
	cancelNotify <-chan struct{}
}

func (p *Portal) Reset() {
	p.Name = ""
	p.sql = ""
	p.pgspecial = pgPassOn
	p.commands = p.commands[:0]
}

func (p *Portal) Bind() {
	p.Add(message.BindComplete)
}

var lookPQL = regexp.MustCompile(`\[.*\].*\)\z`)

const POSTGRESLENSQL = `SELECT t.typlen FROM pg_catalog.pg_type t, pg_catalog.pg_namespace n WHERE t.typnamespace=n.oid AND t.typname='name' AND n.nspname='pg_catalog'`

func (p *Portal) Parse(data []byte) {
	p.queryStart = time.Now()
	queryStr := string(bytes.Trim(data, "\x00"))
	foundPQL := lookPQL.FindStringSubmatch(queryStr)
	if len(foundPQL) > 0 {

		p.sql = foundPQL[0]
		p.Name = "PQL"
		p.pgspecial = pgPassOn
		p.Add(message.ParseOK)
		return
	}
	if strings.Contains(queryStr, "EXTRACT") {
		// had to add this hack because the vitis parser doesn't handle...
		/*
		  	SELECT pid as id,
		   	 query as stmt,
		     EXTRACT(seconds from query_start - NOW()) as elapsed_time
		     FROM pg_stat_activity
		     WHERE usename='docker'`
		*/
		p.pgspecial = pgQueryTime
		p.Name = "SELECT"
		p.sql = queryStr
		p.Add(message.ParseOK)
		return
	}

	if len(queryStr) > 2 {
		query, err := p.mapper.MapSQL(queryStr)
		if err != nil {
			return
		}

		if strings.Contains(strings.ToLower(query.SQL), "select 1") {
			p.pgspecial = pgSelect1
			p.Name = "SELECT"
		} else if strings.Contains(queryStr, POSTGRESLENSQL) {
			p.pgspecial = pgTypeLen
			p.Name = "SELECT"
		} else {
			switch query.SQLType {
			case sql.SQLTypeSet:
				p.Name = "SET"
				set := query.Statement.(*sqlparser.Set)
				p.pgspecial = 0
				for _, item := range set.Exprs {
					if item.Name.String() == "application_name" {
						switch item.Expr.(type) {
						case *sqlparser.SQLVal:
							p.pgspecial = pgSetApplication
						}
					}
				}
			case sql.SQLTypeSelect:
				p.Name = "SELECT"
				p.pgspecial = pgPassOn
				stmt := query.Statement.(*sqlparser.Select)
				for _, item := range stmt.SelectExprs {
					switch expr := item.(type) {
					case *sqlparser.AliasedExpr:
						switch colExpr := expr.Expr.(type) {
						case *sqlparser.FuncExpr:
							funcName := strings.ToLower(colExpr.Name.String())
							switch funcName {
							case "pg_backend_pid":
								//SELECT pg_backend_pid()
								p.pgspecial = pgBackendPid
							case "pg_terminate_backend":
								//select pg_terminate_backend(100)
								p.pgspecial = pgTerminate
							case "version":
								//SELECT VERSION() AS version
								p.pgspecial = pgVersion
							}
							//need to return  the pid from the cancelation object
							//add row description object
							//add data row for item
						}
					}

				}
				for _, item := range stmt.From {
					switch from := item.(type) {
					case *sqlparser.AliasedTableExpr:
						tableName := from.Expr.(sqlparser.TableName).ToViewName().Name.String()
						switch tableName {
						case "pg_type":
							p.pgspecial = pgCountType
						case "pg_stat_activity":
							p.pgspecial = pgQueryTime
						case "tables":
							p.pgspecial = pgSchema
						}
					}
				}
				p.sql = queryStr
			case sql.SQLTypeBegin:
				// Ignore BEGIN
				p.pgspecial = pgBegin
			case sql.SQLTypeShow:
				p.Name = "SHOW"
				p.pgspecial = pgPassOn
				p.sql = queryStr
			}
		}
	} else {
		p.pgspecial = pgEmpty
	}
	p.Add(message.ParseOK)
}
func (p *Portal) Describe() {
	// Placeholder should we need to handle the Decribe request
}

func (p *Portal) Execute() (shouldTerminate bool, queryReady bool, err error) {
	queryReady = true
	switch p.pgspecial {
	case pgBackendPid:
		rowDescription, e := p.Encoder.EncodeColumn("pg_backend_pid", int32(23), 4)
		if e != nil {
			err = e
			return
		}
		p.Add(rowDescription)
		pid := fmt.Sprintf("%v", p.pid)
		dataRow, _ := p.Encoder.TextRow(pid)
		p.Add(dataRow)
		//needs data row with cancel token
	case pgVersion:
		rowDescription, e := p.Encoder.EncodeColumn("version", int32(25), -1)
		if e != nil {
			err = e
			return
		}
		p.Add(rowDescription)
		mesg := fmt.Sprintf("PostgresSQL 13.0 (molecula.%v)", p.server.QueryHandler.Version())
		dataRow, _ := p.Encoder.TextRow(mesg)
		p.Add(dataRow)
	case pgSelect1:
		rowDescription, e := p.Encoder.EncodeColumn("?column?", int32(23), 4)
		if e != nil {
			err = e
			return
		}
		p.Add(rowDescription)
		dataRow, _ := p.Encoder.TextRow("1")
		p.Add(dataRow)
	case pgCountType:
		//need to block
		<-p.server.lookerChannel
		rowDescription, e := p.Encoder.EncodeColumn("count", int32(20), 8)
		if err != nil {
			err = e
			return
		}
		p.Add(rowDescription)
		errorResponse, _ := p.Encoder.Error(
			message.NoticeField{
				Type: message.NoticeFieldSeverity,
				Data: "FATAL",
			},
			message.NoticeField{
				Type: message.NoticeFieldMessage,
				Data: "terminating connection due to administrator command",
			},
			message.NoticeField{
				Type: message.NoticeFieldCode,
				Data: "57P01",
			},
		)
		p.Add(errorResponse)
		e = p.Sync() //send and Reset

		if e != nil {
			err = e
			return
		}
		return true, queryReady, nil
	case pgQueryTime:
		// need to return something so that the id can be queried
		//need to return  SELECT pid as id, query as stmt, EXTRACT(seconds from query_start - NOW()) as elapsed_time          FROM pg_stat_activity
		//seems like we need a map of pids to querys
		e := p.server.dumpPortalsTo(p)
		if e != nil {
			err = e
			return
		}

	case pgTerminate:
		//note just have 1 lock that blocks all who try to count the activities
		//TODO (twg) lock this
		close(p.server.lookerChannel)                //release all the other blockers and allow them to terminate
		p.server.lookerChannel = make(chan struct{}) //create a new one just in case
		// i think it needs to return boolean true
		rowDescription, e := p.Encoder.EncodeColumn("pg_terminate_backend", int32(16), 1)
		if e != nil {
			err = e
			return
		}
		p.Add(rowDescription)
		dataRow, _ := p.Encoder.TextRow("t")
		p.Add(dataRow)
		commandComplete, e := p.Encoder.CommandComplete("SELECT 1")
		if e != nil {
			err = e
			return
		}
		p.Add(commandComplete)
		e = p.Sync()
		if e != nil {
			err = e
			return
		}
		return false, queryReady, nil
	case pgEmpty:
		p.Add(message.NoData)
		p.Add(message.EmptyQueryResponse)
		e := p.Sync()
		if e != nil {
			err = e
			return
		}
		queryReady = true
		return false, queryReady, nil
	case pgSetApplication:
		//needs to add/send status
		msg, e := p.Encoder.ParameterStatus("application_name", "PostgreSQL JDBC Driver")
		if e != nil {
			err = e
			return
		}
		p.Add(msg)
	case pgSchema:
		parts := []message.SimpleColumn{
			{
				Name:    "table_schema",
				Typeid:  int32(19),
				Typelen: 64,
			},
			{
				Name:    "table_name",
				Typeid:  int32(19),
				Typelen: 64,
			},
		}
		rowDescription, e := p.Encoder.EncodeColumns(parts...)
		if e != nil {
			err = e
			return
		}
		p.Add(rowDescription)
		e = p.HandleSchema()
		if e != nil {
			err = e
			return
		}

	case pgPassOn:
		query := SimpleQuery(p.sql)
		e := p.server.handleQuery(p, query, p.cancelNotify)
		if e != nil {
			err = e
			return
		}
		return

	case pgBegin:
		p.Name = "BEGIN"
	case pgTypeLen:
		rowDescription, e := p.Encoder.EncodeColumn("typelen", int32(21), 2)
		if e != nil {
			err = e
			return
		}
		p.Add(rowDescription)
		mesg := "64"
		dataRow, _ := p.Encoder.TextRow(mesg)
		p.Add(dataRow)
	}

	//maybe add in the number of items in select clause
	if len(p.Name) > 0 { //only send command complete for those that have names
		message, _ := p.Encoder.CommandComplete(p.Name)
		p.Add(message)
	}
	return
}

// handleStandard handles a connection in the standard postgres wire protocol.
func (p *Portal) Sync() error {
	for _, m := range p.commands {
		err := p.Writer.WriteMessage(m)
		if err != nil {
			return err
		}
	}
	p.Writer.Flush()
	p.Reset()
	return nil
}
func (p *Portal) Add(m message.Message) {
	cp := message.Message{Type: m.Type, Data: make([]byte, len(m.Data))}
	copy(cp.Data, m.Data)
	p.commands = append(p.commands, cp)
}

func (p *Portal) DumpComands() {

	for _, m := range p.commands {
		m.Dump("DUMPING:")
	}
}
func (p *Portal) HandleSchema() error {
	return p.server.QueryHandler.HandleSchema(context.Background(), p)
}

func (p *Portal) WriteMessage(m message.Message) error {
	p.Add(m)
	return nil
}
func (p *Portal) Flush() error {
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
	err = s.SendParameterStatus(w, "application_name", "", &encoder)
	if err != nil {
		return errors.Wrap(err, "sending parameter status server version")
	}
	err = s.SendParameterStatus(w, "client_encoding", "UTF8", &encoder)
	if err != nil {
		return errors.Wrap(err, "sending parameter status server version")
	}
	err = s.SendParameterStatus(w, "DateStyle", "ISO, MDY", &encoder)
	if err != nil {
		return errors.Wrap(err, "sending parameter status server version")
	}
	err = s.SendParameterStatus(w, "integer_datetimes", "on", &encoder)
	if err != nil {
		return errors.Wrap(err, "sending parameter status server version")
	}
	err = s.SendParameterStatus(w, "IntervalStyle", "postgres", &encoder)
	if err != nil {
		return errors.Wrap(err, "sending parameter status server version")
	}
	err = s.SendParameterStatus(w, "is_superuser", "on", &encoder)
	if err != nil {
		return errors.Wrap(err, "sending parameter status server version")
	}
	err = s.SendParameterStatus(w, "server_encoding", "UTF8", &encoder)
	if err != nil {
		return errors.Wrap(err, "sending parameter status server version")
	}
	err = s.SendParameterStatus(w, "server_version", PgServerVersion, &encoder)
	if err != nil {
		return errors.Wrap(err, "sending parameter status server version")
	}
	/*
		-PARAMETER STATUS name='TimeZone', value='GMT'
	*/
	err = s.SendParameterStatus(w, "session_authorization", "docker", &encoder) //TODO(twg) figure out valid values here
	if err != nil {
		return errors.Wrap(err, "sending parameter status server version")
	}
	err = s.SendParameterStatus(w, "standard_conforming_strings", "on", &encoder) //TODO(twg) figure out valid values here
	if err != nil {
		return errors.Wrap(err, "sending parameter status server version")
	}
	err = s.SendParameterStatus(w, "TimeZone", "GMT", &encoder) //TODO(twg) figure out valid values here
	if err != nil {
		return errors.Wrap(err, "sending parameter status server version")
	}

	var cancelNotify <-chan struct{}
	var pid int32
	if s.CancellationManager != nil {
		notify, cancel, token, err := s.CancellationManager.Token()
		if err != nil {
			return errors.Wrap(err, "setting up cancellation")
		}
		defer cancel()

		msg, err := encoder.BackendKeyData(token.PID, token.Key)
		pid = token.PID
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
	portal := &Portal{
		Writer:       w,
		Encoder:      &encoder,
		commands:     make([]message.Message, 0),
		mapper:       sql.NewMapper(),
		pid:          pid,
		server:       s,
		cancelNotify: cancelNotify,
	}
	s.addPortal(portal)
	defer s.removePortal(portal)
	//mapper.Logger = logger
	for {
		if !queryReady {
			// Indicate that we are ready for a query.
			portal.sql = ""
			msg, err := encoder.ReadyForQuery(message.TransactionStatusIdle)
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
			queryReady = true
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

		case message.TypeParse:
			portal.Parse(msg.Data)
		case message.TypeBind:
			portal.Bind()
		case message.TypeExecute:
			term, qr, err := portal.Execute()
			if err != nil {
				return err
			}
			if term {
				return w.Flush()
			}
			queryReady = qr
		case message.TypeSync:
			err := portal.Sync()
			if err != nil {
				return err
			}
			queryReady = false
		case message.TypeSimpleQuery:
			queryReady = false
			query := SimpleQuery(strings.TrimSuffix(string(msg.Data), "\x00"))

			// Execute the query.
			err := s.handleQuery(w, query, cancelNotify)
			if err != nil {
				return err
			}
		case message.TypeDescribe:
			portal.Describe()
		case message.TypeClose:
			return w.Flush()
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
func (s *Server) addPortal(p *Portal) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.portals = append(s.portals, p)
}
func (s *Server) removePortal(p *Portal) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, portal := range s.portals {
		if portal.pid == p.pid {
			//remove i
			s.portals = append(s.portals[:i], s.portals[i+1:]...)
			return
		}

	}

}
func (s *Server) dumpPortalsTo(p *Portal) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	//need to add the descrition for the 3 fields
	//	<-:-ROW DESCRIPTION: num_fields=3
	//---[Field 01]---  name='id'  type=23  type_len=4  type_mod=4294967295  relid=12250  attnum=3  format=0
	//---[Field 02]---  name='stmt'  type=25  type_len=65535  type_mod=4294967295  relid=12250  attnum=20  format=0  -
	//--[Field 03]---  name='elapsed_time'  type=701  type_len=8  type_mod=4294967295  relid=0  attnum=0  format=0
	parts := []message.SimpleColumn{
		{
			Name:    "id",
			Typeid:  int32(23),
			Typelen: 4,
		},
		{
			Name:    "stmt",
			Typeid:  int32(25),
			Typelen: -1,
		},
		{
			Name:    "elapsed_time",
			Typeid:  int32(701),
			Typelen: 8,
		},
	}
	rowDescription, err := p.Encoder.EncodeColumns(parts...)
	if err != nil {
		return err
	}
	p.Add(rowDescription)

	for _, portal := range s.portals {
		dataRow, err := p.Encoder.TextRow(
			fmt.Sprintf("%v", portal.pid),
			portal.sql,
			fmt.Sprintf("%v", time.Since(portal.queryStart).Seconds()))
		if err != nil {
			return err
		}
		//if sql == "" need to put in a null record
		//need to add the dararow
		//also need to figure out null types
		p.Add(dataRow)
	}
	return nil
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
