package planner

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"io"

	"github.com/featurebasedb/featurebase/v3/sql3"
	"github.com/featurebasedb/featurebase/v3/sql3/planner/types"
	"github.com/featurebasedb/featurebase/v3/wireprotocol"
)

type wireProtocolMessage interface {
	Token() int16
}

func writeOp(op types.PlanOperator) ([]byte, error) {
	buf := new(bytes.Buffer)
	writer := bufio.NewWriter(buf)
	// serialize a plan op - for now we are just supporting
	// system tables, and we'll send the name of the system table
	switch op := op.(type) {
	case *PlanOpSystemTable:
		// write token
		b := make([]byte, 2)
		binary.BigEndian.PutUint16(b, uint16(wireprotocol.TOKEN_PLAN_OP))
		writer.Write(b)
		// write table name len
		t := op.table.name
		b = make([]byte, 4)
		binary.BigEndian.PutUint32(b, uint32(len(t)))
		writer.Write(b)
		// write table name
		writer.WriteString(op.table.name)
		writer.Flush()
	default:
		return []byte{}, sql3.NewErrInternalf("unexpected plan operator type '%T'", op)
	}
	return buf.Bytes(), nil
}

type wireProtocolParser struct {
	planner *ExecutionPlanner
	reader  io.Reader
	schema  types.Schema
}

func newWireProtocolParser(p *ExecutionPlanner, reader io.Reader) *wireProtocolParser {
	return &wireProtocolParser{
		planner: p,
		reader:  reader,
	}
}

func (f *wireProtocolParser) nextMessage() (wireProtocolMessage, error) {
	var t int16
	err := binary.Read(f.reader, binary.BigEndian, &t)
	if err != nil {
		return nil, err
	}
	switch t {
	case wireprotocol.TOKEN_SCHEMA_INFO:
		return newMessageSchemaInfo(f.planner, f.reader)

	case wireprotocol.TOKEN_ROW:
		if f.schema == nil {
			return nil, sql3.NewErrInternalf("schema uninitialized")
		}
		return newMessageRow(f.planner, f.reader, f.schema)

	case wireprotocol.TOKEN_ERROR_MESSAGE:
		return newMessageError(f.planner, f.reader)

	case wireprotocol.TOKEN_DONE:
		return newMessageDone(f.planner, f.reader)

	case wireprotocol.TOKEN_PLAN_OP:
		return newMessagePlanOp(f.planner, f.reader)

	default:
		return nil, sql3.NewErrInternalf("unexpected token %d", t)
	}
}

type messagePlanOp struct {
	token int16
	op    types.PlanOperator
}

var _ wireProtocolMessage = (*messagePlanOp)(nil)

func newMessagePlanOp(p *ExecutionPlanner, reader io.Reader) (*messagePlanOp, error) {

	var len int32
	err := binary.Read(reader, binary.BigEndian, &len)
	if err != nil {
		return nil, err
	}
	bname := make([]byte, len)
	err = binary.Read(reader, binary.BigEndian, &bname)
	if err != nil {
		return nil, err
	}
	name := string(bname)

	st, ok := systemTables[name]
	if !ok {
		return nil, sql3.NewErrInternalf("unexpected system table name %s", name)
	}

	return &messagePlanOp{
		token: wireprotocol.TOKEN_PLAN_OP,
		op:    NewPlanOpSystemTable(p, st),
	}, nil
}

func (m *messagePlanOp) Token() int16 {
	return m.token
}

type messageError struct {
	token int16
	err   error
}

var _ wireProtocolMessage = (*messageError)(nil)

func newMessageError(p *ExecutionPlanner, reader io.Reader) (*messageError, error) {
	var len int32
	err := binary.Read(reader, binary.BigEndian, &len)
	if err != nil {
		return nil, err
	}
	bname := make([]byte, len)
	err = binary.Read(reader, binary.BigEndian, &bname)
	if err != nil {
		return nil, err
	}
	errMsg := string(bname)

	return &messageError{
		token: wireprotocol.TOKEN_ERROR_MESSAGE,
		err:   errors.New(errMsg),
	}, nil
}

func (m *messageError) Token() int16 {
	return m.token
}

type messageSchemaInfo struct {
	token  int16
	schema types.Schema
}

var _ wireProtocolMessage = (*messageSchemaInfo)(nil)

func newMessageSchemaInfo(p *ExecutionPlanner, reader io.Reader) (*messageSchemaInfo, error) {
	schema, err := wireprotocol.ReadSchema(reader)
	if err != nil {
		return nil, err
	}
	return &messageSchemaInfo{
		token:  wireprotocol.TOKEN_SCHEMA_INFO,
		schema: schema,
	}, nil
}

func (m *messageSchemaInfo) Token() int16 {
	return m.token
}

type messageRow struct {
	token int16
	row   types.Row
}

var _ wireProtocolMessage = (*messageRow)(nil)

func newMessageRow(p *ExecutionPlanner, reader io.Reader, schema types.Schema) (*messageRow, error) {
	row, err := wireprotocol.ReadRow(reader, schema)
	if err != nil {
		return nil, err
	}
	return &messageRow{
		token: wireprotocol.TOKEN_ROW,
		row:   row,
	}, nil
}

func (m *messageRow) Token() int16 {
	return m.token
}

type messageDone struct {
	token int16
}

var _ wireProtocolMessage = (*messageDone)(nil)

func newMessageDone(p *ExecutionPlanner, reader io.Reader) (*messageDone, error) {
	return &messageDone{
		token: wireprotocol.TOKEN_DONE,
	}, nil
}

func (m *messageDone) Token() int16 {
	return m.token
}
