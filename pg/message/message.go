package message

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// Type is a byte indicating the type of a Postgres message.
type Type byte

const (
	// TypeAuthentication is a message used to transfer authentication info.
	TypeAuthentication Type = 'R'

	// TypeReadyForQuery is a message used to indicate that the server is ready for another query.
	TypeReadyForQuery Type = 'Z'

	// TypeCommandComplete is a Backend message used to indicate that a query has completed.
	TypeCommandComplete Type = 'C'

	// TypeClos is a message used to indicate that a query has completed. Frontend
	TypeClose Type = 'C'

	// TypeRowDescription is a message indicating the column types of the result rows from a query.
	TypeRowDescription Type = 'T'

	// TypeDataRow is a message with the contents of a single row.
	TypeDataRow Type = 'D'

	// TypeTermination is a message indicating a request to terminate a connection.
	TypeTermination Type = 'X'

	// TypeNegotiateProtocolVersion is a message used when a client attempts to connect with a newer minor version than the server supports.
	TypeNegotiateProtocolVersion Type = 'v'

	// TypeSimpleQuery is a simple query request.
	TypeSimpleQuery Type = 'Q'

	// TypeBackendKeyData contains a cancellation key for the client to use later.
	TypeBackendKeyData Type = 'K'

	TypeParse         Type = 'P'
	TypeParseComplete Type = '1'

	TypeBind         Type = 'B'
	TypeBindComplete Type = '2'

	TypeExecute            Type = 'E' // Frontend
	TypeError              Type = 'E' // Backend
	TypeSync               Type = 'S' // Frontend
	TypeParameterStatus    Type = 'S' // Backend
	TypeDescribe           Type = 'D' // Frontend
	TypeNoData             Type = 'n' // backend
	TypeEmptyQueryResponse Type = 'I' // backend
)

// AuthenticationOK is a message indicating that authentication has completed.
var AuthenticationOK = Message{
	Type: TypeAuthentication,
	Data: []byte{0, 0, 0, 0},
}
var ParseOK = Message{
	Type: TypeParseComplete,
	Data: []byte{},
}
var BindComplete = Message{
	Type: TypeBindComplete,
	Data: []byte{},
}
var NoData = Message{
	Type: TypeNoData,
	Data: []byte{},
}
var EmptyQueryResponse = Message{
	Type: TypeEmptyQueryResponse,
	Data: []byte{},
}

// Message is a Postgres message value.
type Message struct {
	Type Type
	Data []byte
}

//debug tools
func viewString(b []byte) string {
	r := []rune(string(b))
	for i := range r {
		if r[i] < 32 || r[i] > 126 {
			r[i] = '.'
		}
	}
	return string(r)
}
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
func (m *Message) Dump(prefix string) {
	n := len(m.Data)
	rowcount := 0
	stop := (n / 8) * 8
	k := 0
	fmt.Printf("\n %s type: '%c'\n", prefix, m.Type)
	for i := 0; i <= stop; i += 8 {
		k++
		if i+8 < n {
			rowcount = 8
		} else {
			rowcount = min(k*8, n) % 8
		}

		fmt.Printf("pos %02d  hex:  ", i)
		for j := 0; j < rowcount; j++ {
			fmt.Printf("%02x  ", m.Data[i+j])
		}
		for j := rowcount; j < 8; j++ {
			fmt.Printf("    ")
		}
		fmt.Printf("  '%s'\n", viewString(m.Data[i:(i+rowcount)]))
	}
}

//endTools

// TransactionStatus is the current transaction state.
type TransactionStatus byte

const (
	// TransactionStatusIdle indicates that there is no active transaction.
	TransactionStatusIdle TransactionStatus = 'I'

	// TransactionStatusActive indicates that the connection currently has an active transaction.
	TransactionStatusActive TransactionStatus = 'T'

	// TransactionStatusFailed indicates that the connection currently has a failed transaction.
	TransactionStatusFailed TransactionStatus = 'E'
)

// Encoder encodes messages.
type Encoder struct {
	buf     bytes.Buffer
	scratch [4]byte
}

func (e *Encoder) i16(i int16) error {
	binary.BigEndian.PutUint16(e.scratch[:2], uint16(i))
	_, err := e.buf.Write(e.scratch[:2])
	return err
}

func (e *Encoder) i32(i int32) error {
	binary.BigEndian.PutUint32(e.scratch[:], uint32(i))
	_, err := e.buf.Write(e.scratch[:])
	return err
}

/* removed for linter now
func (e *Encoder) u32(i uint32) error {
	binary.BigEndian.PutUint32(e.scratch[:], i)
	_, err := e.buf.Write(e.scratch[:])
	return err
}
*/

// ReadyForQuery encodes a "ready for query" message.
func (e *Encoder) ReadyForQuery(status TransactionStatus) (Message, error) {
	e.buf.Reset()

	err := e.buf.WriteByte(byte(status))
	if err != nil {
		return Message{}, err
	}

	return Message{
		Type: TypeReadyForQuery,
		Data: e.buf.Bytes(),
	}, nil
}

// CommandComplete encodes a command completion message.
func (e *Encoder) CommandComplete(tag string) (Message, error) {
	e.buf.Reset()

	_, err := e.buf.WriteString(tag)
	if err != nil {
		return Message{}, err
	}

	err = e.buf.WriteByte(0)
	if err != nil {
		return Message{}, err
	}

	return Message{
		Type: TypeCommandComplete,
		Data: e.buf.Bytes(),
	}, nil
}

// NoticeFieldType indicates the type of a notice/error field.
// https://www.postgresql.org/docs/9.3/protocol-error-fields.html
type NoticeFieldType byte

const (
	// NoticeFieldSeverity indicates the severity of a notice/error.
	NoticeFieldSeverity NoticeFieldType = 'S'

	// NoticeFieldMessage is a short human-readable error/notice message.
	NoticeFieldMessage NoticeFieldType = 'M'

	// NoticeFieldDetail is an optional extended description of the error.
	NoticeFieldDetail NoticeFieldType = 'D'

	// NoticeFieldHint is a suggestion of how to address the issue.
	NoticeFieldHint NoticeFieldType = 'H'

	// NoticeFieldHint the SQLSTATE code for the error (see Appendix A). Not localizable. Always present.
	NoticeFieldCode NoticeFieldType = 'C'
)

// NoticeField is a field in an error or notice.
type NoticeField struct {
	Type NoticeFieldType
	Data string
}

func (e *Encoder) messageOrNotice(fields ...NoticeField) error {
	for _, f := range fields {
		err := e.buf.WriteByte(byte(f.Type))
		if err != nil {
			return err
		}

		_, err = e.buf.WriteString(f.Data)
		if err != nil {
			return err
		}

		err = e.buf.WriteByte(0)
		if err != nil {
			return err
		}
	}

	return e.buf.WriteByte(0)
}

// Error encodes a Postgres error message.
func (e *Encoder) Error(fields ...NoticeField) (Message, error) {
	e.buf.Reset()
	err := e.messageOrNotice(fields...)
	if err != nil {
		return Message{}, err
	}
	return Message{
		Type: TypeError,
		Data: e.buf.Bytes(),
	}, nil
}

// GoError creates a simple Postgres error message from a Go error value.
func (e *Encoder) GoError(err error) (Message, error) {
	return e.Error(
		NoticeField{
			Type: NoticeFieldSeverity,
			Data: "ERROR",
		},
		NoticeField{
			Type: NoticeFieldMessage,
			Data: err.Error(),
		},
	)
}

// ColumnDescription is a description of a data column.
type ColumnDescription struct {
	Name         string
	TableID      int32 //either a table/col id or 0
	FieldID      int16 //either a table/col id or 0
	TypeID       int32 //field type
	TypeLen      int16 //size in bytes of field
	TypeModifier int32 //type modifer?
	Mode         int16 //0=text 1=binary
}

// RowDescription describes the response rows from a query.
func (e *Encoder) RowDescription(cols ...ColumnDescription) (Message, error) {
	if len(cols) >= 1<<15 {
		return Message{}, ErrMessageTooBig
	}

	e.buf.Reset()

	err := e.i16(int16(len(cols)))
	if err != nil {
		return Message{}, nil
	}

	for _, col := range cols {
		_, err := e.buf.WriteString(col.Name)
		if err != nil {
			return Message{}, err
		}
		err = e.buf.WriteByte(0)
		if err != nil {
			return Message{}, err
		}

		err = e.i32(col.TableID)
		if err != nil {
			return Message{}, err
		}

		err = e.i16(col.FieldID)
		if err != nil {
			return Message{}, err
		}

		err = e.i32(col.TypeID)
		if err != nil {
			return Message{}, err
		}

		err = e.i16(col.TypeLen)
		if err != nil {
			return Message{}, err
		}

		err = e.i32(col.TypeModifier)
		if err != nil {
			return Message{}, err
		}

		err = e.i16(col.Mode)
		if err != nil {
			return Message{}, err
		}
	}

	return Message{
		Type: TypeRowDescription,
		Data: e.buf.Bytes(),
	}, nil
}

// TextRow encodes a data row in textual format.
func (e *Encoder) TextRow(row ...string) (Message, error) {
	if len(row) >= 1<<15 {
		return Message{}, ErrMessageTooBig
	}

	e.buf.Reset()

	err := e.i16(int16(len(row)))
	if err != nil {
		return Message{}, err
	}

	for _, val := range row {
		if uint(len(val)) >= 1<<31 {
			return Message{}, ErrMessageTooBig
		}

		err = e.i32(int32(len(val)))
		if err != nil {
			return Message{}, err
		}

		_, err = e.buf.WriteString(val)
		if err != nil {
			return Message{}, err
		}
	}

	return Message{
		Type: TypeDataRow,
		Data: e.buf.Bytes(),
	}, nil
}

// NegotiateProtocolVersion encodes a protocol negotiation packet.
func (e *Encoder) NegotiateProtocolVersion(maxMinor int32, unrecognizedOptions ...string) (Message, error) {
	if uint64(len(unrecognizedOptions)) >= 1<<31 {
		return Message{}, ErrMessageTooBig
	}

	e.buf.Reset()

	err := e.i32(maxMinor)
	if err != nil {
		return Message{}, err
	}

	err = e.i32(int32(len(unrecognizedOptions)))
	if err != nil {
		return Message{}, err
	}
	for _, opt := range unrecognizedOptions {
		_, err = e.buf.WriteString(opt)
		if err != nil {
			return Message{}, err
		}

		err = e.buf.WriteByte(0)
		if err != nil {
			return Message{}, err
		}
	}

	return Message{
		Type: TypeNegotiateProtocolVersion,
		Data: e.buf.Bytes(),
	}, nil
}

// BackendKeyData encodes a Message with a cancellation key.
func (e *Encoder) BackendKeyData(pid, key int32) (Message, error) {
	e.buf.Reset()

	err := e.i32(pid)
	if err != nil {
		return Message{}, err
	}

	err = e.i32(key)
	if err != nil {
		return Message{}, err
	}

	return Message{
		Type: TypeBackendKeyData,
		Data: e.buf.Bytes(),
	}, nil
}

func (e *Encoder) ParameterStatus(param, value string) (Message, error) {
	e.buf.Reset()
	//param + NULL + value+ NULL
	_, err := e.buf.WriteString(param)
	if err != nil {
		return Message{}, err
	}
	err = e.buf.WriteByte(0)
	if err != nil {
		return Message{}, err
	}
	_, err = e.buf.WriteString(value)
	if err != nil {
		return Message{}, err
	}
	err = e.buf.WriteByte(0)
	if err != nil {
		return Message{}, err
	}
	return Message{
		Type: TypeParameterStatus,
		Data: e.buf.Bytes(),
	}, nil
}

type SimpleColumn struct {
	Name    string
	Typeid  int32
	Typelen int16
}

func (e *Encoder) EncodeColumn(name string, typeid int32, typelen int16) (Message, error) {
	return e.EncodeColumns(SimpleColumn{
		Name:    name,
		Typeid:  typeid,
		Typelen: typelen,
	})
}
func (e *Encoder) EncodeColumns(cols ...SimpleColumn) (Message, error) {

	e.buf.Reset()
	err := e.i16(int16(len(cols))) // number of columns in result
	if err != nil {
		return Message{}, nil
	}
	for _, col := range cols {

		_, err = e.buf.WriteString(col.Name) // column name
		if err != nil {
			return Message{}, err
		}
		err = e.buf.WriteByte(0) //null terminate
		if err != nil {
			return Message{}, err
		}

		err = e.i32(0) //tabel id
		if err != nil {
			return Message{}, err
		}

		err = e.i16(0) //field id(attnum)
		if err != nil {
			return Message{}, err
		}

		err = e.i32(col.Typeid) //type_id
		if err != nil {
			return Message{}, err
		}

		err = e.i16(col.Typelen) //type_len
		if err != nil {
			return Message{}, err
		}

		err = e.i32(-1) //type_mod
		if err != nil {
			return Message{}, err
		}

		err = e.i16(0) //format  0 text 1 binary
		if err != nil {
			return Message{}, err
		}
	}
	return Message{
		Type: TypeRowDescription,
		Data: e.buf.Bytes(),
	}, nil
}
