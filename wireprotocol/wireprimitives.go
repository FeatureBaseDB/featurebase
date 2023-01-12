package wireprotocol

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"time"

	"io"

	"github.com/molecula/featurebase/v3/errors"
	"github.com/molecula/featurebase/v3/pql"
	"github.com/molecula/featurebase/v3/sql3/parser"
	"github.com/molecula/featurebase/v3/sql3/planner/types"
)

const (
	// server --> client
	TOKEN_SCHEMA_INFO   int16 = 0xA1
	TOKEN_ROW           int16 = 0xA2
	TOKEN_DONE          int16 = 0xFD
	TOKEN_INFO_MESSAGE  int16 = 0xFE
	TOKEN_ERROR_MESSAGE int16 = 0xFF

	// client --> server
	TOKEN_SQL     int16 = 0x01
	TOKEN_PLAN_OP int16 = 0x02
)

const (
	TYPE_VOID      int8 = 0x00
	TYPE_ID        int8 = 0x01
	TYPE_BOOL      int8 = 0x02
	TYPE_INT       int8 = 0x03
	TYPE_DECIMAL   int8 = 0x04
	TYPE_TIMESTAMP int8 = 0x05
	TYPE_IDSET     int8 = 0x06
	TYPE_STRING    int8 = 0x07
	TYPE_STRINGSET int8 = 0x08
)

func ExpectToken(reader io.Reader, token int16) (int16, error) {
	var tk int16
	err := binary.Read(reader, binary.BigEndian, &tk)
	if err != nil {
		return 0, err
	}
	if tk != token {
		return 0, errors.Errorf("expected token found %d", token)
	}
	return tk, nil
}

// TOKEN_COLUMN_INFO message
// 					length (bytes)
// token			2
// column count		2
//
// (n) columns
//
// name length		1
// name				(from prev)
// data type		1
//
// (optional)
// if type decimal
// scale			1

// note RelationName and AliasName members from
// PlannerColumn are not sent over the wire
func WriteSchema(schema types.Schema) ([]byte, error) {
	buf := new(bytes.Buffer)
	writer := bufio.NewWriter(buf)
	// write token
	writeToken(writer, TOKEN_SCHEMA_INFO)

	// column count
	writeInt16(writer, int16(len(schema)))

	// for each column
	for _, s := range schema {
		// name len byte
		writeInt8(writer, int8(len(s.ColumnName)))
		// name
		writer.WriteString(s.ColumnName)
		// type byte
		switch ty := s.Type.(type) {
		case *parser.DataTypeID:
			writeInt8(writer, TYPE_ID)

		case *parser.DataTypeBool:
			writeInt8(writer, TYPE_BOOL)

		case *parser.DataTypeInt:
			writeInt8(writer, TYPE_INT)

		case *parser.DataTypeDecimal:
			writeInt8(writer, TYPE_DECIMAL)
			writeInt8(writer, int8(ty.Scale))

		case *parser.DataTypeTimestamp:
			writeInt8(writer, TYPE_TIMESTAMP)

		case *parser.DataTypeIDSet:
			writeInt8(writer, TYPE_IDSET)

		case *parser.DataTypeString:
			writeInt8(writer, TYPE_STRING)

		case *parser.DataTypeStringSet:
			writeInt8(writer, TYPE_STRINGSET)

		default:
			return []byte{}, errors.Errorf("unexpected type '%T'", s.Type)
		}
	}
	writer.Flush()
	return buf.Bytes(), nil
}

func ReadSchema(reader io.Reader) (types.Schema, error) {
	var columnCount int16
	err := binary.Read(reader, binary.BigEndian, &columnCount)
	if err != nil {
		return nil, err
	}

	var schema types.Schema
	for i := 0; i < int(columnCount); i++ {

		var nameLen int8
		err = binary.Read(reader, binary.BigEndian, &nameLen)
		if err != nil {
			return nil, err
		}
		bname := make([]byte, nameLen)
		err = binary.Read(reader, binary.BigEndian, &bname)
		if err != nil {
			return nil, err
		}
		colName := string(bname)

		var typ int8
		err = binary.Read(reader, binary.BigEndian, &typ)
		if err != nil {
			return nil, err
		}
		var dataType parser.ExprDataType

		switch typ {
		case TYPE_ID:
			dataType = parser.NewDataTypeID()

		case TYPE_BOOL:
			dataType = parser.NewDataTypeBool()

		case TYPE_INT:
			dataType = parser.NewDataTypeInt()

		case TYPE_DECIMAL:
			var scale int8
			err = binary.Read(reader, binary.BigEndian, &scale)
			if err != nil {
				return nil, err
			}
			dataType = parser.NewDataTypeDecimal(int64(scale))

		case TYPE_TIMESTAMP:
			dataType = parser.NewDataTypeTimestamp()

		case TYPE_IDSET:
			dataType = parser.NewDataTypeIDSet()

		case TYPE_STRING:
			dataType = parser.NewDataTypeString()

		case TYPE_STRINGSET:
			dataType = parser.NewDataTypeStringSet()
		}

		schema = append(schema, &types.PlannerColumn{
			ColumnName: colName,
			Type:       dataType,
		})
	}
	return schema, nil
}

// TOKEN_ROW message
// 					length (bytes)
// token			2
//
// (n) columns
// if column length is 0 --> null
//
// - for ID, INT
//
// column length	1
// value			8
//
// - for DECIMAL
//
// column length	1
// value			8
//
// - for BOOL
//
// column length	1
// value			1
//
// - for TIMESTAMP
//
// column length	1
// value			8
//
// - for IDSET
//
// set length		2
// (n) values
// 		value		8
//
// - for STRING
//
// column length	2
// value			(from prev)
//
// - for STRINGSET
//
// set length		2
// (n) values
// 		value len	2
// 		value		(from prev)
//

func WriteRow(row types.Row, schema types.Schema) ([]byte, error) {
	buf := new(bytes.Buffer)
	writer := bufio.NewWriter(buf)
	// write token
	writeToken(writer, TOKEN_ROW)

	// for each column
	for i, s := range schema {
		val := row[i]
		switch s.Type.(type) {
		case *parser.DataTypeID, *parser.DataTypeInt:
			if val == nil {
				writeInt8(writer, 0)
			} else {
				writeInt8(writer, 8)
				v, ok := row[i].(int64)
				if !ok {
					return []byte{}, errors.Errorf("unexpected type '%T'", row[i])
				}
				writeInt64(writer, v)
			}

		case *parser.DataTypeDecimal:
			if val == nil {
				writeInt8(writer, 0)
			} else {
				writeInt8(writer, 8)
				v, ok := row[i].(pql.Decimal)
				if !ok {
					return []byte{}, errors.Errorf("unexpected type '%T'", row[i])
				}
				writeInt64(writer, v.ToInt64(v.Scale))
			}

		case *parser.DataTypeBool:
			if val == nil {
				writeInt8(writer, 0)
			} else {
				writeInt8(writer, 1)
				v, ok := row[i].(bool)
				if !ok {
					return []byte{}, errors.Errorf("unexpected type '%T'", row[i])
				}
				if v {
					writeInt8(writer, 1)
				} else {
					writeInt8(writer, 0)
				}
			}

		case *parser.DataTypeTimestamp:
			if val == nil {
				writeInt8(writer, 0)
			} else {
				writeInt8(writer, 8)
				v, ok := row[i].(time.Time)
				if !ok {
					return []byte{}, errors.Errorf("unexpected type '%T'", row[i])
				}
				writeInt64(writer, v.UnixNano())
			}

		case *parser.DataTypeIDSet:
			if val == nil {
				writeInt16(writer, 0)
			} else {
				v, ok := row[i].([]int64)
				if !ok {
					return []byte{}, errors.Errorf("unexpected type '%T'", row[i])
				}
				writeInt16(writer, int16(len(v)))
				for _, s := range v {
					writeInt64(writer, s)
				}
			}

		case *parser.DataTypeString:
			if val == nil {
				writeInt16(writer, 0)
			} else {
				v, ok := row[i].(string)
				if !ok {
					return []byte{}, errors.Errorf("unexpected type '%T'", row[i])
				}
				writeInt16(writer, int16(len(v)))
				writer.WriteString(v)
			}

		case *parser.DataTypeStringSet:
			if val == nil {
				writeInt16(writer, 0)
			} else {
				v, ok := row[i].([]string)
				if !ok {
					return []byte{}, errors.Errorf("unexpected type '%T'", row[i])
				}
				writeInt16(writer, int16(len(v)))
				for _, s := range v {
					writeInt16(writer, int16(len(s)))
					writer.WriteString(s)
				}
			}

		default:
			return []byte{}, errors.Errorf("unexpected type '%T'", s.Type)
		}
	}
	writer.Flush()
	return buf.Bytes(), nil
}

func ReadRow(reader io.Reader, schema types.Schema) (types.Row, error) {

	row := make(types.Row, len(schema))

	for idx, s := range schema {
		switch t := s.Type.(type) {
		case *parser.DataTypeID, *parser.DataTypeInt:
			var len int8
			err := binary.Read(reader, binary.BigEndian, &len)
			if err != nil {
				return nil, err
			}
			if len == 0 {
				row[idx] = nil
			} else {
				var value int64
				err := binary.Read(reader, binary.BigEndian, &value)
				if err != nil {
					return nil, err
				}
				row[idx] = value
			}

		case *parser.DataTypeDecimal:
			var len int8
			err := binary.Read(reader, binary.BigEndian, &len)
			if err != nil {
				return nil, err
			}
			if len == 0 {
				row[idx] = nil
			} else {
				var value int64
				err := binary.Read(reader, binary.BigEndian, &value)
				if err != nil {
					return nil, err
				}
				row[idx] = pql.NewDecimal(value, t.Scale)
			}

		case *parser.DataTypeBool:
			var len int8
			err := binary.Read(reader, binary.BigEndian, &len)
			if err != nil {
				return nil, err
			}
			if len == 0 {
				row[idx] = nil
			} else {
				var value int8
				err := binary.Read(reader, binary.BigEndian, &value)
				if err != nil {
					return nil, err
				}
				row[idx] = value == 1
			}

		case *parser.DataTypeTimestamp:
			var len int8
			err := binary.Read(reader, binary.BigEndian, &len)
			if err != nil {
				return nil, err
			}
			if len == 0 {
				row[idx] = nil
			} else {
				var value int64
				err := binary.Read(reader, binary.BigEndian, &value)
				if err != nil {
					return nil, err
				}
				row[idx] = time.Unix(0, value)
			}

		case *parser.DataTypeIDSet:
			var len int16
			err := binary.Read(reader, binary.BigEndian, &len)
			if err != nil {
				return nil, err
			}
			if len == 0 {
				row[idx] = nil
			} else {
				set := make([]int64, len)
				for j, _ := range set {
					var value int64
					err := binary.Read(reader, binary.BigEndian, &value)
					if err != nil {
						return nil, err
					}
					set[j] = value
				}
				row[idx] = set
			}

		case *parser.DataTypeString:
			var len int16
			err := binary.Read(reader, binary.BigEndian, &len)
			if err != nil {
				return nil, err
			}
			if len == 0 {
				row[idx] = nil
			} else {
				bvalue := make([]byte, len)
				err = binary.Read(reader, binary.BigEndian, &bvalue)
				if err != nil {
					return nil, err
				}
				row[idx] = string(bvalue)
			}

		case *parser.DataTypeStringSet:
			var len int16
			err := binary.Read(reader, binary.BigEndian, &len)
			if err != nil {
				return nil, err
			}
			if len == 0 {
				row[idx] = nil
			} else {
				set := make([]string, len)
				for j, _ := range set {
					var vlen int16
					err = binary.Read(reader, binary.BigEndian, &vlen)
					if err != nil {
						return nil, err
					}
					bvalue := make([]byte, vlen)
					err = binary.Read(reader, binary.BigEndian, &bvalue)
					if err != nil {
						return nil, err
					}
					set[j] = string(bvalue)
				}
				row[idx] = set
			}

		default:
			return nil, errors.Errorf("unexpected type '%T'", s.Type)
		}
	}

	return row, nil
}

// TOKEN_DONE message
// 					length (bytes)
// token			2

func WriteDone() []byte {
	buf := new(bytes.Buffer)
	writer := bufio.NewWriter(buf)
	// write token
	writeToken(writer, TOKEN_DONE)
	writer.Flush()
	return buf.Bytes()
}

// TOKEN_ERROR_MESSAGE message
// 					length (bytes)
// token			2
//
// message len		4
// message			(from prev)

func WriteError(err error) []byte {
	buf := new(bytes.Buffer)
	writer := bufio.NewWriter(buf)
	// write token
	writeToken(writer, TOKEN_ERROR_MESSAGE)
	// write error len
	t := err.Error()
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, uint32(len(t)))
	writer.Write(b)
	// write error
	writer.WriteString(t)
	writer.Flush()

	return buf.Bytes()
}

func ReadError(reader io.Reader) (string, error) {
	var len int16
	err := binary.Read(reader, binary.BigEndian, &len)
	if err != nil {
		return "", err
	}
	bvalue := make([]byte, len)
	err = binary.Read(reader, binary.BigEndian, &bvalue)
	if err != nil {
		return "", err
	}
	return string(bvalue), nil
}

func writeToken(w io.Writer, token int16) {
	writeInt16(w, token)
}

func writeInt8(w io.Writer, i int8) {
	b := make([]byte, 1)
	b[0] = byte(i)
	w.Write(b)
}

func writeInt16(w io.Writer, i int16) {
	b := make([]byte, 2)
	binary.BigEndian.PutUint16(b, uint16(i))
	w.Write(b)
}

func writeInt64(w io.Writer, i int64) {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(i))
	w.Write(b)
}
