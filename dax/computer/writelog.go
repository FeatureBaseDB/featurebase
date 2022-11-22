package computer

import (
	"context"
	"encoding/json"
	"io"
	"time"

	"github.com/molecula/featurebase/v3/dax"
	"github.com/molecula/featurebase/v3/errors"
)

// WriteLogWriter provides the interface for all data writes to FeatureBase. After
// data has been written to the local FeatureBase node, the respective interface
// method(s) will be called.
type WriteLogWriter interface {
	// CreateTableKeys sends a map of string key to uint64 ID for the table and
	// partition provided.
	CreateTableKeys(ctx context.Context, qtid dax.QualifiedTableID, partition dax.PartitionNum, version int, _ map[string]uint64) error

	// DeleteTableKeys deletes all table keys for the table and partition
	// provided.
	DeleteTableKeys(ctx context.Context, qtid dax.QualifiedTableID, partition dax.PartitionNum, version int) error

	// CreateFieldKeys sends a map of string key to uint64 ID for the table and
	// field provided.
	CreateFieldKeys(ctx context.Context, qtid dax.QualifiedTableID, field dax.FieldName, version int, _ map[string]uint64) error

	// DeleteTableKeys deletes all field keys for the table and field provided.
	DeleteFieldKeys(ctx context.Context, qtid dax.QualifiedTableID, field dax.FieldName, version int) error

	// WriteShard sends shard data for the table and shard provided.
	WriteShard(ctx context.Context, qtid dax.QualifiedTableID, partition dax.PartitionNum, shard dax.ShardNum, version int, msg LogMessage) error

	// DeleteShard deletes all data for the table and shard provided.
	DeleteShard(ctx context.Context, qtid dax.QualifiedTableID, partition dax.PartitionNum, shard dax.ShardNum, version int) error
}

// Ensure type implements interface.
var _ WriteLogWriter = (*NopWriteLogWriter)(nil)

// NopWriteLogWriter is a no-op implementation of the WriteLogWriter interface.
type NopWriteLogWriter struct{}

func NewNopWriteLogWriter() *NopWriteLogWriter {
	return &NopWriteLogWriter{}
}

func (w *NopWriteLogWriter) CreateTableKeys(ctx context.Context, qtid dax.QualifiedTableID, partition dax.PartitionNum, version int, m map[string]uint64) error {
	return nil
}

func (w *NopWriteLogWriter) DeleteTableKeys(ctx context.Context, qtid dax.QualifiedTableID, partition dax.PartitionNum, version int) error {
	return nil
}

func (w *NopWriteLogWriter) CreateFieldKeys(ctx context.Context, qtid dax.QualifiedTableID, field dax.FieldName, version int, m map[string]uint64) error {
	return nil
}

func (w *NopWriteLogWriter) DeleteFieldKeys(ctx context.Context, qtid dax.QualifiedTableID, field dax.FieldName, version int) error {
	return nil
}

func (w *NopWriteLogWriter) WriteShard(ctx context.Context, qtid dax.QualifiedTableID, partition dax.PartitionNum, shard dax.ShardNum, version int, msg LogMessage) error {
	return nil
}

func (w *NopWriteLogWriter) DeleteShard(ctx context.Context, qtid dax.QualifiedTableID, partition dax.PartitionNum, shard dax.ShardNum, version int) error {
	return nil
}

// WriteLogReader provides the interface for all reads from the write log.
type WriteLogReader interface {
	ShardReader(ctx context.Context, qtid dax.QualifiedTableID, partition dax.PartitionNum, shard dax.ShardNum, version int) ShardReader
	TableKeyReader(ctx context.Context, qtid dax.QualifiedTableID, partition dax.PartitionNum, version int) TableKeyReader
	FieldKeyReader(ctx context.Context, qtid dax.QualifiedTableID, field dax.FieldName, version int) FieldKeyReader
}

// Ensure type implements interface.
var _ WriteLogReader = (*NopWriteLogReader)(nil)

// NopWriteLogReader is a no-op implementation of the WriteLogReader interface.
type NopWriteLogReader struct{}

func NewNopWriteLogReader() *NopWriteLogReader {
	return &NopWriteLogReader{}
}

func (w *NopWriteLogReader) TableKeyReader(ctx context.Context, qtid dax.QualifiedTableID, partition dax.PartitionNum, version int) TableKeyReader {
	return NewNopTableKeyReader()
}

func (w *NopWriteLogReader) FieldKeyReader(ctx context.Context, qtid dax.QualifiedTableID, field dax.FieldName, version int) FieldKeyReader {
	return NewNopFieldKeyReader()
}

func (w *NopWriteLogReader) ShardReader(ctx context.Context, qtid dax.QualifiedTableID, partition dax.PartitionNum, shard dax.ShardNum, version int) ShardReader {
	return NewNopShardReader()
}

////////////////////////////////////////////////

type TableKeyReader interface {
	Open() error
	Read() (PartitionKeyMap, error)
	Close() error
}

// Ensure type implements interface.
var _ TableKeyReader = &NopTableKeyReader{}

// NopTableKeyReader is a no-op implementation of the TableKeyReader
// interface.
type NopTableKeyReader struct{}

func NewNopTableKeyReader() *NopTableKeyReader {
	return &NopTableKeyReader{}
}

func (r *NopTableKeyReader) Open() error { return nil }
func (r *NopTableKeyReader) Read() (PartitionKeyMap, error) {
	return PartitionKeyMap{}, io.EOF
}
func (r *NopTableKeyReader) Close() error { return nil }

////////////////////////////////////////////////

type FieldKeyReader interface {
	Open() error
	Read() (FieldKeyMap, error)
	Close() error
}

// Ensure type implements interface.
var _ FieldKeyReader = &NopFieldKeyReader{}

// NopFieldKeyReader is a no-op implementation of the FieldKeyReader
// interface.
type NopFieldKeyReader struct{}

func NewNopFieldKeyReader() *NopFieldKeyReader {
	return &NopFieldKeyReader{}
}

func (r *NopFieldKeyReader) Open() error { return nil }
func (r *NopFieldKeyReader) Read() (FieldKeyMap, error) {
	return FieldKeyMap{}, io.EOF
}
func (r *NopFieldKeyReader) Close() error { return nil }

////////////////////////////////////////////////

type ShardReader interface {
	Open() error
	Read() (LogMessage, error)
	Close() error
}

// Ensure type implements interface.
var _ ShardReader = &NopShardReader{}

// NopShardReader is a no-op implementation of the ShardReader interface.
type NopShardReader struct{}

func NewNopShardReader() *NopShardReader {
	return &NopShardReader{}
}

func (r *NopShardReader) Open() error { return nil }
func (r *NopShardReader) Read() (LogMessage, error) {
	return nil, io.EOF
}
func (r *NopShardReader) Close() error { return nil }

//////////////// Messages ///////////////////////

type PartitionKeyMap struct {
	TableKey   dax.TableKey      `json:"table-key"`
	Partition  dax.PartitionNum  `json:"partition"`
	StringToID map[string]uint64 `json:"string-to-id"`
}

type FieldKeyMap struct {
	TableKey   dax.TableKey      `json:"table-key"`
	Field      dax.FieldName     `json:"field"`
	StringToID map[string]uint64 `json:"string-to-id"`
}

const (
	logMessageTypeImportRoaring byte = iota
	logMessageTypeImport
	logMessageTypeImportValue
	logMessageTypeImportRoaringShard
)

// encoderKey* are part of the log message header. They indicate what encoding
// type a specific log message is serialized with.
const (
	encoderKeyJSON byte = iota
)

const (
	EncodeTypeJSON string = "json"

	// encodeVersion refers to the version of the structs used to represent the
	// log messages. If we change structs, we'll need to modify this version
	// number and maintain the previous version of the structs somewhere for
	// deserialization.
	encodeVersion byte = 1
)

// logMessageEncoder is implemented by any encoder used to serialize LogMessages
// to []byte.
type logMessageEncoder interface {
	Key() byte
	Marshal(LogMessage) ([]byte, error)
	Unmarshal([]byte, LogMessage) error
}

// LogMessage is implemented by a variety of types which can be serialized as
// messages to the WriteLogger.
type LogMessage interface{}

// MarshalLogMessage serializes the log message and prepends additional encoding
// information to each message. Currently, we prepend three bytes to each log
// message:
// byte[0]: encodeVersion - this is currently a constant within the code. If we
// modify structs such that they encode differently, we'll have to change the
// constant and keep previous versions of structs for deserialization.
// byte[1]: encodeType (e.g. "json", etc.)
// byte[2]: logMessageType
//
// If we get into a situation where we want more flexibility in these message
// header bytes—for example, if we want to use more than three bytes—we could do
// something with the first bit of the encodeVersion: if it's 1, that could
// indicate that there are additional header bytes, and the following seven bits
// could indicate how many.
func MarshalLogMessage(msg LogMessage, encode string) ([]byte, error) {
	encoder, err := getEncoderByType(encode)
	if err != nil {
		return nil, errors.Wrap(err, "getting encoder by type")
	}

	logMessageType, err := getLogMessageType(msg)
	if err != nil {
		return nil, errors.Wrap(err, "getting log message type")
	}

	var buf []byte

	buf, err = encoder.Marshal(msg)
	if err != nil {
		return nil, errors.Wrap(err, "marshaling log message")
	}

	return append([]byte{encodeVersion, encoder.Key(), logMessageType}, buf...), nil
}

// UnmarshalLogMessage deserializes the log message based on the log message
// type info.
func UnmarshalLogMessage(b []byte) (LogMessage, error) {
	if len(b) < 3 {
		return nil, errors.New(errors.ErrUncoded, "log record does not contain a full header")
	}

	encVersion := b[0]
	encKey := b[1]
	logMessageType := b[2]

	// Ensure that the log message is able to be handled by this code. If we
	// increment the constant encodeVersion, we'll need to modify this to handle
	// the log based on previous encodeVersions.
	if encVersion != encodeVersion {
		return nil, errors.Errorf("encode version is unsupported: %d", encVersion)
	}

	msg, err := logMessageByType(logMessageType)
	if err != nil {
		return nil, errors.Wrap(err, "getting log message by type")
	}

	encoder, err := getEncoderByKey(encKey)
	if err != nil {
		return nil, errors.Wrap(err, "getting encoder by key")
	}

	if err := encoder.Unmarshal(b[3:], &msg); err != nil {
		return nil, errors.Wrap(err, "unmarshaling log message")
	}

	return msg, nil
}

func logMessageByType(typ byte) (LogMessage, error) {
	switch typ {
	case logMessageTypeImportRoaring:
		return &ImportRoaringMessage{}, nil
	case logMessageTypeImport:
		return &ImportMessage{}, nil
	case logMessageTypeImportValue:
		return &ImportValueMessage{}, nil
	case logMessageTypeImportRoaringShard:
		return &ImportRoaringShardMessage{}, nil
	default:
		return nil, errors.Errorf("unknown message type %d", typ)
	}
}

func getLogMessageType(m LogMessage) (byte, error) {
	switch m.(type) {
	case *ImportRoaringMessage:
		return logMessageTypeImportRoaring, nil
	case *ImportMessage:
		return logMessageTypeImport, nil
	case *ImportValueMessage:
		return logMessageTypeImportValue, nil
	case *ImportRoaringShardMessage:
		return logMessageTypeImportRoaringShard, nil
	default:
		return 0, errors.Errorf("don't have type for message %#v", m)
	}
}

func getEncoderByType(encode string) (logMessageEncoder, error) {
	switch encode {
	case EncodeTypeJSON:
		return &encoderJSON{}, nil
	default:
		return nil, errors.Errorf("invalid encode type: %s", encode)
	}
}

func getEncoderByKey(id byte) (logMessageEncoder, error) {
	switch id {
	case encoderKeyJSON:
		return &encoderJSON{}, nil
	default:
		return nil, errors.Errorf("invalid encode type: %d", id)
	}
}

type ImportRoaringMessage struct {
	LogMessage `json:"-"`

	Table           string            `json:"table"`
	Field           string            `json:"field"`
	Partition       int               `json:"partition"`
	Shard           uint64            `json:"shard"`
	Clear           bool              `json:"clear"`
	Action          string            `json:"action"` // [set, clear, overwrite]
	Block           int               `json:"block"`
	Views           map[string][]byte `json:"views"`
	UpdateExistence bool              `json:"update-existence"`
}

type ImportMessage struct {
	LogMessage `json:"-"`

	Table      string   `json:"table"`
	Field      string   `json:"field"`
	Partition  int      `json:"partition"`
	Shard      uint64   `json:"shard"`
	RowIDs     []uint64 `json:"row-ids"`
	ColumnIDs  []uint64 `json:"column-ids"`
	RowKeys    []string `json:"row-keys"`
	ColumnKeys []string `json:"column-keys"`
	Timestamps []int64  `json:"timestamps"`
	Clear      bool     `json:"clear"`

	// options
	IgnoreKeyCheck bool `json:"ignore-key-check"`
	Presorted      bool `json:"presorted"`
}

type ImportValueMessage struct {
	LogMessage `json:"-"`

	Table           string      `json:"table"`
	Field           string      `json:"field"`
	Partition       int         `json:"partition"`
	Shard           uint64      `json:"shard"`
	ColumnIDs       []uint64    `json:"column-ids"`
	ColumnKeys      []string    `json:"column-keys"`
	Values          []int64     `json:"values"`
	FloatValues     []float64   `json:"float-values"`
	TimestampValues []time.Time `json:"timestamp-values"`
	StringValues    []string    `json:"string-values"`
	Clear           bool        `json:"clear"`

	// options
	IgnoreKeyCheck bool `json:"ignore-key-check"`
	Presorted      bool `json:"presorted"`
}

type ImportRoaringShardMessage struct {
	LogMessage `json:"-"`

	Table     string          `json:"table"`
	Partition int             `json:"partition"`
	Shard     uint64          `json:"shard"`
	Views     []RoaringUpdate `json:"views"`
}

// RoaringUpdate is identical to featurebase.RoaringUpdate, but we
// can't import it due to import cycles. TODO featurebase top level
// shouldn't import dax stuff... all the types it needs should just be
// in the top level.
type RoaringUpdate struct {
	Field        string `json:"field"`
	View         string `json:"view"`
	Clear        []byte `json:"clear"`
	Set          []byte `json:"set"`
	ClearRecords bool   `json:"clear-records"`
}

// Ensure type implements interface.
var _ logMessageEncoder = (*encoderJSON)(nil)

// encoderJSON is an implementation of the logMessageEncoder interface which
// encodes LogMessages as JSON.
type encoderJSON struct{}

func (e *encoderJSON) Key() byte {
	return encoderKeyJSON
}

func (e *encoderJSON) Marshal(msg LogMessage) ([]byte, error) {
	return json.Marshal(msg)
}

func (e *encoderJSON) Unmarshal(b []byte, msg LogMessage) error {
	return json.Unmarshal(b, msg)
}
