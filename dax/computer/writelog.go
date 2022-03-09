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
	DeleteTableKeys(ctx context.Context, qtid dax.QualifiedTableID, partition dax.PartitionNum, version int) error

	// CreateFieldKeys sends a map of string key to uint64 ID for the table and
	// field provided.
	CreateFieldKeys(ctx context.Context, qtid dax.QualifiedTableID, field dax.FieldName, version int, _ map[string]uint64) error
	DeleteFieldKeys(ctx context.Context, qtid dax.QualifiedTableID, field dax.FieldName, version int) error

	WriteShard(ctx context.Context, qtid dax.QualifiedTableID, partition dax.PartitionNum, shard dax.ShardNum, version int, msg LogMessage) error
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
	logMessageTypeImportRoaring = iota
	logMessageTypeImport
	logMessageTypeImportValue
	logMessageTypeImportRoaringShard
)

type LogMessage interface{}

// MarshalLogMessage serializes the log message and adds log message type info.
func MarshalLogMessage(msg LogMessage) ([]byte, error) {
	typ, err := getLogMessageType(msg)
	if err != nil {
		return nil, errors.Wrap(err, "getting log message type")
	}

	buf, err := json.Marshal(msg)
	if err != nil {
		return nil, errors.Wrap(err, "marshaling log message")
	}
	return append([]byte{typ}, buf...), nil
}

func LogMessageByType(typ byte) (LogMessage, error) {
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
