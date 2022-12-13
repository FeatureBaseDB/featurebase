package computer

import (
	"bufio"
	"context"
	"encoding/json"
	"io"

	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/errors"
)

// Ensure type implements interface.
var _ WriteLogReader = &writeLogReadWriter{}
var _ WriteLogWriter = &writeLogReadWriter{}

// writeLogReadWriter is an implementation of the WriteLogReader and WriteLogWriter
// interfaces. It uses a WriteLogService implementation (which could be, for
// example, an http client or a locally running sub-service) to store its log
// messages.
type writeLogReadWriter struct {
	wls WriteLogService
}

func NewWriteLogReadWriter(wls WriteLogService) *writeLogReadWriter {
	return &writeLogReadWriter{
		wls: wls,
	}
}

func (w *writeLogReadWriter) CreateTableKeys(ctx context.Context, qtid dax.QualifiedTableID, partition dax.PartitionNum, version int, m map[string]uint64) error {
	msg := PartitionKeyMap{
		TableKey:   qtid.Key(),
		Partition:  partition,
		StringToID: m,
	}

	b, err := json.Marshal(msg)
	if err != nil {
		return errors.Wrap(err, "marshalling partition key map to json")
	}

	bucket := partitionBucket(qtid.Key(), partition)

	if err := w.wls.AppendMessage(bucket, keysFileName, version, b); err != nil {
		return errors.Wrapf(err, "appending partition key message: %s, %d", keysFileName, version)
	}

	return nil
}

func (w *writeLogReadWriter) DeleteTableKeys(ctx context.Context, qtid dax.QualifiedTableID, partition dax.PartitionNum, version int) error {
	bucket := partitionBucket(qtid.Key(), partition)
	return w.wls.DeleteLog(bucket, keysFileName, version)
}

func (w *writeLogReadWriter) CreateFieldKeys(ctx context.Context, qtid dax.QualifiedTableID, field dax.FieldName, version int, m map[string]uint64) error {
	msg := FieldKeyMap{
		TableKey:   qtid.Key(),
		Field:      field,
		StringToID: m,
	}

	b, err := json.Marshal(msg)
	if err != nil {
		return errors.Wrap(err, "marshalling field key map to json")
	}

	bucket := fieldBucket(qtid.Key(), field)

	if err := w.wls.AppendMessage(bucket, keysFileName, version, b); err != nil {
		return errors.Wrapf(err, "appending field key message: %s, %d", keysFileName, version)
	}

	return nil
}

func (w *writeLogReadWriter) DeleteFieldKeys(ctx context.Context, qtid dax.QualifiedTableID, field dax.FieldName, version int) error {
	bucket := fieldBucket(qtid.Key(), field)
	return w.wls.DeleteLog(bucket, keysFileName, version)
}

func (w *writeLogReadWriter) WriteShard(ctx context.Context, qtid dax.QualifiedTableID, partition dax.PartitionNum, shard dax.ShardNum, version int, msg LogMessage) error {
	b, err := MarshalLogMessage(msg, EncodeTypeJSON)
	if err != nil {
		return errors.Wrap(err, "marshalling log message")
	}

	bucket := partitionBucket(qtid.Key(), partition)
	shardKey := shardKey(shard)

	if err := w.wls.AppendMessage(bucket, shardKey, version, b); err != nil {
		return errors.Wrapf(err, "appending shard key message: %s, %d", shardKey, version)
	}

	return nil
}

func (w *writeLogReadWriter) DeleteShard(ctx context.Context, qtid dax.QualifiedTableID, partition dax.PartitionNum, shard dax.ShardNum, version int) error {
	bucket := partitionBucket(qtid.Key(), partition)
	shardKey := shardKey(shard)

	return w.wls.DeleteLog(bucket, shardKey, version)
}

////////////////////////////////////////////////

func (w *writeLogReadWriter) TableKeyReader(ctx context.Context, qtid dax.QualifiedTableID, partition dax.PartitionNum, version int) TableKeyReader {
	return newTableKeyReader(w.wls, qtid, partition, version)
}

type tableKeyReader struct {
	wl        WriteLogService
	table     dax.TableKey
	partition dax.PartitionNum
	version   int
	scanner   *bufio.Scanner
	closer    io.Closer
}

func newTableKeyReader(wl WriteLogService, qtid dax.QualifiedTableID, partition dax.PartitionNum, version int) *tableKeyReader {
	r := &tableKeyReader{
		wl:        wl,
		table:     qtid.Key(),
		partition: partition,
		version:   version,
	}

	return r
}

func (r *tableKeyReader) Open() error {
	bucket := partitionBucket(r.table, r.partition)

	readcloser, err := r.wl.LogReader(bucket, keysFileName, r.version)
	if err != nil {
		return errors.Wrapf(err, "getting log reader: %s, %s, %d", bucket, keysFileName, r.version)
	}

	r.closer = readcloser
	r.scanner = bufio.NewScanner(readcloser)

	return nil
}

func (r *tableKeyReader) Read() (PartitionKeyMap, error) {
	if r.scanner == nil {
		return PartitionKeyMap{}, io.EOF
	}

	var b []byte
	var out PartitionKeyMap

	if r.scanner.Scan() {
		b = r.scanner.Bytes()
		if err := json.Unmarshal(b, &out); err != nil {
			return out, err
		}
		return out, nil
	}
	if err := r.scanner.Err(); err != nil {
		return out, err
	}

	return out, io.EOF
}

func (r *tableKeyReader) Close() error {
	if r.closer != nil {
		return r.closer.Close()
	}
	return nil
}

////////////////////////////////////////////////

func (w *writeLogReadWriter) FieldKeyReader(ctx context.Context, qtid dax.QualifiedTableID, field dax.FieldName, version int) FieldKeyReader {
	return newFieldKeyReader(w.wls, qtid, field, version)
}

type fieldKeyReader struct {
	wl      WriteLogService
	table   dax.TableKey
	field   dax.FieldName
	version int
	scanner *bufio.Scanner
	closer  io.Closer
}

func newFieldKeyReader(wl WriteLogService, qtid dax.QualifiedTableID, field dax.FieldName, version int) *fieldKeyReader {
	r := &fieldKeyReader{
		wl:      wl,
		table:   qtid.Key(),
		field:   field,
		version: version,
	}

	return r
}

func (r *fieldKeyReader) Open() error {
	bucket := fieldBucket(r.table, r.field)

	readcloser, err := r.wl.LogReader(bucket, keysFileName, r.version)
	if err != nil {
		return errors.Wrapf(err, "getting log reader: %s, %s, %d", bucket, keysFileName, r.version)
	}

	r.closer = readcloser
	r.scanner = bufio.NewScanner(readcloser)

	return nil
}

func (r *fieldKeyReader) Read() (FieldKeyMap, error) {
	if r.scanner == nil {
		return FieldKeyMap{}, io.EOF
	}

	var b []byte
	var out FieldKeyMap

	if r.scanner.Scan() {
		b = r.scanner.Bytes()
		if err := json.Unmarshal(b, &out); err != nil {
			return out, err
		}
		return out, nil
	}
	if err := r.scanner.Err(); err != nil {
		return out, err
	}

	return out, io.EOF
}

func (r *fieldKeyReader) Close() error {
	if r.closer != nil {
		return r.closer.Close()
	}
	return nil
}

////////////////////////////////////////////////

func (w *writeLogReadWriter) ShardReader(ctx context.Context, qtid dax.QualifiedTableID, partition dax.PartitionNum, shard dax.ShardNum, version int) ShardReader {
	return newShardReader(w.wls, qtid, partition, shard, version)
}

type shardReader struct {
	wl        WriteLogService
	table     dax.TableKey
	partition dax.PartitionNum
	shard     dax.ShardNum
	version   int
	scanner   *bufio.Scanner
	closer    io.Closer
}

func newShardReader(wl WriteLogService, qtid dax.QualifiedTableID, partition dax.PartitionNum, shard dax.ShardNum, version int) *shardReader {
	r := &shardReader{
		wl:        wl,
		table:     qtid.Key(),
		partition: partition,
		shard:     shard,
		version:   version,
	}

	return r
}

func (r *shardReader) Open() error {
	bucket := partitionBucket(r.table, r.partition)
	shardKey := shardKey(r.shard)

	readcloser, err := r.wl.LogReader(bucket, shardKey, r.version)
	if err != nil {
		return errors.Wrapf(err, "getting log reader: %s, %s, %d", bucket, shardKey, r.version)
	}

	r.closer = readcloser
	r.scanner = bufio.NewScanner(readcloser)

	return nil
}

func (r *shardReader) Read() (LogMessage, error) {
	if r.scanner == nil {
		return nil, io.EOF
	}

	if r.scanner.Scan() {
		return UnmarshalLogMessage(r.scanner.Bytes())
	}
	if err := r.scanner.Err(); err != nil {
		return nil, err
	}

	return nil, io.EOF
}

func (r *shardReader) Close() error {
	if r.closer != nil {
		return r.closer.Close()
	}
	return nil
}
