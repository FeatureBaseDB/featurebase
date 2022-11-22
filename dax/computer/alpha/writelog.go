// Package alpha contains an implementation of the WriteLogReader and
// WriteLogWriter interfaces. In the case where a sub-service (such as
// writelogger) implements these interfaces directly with both its service and
// its http client, then we don't need this middle implementation layer. But in
// this case, the WriteLogger operates as a third-party service might, meaning
// its API methods don't align with what FeatureBase needs to call. So this
// implementation acts as a translation later between the
// featurebase-to-writelogger interface, and the third-party WriteLogger
// service.
package alpha

import (
	"bufio"
	"context"
	"encoding/json"
	"io"

	featurebase "github.com/molecula/featurebase/v3"
	"github.com/molecula/featurebase/v3/dax"
	"github.com/molecula/featurebase/v3/dax/computer"
	"github.com/molecula/featurebase/v3/errors"
)

// Ensure type implements interface.
var _ computer.WriteLogReader = &alphaWriteLog{}
var _ computer.WriteLogWriter = &alphaWriteLog{}

// alphaWriteLog is an implementation of the WriteLogReader and WriteLogWriter
// interfaces. It uses a WriteLogger implementation (which could be, for
// example, an http client or a locally running sub-service) to store its log
// messages. I can't remember why we put this implementation in its own package
// (a meaningless name called "alpha"); it probably has something to do with
// wanting to adhere to a typical interface/implementation package structure,
// but was confused by the current state of things where the "storage" package
// (or perhaps "computer") is the top level package (i.e. "pilosa"). Until we
// can correct the packaging, we will likely have weird cases like this.
type alphaWriteLog struct {
	wl featurebase.WriteLogger
}

func NewAlphaWriteLog(wler featurebase.WriteLogger) *alphaWriteLog {
	return &alphaWriteLog{
		wl: wler,
	}
}

func (w *alphaWriteLog) CreateTableKeys(ctx context.Context, qtid dax.QualifiedTableID, partition dax.PartitionNum, version int, m map[string]uint64) error {
	msg := computer.PartitionKeyMap{
		TableKey:   qtid.Key(),
		Partition:  partition,
		StringToID: m,
	}

	b, err := json.Marshal(msg)
	if err != nil {
		return errors.Wrap(err, "marshalling partition key map to json")
	}

	bucket := partitionBucket(qtid.Key(), partition)

	if err := w.wl.AppendMessage(bucket, keysFileName, version, b); err != nil {
		return errors.Wrapf(err, "appending partition key message: %s, %d", keysFileName, version)
	}

	return nil
}

func (w *alphaWriteLog) DeleteTableKeys(ctx context.Context, qtid dax.QualifiedTableID, partition dax.PartitionNum, version int) error {
	bucket := partitionBucket(qtid.Key(), partition)
	return w.wl.DeleteLog(bucket, keysFileName, version)
}

func (w *alphaWriteLog) CreateFieldKeys(ctx context.Context, qtid dax.QualifiedTableID, field dax.FieldName, version int, m map[string]uint64) error {
	msg := computer.FieldKeyMap{
		TableKey:   qtid.Key(),
		Field:      field,
		StringToID: m,
	}

	b, err := json.Marshal(msg)
	if err != nil {
		return errors.Wrap(err, "marshalling field key map to json")
	}

	bucket := fieldBucket(qtid.Key(), field)

	if err := w.wl.AppendMessage(bucket, keysFileName, version, b); err != nil {
		return errors.Wrapf(err, "appending field key message: %s, %d", keysFileName, version)
	}

	return nil
}

func (w *alphaWriteLog) DeleteFieldKeys(ctx context.Context, qtid dax.QualifiedTableID, field dax.FieldName, version int) error {
	bucket := fieldBucket(qtid.Key(), field)
	return w.wl.DeleteLog(bucket, keysFileName, version)
}

func (w *alphaWriteLog) WriteShard(ctx context.Context, qtid dax.QualifiedTableID, partition dax.PartitionNum, shard dax.ShardNum, version int, msg computer.LogMessage) error {
	b, err := computer.MarshalLogMessage(msg, computer.EncodeTypeJSON)
	if err != nil {
		return errors.Wrap(err, "marshalling log message")
	}

	bucket := partitionBucket(qtid.Key(), partition)
	shardKey := shardKey(shard)

	if err := w.wl.AppendMessage(bucket, shardKey, version, b); err != nil {
		return errors.Wrapf(err, "appending shard key message: %s, %d", shardKey, version)
	}

	return nil
}

func (w *alphaWriteLog) DeleteShard(ctx context.Context, qtid dax.QualifiedTableID, partition dax.PartitionNum, shard dax.ShardNum, version int) error {
	bucket := partitionBucket(qtid.Key(), partition)
	shardKey := shardKey(shard)

	return w.wl.DeleteLog(bucket, shardKey, version)
}

////////////////////////////////////////////////

func (w *alphaWriteLog) TableKeyReader(ctx context.Context, qtid dax.QualifiedTableID, partition dax.PartitionNum, version int) computer.TableKeyReader {
	return newTableKeyReader(w.wl, qtid, partition, version)
}

type tableKeyReader struct {
	wl        featurebase.WriteLogger
	table     dax.TableKey
	partition dax.PartitionNum
	version   int
	scanner   *bufio.Scanner
	closer    io.Closer
}

func newTableKeyReader(wl featurebase.WriteLogger, qtid dax.QualifiedTableID, partition dax.PartitionNum, version int) *tableKeyReader {
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

	reader, closer, err := r.wl.LogReader(bucket, keysFileName, r.version)
	if err != nil {
		return errors.Wrapf(err, "getting log reader: %s, %s, %d", bucket, keysFileName, r.version)
	}

	r.closer = closer
	r.scanner = bufio.NewScanner(reader)

	return nil
}

func (r *tableKeyReader) Read() (computer.PartitionKeyMap, error) {
	if r.scanner == nil {
		return computer.PartitionKeyMap{}, io.EOF
	}

	var b []byte
	var out computer.PartitionKeyMap

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

func (w *alphaWriteLog) FieldKeyReader(ctx context.Context, qtid dax.QualifiedTableID, field dax.FieldName, version int) computer.FieldKeyReader {
	return newFieldKeyReader(w.wl, qtid, field, version)
}

type fieldKeyReader struct {
	wl      featurebase.WriteLogger
	table   dax.TableKey
	field   dax.FieldName
	version int
	scanner *bufio.Scanner
	closer  io.Closer
}

func newFieldKeyReader(wl featurebase.WriteLogger, qtid dax.QualifiedTableID, field dax.FieldName, version int) *fieldKeyReader {
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

	reader, closer, err := r.wl.LogReader(bucket, keysFileName, r.version)
	if err != nil {
		return errors.Wrapf(err, "getting log reader: %s, %s, %d", bucket, keysFileName, r.version)
	}

	r.closer = closer
	r.scanner = bufio.NewScanner(reader)

	return nil
}

func (r *fieldKeyReader) Read() (computer.FieldKeyMap, error) {
	if r.scanner == nil {
		return computer.FieldKeyMap{}, io.EOF
	}

	var b []byte
	var out computer.FieldKeyMap

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

func (w *alphaWriteLog) ShardReader(ctx context.Context, qtid dax.QualifiedTableID, partition dax.PartitionNum, shard dax.ShardNum, version int) computer.ShardReader {
	return newShardReader(w.wl, qtid, partition, shard, version)
}

type shardReader struct {
	wl        featurebase.WriteLogger
	table     dax.TableKey
	partition dax.PartitionNum
	shard     dax.ShardNum
	version   int
	scanner   *bufio.Scanner
	closer    io.Closer
}

func newShardReader(wl featurebase.WriteLogger, qtid dax.QualifiedTableID, partition dax.PartitionNum, shard dax.ShardNum, version int) *shardReader {
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

	reader, closer, err := r.wl.LogReader(bucket, shardKey, r.version)
	if err != nil {
		return errors.Wrapf(err, "getting log reader: %s, %s, %d", bucket, shardKey, r.version)
	}

	r.closer = closer
	r.scanner = bufio.NewScanner(reader)

	return nil
}

func (r *shardReader) Read() (computer.LogMessage, error) {
	if r.scanner == nil {
		return nil, io.EOF
	}

	if r.scanner.Scan() {
		return computer.UnmarshalLogMessage(r.scanner.Bytes())
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
