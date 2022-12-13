package storage

import (
	"bufio"
	"encoding/json"
	"io"

	"github.com/molecula/featurebase/v3/dax"
	"github.com/molecula/featurebase/v3/dax/computer"
)

// TODO this needs to be genericized and moved

type TableKeyReader struct {
	table     dax.TableKey
	partition dax.PartitionNum
	scanner   *bufio.Scanner
	closer    io.Closer
}

func NewTableKeyReader(qtid dax.QualifiedTableID, partition dax.PartitionNum, writelog io.ReadCloser) *TableKeyReader {
	r := &TableKeyReader{
		table:     qtid.Key(),
		partition: partition,
		scanner:   bufio.NewScanner(writelog),
		closer:    writelog,
	}

	return r
}

func (r *TableKeyReader) Read() (computer.PartitionKeyMap, error) {
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

func (r *TableKeyReader) Close() error {
	if r.closer != nil {
		return r.closer.Close()
	}
	return nil
}

type FieldKeyReader struct {
	table   dax.TableKey
	field   dax.FieldName
	scanner *bufio.Scanner
	closer  io.Closer
}

func NewFieldKeyReader(qtid dax.QualifiedTableID, field dax.FieldName, writelog io.ReadCloser) *FieldKeyReader {
	r := &FieldKeyReader{
		table:   qtid.Key(),
		field:   field,
		scanner: bufio.NewScanner(writelog),
		closer:  writelog,
	}

	return r
}

func (r *FieldKeyReader) Read() (computer.FieldKeyMap, error) {
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

func (r *FieldKeyReader) Close() error {
	if r.closer != nil {
		return r.closer.Close()
	}
	return nil
}

type ShardReader struct {
	table     dax.TableKey
	partition dax.PartitionNum
	shard     dax.ShardNum
	version   int
	scanner   *bufio.Scanner
	closer    io.Closer
}

func NewShardReader(qtid dax.QualifiedTableID, partition dax.PartitionNum, shard dax.ShardNum, writelog io.ReadCloser) *ShardReader {
	r := &ShardReader{
		table:     qtid.Key(),
		partition: partition,
		shard:     shard,
		scanner:   bufio.NewScanner(writelog),
		closer:    writelog,
	}

	return r
}

func (r *ShardReader) Read() (computer.LogMessage, error) {
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

func (r *ShardReader) Close() error {
	if r.closer != nil {
		return r.closer.Close()
	}
	return nil
}
