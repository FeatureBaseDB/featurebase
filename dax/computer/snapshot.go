package computer

import (
	"context"
	"io"

	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/errors"
)

// Ensure type implements interface.
var _ SnapshotReadWriter = &snapshotReadWriter{}

// snapshotReadWriter uses a SnapshotService implementation (which could be, for
// example, an http client or a locally running sub-service) to store its
// snapshots.
type snapshotReadWriter struct {
	ss SnapshotService
}

func NewSnapshotReadWriter(ss SnapshotService) *snapshotReadWriter {
	return &snapshotReadWriter{
		ss: ss,
	}
}

func (s *snapshotReadWriter) WriteShardData(ctx context.Context, qtid dax.QualifiedTableID, partition dax.PartitionNum, shard dax.ShardNum, version int, rc io.ReadCloser) error {
	bucket := partitionBucket(qtid.Key(), partition)
	key := shardKey(shard)

	if err := s.ss.Write(bucket, key, version, rc); err != nil {
		return errors.Wrapf(err, "writing shard data: %s, %d", key, version)
	}

	return nil
}

func (s *snapshotReadWriter) ReadShardData(ctx context.Context, qtid dax.QualifiedTableID, partition dax.PartitionNum, shard dax.ShardNum, version int) (io.ReadCloser, error) {
	bucket := partitionBucket(qtid.Key(), partition)
	key := shardKey(shard)

	rc, err := s.ss.Read(bucket, key, version)
	if err != nil {
		return nil, errors.Wrapf(err, "reading shard data: %s, %s, %d", bucket, key, version)
	}

	return rc, nil
}

func (s *snapshotReadWriter) WriteTableKeys(ctx context.Context, qtid dax.QualifiedTableID, partition dax.PartitionNum, version int, wrTo io.WriterTo) error {
	bucket := partitionBucket(qtid.Key(), partition)
	key := keysFileName

	if err := s.ss.WriteTo(bucket, key, version, wrTo); err != nil {
		return errors.Wrapf(err, "writing table keys: %s, %d", key, version)
	}

	return nil
}

func (s *snapshotReadWriter) ReadTableKeys(ctx context.Context, qtid dax.QualifiedTableID, partition dax.PartitionNum, version int) (io.ReadCloser, error) {
	bucket := partitionBucket(qtid.Key(), partition)
	key := keysFileName

	rc, err := s.ss.Read(bucket, key, version)
	if err != nil {
		return nil, errors.Wrapf(err, "reading table keys: %s, %s, %d", bucket, key, version)
	}

	return rc, nil
}

func (s *snapshotReadWriter) WriteFieldKeys(ctx context.Context, qtid dax.QualifiedTableID, field dax.FieldName, version int, wrTo io.WriterTo) error {
	bucket := fieldBucket(qtid.Key(), field)
	key := keysFileName

	if err := s.ss.WriteTo(bucket, key, version, wrTo); err != nil {
		return errors.Wrapf(err, "writing field keys: %s, %d", key, version)
	}

	return nil
}

func (s *snapshotReadWriter) ReadFieldKeys(ctx context.Context, qtid dax.QualifiedTableID, field dax.FieldName, version int) (io.ReadCloser, error) {
	bucket := fieldBucket(qtid.Key(), field)
	key := keysFileName

	rc, err := s.ss.Read(bucket, key, version)
	if err != nil {
		return nil, errors.Wrapf(err, "reading field keys: %s, %s, %d", bucket, key, version)
	}

	return rc, nil
}
