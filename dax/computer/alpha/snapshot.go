// Package alpha contains an implementation of the SnapshotReadWriter interface.
// In the case where a sub-service (such as snapshotter) implements these
// interfaces directly with both its service and its http client, then we don't
// need this middle implementation layer. But in this case, the Snapshotter
// operates as a third-party service might, meaning its API methods don't align
// with what FeatureBase needs to call. So this implementation acts as a
// translation later between the featurebase-to-snapshotter interface, and the
// third-party Snapshotter service.
package alpha

import (
	"context"
	"io"

	"github.com/molecula/featurebase/v3/dax"
	"github.com/molecula/featurebase/v3/dax/computer"
	"github.com/molecula/featurebase/v3/errors"
)

// Ensure type implements interface.
var _ computer.SnapshotReadWriter = &alphaSnapshot{}

// alphaSnapshot uses a Snapshotter implementation (which could be, for
// example, an http client or a locally running sub-service) to store its
// snapshots.
type alphaSnapshot struct {
	ss computer.Snapshotter
}

func NewAlphaSnapshot(sser computer.Snapshotter) *alphaSnapshot {
	return &alphaSnapshot{
		ss: sser,
	}
}

func (s *alphaSnapshot) WriteShardData(ctx context.Context, qtid dax.QualifiedTableID, partition dax.PartitionNum, shard dax.ShardNum, version int, rc io.ReadCloser) error {
	bucket := partitionBucket(qtid.Key(), partition)
	key := shardKey(shard)

	if err := s.ss.Write(bucket, key, version, rc); err != nil {
		return errors.Wrapf(err, "writing shard data: %s, %d", key, version)
	}

	return nil
}

func (s *alphaSnapshot) ReadShardData(ctx context.Context, qtid dax.QualifiedTableID, partition dax.PartitionNum, shard dax.ShardNum, version int) (io.ReadCloser, error) {
	bucket := partitionBucket(qtid.Key(), partition)
	key := shardKey(shard)

	rc, err := s.ss.Read(bucket, key, version)
	if err != nil {
		return nil, errors.Wrapf(err, "reading shard data: %s, %s, %d", bucket, key, version)
	}

	return rc, nil
}

func (s *alphaSnapshot) WriteTableKeys(ctx context.Context, qtid dax.QualifiedTableID, partition dax.PartitionNum, version int, wrTo io.WriterTo) error {
	bucket := partitionBucket(qtid.Key(), partition)
	key := keysFileName

	if err := s.ss.WriteTo(bucket, key, version, wrTo); err != nil {
		return errors.Wrapf(err, "writing table keys: %s, %d", key, version)
	}

	return nil
}

func (s *alphaSnapshot) ReadTableKeys(ctx context.Context, qtid dax.QualifiedTableID, partition dax.PartitionNum, version int) (io.ReadCloser, error) {
	bucket := partitionBucket(qtid.Key(), partition)
	key := keysFileName

	rc, err := s.ss.Read(bucket, key, version)
	if err != nil {
		return nil, errors.Wrapf(err, "reading table keys: %s, %s, %d", bucket, key, version)
	}

	return rc, nil
}

func (s *alphaSnapshot) WriteFieldKeys(ctx context.Context, qtid dax.QualifiedTableID, field dax.FieldName, version int, wrTo io.WriterTo) error {
	bucket := fieldBucket(qtid.Key(), field)
	key := keysFileName

	if err := s.ss.WriteTo(bucket, key, version, wrTo); err != nil {
		return errors.Wrapf(err, "writing field keys: %s, %d", key, version)
	}

	return nil
}

func (s *alphaSnapshot) ReadFieldKeys(ctx context.Context, qtid dax.QualifiedTableID, field dax.FieldName, version int) (io.ReadCloser, error) {
	bucket := fieldBucket(qtid.Key(), field)
	key := keysFileName

	rc, err := s.ss.Read(bucket, key, version)
	if err != nil {
		return nil, errors.Wrapf(err, "reading field keys: %s, %s, %d", bucket, key, version)
	}

	return rc, nil
}
