package dax

import (
	"context"
)

// VersionStore is an interface for tracking Shard, Partition, and Field[Key]
// versions. For example, when the contents of a shard are checkpointed, and a
// snapshot is generated, and the write log messages for that shard are
// truncated, the ShardVersion for that shard is incremented. The VersionStore
// is the interface through which various services read/write that version.
type VersionStore interface {
	AddTable(ctx context.Context, qtid QualifiedTableID) error
	RemoveTable(ctx context.Context, qtid QualifiedTableID) (VersionedShards, VersionedPartitions, error)

	// Shards (shardData)
	AddShards(ctx context.Context, qtid QualifiedTableID, shards ...VersionedShard) error
	Shards(ctx context.Context, qtid QualifiedTableID) (VersionedShards, bool, error)
	ShardVersion(ctx context.Context, qtid QualifiedTableID, shardNum ShardNum) (int, bool, error)
	ShardTables(ctx context.Context, qual TableQualifier) (TableIDs, error)

	// Partitions (tableKeys)
	AddPartitions(ctx context.Context, qtid QualifiedTableID, partitions ...VersionedPartition) error
	Partitions(ctx context.Context, qtid QualifiedTableID) (VersionedPartitions, bool, error)
	PartitionVersion(ctx context.Context, qtid QualifiedTableID, partitionNum PartitionNum) (int, bool, error)
	PartitionTables(ctx context.Context, qual TableQualifier) (TableIDs, error)

	// Fields (fieldKeys)
	AddFields(ctx context.Context, qtid QualifiedTableID, fields ...VersionedField) error
	Fields(ctx context.Context, qtid QualifiedTableID) (VersionedFields, bool, error)
	FieldVersion(ctx context.Context, qtid QualifiedTableID, field FieldName) (int, bool, error)
	FieldTables(ctx context.Context, qual TableQualifier) (TableIDs, error)

	Copy(ctx context.Context) (VersionStore, error)
}

type DirectiveVersion interface {
	Increment(ctx context.Context, delta uint64) (uint64, error)
}

// Ensure type implements interface.
var _ VersionStore = (*nopVersionStore)(nil)

// nopVersionStore is a no-op implementation of the VersionStore interface.
type nopVersionStore struct{}

// NewNopVersionStore returns a new no-op instance of VersionStore.
func NewNopVersionStore() *nopVersionStore {
	return &nopVersionStore{}
}

func (s *nopVersionStore) AddTable(ctx context.Context, qtid QualifiedTableID) error {
	return nil
}

func (s *nopVersionStore) RemoveTable(ctx context.Context, qtid QualifiedTableID) (VersionedShards, VersionedPartitions, error) {
	return nil, nil, nil
}

func (s *nopVersionStore) AddShards(ctx context.Context, qtid QualifiedTableID, shards ...VersionedShard) error {
	return nil
}

func (s *nopVersionStore) Shards(ctx context.Context, qtid QualifiedTableID) (VersionedShards, bool, error) {
	return nil, false, nil
}

func (s *nopVersionStore) ShardVersion(ctx context.Context, qtid QualifiedTableID, shardNum ShardNum) (int, bool, error) {
	return 0, true, nil
}

func (s *nopVersionStore) ShardTables(ctx context.Context, qual TableQualifier) (TableIDs, error) {
	return TableIDs{}, nil
}

func (s *nopVersionStore) AddPartitions(ctx context.Context, qtid QualifiedTableID, partitions ...VersionedPartition) error {
	return nil
}

func (s *nopVersionStore) Partitions(ctx context.Context, qtid QualifiedTableID) (VersionedPartitions, bool, error) {
	return nil, false, nil
}

func (s *nopVersionStore) PartitionVersion(ctx context.Context, qtid QualifiedTableID, partitionNum PartitionNum) (int, bool, error) {
	return 0, true, nil
}

func (s *nopVersionStore) PartitionTables(ctx context.Context, qual TableQualifier) (TableIDs, error) {
	return TableIDs{}, nil
}

func (s *nopVersionStore) AddFields(ctx context.Context, qtid QualifiedTableID, fields ...VersionedField) error {
	return nil
}

func (s *nopVersionStore) Fields(ctx context.Context, qtid QualifiedTableID) (VersionedFields, bool, error) {
	return nil, false, nil
}

func (s *nopVersionStore) FieldVersion(ctx context.Context, qtid QualifiedTableID, field FieldName) (int, bool, error) {
	return 0, true, nil
}

func (s *nopVersionStore) FieldTables(ctx context.Context, qual TableQualifier) (TableIDs, error) {
	return TableIDs{}, nil
}

func (s *nopVersionStore) Copy(ctx context.Context) (VersionStore, error) {
	return nil, nil
}
