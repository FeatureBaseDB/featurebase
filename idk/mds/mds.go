// Package mds contains the implementation of the SchemaManager interface.
package mds

import (
	"context"

	"github.com/featurebasedb/featurebase/v3/dax"
)

// MDS represents the MDS methods which importer uses.
type MDS interface {
	IngestPartition(ctx context.Context, qtid dax.QualifiedTableID, partition dax.PartitionNum) (dax.Address, error)
	IngestShard(ctx context.Context, qtid dax.QualifiedTableID, shard dax.ShardNum) (dax.Address, error)

	// Table was added so the `importer` instance (in this package) of the
	// batch.Importer interface could lookup up a table based on the name
	// provided in a method, as opposed to setting the table up front. This is
	// because in queryer, we don't know the table yet, because we haven't
	// parsed the sql yet.
	Table(ctx context.Context, qtid dax.QualifiedTableID) (*dax.QualifiedTable, error)
}
