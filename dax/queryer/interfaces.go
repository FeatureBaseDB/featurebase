package queryer

import (
	"context"

	featurebase "github.com/featurebasedb/featurebase/v3"
	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/dax/mds/controller"
	"github.com/featurebasedb/featurebase/v3/dax/mds/schemar"
)

type MDS interface {
	// Controller-related methods.
	ComputeNodes(ctx context.Context, qtid dax.QualifiedTableID, shards ...dax.ShardNum) ([]controller.ComputeNode, error)
	IngestPartition(ctx context.Context, qtid dax.QualifiedTableID, partition dax.PartitionNum) (dax.Address, error)
	IngestShard(ctx context.Context, qtid dax.QualifiedTableID, shard dax.ShardNum) (dax.Address, error)
	TranslateNodes(ctx context.Context, qtid dax.QualifiedTableID, partitions ...dax.PartitionNum) ([]controller.TranslateNode, error)

	// Schemar-related methods.
	schemar.Schemar
}

// Ensure type implements interface.
var _ MDS = &NopMDS{}

// NopMDS is a no-op implementation of the MDS interface.
type NopMDS struct {
	schemar.NopSchemar
}

func NewNopMDS() *NopMDS {
	return &NopMDS{}
}

func (m *NopMDS) ComputeNodes(ctx context.Context, qtid dax.QualifiedTableID, shards ...dax.ShardNum) ([]controller.ComputeNode, error) {
	return nil, nil
}
func (m *NopMDS) IngestPartition(ctx context.Context, qtid dax.QualifiedTableID, partition dax.PartitionNum) (dax.Address, error) {
	return "", nil
}
func (m *NopMDS) IngestShard(ctx context.Context, qtid dax.QualifiedTableID, shard dax.ShardNum) (dax.Address, error) {
	return "", nil
}
func (m *NopMDS) TranslateNodes(ctx context.Context, qtid dax.QualifiedTableID, partitions ...dax.PartitionNum) ([]controller.TranslateNode, error) {
	return nil, nil
}

type Importer interface {
	CreateIndexKeys(ctx context.Context, index string, keys ...string) (map[string]uint64, error)
	CreateFieldKeys(ctx context.Context, index, field string, keys ...string) (map[string]uint64, error)
	Import(ctx context.Context, req *featurebase.ImportRequest, opts ...featurebase.ImportOption) error
	ImportValue(ctx context.Context, req *featurebase.ImportValueRequest, opts ...featurebase.ImportOption) error
}

// Ensure type implements interface.
var _ Importer = &NopImporter{}

// NopImporter is a no-op implementation of the Importer interface.
type NopImporter struct{}

func NewNopImporter() *NopImporter {
	return &NopImporter{}
}

func (n *NopImporter) CreateIndexKeys(ctx context.Context, index string, keys ...string) (map[string]uint64, error) {
	return nil, nil
}

func (n *NopImporter) CreateFieldKeys(ctx context.Context, index, field string, keys ...string) (map[string]uint64, error) {
	return nil, nil
}

func (n *NopImporter) Import(ctx context.Context, req *featurebase.ImportRequest, opts ...featurebase.ImportOption) error {
	return nil
}
func (n *NopImporter) ImportValue(ctx context.Context, req *featurebase.ImportValueRequest, opts ...featurebase.ImportOption) error {
	return nil
}
