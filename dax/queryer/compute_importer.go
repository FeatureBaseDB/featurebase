package queryer

import (
	"context"

	featurebase "github.com/molecula/featurebase/v3"
	"github.com/molecula/featurebase/v3/client"
	"github.com/molecula/featurebase/v3/dax"
	"github.com/molecula/featurebase/v3/errors"
)

// Ensure type implements interface.
var _ Importer = &ComputeImporter{}

// ComputeImporter is an implementation of the Importer interface which uses a
// featurebase client to communicate with the compute node.
type ComputeImporter struct {
	addr dax.Address
}

func NewComputeImporter(addr dax.Address) *ComputeImporter {
	return &ComputeImporter{
		addr: addr,
	}
}

func (ci *ComputeImporter) CreateIndexKeys(ctx context.Context, index string, keys ...string) (map[string]uint64, error) {
	fbClient, err := fbClient(ci.addr)
	if err != nil {
		return nil, errors.Wrap(err, "getting fb client")
	}

	idx := client.NewIndex(index)
	return fbClient.CreateIndexKeys(idx, keys...)
}

func (ci *ComputeImporter) CreateFieldKeys(ctx context.Context, index, field string, keys ...string) (map[string]uint64, error) {
	fbClient, err := fbClient(ci.addr)
	if err != nil {
		return nil, errors.Wrap(err, "getting fb client")
	}

	idx := client.NewIndex(index)
	fld := idx.Field(field)
	return fbClient.CreateFieldKeys(fld, keys...)
}

func (ci *ComputeImporter) Import(ctx context.Context, req *featurebase.ImportRequest, opts ...featurebase.ImportOption) error {
	fbClient, err := fbClient(ci.addr)
	if err != nil {
		return errors.Wrap(err, "getting fb client")
	}

	idx := client.NewIndex(req.Index)
	fld := idx.Field(req.Field)
	return fbClient.Import(fld, req.Shard, req.RowIDs, req.ColumnIDs, req.Clear)
}

func (ci *ComputeImporter) ImportValue(ctx context.Context, req *featurebase.ImportValueRequest, opts ...featurebase.ImportOption) error {
	fbClient, err := fbClient(ci.addr)
	if err != nil {
		return errors.Wrap(err, "getting fb client")
	}

	idx := client.NewIndex(req.Index)
	fld := idx.Field(req.Field)
	return fbClient.ImportValues(fld, req.Shard, req.Values, req.ColumnIDs, req.Clear)
}
