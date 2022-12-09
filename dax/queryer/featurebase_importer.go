package queryer

import (
	"context"

	featurebase "github.com/featurebasedb/featurebase/v3"
)

// Ensure type implements interface.
var _ Importer = &FeatureBaseImporter{}

// FeatureBaseImporter is an implementation of the Importer interface which uses
// a pointer to a featurebase.API to make the underlying calls. This assumes
// those calls need to be Qcx aware, so this takes that into account.
type FeatureBaseImporter struct {
	api *featurebase.API
}

func NewFeatureBaseImporter(api *featurebase.API) *FeatureBaseImporter {
	return &FeatureBaseImporter{
		api: api,
	}
}

func (fi *FeatureBaseImporter) CreateIndexKeys(ctx context.Context, index string, keys ...string) (map[string]uint64, error) {
	return fi.api.CreateIndexKeys(ctx, index, keys...)
}

func (fi *FeatureBaseImporter) CreateFieldKeys(ctx context.Context, index, field string, keys ...string) (map[string]uint64, error) {
	return fi.api.CreateFieldKeys(ctx, index, field, keys...)
}

func (fi *FeatureBaseImporter) Import(ctx context.Context, req *featurebase.ImportRequest, opts ...featurebase.ImportOption) error {
	qcx := fi.api.Txf().NewQcx()
	return fi.api.Import(ctx, qcx, req, opts...)
}

func (fi *FeatureBaseImporter) ImportValue(ctx context.Context, req *featurebase.ImportValueRequest, opts ...featurebase.ImportOption) error {
	qcx := fi.api.Txf().NewQcx()
	return fi.api.ImportValue(ctx, qcx, req, opts...)
}
