package pilosa

import (
	"context"
	"time"

	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/roaring"
)

type Importer interface {
	StartTransaction(ctx context.Context, id string, timeout time.Duration, exclusive bool, requestTimeout time.Duration) (*Transaction, error)
	FinishTransaction(ctx context.Context, id string) (*Transaction, error)
	CreateTableKeys(ctx context.Context, tid dax.TableID, keys ...string) (map[string]uint64, error)
	CreateFieldKeys(ctx context.Context, tid dax.TableID, fname dax.FieldName, keys ...string) (map[string]uint64, error)
	ImportRoaringBitmap(ctx context.Context, tid dax.TableID, fld *dax.Field, shard uint64, views map[string]*roaring.Bitmap, clear bool) error
	ImportRoaringShard(ctx context.Context, tid dax.TableID, shard uint64, request *ImportRoaringShardRequest) error
	EncodeImportValues(ctx context.Context, tid dax.TableID, fld *dax.Field, shard uint64, vals []int64, ids []uint64, clear bool) (path string, data []byte, err error)
	EncodeImport(ctx context.Context, tid dax.TableID, fld *dax.Field, shard uint64, vals, ids []uint64, clear bool) (path string, data []byte, err error)
	DoImport(ctx context.Context, tid dax.TableID, fld *dax.Field, shard uint64, path string, data []byte) error

	StatsTiming(name string, value time.Duration, rate float64)
}

// Ensure type implements interface.
var _ Importer = &onPremImporter{}

// onPremImporter is a wrapper around API which implements the Importer
// interface. This is currently only used by sql3 running locally in standard
// (i.e not "serverless") mode. Because sql3 always sets
// `useShardTransactionalEndpoint = true`, There are several methods which this
// implemtation of the Importer interface does not use, and therefore they
// intentionally no-op.
type onPremImporter struct {
	api *API
}

func NewOnPremImporter(api *API) *onPremImporter {
	return &onPremImporter{
		api: api,
	}
}

func (i *onPremImporter) StartTransaction(ctx context.Context, id string, timeout time.Duration, exclusive bool, requestTimeout time.Duration) (*Transaction, error) {
	return i.api.StartTransaction(ctx, id, timeout, exclusive, false)
}

func (i *onPremImporter) FinishTransaction(ctx context.Context, id string) (*Transaction, error) {
	return i.api.FinishTransaction(ctx, id, false)
}

func (i *onPremImporter) CreateTableKeys(ctx context.Context, tid dax.TableID, keys ...string) (map[string]uint64, error) {
	return i.api.CreateIndexKeys(ctx, string(tid), keys...)
}

func (i *onPremImporter) CreateFieldKeys(ctx context.Context, tid dax.TableID, fname dax.FieldName, keys ...string) (map[string]uint64, error) {
	return i.api.CreateFieldKeys(ctx, string(tid), string(fname), keys...)
}

func (i *onPremImporter) ImportRoaringBitmap(ctx context.Context, tid dax.TableID, fld *dax.Field, shard uint64, views map[string]*roaring.Bitmap, clear bool) error {
	// This intentionally no-ops. See comment on struct.
	return nil
}

func (i *onPremImporter) ImportRoaringShard(ctx context.Context, tid dax.TableID, shard uint64, request *ImportRoaringShardRequest) error {
	return i.api.ImportRoaringShard(ctx, string(tid), shard, request)
}

// EncodeImportValues is kind of weird. We're trying to mimic what the client
// does here (because the Importer interface was originally based off of the
// client methods). So we end up generating a protobuf-encode byte slice. And we
// don't really use path.
func (i *onPremImporter) EncodeImportValues(ctx context.Context, tid dax.TableID, fld *dax.Field, shard uint64, vals []int64, ids []uint64, clear bool) (path string, data []byte, err error) {
	// This intentionally no-ops. See comment on struct.
	return "", nil, nil
}

func (i *onPremImporter) EncodeImport(ctx context.Context, tid dax.TableID, fld *dax.Field, shard uint64, vals, ids []uint64, clear bool) (path string, data []byte, err error) {
	// This intentionally no-ops. See comment on struct.
	return "", nil, nil
}

func (i *onPremImporter) DoImport(ctx context.Context, _ dax.TableID, fld *dax.Field, shard uint64, path string, data []byte) error {
	// This intentionally no-ops. See comment on struct.
	return nil
}

func (i *onPremImporter) StatsTiming(name string, value time.Duration, rate float64) {}
