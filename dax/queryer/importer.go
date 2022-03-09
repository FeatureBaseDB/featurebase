package queryer

import (
	"context"
	"strings"
	"time"

	featurebase "github.com/molecula/featurebase/v3"
	"github.com/molecula/featurebase/v3/batch"
	"github.com/molecula/featurebase/v3/dax"
	"github.com/molecula/featurebase/v3/dax/mds/schemar"
	"github.com/molecula/featurebase/v3/errors"
	"github.com/molecula/featurebase/v3/roaring"
)

// Ensure type implements interface.
var _ batch.Importer = &batchImporter{}

func newBatchImporter(importer batch.Importer, qual dax.TableQualifier, schemar schemar.Schemar) *batchImporter {
	return &batchImporter{
		importer: importer,
		qual:     qual,
		schemar:  schemar,
	}
}

// batchImporter is an implementation of the batch.Importer. It is a wrapper
// around idk/mds/Importer that can take index values which are either indexName
// (like "foo") or TableKey (like "tbl__acme__db1__foo123"). This wrapper looks
// at the value to determine if it is a TableKey or not and converts it
// appropriately. It's kind of annoying; we really need to be certain where
// we're expecting indexName vs TableKey.
type batchImporter struct {
	importer batch.Importer
	qual     dax.TableQualifier
	schemar  schemar.Schemar
}

func (b *batchImporter) StartTransaction(ctx context.Context, id string, timeout time.Duration, exclusive bool, requestTimeout time.Duration) (*featurebase.Transaction, error) {
	return b.importer.StartTransaction(ctx, id, timeout, exclusive, requestTimeout)
}

func (b *batchImporter) FinishTransaction(ctx context.Context, id string) (*featurebase.Transaction, error) {
	return b.importer.FinishTransaction(ctx, id)
}

func (b *batchImporter) CreateIndexKeys(ctx context.Context, idx *featurebase.IndexInfo, keys ...string) (map[string]uint64, error) {
	// Used as an example:
	//   qual: [acme:db1]
	//   INSERT INTO foo VALUE (1, 10)
	// Currently, the table name coming through IndexInfo right now is the sql
	// table name (i.e. foo, not tbl__acme__db1__foo123). For SELECT queries,
	// we're currently doing that conversion in the orchestrator.Execute()
	// method (which means that we currently only support a single index in SQL
	// queries). Therefore, we need to convert idx.Name to a TableKey.
	tkey, err := b.indexToQualifiedTableKey(ctx, idx.Name)
	if err != nil {
		return nil, errors.Wrapf(err, "converting index to qualified table key: %s", idx.Name)
	}
	idx.Name = string(tkey)

	return b.importer.CreateIndexKeys(ctx, idx, keys...)
}

func (b *batchImporter) CreateFieldKeys(ctx context.Context, index string, field *featurebase.FieldInfo, keys ...string) (map[string]uint64, error) {
	tkey, err := b.indexToQualifiedTableKey(ctx, index)
	if err != nil {
		return nil, errors.Wrapf(err, "converting index to qualified table key: %s", index)
	}
	return b.importer.CreateFieldKeys(ctx, string(tkey), field, keys...)
}

func (b *batchImporter) ImportRoaringBitmap(ctx context.Context, index string, field *featurebase.FieldInfo, shard uint64, views map[string]*roaring.Bitmap, clear bool) error {
	tkey, err := b.indexToQualifiedTableKey(ctx, index)
	if err != nil {
		return errors.Wrapf(err, "converting index to qualified table key: %s", index)
	}
	return b.importer.ImportRoaringBitmap(ctx, string(tkey), field, shard, views, clear)
}

func (b *batchImporter) ImportRoaringShard(ctx context.Context, index string, shard uint64, request *featurebase.ImportRoaringShardRequest) error {
	tkey, err := b.indexToQualifiedTableKey(ctx, index)
	if err != nil {
		return errors.Wrapf(err, "converting index to qualified table key: %s", index)
	}
	return b.importer.ImportRoaringShard(ctx, string(tkey), shard, request)
}

func (b *batchImporter) EncodeImportValues(ctx context.Context, index string, field *featurebase.FieldInfo, shard uint64, vals []int64, ids []uint64, clear bool) (path string, data []byte, err error) {
	tkey, err := b.indexToQualifiedTableKey(ctx, index)
	if err != nil {
		return "", nil, errors.Wrapf(err, "converting index to qualified table key: %s", index)
	}
	return b.importer.EncodeImportValues(ctx, string(tkey), field, shard, vals, ids, clear)
}

func (b *batchImporter) EncodeImport(ctx context.Context, index string, field *featurebase.FieldInfo, shard uint64, vals, ids []uint64, clear bool) (path string, data []byte, err error) {
	tkey, err := b.indexToQualifiedTableKey(ctx, index)
	if err != nil {
		return "", nil, errors.Wrapf(err, "converting index to qualified table key: %s", index)
	}
	return b.importer.EncodeImport(ctx, string(tkey), field, shard, vals, ids, clear)
}

func (b *batchImporter) DoImport(ctx context.Context, index string, field *featurebase.FieldInfo, shard uint64, path string, data []byte) error {
	tkey, err := b.indexToQualifiedTableKey(ctx, index)
	if err != nil {
		return errors.Wrapf(err, "converting index to qualified table key: %s", index)
	}
	return b.importer.DoImport(ctx, string(tkey), field, shard, path, data)
}

func (b *batchImporter) StatsTiming(name string, value time.Duration, rate float64) {
	b.importer.StatsTiming(name, value, rate)
}

// TODO(tlt): this method was copied from orchestrator.go. Can we centralize
// this logic?
func (b *batchImporter) indexToQualifiedTableKey(ctx context.Context, index string) (dax.TableKey, error) {
	if strings.HasPrefix(index, dax.PrefixTable+dax.TableKeyDelimiter) {
		return dax.TableKey(index), nil
	}

	qtid, err := b.schemar.TableID(ctx, b.qual, dax.TableName(index))
	if err != nil {
		return "", errors.Wrap(err, "converting index to qualified table id")
	}
	return qtid.Key(), nil
}
