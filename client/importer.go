package client

import (
	"context"
	"time"

	featurebase "github.com/featurebasedb/featurebase/v3"
	"github.com/featurebasedb/featurebase/v3/roaring"
	"github.com/pkg/errors"
)

//////////////////////////////////////////////////////////////////////////////
//
// The following was introduced upon splitting batch out of the client package
// (and into its own package). During that work, we changed batch from using
// client.Index, and instead using featurebase.IndexInfo. The functions below
// convert client types from/to featurebase types in order to satisfy the batch
// requirements.
//
//////////////////////////////////////////////////////////////////////////////

// FromClientIndex converts a client Index to a featurebase IndexInfo.
func FromClientIndex(ci *Index) *featurebase.IndexInfo {
	return &featurebase.IndexInfo{
		Name:       ci.Name(),
		CreatedAt:  ci.CreatedAt(),
		Options:    fromClientIndexOptions(ci.Opts()),
		Fields:     fromClientFieldsMap(ci.Fields()),
		ShardWidth: ci.ShardWidth(),
	}
}

// fromClientIndexOptions
func fromClientIndexOptions(cio IndexOptions) featurebase.IndexOptions {
	return featurebase.IndexOptions{
		Keys:           cio.Keys(),
		TrackExistence: cio.TrackExistence(),
		PartitionN:     0, // TODO(tlt): this shouldn't be 0 once we support it.
	}
}

// ToClientIndex
func ToClientIndex(fi *featurebase.IndexInfo) *Index {
	sch := NewSchema()
	return sch.Index(fi.Name,
		OptIndexKeys(fi.Options.Keys),
		OptIndexTrackExistence(fi.Options.TrackExistence),
	)
}

// fromClientField
func fromClientField(cf *Field) *featurebase.FieldInfo {
	return &featurebase.FieldInfo{
		Name:      cf.Name(),
		CreatedAt: cf.CreatedAt(),
		Options:   fromClientFieldOptions(cf.Opts()),
		// TODO(tlt): do we need Views? Because we can't currently get views
		// from client.
		// Views: ??
	}
}

// fromClientFieldOptions
func fromClientFieldOptions(cfo FieldOptions) featurebase.FieldOptions {
	return featurebase.FieldOptions{
		Base:           cfo.Base(),
		BitDepth:       0, // TODO(tlt): set this?
		Min:            cfo.Min(),
		Max:            cfo.Max(),
		Scale:          cfo.Scale(),
		Keys:           cfo.Keys(),
		NoStandardView: cfo.NoStandardView(),
		CacheSize:      uint32(cfo.CacheSize()),
		CacheType:      string(cfo.CacheType()),
		Type:           string(cfo.Type()),
		TimeUnit:       cfo.TimeUnit(),
		TimeQuantum:    featurebase.TimeQuantum(cfo.TimeQuantum()),
		ForeignIndex:   cfo.ForeignIndex(),
		TTL:            cfo.TTL(),
	}
}

// ToClientField
func ToClientField(index string, ff *featurebase.FieldInfo) (*Field, error) {
	sch := NewSchema()
	idx := sch.Index(index)

	opts := []FieldOption{}

	switch ff.Options.Type {
	case featurebase.FieldTypeBool:
		opts = append(opts,
			OptFieldTypeBool(),
		)
	case featurebase.FieldTypeDecimal:
		opts = append(opts,
			OptFieldTypeDecimal(ff.Options.Scale, ff.Options.Min, ff.Options.Max),
		)
	case featurebase.FieldTypeMutex:
		opts = append(opts,
			OptFieldTypeMutex(CacheType(ff.Options.CacheType), int(ff.Options.CacheSize)),
			OptFieldKeys(ff.Options.Keys),
		)
	case featurebase.FieldTypeSet:
		opts = append(opts,
			OptFieldTypeSet(CacheType(ff.Options.CacheType), int(ff.Options.CacheSize)),
			OptFieldKeys(ff.Options.Keys),
		)
	case featurebase.FieldTypeInt:
		opts = append(opts,
			OptFieldTypeInt(ff.Options.Min.ToInt64(0), ff.Options.Max.ToInt64(0)),
		)
	case featurebase.FieldTypeTimestamp:
		epoch, err := featurebase.ValToTimestamp(ff.Options.TimeUnit, ff.Options.Base)
		if err != nil {
			return nil, errors.Wrapf(err, "calculating epoch: %s, %d", ff.Options.TimeUnit, ff.Options.Base)
		}
		opts = append(opts,
			OptFieldTypeTimestamp(epoch, ff.Options.TimeUnit),
		)
	}

	return idx.Field(ff.Name, opts...), nil
}

// FromClientFields converts a slice of client Fields to a slice of featurebase
// FieldInfo.
func FromClientFields(cf []*Field) []*featurebase.FieldInfo {
	ff := make([]*featurebase.FieldInfo, len(cf))
	for i := range cf {
		ff[i] = fromClientField(cf[i])
	}
	return ff
}

// fromClientFieldsMap
func fromClientFieldsMap(cf map[string]*Field) []*featurebase.FieldInfo {
	ff := make([]*featurebase.FieldInfo, 0, len(cf))
	for _, v := range cf {
		ff = append(ff, fromClientField(v))
	}
	return ff
}

// We can't import the batch package into client because it results in an import
// loop. That's probably an indication that this interface implementation should
// be moved somewhere else; for example, into a sub-package of the batch package
// (since it's an implementation of one of batch's interfaces).
// var _ batch.Importer = &importer{}

// importer is a pilosa client which implements the batch.Importer interface.
// This wrapper is necessary because of the call into client.Stats.Timing(), and
// because the client takes client specific types (like Index and Field), but
// the interface takes FeatureBase specific types.
type importer struct {
	*Client
}

func NewImporter(c *Client) *importer {
	return &importer{
		Client: c,
	}
}

func (i *importer) StartTransaction(ctx context.Context, id string, timeout time.Duration, exclusive bool, requestTimeout time.Duration) (*featurebase.Transaction, error) {
	return i.Client.StartTransaction(id, timeout, exclusive, requestTimeout)
}

func (i *importer) FinishTransaction(ctx context.Context, id string) (*featurebase.Transaction, error) {
	return i.Client.FinishTransaction(id)
}

func (i *importer) CreateIndexKeys(ctx context.Context, idx *featurebase.IndexInfo, keys ...string) (map[string]uint64, error) {
	return i.Client.CreateIndexKeys(ToClientIndex(idx), keys...)
}

func (i *importer) CreateFieldKeys(ctx context.Context, index string, field *featurebase.FieldInfo, keys ...string) (map[string]uint64, error) {
	fld, err := ToClientField(index, field)
	if err != nil {
		return nil, errors.Wrap(err, "converting to client field")
	}
	return i.Client.CreateFieldKeys(fld, keys...)
}

func (i *importer) ImportRoaringBitmap(ctx context.Context, index string, field *featurebase.FieldInfo, shard uint64, views map[string]*roaring.Bitmap, clear bool) error {
	fld, err := ToClientField(index, field)
	if err != nil {
		return errors.Wrap(err, "converting to client field")
	}
	return i.Client.ImportRoaringBitmap(fld, shard, views, clear)
}

func (i *importer) ImportRoaringShard(ctx context.Context, index string, shard uint64, request *featurebase.ImportRoaringShardRequest) error {
	return i.Client.ImportRoaringShard(index, shard, request)
}

func (i *importer) EncodeImportValues(ctx context.Context, index string, field *featurebase.FieldInfo, shard uint64, vals []int64, ids []uint64, clear bool) (path string, data []byte, err error) {
	fld, err := ToClientField(index, field)
	if err != nil {
		return "", nil, errors.Wrap(err, "converting to client field")
	}
	return i.Client.EncodeImportValues(fld, shard, vals, ids, clear)
}

func (i *importer) EncodeImport(ctx context.Context, index string, field *featurebase.FieldInfo, shard uint64, vals, ids []uint64, clear bool) (path string, data []byte, err error) {
	fld, err := ToClientField(index, field)
	if err != nil {
		return "", nil, errors.Wrap(err, "converting to client field")
	}
	return i.Client.EncodeImport(fld, shard, vals, ids, clear)
}

func (i *importer) DoImport(ctx context.Context, index string, field *featurebase.FieldInfo, shard uint64, path string, data []byte) error {
	return i.Client.DoImport(index, shard, path, data)
}

func (i *importer) StatsTiming(name string, value time.Duration, rate float64) {
	i.Client.Stats.Timing(name, value, rate)
}
