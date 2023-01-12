package client

import (
	"context"
	"time"

	featurebase "github.com/molecula/featurebase/v3"
	"github.com/molecula/featurebase/v3/dax"
	"github.com/molecula/featurebase/v3/roaring"
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

// QTableToClientIndex
func QTableToClientIndex(qtbl *dax.QualifiedTable) *Index {
	sch := NewSchema()
	return sch.Index(string(qtbl.Key()),
		OptIndexKeys(qtbl.StringKeys()),
		OptIndexTrackExistence(true),
	)
}

// TableToClientIndex
func TableToClientIndex(tbl *dax.Table) *Index {
	sch := NewSchema()
	return sch.Index(string(tbl.ID),
		OptIndexKeys(tbl.StringKeys()),
		OptIndexTrackExistence(true),
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

const (
	existenceFieldName = "_exists"
)

// tableField is a helper function that returns the _exists field as a
// dax.Field when necessary (because dax.Table doesn't normally track the
// _exists field.
func tableField(tbl *dax.Table, fname dax.FieldName) (*dax.Field, error) {
	if fname == existenceFieldName {
		return &dax.Field{
			Name: existenceFieldName,
		}, nil
	}

	fld, ok := tbl.Field(fname)
	if !ok {
		return nil, errors.Errorf("field not in table: %s", fname)
	}
	return fld, nil
}

// TableFieldToClientField
func TableFieldToClientField(qtbl *dax.QualifiedTable, fname dax.FieldName) (*Field, error) {
	fld, err := tableField(&qtbl.Table, fname)
	if err != nil {
		return nil, errors.Wrapf(err, "getting field from table: %s", fname)
	}

	sch := NewSchema()
	idx := sch.Index(string(qtbl.Key()))

	opts := []FieldOption{}

	switch fld.Type {
	case dax.BaseTypeBool:
		opts = append(opts,
			OptFieldTypeBool(),
		)
	case dax.BaseTypeDecimal:
		opts = append(opts,
			OptFieldTypeDecimal(fld.Options.Scale, fld.Options.Min, fld.Options.Max),
		)
	case dax.BaseTypeID:
		opts = append(opts,
			OptFieldTypeMutex(CacheType(fld.Options.CacheType), int(fld.Options.CacheSize)),
			OptFieldKeys(false),
		)
	case dax.BaseTypeString:
		opts = append(opts,
			OptFieldTypeMutex(CacheType(fld.Options.CacheType), int(fld.Options.CacheSize)),
			OptFieldKeys(true),
		)
	case dax.BaseTypeIDSet:
		opts = append(opts,
			OptFieldTypeSet(CacheType(fld.Options.CacheType), int(fld.Options.CacheSize)),
			OptFieldKeys(false),
		)
	case dax.BaseTypeStringSet:
		opts = append(opts,
			OptFieldTypeSet(CacheType(fld.Options.CacheType), int(fld.Options.CacheSize)),
			OptFieldKeys(true),
		)
	case dax.BaseTypeInt:
		opts = append(opts,
			OptFieldTypeInt(fld.Options.Min.ToInt64(0), fld.Options.Max.ToInt64(0)),
		)
	case dax.BaseTypeTimestamp:
		opts = append(opts,
			OptFieldTypeTimestamp(fld.Options.Epoch, fld.Options.TimeUnit),
		)
	}

	return idx.Field(string(fld.Name), opts...), nil
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

///////////////////////////////////////////////////////////////

var _ featurebase.Importer = &importer{}

// importer is a pilosa client which implements the featurebase.Importer
// interface. This wrapper is necessary because of the call into
// client.Stats.Timing(), and because the client takes client specific types
// (like Index and Field), but the interface takes FeatureBase (and dax)
// specific types.
type importer struct {
	client    *Client
	schemaAPI featurebase.SchemaAPI
}

func NewImporter(c *Client, sapi featurebase.SchemaAPI) *importer {
	return &importer{
		client:    c,
		schemaAPI: sapi,
	}
}

func (i *importer) StartTransaction(ctx context.Context, id string, timeout time.Duration, exclusive bool, requestTimeout time.Duration) (*featurebase.Transaction, error) {
	return i.client.StartTransaction(id, timeout, exclusive, requestTimeout)
}

func (i *importer) FinishTransaction(ctx context.Context, id string) (*featurebase.Transaction, error) {
	return i.client.FinishTransaction(id)
}

func (i *importer) CreateTableKeys(ctx context.Context, tid dax.TableID, keys ...string) (map[string]uint64, error) {
	tbl, err := i.schemaAPI.TableByID(ctx, tid)
	if err != nil {
		return nil, err
	}
	return i.client.CreateIndexKeys(TableToClientIndex(tbl), keys...)
}

func (i *importer) CreateFieldKeys(ctx context.Context, tid dax.TableID, fname dax.FieldName, keys ...string) (map[string]uint64, error) {
	tbl, err := i.schemaAPI.TableByID(ctx, tid)
	if err != nil {
		return nil, err
	}
	fld, ok := tbl.Field(fname)
	if !ok {
		return nil, errors.Errorf("field not in table: %s", fname)
	}
	fi := featurebase.FieldToFieldInfo(fld)

	cfld, err := ToClientField(string(tbl.ID), fi)
	if err != nil {
		return nil, errors.Wrap(err, "converting to client field")
	}
	return i.client.CreateFieldKeys(cfld, keys...)
}

func (i *importer) ImportRoaringBitmap(ctx context.Context, tid dax.TableID, fld *dax.Field, shard uint64, views map[string]*roaring.Bitmap, clear bool) error {
	fi := featurebase.FieldToFieldInfo(fld)

	cfld, err := ToClientField(string(tid), fi)
	if err != nil {
		return errors.Wrap(err, "converting to client field")
	}
	return i.client.ImportRoaringBitmap(cfld, shard, views, clear)
}

func (i *importer) ImportRoaringShard(ctx context.Context, tid dax.TableID, shard uint64, request *featurebase.ImportRoaringShardRequest) error {
	return i.client.ImportRoaringShard(string(tid), shard, request)
}

func (i *importer) EncodeImportValues(ctx context.Context, tid dax.TableID, fld *dax.Field, shard uint64, vals []int64, ids []uint64, clear bool) (path string, data []byte, err error) {
	fi := featurebase.FieldToFieldInfo(fld)

	cfld, err := ToClientField(string(tid), fi)
	if err != nil {
		return "", nil, errors.Wrap(err, "converting to client field")
	}
	return i.client.EncodeImportValues(cfld, shard, vals, ids, clear)
}

func (i *importer) EncodeImport(ctx context.Context, tid dax.TableID, fld *dax.Field, shard uint64, vals, ids []uint64, clear bool) (path string, data []byte, err error) {
	fi := featurebase.FieldToFieldInfo(fld)

	cfld, err := ToClientField(string(tid), fi)
	if err != nil {
		return "", nil, errors.Wrap(err, "converting to client field")
	}
	return i.client.EncodeImport(cfld, shard, vals, ids, clear)
}

func (i *importer) DoImport(ctx context.Context, tid dax.TableID, fld *dax.Field, shard uint64, path string, data []byte) error {
	return i.client.DoImport(string(tid), shard, path, data)
}
