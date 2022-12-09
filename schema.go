package pilosa

import (
	"context"
	"fmt"
	"log"
	"sort"
	"time"

	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/errors"
	"github.com/featurebasedb/featurebase/v3/pql"
)

// Ensure type implements interface.
var _ SchemaAPI = (*onPremSchema)(nil)

type onPremSchema struct {
	api *API
}

func NewOnPremSchema(api *API) *onPremSchema {
	return &onPremSchema{
		api: api,
	}
}

func (s *onPremSchema) TableByName(ctx context.Context, tname dax.TableName) (*dax.Table, error) {
	idx, err := s.api.IndexInfo(context.Background(), string(tname))
	if err != nil {
		return nil, errors.Wrapf(err, "getting index info for table name: %s", tname)
	}

	return IndexInfoToTable(idx), nil
}

func (s *onPremSchema) TableByID(ctx context.Context, tid dax.TableID) (*dax.Table, error) {
	idx, err := s.api.IndexInfo(context.Background(), string(tid))
	if err != nil {
		return nil, errors.Wrapf(err, "getting index info for table id: %s", tid)
	}

	return IndexInfoToTable(idx), nil
}

func (s *onPremSchema) Tables(ctx context.Context) ([]*dax.Table, error) {
	idxs, err := s.api.Schema(ctx, false)
	if err != nil {
		return nil, errors.Wrap(err, "getting schema")
	}

	return IndexInfosToTables(idxs), nil
}

func (s *onPremSchema) CreateTable(ctx context.Context, tbl *dax.Table) error {
	// We make a slice of fields with the _id field removed. Also, while we're
	// at it, we can use the type of the _id field to determine if the index
	// should be keyed.
	var keyed bool
	flds := make([]*dax.Field, 0)
	for _, fld := range tbl.Fields {
		if fld.Name == "_id" {
			if fld.Type == dax.BaseTypeString {
				keyed = true
			}
			continue
		}
		flds = append(flds, fld)
	}

	iopts := IndexOptions{
		Keys:           keyed,
		TrackExistence: true,
		PartitionN:     tbl.PartitionN,
		Description:    tbl.Description,
	}

	// Add the index.
	if _, err := s.api.CreateIndex(ctx, string(tbl.Name), iopts); err != nil {
		return err
	}

	// Now add fields.
	for _, fld := range flds {
		if err := s.CreateField(ctx, tbl.Name, fld); err != nil {
			return errors.Wrapf(err, "creating field: %s", fld.Name)
		}
	}

	return nil
}

func (s *onPremSchema) CreateField(ctx context.Context, tname dax.TableName, fld *dax.Field) error {
	opts, err := FieldOptionsFromField(fld)
	if err != nil {
		return errors.Wrapf(err, "creating field options from field: %s", fld.Name)
	}

	_, err = s.api.CreateField(ctx, string(tname), string(fld.Name), opts...)
	return err
}

func (s *onPremSchema) DeleteTable(ctx context.Context, tname dax.TableName) error {
	return s.api.DeleteIndex(ctx, string(tname))
}

func (s *onPremSchema) DeleteField(ctx context.Context, tname dax.TableName, fname dax.FieldName) error {
	return s.api.DeleteField(ctx, string(tname), string(fname))
}

//////////////////////////////////////////////////////////////////////////////
// The following are helper functions which convert between
// featurebase.IndexInfo and dax.Table, and between featurebase.FieldInfo and
// dax.Field.
//////////////////////////////////////////////////////////////////////////////

//
// Functions to convert from featurebase to dax.
//

// IndexInfosToTables converts a slice of featurebase.IndexInfo to a slice of
// dax.Table.
func IndexInfosToTables(iis []*IndexInfo) []*dax.Table {
	tbls := make([]*dax.Table, 0, len(iis))
	for _, ii := range iis {
		tbls = append(tbls, IndexInfoToTable(ii))
	}
	return tbls
}

// IndexInfoToTable converts a featurebase.IndexInfo to a dax.Table.
func IndexInfoToTable(ii *IndexInfo) *dax.Table {
	tbl := &dax.Table{
		// TODO(tlt): be careful here. This ID=Name logic only applies to "onPrem".
		ID:         dax.TableID(ii.Name),
		Name:       dax.TableName(ii.Name),
		Fields:     make([]*dax.Field, 0, len(ii.Fields)+1), // +1 to account for the _id field
		PartitionN: dax.DefaultPartitionN,

		Description: ii.Options.Description,
		CreatedAt:   ii.CreatedAt,
	}

	// Sort ii.Fields by CreatedAt before adding them to sortedFields.
	sort.Slice(ii.Fields, func(i, j int) bool {
		return ii.Fields[i].CreatedAt < ii.Fields[j].CreatedAt
	})

	// Add the _id Field.
	var idType dax.BaseType = dax.BaseTypeID
	if ii.Options.Keys {
		idType = dax.BaseTypeString
	}
	tbl.Fields = append(tbl.Fields, &dax.Field{
		Name:      "_id",
		Type:      idType,
		CreatedAt: ii.CreatedAt,
	})

	// Populate the rest of the fields.
	for _, fld := range ii.Fields {
		tbl.Fields = append(tbl.Fields, FieldInfoToField(fld))
	}

	return tbl
}

// FieldInfoToField converts a featurebase.FieldInfo to a dax.Field.
func FieldInfoToField(fi *FieldInfo) *dax.Field {
	// Initialize field options; to be overridden based on field type specific
	// options.
	var fieldType dax.BaseType
	var min pql.Decimal
	var max pql.Decimal
	var scale int64
	var cacheType string
	var cacheSize uint32
	var timeUnit string
	var epoch time.Time
	var foreignIndex string
	var timeQuantum dax.TimeQuantum

	fo := &fi.Options

	switch fo.Type {
	case FieldTypeMutex:
		if fo.Keys {
			fieldType = dax.BaseTypeString
		} else {
			fieldType = dax.BaseTypeID
		}
		cacheType = fo.CacheType
		cacheSize = fo.CacheSize
	case FieldTypeSet:
		if fo.Keys {
			fieldType = dax.BaseTypeStringSet
		} else {
			fieldType = dax.BaseTypeIDSet
		}
		cacheType = fo.CacheType
		cacheSize = fo.CacheSize
	case FieldTypeInt:
		min = fo.Min
		max = fo.Max
		fieldType = dax.BaseTypeInt
		foreignIndex = fo.ForeignIndex
	case FieldTypeDecimal:
		min = fo.Min
		max = fo.Max
		scale = fo.Scale
		fieldType = dax.BaseTypeDecimal
	case FieldTypeTimestamp:
		epoch = featurebaseFieldOptionsToEpoch(fo)
		timeUnit = fo.TimeUnit
		fieldType = dax.BaseTypeTimestamp
	case FieldTypeBool:
		fieldType = dax.BaseTypeBool
	case FieldTypeTime:
		if fo.Keys {
			fieldType = dax.BaseTypeStringSet
		} else {
			fieldType = dax.BaseTypeIDSet
		}
		timeQuantum = dax.TimeQuantum(fo.TimeQuantum)
	default:
		panic(fmt.Sprintf("unhandled featurebase field type: %s", fo.Type))
	}

	return &dax.Field{
		Name: dax.FieldName(fi.Name),
		Type: fieldType,
		Options: dax.FieldOptions{
			Min:            min,
			Max:            max,
			Scale:          scale,
			NoStandardView: fo.NoStandardView,
			CacheType:      cacheType,
			CacheSize:      cacheSize,
			TimeUnit:       timeUnit,
			Epoch:          epoch,
			TimeQuantum:    timeQuantum,
			TTL:            fo.TTL,
			ForeignIndex:   foreignIndex,
		},

		CreatedAt: fi.CreatedAt,
	}
}

// featurebaseFieldOptionsToEpoch produces an Epoch (time.Time) value based on
// the given featurebase FieldOptions.
func featurebaseFieldOptionsToEpoch(fo *FieldOptions) time.Time {
	epochNano := fo.Base * TimeUnitNanos(fo.TimeUnit)
	return time.Unix(0, epochNano)
}

//
// Functions to convert from dax to featurebase.
//

// TablesToIndexInfos converts a slice of dax.Table to a slice of
// featurease.IndexInfo.
func TablesToIndexInfos(tbls []*dax.Table) []*IndexInfo {
	iis := make([]*IndexInfo, 0, len(tbls))
	for _, tbl := range tbls {
		iis = append(iis, TableToIndexInfo(tbl))
	}
	return iis
}

// TableToIndexInfo converts a dax.Table to a featurease.IndexInfo.
func TableToIndexInfo(tbl *dax.Table) *IndexInfo {
	ii := &IndexInfo{
		Name:      string(tbl.Name), // TODO(tlt): this should be TableKey i think
		CreatedAt: tbl.CreatedAt,
		Options: IndexOptions{
			Keys:           tbl.StringKeys(),
			TrackExistence: true,
			Description:    tbl.Description,
		},
		ShardWidth: ShardWidth,
	}

	// fields
	fields := make([]*FieldInfo, 0, len(tbl.Fields)-1)
	for i := range tbl.Fields {
		if tbl.Fields[i].Name == "_id" {
			continue
		}
		fields = append(fields, FieldToFieldInfo(tbl.Fields[i]))
	}
	ii.Fields = fields

	return ii
}

// FieldToFieldInfo converts a dax.Field to a featurebase.FieldInfo. Note: it
// does not return errors; there is one scenario where a timestamp epoch could
// be out of range. In that case, this function will only log the error, and the
// proceed with timestamp option values which are likely incorrect. We are going
// to leave this as is for now because, since this is used for internal
// conversions of types which already exist and have been validated, we assume
// the option values are valid.
// TODO(tlt): add error handling to this function; worst case: panic.
func FieldToFieldInfo(fld *dax.Field) *FieldInfo {
	var timeUnit string
	var base int64
	min := fld.Options.Min
	max := fld.Options.Max

	switch fld.Type {
	case dax.BaseTypeTimestamp:
		timestampOptions, err := fieldOptionsForTimestamp(fld.Options)
		if err != nil {
			log.Printf("ERROR: converting timestamp options: %v", err)
		}
		timeUnit = timestampOptions.TimeUnit
		base = timestampOptions.Base
		min = timestampOptions.Min
		max = timestampOptions.Max
	}

	return &FieldInfo{
		Name:      string(fld.Name),
		CreatedAt: fld.CreatedAt,
		Options: FieldOptions{
			Type:           fieldToFieldType(fld),
			Base:           base,
			Min:            min,
			Max:            max,
			Scale:          fld.Options.Scale,
			Keys:           fld.StringKeys(),
			NoStandardView: fld.Options.NoStandardView,
			CacheType:      fld.Options.CacheType,
			CacheSize:      fld.Options.CacheSize,
			TimeUnit:       timeUnit,
			TimeQuantum:    TimeQuantum(fld.Options.TimeQuantum),
			TTL:            fld.Options.TTL,
			ForeignIndex:   fld.Options.ForeignIndex,
		},
		Views: nil, // TODO(tlt): do we need views populated?
	}
}

// fieldOptionsForTimestamp produces a featurebase.FieldOptions value with the
// timestamp-related options populated.
func fieldOptionsForTimestamp(fo dax.FieldOptions) (*FieldOptions, error) {
	out := &FieldOptions{}

	// Check if the epoch will overflow when converted to nano.
	if err := CheckEpochOutOfRange(fo.Epoch, MinTimestampNano, MaxTimestampNano); err != nil {
		return out, errors.Wrap(err, "checking overflow")
	}

	out.TimeUnit = fo.TimeUnit
	out.Base = fo.Epoch.UnixNano() / TimeUnitNanos(fo.TimeUnit)
	out.Min = pql.NewDecimal(MinTimestamp.UnixNano()/TimeUnitNanos(fo.TimeUnit), 0)
	out.Max = pql.NewDecimal(MaxTimestamp.UnixNano()/TimeUnitNanos(fo.TimeUnit), 0)

	return out, nil
}

// fieldToFieldType returns the featurebase.FieldType for the given dax.Field.
func fieldToFieldType(f *dax.Field) string {
	switch f.Type {
	case dax.BaseTypeID, dax.BaseTypeString:
		if f.Name == dax.PrimaryKeyFieldName {
			return string(f.Type)
		}
		return "mutex"
	case dax.BaseTypeIDSet, dax.BaseTypeStringSet:
		if f.Options.TimeQuantum != "" {
			return "time"
		}
		return "set"
	default:
		return string(f.Type)
	}
}

func FieldFromFieldOptions(fname dax.FieldName, opts ...FieldOption) (*dax.Field, error) {
	fo, err := newFieldOptions(opts...)
	if err != nil {
		return nil, errors.Wrap(err, "creating new field options")
	}

	fi := &FieldInfo{
		Name:    string(fname),
		Options: *fo,
	}

	return FieldInfoToField(fi), nil
}

// FieldOptionsFromField returns a slice of featurebase.FieldOption based on the
// given dax.Field.
func FieldOptionsFromField(fld *dax.Field) ([]FieldOption, error) {
	// Set the cache type and size (or use default) for those fields which
	// require them.
	cacheType := DefaultCacheType
	cacheSize := uint32(DefaultCacheSize)
	if fld.Options.CacheType != "" {
		cacheType = fld.Options.CacheType
		cacheSize = fld.Options.CacheSize
	}

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
			OptFieldTypeMutex(cacheType, cacheSize),
		)
	case dax.BaseTypeIDSet:
		if fld.Options.TimeQuantum != "" {
			opts = append(opts,
				OptFieldTypeTime(TimeQuantum(fld.Options.TimeQuantum), fld.Options.TTL.String()),
			)
		} else {
			opts = append(opts,
				OptFieldTypeSet(cacheType, cacheSize),
			)
		}
	case dax.BaseTypeInt:
		opts = append(opts,
			OptFieldTypeInt(fld.Options.Min.ToInt64(0), fld.Options.Max.ToInt64(0)),
		)
	case dax.BaseTypeString:
		opts = append(opts,
			OptFieldTypeMutex(cacheType, cacheSize),
			OptFieldKeys(),
		)
	case dax.BaseTypeStringSet:
		if fld.Options.TimeQuantum != "" {
			opts = append(opts,
				OptFieldTypeTime(TimeQuantum(fld.Options.TimeQuantum), fld.Options.TTL.String()),
				OptFieldKeys(),
			)
		} else {
			opts = append(opts,
				OptFieldTypeSet(cacheType, cacheSize),
				OptFieldKeys(),
			)
		}
	case dax.BaseTypeTimestamp:
		opts = append(opts,
			OptFieldTypeTimestamp(fld.Options.Epoch, fld.Options.TimeUnit),
		)
	default:
		return nil, errors.Errorf("unsupport field type: %s", fld.Type)
	}

	return opts, nil
}
