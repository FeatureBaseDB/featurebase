package queryer

import (
	"context"
	"fmt"
	"time"

	pilosa "github.com/molecula/featurebase/v3"
	"github.com/molecula/featurebase/v3/dax"
	"github.com/molecula/featurebase/v3/dax/mds/schemar"
	"github.com/molecula/featurebase/v3/errors"
	"github.com/molecula/featurebase/v3/pql"
)

// Ensure type implements interface.
var _ pilosa.SchemaInfoAPI = (*schemaInfoAPI)(nil)

type schemaInfoAPI struct {
	schemar schemar.Schemar
}

func NewSchemaInfoAPI(schemar schemar.Schemar) *schemaInfoAPI {
	return &schemaInfoAPI{
		schemar: schemar,
	}
}

func (a *schemaInfoAPI) IndexInfo(ctx context.Context, indexName string) (*pilosa.IndexInfo, error) {
	qtid := dax.TableKey(indexName).QualifiedTableID()
	tbl, err := a.schemar.Table(ctx, qtid)
	if err != nil {
		return nil, errors.Wrap(err, "getting table for indexinfo")
	}

	return daxTableToFeaturebaseIndexInfo(tbl, false)
}

func (a *schemaInfoAPI) FieldInfo(ctx context.Context, indexName, fieldName string) (*pilosa.FieldInfo, error) {
	qtid := dax.TableKey(indexName).QualifiedTableID()
	tbl, err := a.schemar.Table(ctx, qtid)
	fldName := dax.FieldName(fieldName)

	if err != nil {
		return nil, errors.Wrap(err, "getting table for fieldinfo")
	}

	fld, ok := tbl.Field(dax.FieldName(fieldName))
	if !ok {
		return nil, dax.NewErrFieldDoesNotExist(fldName)
	}

	return daxFieldToFeaturebaseFieldInfo(fld)
}

// daxTableToFeaturebaseIndexInfo converts a dax.Table to a
// featurebase.IndexInfo. If useName is true, the IndexInfo.Name value will
// be set to the qualified table name. Otherwise it will be set to the table key.
func daxTableToFeaturebaseIndexInfo(qtbl *dax.QualifiedTable, useName bool) (*pilosa.IndexInfo, error) {
	name := string(qtbl.Key())
	if useName {
		name = string(qtbl.Name)
	}
	ii := &pilosa.IndexInfo{
		Name:      name,
		CreatedAt: 0,
		Options: pilosa.IndexOptions{
			Keys:           qtbl.StringKeys(),
			TrackExistence: true,
		},
		ShardWidth: pilosa.ShardWidth,
	}

	// fields
	fields := make([]*pilosa.FieldInfo, len(qtbl.Fields))
	var err error
	for i := range qtbl.Fields {
		fields[i], err = daxFieldToFeaturebaseFieldInfo(qtbl.Fields[i])
		if err != nil {
			return nil, errors.Wrap(err, "converting field to FieldInfo")
		}
	}
	ii.Fields = fields

	return ii, nil
}

// daxFieldToFeaturebaseFieldInfo converts a dax.Field to a
// featurebase.FieldInfo.
func daxFieldToFeaturebaseFieldInfo(field *dax.Field) (*pilosa.FieldInfo, error) {
	var timeUnit string
	var base int64
	min := field.Options.Min
	max := field.Options.Max

	switch field.Type {
	case dax.BaseTypeTimestamp:
		timestampOptions, err := daxFieldOptionsToFeaturebaseTimestamp(field.Options)
		if err != nil {
			return nil, errors.Wrap(err, "getting timestamp options")
		}
		timeUnit = timestampOptions.TimeUnit
		base = timestampOptions.Base
		min = timestampOptions.Min
		max = timestampOptions.Max
	}

	fi := &pilosa.FieldInfo{
		Name:      string(field.Name),
		CreatedAt: 0, // TODO(tlt): we need to handle this on MDS schemar
		Options: pilosa.FieldOptions{
			Type:           featurebaseFieldType(field),
			Base:           base,
			Min:            min,
			Max:            max,
			Scale:          field.Options.Scale,
			Keys:           field.StringKeys(),
			NoStandardView: field.Options.NoStandardView,
			CacheType:      field.Options.CacheType,
			CacheSize:      field.Options.CacheSize,
			TimeUnit:       timeUnit,
			TimeQuantum:    pilosa.TimeQuantum(field.Options.TimeQuantum),
			TTL:            field.Options.TTL,
			ForeignIndex:   field.Options.ForeignIndex,
		},
		Views: nil, // TODO: do we need views populated?
	}

	return fi, nil
}

// featurebaseFieldType returns the featurebase.FieldType for the given
// dax.Field.
func featurebaseFieldType(f *dax.Field) string {
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

// featurebaseFieldOptionsToEpoch produces an Epoch (time.Time) value based on
// the given featurebase FieldOptions.
func featurebaseFieldOptionsToEpoch(fo *pilosa.FieldOptions) time.Time {
	epochNano := fo.Base * pilosa.TimeUnitNanos(fo.TimeUnit)
	return time.Unix(0, epochNano)
}

// daxFieldOptionsToFeaturebaseTimestamp produces a featurebase.FieldOptions
// value with the applicable options populated.
func daxFieldOptionsToFeaturebaseTimestamp(fo dax.FieldOptions) (*pilosa.FieldOptions, error) {
	out := &pilosa.FieldOptions{}

	// Check if the epoch will overflow when converted to nano.
	if err := pilosa.CheckEpochOutOfRange(fo.Epoch, pilosa.MinTimestampNano, pilosa.MaxTimestampNano); err != nil {
		return nil, errors.Wrap(err, "checking overflow")
	}

	out.TimeUnit = fo.TimeUnit
	out.Base = fo.Epoch.UnixNano() / pilosa.TimeUnitNanos(fo.TimeUnit)
	out.Min = pql.NewDecimal(pilosa.MinTimestamp.UnixNano()/pilosa.TimeUnitNanos(fo.TimeUnit), 0)
	out.Max = pql.NewDecimal(pilosa.MaxTimestamp.UnixNano()/pilosa.TimeUnitNanos(fo.TimeUnit), 0)

	return out, nil
}

func featurebaseFieldOptionSliceToDaxField(name string, opts []pilosa.FieldOption) (*dax.Field, error) {
	fo := &pilosa.FieldOptions{}
	for _, opt := range opts {
		if err := opt(fo); err != nil {
			return nil, errors.Wrap(err, "applying field option")
		}
	}

	return featurebaseFieldOptionsToDaxField(name, fo)
}

func featurebaseFieldOptionsToDaxField(name string, fo *pilosa.FieldOptions) (*dax.Field, error) {
	// Initialize field options; to be overridden based on field type
	// specific options. Unless determined otherwise, the defaults for these
	// values are applied in sql3/planner/createtable.go, so we don't
	// initialize with defaults here. In other words, we set these value to
	// exactly as we receive them from the caller.
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

	switch fo.Type {
	case pilosa.FieldTypeMutex:
		if fo.Keys {
			fieldType = dax.BaseTypeString
		} else {
			fieldType = dax.BaseTypeID
		}
		cacheType = fo.CacheType
		cacheSize = fo.CacheSize
	case pilosa.FieldTypeSet:
		if fo.Keys {
			fieldType = dax.BaseTypeStringSet
		} else {
			fieldType = dax.BaseTypeIDSet
		}
		cacheType = fo.CacheType
		cacheSize = fo.CacheSize
	case pilosa.FieldTypeInt:
		min = fo.Min
		max = fo.Max
		fieldType = dax.BaseTypeInt
		foreignIndex = fo.ForeignIndex
	case pilosa.FieldTypeDecimal:
		min = fo.Min
		max = fo.Max
		scale = fo.Scale
		fieldType = dax.BaseTypeDecimal
	case pilosa.FieldTypeTimestamp:
		epoch = featurebaseFieldOptionsToEpoch(fo)
		timeUnit = fo.TimeUnit
		fieldType = dax.BaseTypeTimestamp
	case pilosa.FieldTypeBool:
		fieldType = dax.BaseTypeBool
	case pilosa.FieldTypeTime:
		if fo.Keys {
			fieldType = dax.BaseTypeStringSet
		} else {
			fieldType = dax.BaseTypeIDSet
		}
		timeQuantum = dax.TimeQuantum(fo.TimeQuantum)
	default:
		return nil, errors.New(errors.ErrUncoded, fmt.Sprintf("unhandled featurebase field type: %s", fo.Type))
	}

	daxField := &dax.Field{
		Name: dax.FieldName(name),
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
	}

	return daxField, nil
}

// Ensure type implements interface.
var _ pilosa.SchemaAPI = (*qualifiedSchemaAPI)(nil)

// qualifiedSchemaAPI is a wrapper around schemaAPI. It is initialized with a
// TableQualifer, and it uses this qualifer to convert between, for example,
// FeatureBase index name (a string) and TableKey. It requires a Schemar to do
// that lookup/conversion.
type qualifiedSchemaAPI struct {
	qual    dax.TableQualifier
	schemar schemar.Schemar
}

func NewQualifiedSchemaAPI(qual dax.TableQualifier, schemar schemar.Schemar) *qualifiedSchemaAPI {
	return &qualifiedSchemaAPI{
		qual:    qual,
		schemar: schemar,
	}
}

func (s *qualifiedSchemaAPI) CreateIndexAndFields(ctx context.Context, indexName string, options pilosa.IndexOptions, fields []pilosa.CreateFieldObj) error {
	// Make sure the table for this qualifier doesn't already exist.
	//_, err := s.schemar.TableID(ctx, s.qual, dax.TableName(indexName))
	_, err := s.schemar.TableID(ctx, s.qual, dax.TableName(indexName))
	// TODO(tlt): the following doesn't work when the TableID() call is made
	// over http because the error that comes back is `status code: 400: table
	// name 'tbl' does not exist\n`, which does not match the check. We really
	// need to be able to check these error codes both directly on the error AND
	// when they come back via http.
	// if !errors.Is(err, dax.ErrTableNameDoesNotExist) {
	// 	if err != nil {
	// 		return errors.Wrapf(err, "checking if table name already exists: %s, %s", s.qual, indexName)
	// 	}
	// 	return dax.NewErrTableNameExists(dax.TableName(indexName))
	// }
	if err == nil {
		return dax.NewErrTableNameExists(dax.TableName(indexName))
	}

	partitionN := dax.DefaultPartitionN
	// TODO(tlt): until we can thread partitionN through featurebase correctly
	// (instead of having it use holder.partitionN or DefaultPartitionN), then
	// we can't use a custom partitionN.
	// if options.PartitionN > 0 {
	// 	partitionN = options.PartitionN
	// }

	// Initialize the fields slice with one additional slot for the primary key
	// field.
	daxFields := make([]*dax.Field, 0, len(fields)+1)

	// Add the primary key field.
	var fieldType dax.BaseType
	if options.Keys {
		fieldType = dax.BaseTypeString
	} else {
		fieldType = dax.BaseTypeID
	}
	daxFields = append(daxFields, &dax.Field{
		Name: dax.PrimaryKeyFieldName,
		Type: fieldType,
	})

	// Add the fields provided in the method call.
	for _, fldObj := range fields {
		daxField, err := featurebaseFieldOptionSliceToDaxField(fldObj.Name, fldObj.Options)
		if err != nil {
			return errors.Wrap(err, "converting featurebase field options to dax field")
		}

		daxFields = append(daxFields, daxField)
	}

	tbl := &dax.Table{
		Name:       dax.TableName(indexName),
		Fields:     daxFields,
		PartitionN: partitionN,
	}

	qtbl := dax.NewQualifiedTable(
		s.qual,
		tbl,
	)

	return s.schemar.CreateTable(ctx, qtbl)
}

func (s *qualifiedSchemaAPI) CreateField(ctx context.Context, indexName string, fieldName string, opts ...pilosa.FieldOption) (*pilosa.Field, error) {
	tkey, err := s.indexToQualifiedTableKey(ctx, indexName)
	if err != nil {
		return nil, errors.Wrap(err, "converting index to qualified table key")
	}

	daxField, err := featurebaseFieldOptionSliceToDaxField(fieldName, opts)
	if err != nil {
		return nil, errors.Wrap(err, "converting featurebase field options to dax field")
	}

	qtid := tkey.QualifiedTableID()

	if err := s.schemar.CreateField(ctx, qtid, daxField); err != nil {
		return nil, errors.New(errors.ErrUncoded, err.Error())
	}

	return nil, nil
}

func (s *qualifiedSchemaAPI) DeleteField(ctx context.Context, indexName string, fieldName string) error {
	tkey, err := s.indexToQualifiedTableKey(ctx, indexName)
	if err != nil {
		return errors.Wrap(err, "converting index to qualified table key")
	}

	qtid := tkey.QualifiedTableID()
	fldName := dax.FieldName(fieldName)

	if err := s.schemar.DropField(ctx, qtid, fldName); err != nil {
		return errors.New(errors.ErrUncoded, err.Error())
	}

	return nil
}

func (s *qualifiedSchemaAPI) DeleteIndex(ctx context.Context, indexName string) error {
	tkey, err := s.indexToQualifiedTableKey(ctx, indexName)
	if err != nil {
		return errors.Wrap(err, "converting index to qualified table key")
	}
	qtid := tkey.QualifiedTableID()
	return s.schemar.DropTable(ctx, qtid)
}

func (s *qualifiedSchemaAPI) IndexInfo(ctx context.Context, indexName string) (*pilosa.IndexInfo, error) {
	tkey, err := s.indexToQualifiedTableKey(ctx, indexName)
	if err != nil {
		return nil, errors.Wrap(err, "converting index to qualified table key")
	}

	qtid := tkey.QualifiedTableID()
	tbl, err := s.schemar.Table(ctx, qtid)
	if err != nil {
		return nil, errors.Wrap(err, "getting table for qualified indexinfo")
	}

	return daxTableToFeaturebaseIndexInfo(tbl, true)
}

func (s *qualifiedSchemaAPI) FieldInfo(ctx context.Context, indexName, fieldName string) (*pilosa.FieldInfo, error) {
	tkey, err := s.indexToQualifiedTableKey(ctx, indexName)
	if err != nil {
		return nil, errors.Wrap(err, "converting index to qualified table key")
	}

	qtid := tkey.QualifiedTableID()
	tbl, err := s.schemar.Table(ctx, qtid)
	fldName := dax.FieldName(fieldName)

	if err != nil {
		return nil, errors.Wrap(err, "getting table for qualified fieldinfo")
	}

	fld, ok := tbl.Field(dax.FieldName(fieldName))
	if !ok {
		return nil, dax.NewErrFieldDoesNotExist(fldName)
	}

	return daxFieldToFeaturebaseFieldInfo(fld)
}

func (s *qualifiedSchemaAPI) Schema(ctx context.Context, withViews bool) ([]*pilosa.IndexInfo, error) {
	tbls, err := s.schemar.Tables(ctx, s.qual)
	if err != nil {
		return nil, errors.Wrap(err, "getting tables for qualified schema")
	}

	indexes := make([]*pilosa.IndexInfo, len(tbls))
	for i := range tbls {
		// This method appears to be used primarily in "SHOW TABLES", and in
		// that case we want to return the human friendly table name used when
		// creating the table (i.e. not the table key).
		indexes[i], err = daxTableToFeaturebaseIndexInfo(tbls[i], true)
		if err != nil {
			return nil, errors.Wrap(err, "converting table to IndexInfo")
		}
	}

	return indexes, nil
}

func (s *qualifiedSchemaAPI) indexToQualifiedTableKey(ctx context.Context, index string) (dax.TableKey, error) {
	qtid, err := s.schemar.TableID(ctx, s.qual, dax.TableName(index))
	if err != nil {
		return "", errors.Wrap(err, "converting index to qualified table id")
	}
	return qtid.Key(), nil
}
