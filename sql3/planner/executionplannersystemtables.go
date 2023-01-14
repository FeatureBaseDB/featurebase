// Copyright 2022 Molecula Corp. All rights reserved.

package planner

import (
	"context"

	pilosa "github.com/molecula/featurebase/v3"
	"github.com/molecula/featurebase/v3/dax"
	"github.com/molecula/featurebase/v3/errors"
	"github.com/molecula/featurebase/v3/sql3"
	"github.com/molecula/featurebase/v3/sql3/parser"
)

// Ensure type implements interface.
var _ pilosa.SchemaAPI = (*systemTableDefintionsWrapper)(nil)

type systemTableDefintionsWrapper struct {
	schemaAPI pilosa.SchemaAPI
}

func newSystemTableDefintionsWrapper(api pilosa.SchemaAPI) *systemTableDefintionsWrapper {
	return &systemTableDefintionsWrapper{
		schemaAPI: api,
	}
}

func (s *systemTableDefintionsWrapper) TableByName(ctx context.Context, tname dax.TableName) (*dax.Table, error) {
	tbl, err := s.schemaAPI.TableByName(ctx, tname)
	if err != nil {
		if isTableNotFoundError(err) {
			st, ok := systemTables[string(tname)]
			if !ok {
				return nil, dax.NewErrTableNameDoesNotExist(tname)
			}

			return indexInfoFromSystemTableB(st)
		}
		return nil, err
	}
	return tbl, nil
}

func (s *systemTableDefintionsWrapper) TableByID(ctx context.Context, tid dax.TableID) (*dax.Table, error) {
	return s.schemaAPI.TableByID(ctx, tid)
}

func (s *systemTableDefintionsWrapper) Tables(ctx context.Context) ([]*dax.Table, error) {
	tbls, err := s.schemaAPI.Tables(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "getting tables")
	}

	// Append the system tables.
	for tblName, st := range systemTables {
		ii, err := indexInfoFromSystemTable(st)
		if err != nil {
			return nil, errors.Wrapf(err, "converting system table to table: %s", tblName)
		}
		tbls = append(tbls, pilosa.IndexInfoToTable(ii))
	}

	return tbls, nil
}

func (s *systemTableDefintionsWrapper) CreateTable(ctx context.Context, tbl *dax.Table) error {
	return s.schemaAPI.CreateTable(ctx, tbl)
}

func (s *systemTableDefintionsWrapper) CreateField(ctx context.Context, tname dax.TableName, fld *dax.Field) error {
	return s.schemaAPI.CreateField(ctx, tname, fld)
}

func (s *systemTableDefintionsWrapper) DeleteTable(ctx context.Context, tname dax.TableName) error {
	return s.schemaAPI.DeleteTable(ctx, tname)
}

func (s *systemTableDefintionsWrapper) DeleteField(ctx context.Context, tname dax.TableName, fname dax.FieldName) error {
	return s.schemaAPI.DeleteField(ctx, tname, fname)
}

func indexInfoFromSystemTableB(st *systemTable) (*dax.Table, error) {
	fields := make([]*dax.Field, 0)

	for _, f := range st.schema {
		var baseType dax.BaseType
		switch f.Type.(type) {
		case *parser.DataTypeInt:
			baseType = dax.BaseTypeInt
		case *parser.DataTypeBool:
			baseType = dax.BaseTypeBool
		case *parser.DataTypeString:
			baseType = dax.BaseTypeString
		case *parser.DataTypeTimestamp:
			baseType = dax.BaseTypeTimestamp
		default:
			return nil, sql3.NewErrInternalf("unexpected system table field type '%T'", f.Type)
		}

		fld := &dax.Field{
			Name: dax.FieldName(f.ColumnName),
			Type: baseType,
		}
		fields = append(fields, fld)
	}

	tbl := &dax.Table{
		Name:   dax.TableName(st.name),
		Fields: fields,
	}

	return tbl, nil
}

func indexInfoFromSystemTable(st *systemTable) (*pilosa.IndexInfo, error) {
	fields := make([]*pilosa.FieldInfo, 0)

	for _, f := range st.schema {

		var opts pilosa.FieldOptions
		switch f.Type.(type) {
		case *parser.DataTypeInt:
			opts.Type = pilosa.FieldTypeInt
		case *parser.DataTypeBool:
			opts.Type = pilosa.FieldTypeBool
		case *parser.DataTypeString:
			opts.Type = pilosa.FieldTypeMutex
			opts.Keys = true
		case *parser.DataTypeTimestamp:
			opts.Type = pilosa.FieldTypeTimestamp
		default:
			return nil, sql3.NewErrInternalf("unexpected system table field type '%T'", f.Type)
		}

		fld := &pilosa.FieldInfo{
			Name:    f.ColumnName,
			Options: opts,
		}
		fields = append(fields, fld)
	}

	i := &pilosa.IndexInfo{
		Name:   st.name,
		Fields: fields,
	}

	return i, nil
}
