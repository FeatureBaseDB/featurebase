// Copyright 2022 Molecula Corp. All rights reserved.

package planner

import (
	"context"
	"time"

	pilosa "github.com/featurebasedb/featurebase/v3"
	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/sql3"
	"github.com/featurebasedb/featurebase/v3/sql3/parser"
	"github.com/pkg/errors"
)

// Ensure type implements interface.
var _ pilosa.SchemaAPI = (*systemTableDefinitionsWrapper)(nil)

type systemTableDefinitionsWrapper struct {
	schemaAPI pilosa.SchemaAPI
}

func newSystemTableDefinitionsWrapper(api pilosa.SchemaAPI) *systemTableDefinitionsWrapper {
	return &systemTableDefinitionsWrapper{
		schemaAPI: api,
	}
}

func (s *systemTableDefinitionsWrapper) CreateDatabase(ctx context.Context, db *dax.Database) error {
	return s.schemaAPI.CreateDatabase(ctx, db)
}
func (s *systemTableDefinitionsWrapper) DropDatabase(ctx context.Context, dbid dax.DatabaseID) error {
	return s.schemaAPI.DropDatabase(ctx, dbid)
}

func (s *systemTableDefinitionsWrapper) DatabaseByName(ctx context.Context, dbname dax.DatabaseName) (*dax.Database, error) {
	return s.schemaAPI.DatabaseByName(ctx, dbname)
}
func (s *systemTableDefinitionsWrapper) DatabaseByID(ctx context.Context, dbid dax.DatabaseID) (*dax.Database, error) {
	return s.schemaAPI.DatabaseByID(ctx, dbid)
}
func (s *systemTableDefinitionsWrapper) SetDatabaseOption(ctx context.Context, dbid dax.DatabaseID, option string, value string) error {
	return s.schemaAPI.SetDatabaseOption(ctx, dbid, option, value)
}
func (s *systemTableDefinitionsWrapper) Databases(ctx context.Context, dbids ...dax.DatabaseID) ([]*dax.Database, error) {
	return s.schemaAPI.Databases(ctx, dbids...)
}

func (s *systemTableDefinitionsWrapper) TableByName(ctx context.Context, tname dax.TableName) (*dax.Table, error) {
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

func (s *systemTableDefinitionsWrapper) TableByID(ctx context.Context, tid dax.TableID) (*dax.Table, error) {
	return s.schemaAPI.TableByID(ctx, tid)
}

func (s *systemTableDefinitionsWrapper) Tables(ctx context.Context) ([]*dax.Table, error) {
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

func (s *systemTableDefinitionsWrapper) CreateTable(ctx context.Context, tbl *dax.Table) error {
	return s.schemaAPI.CreateTable(ctx, tbl)
}

func (s *systemTableDefinitionsWrapper) CreateField(ctx context.Context, tname dax.TableName, fld *dax.Field) error {
	return s.schemaAPI.CreateField(ctx, tname, fld)
}

func (s *systemTableDefinitionsWrapper) DeleteTable(ctx context.Context, tname dax.TableName) error {
	return s.schemaAPI.DeleteTable(ctx, tname)
}

func (s *systemTableDefinitionsWrapper) DeleteField(ctx context.Context, tname dax.TableName, fname dax.FieldName) error {
	return s.schemaAPI.DeleteField(ctx, tname, fname)
}

func indexInfoFromSystemTableB(st *systemTable) (*dax.Table, error) {
	fields := make([]*dax.Field, 0)

	for _, f := range st.schema {
		var baseType dax.BaseType
		var epoch time.Time
		switch f.Type.(type) {
		case *parser.DataTypeInt:
			baseType = dax.BaseTypeInt
		case *parser.DataTypeBool:
			baseType = dax.BaseTypeBool
		case *parser.DataTypeString:
			baseType = dax.BaseTypeString
		case *parser.DataTypeTimestamp:
			baseType = dax.BaseTypeTimestamp
			epoch = time.Unix(0, 0)
		default:
			return nil, sql3.NewErrInternalf("unexpected system table field type '%T'", f.Type)
		}
		_ = dax.FieldOptions{}
		fld := &dax.Field{
			Name:    dax.FieldName(f.ColumnName),
			Type:    baseType,
			Options: dax.FieldOptions{Epoch: epoch},
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
