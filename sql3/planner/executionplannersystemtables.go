// Copyright 2022 Molecula Corp. All rights reserved.

package planner

import (
	"context"

	pilosa "github.com/molecula/featurebase/v3"
	"github.com/molecula/featurebase/v3/sql3"
	"github.com/molecula/featurebase/v3/sql3/parser"
	"github.com/pkg/errors"
)

// Ensure type implements interface.
var _ pilosa.SchemaAPI = (*systemTableDefintionsWrapper)(nil)

type systemTableDefintionsWrapper struct {
	schemaAPI pilosa.SchemaAPI
}

func newSystemTableDefintionsWrapper(schemaAPI pilosa.SchemaAPI) *systemTableDefintionsWrapper {
	return &systemTableDefintionsWrapper{
		schemaAPI: schemaAPI,
	}
}

func (s *systemTableDefintionsWrapper) CreateIndexAndFields(ctx context.Context, indexName string, options pilosa.IndexOptions, fields []pilosa.CreateFieldObj) error {
	return s.schemaAPI.CreateIndexAndFields(ctx, indexName, options, fields)
}

func (s *systemTableDefintionsWrapper) CreateField(ctx context.Context, indexName string, fieldName string, opts ...pilosa.FieldOption) (*pilosa.Field, error) {
	return s.schemaAPI.CreateField(ctx, indexName, fieldName, opts...)
}

func (s *systemTableDefintionsWrapper) DeleteField(ctx context.Context, indexName string, fieldName string) error {
	return s.schemaAPI.DeleteField(ctx, indexName, fieldName)
}

func (s *systemTableDefintionsWrapper) DeleteIndex(ctx context.Context, indexName string) error {
	return s.schemaAPI.DeleteIndex(ctx, indexName)
}

func (s *systemTableDefintionsWrapper) IndexInfo(ctx context.Context, indexName string) (*pilosa.IndexInfo, error) {
	i, err := s.schemaAPI.IndexInfo(ctx, indexName)
	if err != nil {
		if errors.Is(err, pilosa.ErrIndexNotFound) {
			st, ok := systemTables[indexName]
			if !ok {
				return nil, pilosa.ErrIndexNotFound
			}

			return indexInfoFromSystemTable(st)
		}
		return nil, err
	}
	return i, nil
}

func (s *systemTableDefintionsWrapper) FieldInfo(ctx context.Context, indexName, fieldName string) (*pilosa.FieldInfo, error) {
	return nil, pilosa.ErrNotImplemented
}

func (s *systemTableDefintionsWrapper) Schema(ctx context.Context, withViews bool) ([]*pilosa.IndexInfo, error) {
	schema, err := s.schemaAPI.Schema(ctx, withViews)
	if err != nil {
		return nil, err
	}
	for _, st := range systemTables {

		i, err := indexInfoFromSystemTable(st)
		if err != nil {
			return nil, err
		}
		schema = append(schema, i)
	}
	return schema, err
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
