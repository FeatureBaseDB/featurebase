// Copyright 2021 Molecula Corp. All rights reserved.
package sql

import (
	"context"
	"fmt"

	pilosa "github.com/molecula/featurebase/v2"
	pproto "github.com/molecula/featurebase/v2/proto"
	"github.com/pkg/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"vitess.io/vitess/go/vt/sqlparser"
)

// ShowHandler executes SQL show table/field statements
type ShowHandler struct {
	api *pilosa.API
}

// NewShowHandler constructor
func NewShowHandler(api *pilosa.API) *ShowHandler {
	return &ShowHandler{
		api: api,
	}
}

// Handle executes mapped SQL
func (s *ShowHandler) Handle(ctx context.Context, mapped *MappedSQL) (pproto.ToRowser, error) {
	stmt, ok := mapped.Statement.(*sqlparser.Show)
	if !ok {
		return nil, fmt.Errorf("statement is not type show: %T", mapped.Statement)
	}

	switch stmt.Type {
	case "tables":
		return s.execShowTables(ctx, stmt)
	case "fields":
		return s.execShowFields(ctx, stmt)
	default:
		return nil, fmt.Errorf("cannot show: %s", stmt.Type)
	}
}

func (s *ShowHandler) execShowTables(ctx context.Context, showStmt *sqlparser.Show) (pproto.ToRowser, error) {
	indexInfo, err := s.api.Schema(ctx, false)
	if err != nil {
		return nil, errors.Wrap(err, "getting schema")
	}

	allowed, ok := ctx.Value("indices").([]string)

	result := make(pproto.ConstRowser, 0)
	for _, ii := range indexInfo {
		if ok {
			// if authorization is turned on, allowed will be a list
			// so we have to check if the index is in the allowed list
			found := false
			for _, idx := range allowed {
				if ii.Name == idx {
					found = true
					break
				}
			}
			if !found {
				continue
			}
		}
		result = append(result, pproto.RowResponse{
			Headers: []*pproto.ColumnInfo{
				{Name: "Table", Datatype: "string"},
			},
			Columns: []*pproto.ColumnResponse{
				{ColumnVal: &pproto.ColumnResponse_StringVal{StringVal: ii.Name}},
			},
		})
	}

	// Sort the result.
	return OrderBy(result, []string{"Table"}, []string{"asc"}), nil
}

func (s *ShowHandler) execShowFields(ctx context.Context, showStmt *sqlparser.Show) (pproto.ToRowser, error) {
	indexName := showStmt.OnTable.ToViewName().Name.String()
	allowed, ok := ctx.Value("indices").([]string)
	if ok {
		found := false
		for _, idx := range allowed {
			if idx == indexName {
				found = true
				break
			}
		}
		if !found {
			return nil, status.Error(codes.PermissionDenied, "insufficient permissions to access requested tables")
		}
	}
	index, err := s.api.Index(ctx, indexName)
	if err != nil {
		return nil, errors.Wrap(err, "getting schema")
	}
	if index == nil {
		return nil, errors.WithMessage(pilosa.ErrIndexNotFound, indexName)
	}
	fields := index.Fields()

	result := make(pproto.ConstRowser, 0, len(fields))
	for _, f := range fields {
		if f.Name() == "_exists" {
			continue
		}

		typeName := f.Type()
		if f.Keys() {
			typeName = "keyed-" + typeName
		}
		if f.ForeignIndex() != "" {
			typeName = "foreign-" + typeName
		}

		result = append(result, pproto.RowResponse{
			Headers: []*pproto.ColumnInfo{
				{Name: "Field", Datatype: "string"},
				{Name: "Type", Datatype: "string"},
			},
			Columns: []*pproto.ColumnResponse{
				{ColumnVal: &pproto.ColumnResponse_StringVal{StringVal: f.Name()}},
				{ColumnVal: &pproto.ColumnResponse_StringVal{StringVal: typeName}},
			},
		})
	}

	// Sort the result.
	return OrderBy(result, []string{"Field"}, []string{"asc"}), nil
}
