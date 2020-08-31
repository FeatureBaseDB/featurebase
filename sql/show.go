// Copyright 2020 Pilosa Corp.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sql

import (
	"context"
	"fmt"

	"github.com/pilosa/pilosa/v2"
	pproto "github.com/pilosa/pilosa/v2/proto"
	"github.com/pkg/errors"
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
	indexInfo := s.api.Schema(ctx)

	result := make(pproto.ConstRowser, len(indexInfo))
	for i, ii := range indexInfo {
		result[i] = pproto.RowResponse{
			Headers: []*pproto.ColumnInfo{
				{Name: "Table", Datatype: "string"},
			},
			Columns: []*pproto.ColumnResponse{
				{ColumnVal: &pproto.ColumnResponse_StringVal{StringVal: ii.Name}},
			},
		}
	}

	// Sort the result.
	return OrderBy(result, []string{"Table"}, []string{"asc"}), nil
}

func (s *ShowHandler) execShowFields(ctx context.Context, showStmt *sqlparser.Show) (pproto.ToRowser, error) {
	indexName := showStmt.OnTable.ToViewName().Name.String()
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

		dt, err := f.Datatype()
		if err != nil {
			return nil, errors.Wrapf(err, "field %s", f.Name())
		}
		result = append(result, pproto.RowResponse{
			Headers: []*pproto.ColumnInfo{
				{Name: "Field", Datatype: "string"},
				{Name: "Type", Datatype: "string"},
			},
			Columns: []*pproto.ColumnResponse{
				{ColumnVal: &pproto.ColumnResponse_StringVal{StringVal: f.Name()}},
				{ColumnVal: &pproto.ColumnResponse_StringVal{StringVal: dt}},
			},
		})
	}

	// Sort the result.
	return OrderBy(result, []string{"Field"}, []string{"asc"}), nil
}
