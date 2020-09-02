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

package pgtest

import (
	"context"
	"errors"

	"github.com/pilosa/pilosa/v2/pg"
)

// HandlerFunc implements a postgres query handler with a function.
type HandlerFunc func(context.Context, pg.QueryResultWriter, pg.Query) error

// HandleQuery calls the user's query handler function.
func (h HandlerFunc) HandleQuery(ctx context.Context, w pg.QueryResultWriter, q pg.Query) error {
	return h(ctx, w, q)
}

var _ pg.QueryHandler = HandlerFunc(nil)

// ResultSet is a QueryResultWriter that accumulates results in a slice.
type ResultSet struct {
	Columns   []pg.ColumnInfo
	Data      [][]string
	ResultTag string
}

// WriteHeader writes headers to the result set.
func (rs *ResultSet) WriteHeader(cols ...pg.ColumnInfo) error {
	if rs.Columns != nil {
		return errors.New("double-write of headers")
	}

	colsCopy := make([]pg.ColumnInfo, len(cols))
	copy(colsCopy, cols)
	rs.Columns = colsCopy

	return nil
}

// WriteRowText writes a row to the result set.
func (rs *ResultSet) WriteRowText(vals ...string) error {
	if rs.Columns == nil {
		return errors.New("wrote a row without headers")
	}

	row := make([]string, len(vals))
	copy(row, vals)

	rs.Data = append(rs.Data, row)

	return nil
}

// Tag applies a tag to the result set.
func (rs *ResultSet) Tag(tag string) {
	rs.ResultTag = tag
}

var _ pg.QueryResultWriter = (*ResultSet)(nil)
