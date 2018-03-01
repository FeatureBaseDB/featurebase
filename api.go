// Copyright 2017 Pilosa Corp.
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

package pilosa

import (
	"context"
	"strings"

	"github.com/pilosa/pilosa/pql"
)

type QueryOptions struct {
	Remote       bool
	ExcludeAttrs bool
	ExcludeBits  bool
	ColumnAttrs  bool
}

type API struct {
	holder *Holder
	// The execution engine for running queries.
	executor interface {
		Execute(context context.Context, index string, query *pql.Query, slices []uint64, opt *ExecOptions) ([]interface{}, error)
	}
}

func (a *API) ExecuteQuery(ctx context.Context, req *QueryRequest) (QueryResponse, error) {
	resp := QueryResponse{}

	q, err := pql.NewParser(strings.NewReader(req.Query)).Parse()
	if err != nil {
		// TODO: Wrap
		return resp, err
	}
	execOpts := &ExecOptions{
		Remote:       req.Remote,
		ExcludeAttrs: req.ExcludeAttrs,
		ExcludeBits:  req.ExcludeBits,
	}
	results, err := a.executor.Execute(ctx, req.Index, q, req.Slices, execOpts)
	if err != nil {
		return resp, err
	}
	resp.Results = results

	// Fill column attributes if requested.
	if req.ColumnAttrs && !req.ExcludeBits {
		// Consolidate all column ids across all calls.
		var columnIDs []uint64
		for _, result := range results {
			bm, ok := result.(*Bitmap)
			if !ok {
				continue
			}
			columnIDs = uint64Slice(columnIDs).merge(bm.Bits())
		}

		// Retrieve column attributes across all calls.
		columnAttrSets, err := a.readColumnAttrSets(a.holder.Index(req.Index), columnIDs)
		if err != nil {
			return resp, err
		}
		resp.ColumnAttrSets = columnAttrSets
	}
	return resp, nil
}

// readColumnAttrSets returns a list of column attribute objects by id.
func (api *API) readColumnAttrSets(index *Index, ids []uint64) ([]*ColumnAttrSet, error) {
	if index == nil {
		return nil, nil
	}

	ax := make([]*ColumnAttrSet, 0, len(ids))
	for _, id := range ids {
		// Read attributes for column. Skip column if empty.
		attrs, err := index.ColumnAttrStore().Attrs(id)
		if err != nil {
			return nil, err
		} else if len(attrs) == 0 {
			continue
		}

		// Append column with attributes.
		ax = append(ax, &ColumnAttrSet{ID: id, Attrs: attrs})
	}

	return ax, nil
}
