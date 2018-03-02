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
	"fmt"
	"io/ioutil"
	"log"
	"strings"

	"github.com/pilosa/pilosa/internal"
	"github.com/pilosa/pilosa/pql"
)

type API struct {
	Holder *Holder
	// The execution engine for running queries.
	Executor interface {
		Execute(context context.Context, index string, query *pql.Query, slices []uint64, opt *ExecOptions) ([]interface{}, error)
	}
	Broadcaster Broadcaster
	logger      *log.Logger
}

func NewAPI(logger *log.Logger) *API {
	if logger == nil {
		logger = log.New(ioutil.Discard, "", 0)
	}
	return &API{
		logger: logger,
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
	results, err := a.Executor.Execute(ctx, req.Index, q, req.Slices, execOpts)
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
		columnAttrSets, err := a.readColumnAttrSets(a.Holder.Index(req.Index), columnIDs)
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

func (api *API) CreateIndex(ctx context.Context, indexName string, options IndexOptions) (*Index, error) {
	// Create index.
	index, err := api.Holder.CreateIndex(indexName, options)
	if err != nil {
		return nil, err
	}
	// Send the create index message to all nodes.
	err = api.Broadcaster.SendSync(
		&internal.CreateIndexMessage{
			Index: indexName,
			Meta:  options.Encode(),
		})
	if err != nil {
		api.logger.Printf("problem sending CreateIndex message: %s", err)
		return nil, err
	}
	api.Holder.Stats.Count("createIndex", 1, 1.0)
	return index, nil
}

func (api *API) ReadIndex(ctx context.Context, indexName string) (*Index, error) {
	index := api.Holder.Index(indexName)
	if index == nil {
		return nil, ErrIndexNotFound
	}
	return index, nil
}

func (api *API) DeleteIndex(ctx context.Context, indexName string) error {
	// Delete index from the holder.
	err := api.Holder.DeleteIndex(indexName)
	if err != nil {
		return err
	}
	// Send the delete index message to all nodes.
	err = api.Broadcaster.SendSync(
		&internal.DeleteIndexMessage{
			Index: indexName,
		})
	if err != nil {
		api.logger.Printf("problem sending DeleteIndex message: %s", err)
		return err
	}
	api.Holder.Stats.Count("deleteIndex", 1, 1.0)
	return nil
}

func (api *API) CreateFrame(ctx context.Context, indexName string, frameName string, options FrameOptions) (*Frame, error) {
	// Find index.
	index := api.Holder.Index(indexName)
	if index == nil {
		return nil, ErrIndexNotFound
	}

	// Create frame.
	frame, err := index.CreateFrame(frameName, options)
	if err != nil {
		return nil, err
	}

	// Send the create frame message to all nodes.
	err = api.Broadcaster.SendSync(
		&internal.CreateFrameMessage{
			Index: indexName,
			Frame: frameName,
			Meta:  options.Encode(),
		})
	if err != nil {
		api.logger.Printf("problem sending CreateFrame message: %s", err)
		return nil, err
	}
	api.Holder.Stats.CountWithCustomTags("createFrame", 1, 1.0, []string{fmt.Sprintf("index:%s", indexName)})
	return frame, nil
}

func (api *API) DeleteFrame(ctx context.Context, indexName string, frameName string) error {
	// Find index.
	index := api.Holder.Index(indexName)
	if index == nil {
		return ErrIndexNotFound
	}

	// Delete frame from the index.
	if err := index.DeleteFrame(frameName); err != nil {
		return err
	}

	// Send the delete frame message to all nodes.
	err := api.Broadcaster.SendSync(
		&internal.DeleteFrameMessage{
			Index: indexName,
			Frame: frameName,
		})
	if err != nil {
		api.logger.Printf("problem sending DeleteFrame message: %s", err)
		return err
	}
	api.Holder.Stats.CountWithCustomTags("deleteFrame", 1, 1.0, []string{fmt.Sprintf("index:%s", indexName)})
	return nil
}
