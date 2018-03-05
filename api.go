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
	"encoding/csv"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"reflect"
	"strconv"
	"strings"

	"github.com/gogo/protobuf/proto"
	"github.com/pilosa/pilosa/internal"
	"github.com/pilosa/pilosa/pql"
)

type API struct {
	Holder *Holder
	// The execution engine for running queries.
	Executor interface {
		Execute(context context.Context, index string, query *pql.Query, slices []uint64, opt *ExecOptions) ([]interface{}, error)
	}
	Broadcaster      Broadcaster
	BroadcastHandler BroadcastHandler
	StatusHandler    StatusHandler
	Cluster          *Cluster
	URI              *URI
	RemoteClient     *http.Client
	logger           *log.Logger
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

func (api *API) ExportCSV(ctx context.Context, indexName string, frameName string, viewName string, slice uint64, w io.Writer) error {
	// Find the fragment.
	f := api.Holder.Fragment(indexName, frameName, viewName, slice)
	if f == nil {
		return ErrFragmentNotFound
	}

	// Wrap writer with a CSV writer.
	cw := csv.NewWriter(w)

	// Iterate over each bit.
	if err := f.ForEachBit(func(rowID, columnID uint64) error {
		return cw.Write([]string{
			strconv.FormatUint(rowID, 10),
			strconv.FormatUint(columnID, 10),
		})
	}); err != nil {
		return err
	}

	// Ensure data is flushed.
	cw.Flush()

	return nil
}

func (api *API) FragmentNodes(ctx context.Context, indexName string, slice uint64) []*Node {
	return api.Cluster.FragmentNodes(indexName, slice)
}

func (api *API) FragmentData(ctx context.Context, indexName string, frameName string, viewName string, slice uint64) (*Fragment, error) {
	// Retrieve fragment from holder.
	f := api.Holder.Fragment(indexName, frameName, viewName, slice)
	if f == nil {
		return nil, ErrFragmentNotFound
	}
	return f, nil
}

func (api *API) WriteFragmentData(ctx context.Context, indexName string, frameName string, viewName string, slice uint64, reader io.ReadCloser) error {
	// Retrieve frame.
	f := api.Holder.Frame(indexName, frameName)
	if f == nil {
		return ErrFrameNotFound
	}

	// Retrieve view.
	view, err := f.CreateViewIfNotExists(viewName)
	if err != nil {
		return err
	}

	// Retrieve fragment from frame.
	frag, err := view.CreateFragmentIfNotExists(slice)
	if err != nil {
		return err
	}

	// Read fragment in from request body.
	if _, err := frag.ReadFrom(reader); err != nil {
		return err
	}
	return nil
}

func (api *API) FragmentBlockData(ctx context.Context, req internal.BlockDataRequest) (internal.BlockDataResponse, error) {
	// Retrieve fragment from holder.
	f := api.Holder.Fragment(req.Index, req.Frame, req.View, req.Slice)
	if f == nil {
		return internal.BlockDataResponse{}, ErrFragmentNotFound
	}

	// Read data
	var resp internal.BlockDataResponse
	resp.RowIDs, resp.ColumnIDs = f.BlockData(int(req.Block))
	return resp, nil
}

func (api *API) FragmentBlocks(ctx context.Context, indexName string, frameName string, viewName string, slice uint64) ([]FragmentBlock, error) {
	// Retrieve fragment from holder.
	f := api.Holder.Fragment(indexName, frameName, viewName, slice)
	if f == nil {
		return nil, ErrFragmentNotFound
	}

	// Retrieve blocks.
	blocks := f.Blocks()
	return blocks, nil
}

func (api *API) RestoreFrame(ctx context.Context, indexName string, frameName string, host *URI) error {
	// Create a client for the remote cluster.
	client := NewInternalHTTPClientFromURI(host, api.RemoteClient)

	// Determine the maximum number of slices.
	maxSlices, err := client.MaxSliceByIndex(ctx)
	if err != nil {
		return err
	}

	// Retrieve frame.
	f := api.Holder.Frame(indexName, frameName)
	if f == nil {
		return ErrFrameNotFound
	}

	// Retrieve list of all views.
	views, err := client.FrameViews(ctx, indexName, frameName)
	if err != nil {
		return err
	}

	// Loop over each slice and import it if this node owns it.
	for slice := uint64(0); slice <= maxSlices[indexName]; slice++ {
		// Ignore this slice if we don't own it.
		if !api.Cluster.OwnsFragment(api.URI.HostPort(), indexName, slice) {
			continue
		}

		// Loop over view names.
		for _, view := range views {
			// Create view.
			v, err := f.CreateViewIfNotExists(view)
			if err != nil {
				return err
			}

			// Otherwise retrieve the local fragment.
			frag, err := v.CreateFragmentIfNotExists(slice)
			if err != nil {
				return err
			}

			// Stream backup from remote node.
			rd, err := client.BackupSlice(ctx, indexName, frameName, view, slice)
			if err != nil {
				return err
			} else if rd == nil {
				continue // slice doesn't exist
			}

			// Restore to local frame and always close reader.
			if err := func() error {
				defer rd.Close()
				if _, err := frag.ReadFrom(rd); err != nil {
					return err
				}
				return nil
			}(); err != nil {
				return err
			}
		}
	}

	return nil
}

func (api *API) ClusterHosts(ctx context.Context) []*Node {
	return api.Cluster.Nodes
}

func (api *API) CreateInputDefinition(ctx context.Context, indexName string, inputDefName string, inputDef InputDefinitionInfo) error {
	// Find index.
	index := api.Holder.Index(indexName)
	if index == nil {
		return ErrIndexNotFound
	}

	if err := inputDef.Validate(); err != nil {
		return err
	}

	// Encode InputDefinition to its internal representation.
	def := inputDef.Encode()
	def.Name = inputDefName

	// Create InputDefinition.
	if _, err := index.CreateInputDefinition(def); err != nil {
		return err
	}

	err := api.Broadcaster.SendSync(
		&internal.CreateInputDefinitionMessage{
			Index:      indexName,
			Definition: def,
		})
	if err != nil {
		api.logger.Printf("problem sending CreateInputDefinition message: %s", err)
	}
	return nil
}

func (api *API) InputDefinition(ctx context.Context, indexName string, inputDefName string) (*InputDefinition, error) {
	// Find index.
	index := api.Holder.Index(indexName)
	if index == nil {
		return nil, ErrIndexNotFound
	}

	inputDef, err := index.InputDefinition(inputDefName)
	if err != nil {
		return nil, err
	}
	return inputDef, nil
}

func (api *API) DeleteInputDefinition(ctx context.Context, indexName string, inputDefName string) error {
	// Find index.
	index := api.Holder.Index(indexName)
	if index == nil {
		return ErrIndexNotFound
	}

	// Delete input definition from the index.
	if err := index.DeleteInputDefinition(inputDefName); err != nil {
		return err
	}

	err := api.Broadcaster.SendSync(
		&internal.DeleteInputDefinitionMessage{
			Index: indexName,
			Name:  inputDefName,
		})
	if err != nil {
		api.logger.Printf("problem sending DeleteInputDefinition message: %s", err)
	}
	return nil
}

func (api *API) WriteInput(ctx context.Context, indexName string, inputDefName string, reqs []interface{}) error {
	// Find index.
	index := api.Holder.Index(indexName)
	if index == nil {
		return ErrIndexNotFound
	}

	for _, req := range reqs {
		bits, err := api.inputJSONDataParser(req.(map[string]interface{}), index, inputDefName)
		if err != nil {
			return err
		}
		for fr, bs := range bits {
			if err := index.InputBits(fr, bs); err != nil {
				return err
			}
		}
	}

	return nil
}

func (api *API) RecalculateCaches(ctx context.Context) {
	api.Holder.RecalculateCaches()
}

func (api *API) PostClusterMessage(ctx context.Context, pb proto.Message) error {
	// Forward the error message.
	if err := api.BroadcastHandler.ReceiveMessage(pb); err != nil {
		return err
	}
	return nil
}

func (api *API) LocalID(ctx context.Context) string {
	return api.Cluster.Node.ID
}

func (api *API) Schema(ctx context.Context) []*IndexInfo {
	return api.Holder.Schema()
}

func (api *API) Status(ctx context.Context) (proto.Message, error) {
	return api.StatusHandler.ClusterStatus()
}

func (api *API) CreateFrameField(ctx context.Context, indexName string, frameName string, field *Field) error {
	// Retrieve frame by name.
	f := api.Holder.Frame(indexName, frameName)
	if f == nil {
		return ErrFrameNotFound
	}

	// Create new field.
	if err := f.CreateField(field); err != nil {
		return err
	}

	// Send the create field message to all nodes.
	err := api.Broadcaster.SendSync(
		&internal.CreateFieldMessage{
			Index: indexName,
			Frame: frameName,
			Field: encodeField(field),
		})
	if err != nil {
		api.logger.Printf("problem sending CreateField message: %s", err)
	}
	return err
}

func (api *API) DeleteFrameField(ctx context.Context, indexName string, frameName string, fieldName string) error {
	// Retrieve frame by name.
	f := api.Holder.Frame(indexName, frameName)
	if f == nil {
		return ErrFrameNotFound
	}

	// Delete field.
	if err := f.DeleteField(fieldName); err != nil {
		return err
	}

	// Send the delete field message to all nodes.
	err := api.Broadcaster.SendSync(
		&internal.DeleteFieldMessage{
			Index: indexName,
			Frame: frameName,
			Field: fieldName,
		})
	if err != nil {
		api.logger.Printf("problem sending DeleteField message: %s", err)
	}
	return err
}

func (api *API) FrameFields(ctx context.Context, indexName string, frameName string) (*FrameSchema, error) {
	index := api.Holder.index(indexName)
	if index == nil {
		return nil, ErrIndexNotFound
	}

	frame := index.frame(frameName)
	if frame == nil {
		return nil, ErrFrameNotFound
	}

	return frame.GetFields()
}

func (api *API) FrameViews(ctx context.Context, indexName string, frameName string) ([]*View, error) {
	// Retrieve views.
	f := api.Holder.Frame(indexName, frameName)
	if f == nil {
		return nil, ErrFrameNotFound
	}

	// Fetch views.
	views := f.Views()
	return views, nil
}

func (api *API) DeleteView(ctx context.Context, indexName string, frameName string, viewName string) error {
	// Retrieve frame.
	f := api.Holder.Frame(indexName, frameName)
	if f == nil {
		return ErrFrameNotFound
	}

	// Delete the view.
	if err := f.DeleteView(viewName); err != nil {
		// Ingore this error becuase views do not exist on all nodes due to slice distribution.
		if err != ErrInvalidView {
			return err
		}
	}

	// Send the delete view message to all nodes.
	err := api.Broadcaster.SendSync(
		&internal.DeleteViewMessage{
			Index: indexName,
			Frame: frameName,
			View:  viewName,
		})
	if err != nil {
		api.logger.Printf("problem sending DeleteView message: %s", err)
	}

	return err
}

// InputJSONDataParser validates input json file and executes SetBit.
func (api *API) inputJSONDataParser(req map[string]interface{}, index *Index, name string) (map[string][]*Bit, error) {
	inputDef, err := index.InputDefinition(name)
	if err != nil {
		return nil, err
	}
	// If field in input data is not in defined definition, return error.
	var colValue uint64
	validFields := make(map[string]bool)
	timestampFrame := make(map[string]int64)
	for _, field := range inputDef.Fields() {
		validFields[field.Name] = true
		if field.PrimaryKey {
			value, ok := req[field.Name]
			if !ok {
				return nil, fmt.Errorf("primary key does not exist")
			}
			rawValue, ok := value.(float64) // The default JSON marshalling will interpret this as a float
			if !ok {
				return nil, fmt.Errorf("float64 require, got value:%s, type: %s", value, reflect.TypeOf(value))
			}
			colValue = uint64(rawValue)
		}
		// Find frame that need to add timestamp.
		for _, action := range field.Actions {
			if action.ValueDestination == InputSetTimestamp {
				timestampFrame[action.Frame], err = GetTimeStamp(req, field.Name)
				if err != nil {
					return nil, err
				}
			}
		}
	}

	for key := range req {
		_, ok := validFields[key]
		if !ok {
			return nil, fmt.Errorf("field not found: %s", key)
		}
	}

	setBits := make(map[string][]*Bit)

	for _, field := range inputDef.Fields() {
		// skip field that defined in definition but not in input data
		if _, ok := req[field.Name]; !ok {
			continue
		}

		// Looking into timestampFrame map and set timestamp to the whole frame
		for _, action := range field.Actions {
			frame := action.Frame
			timestamp := timestampFrame[action.Frame]
			// Skip input data field values that are set to null
			if req[field.Name] == nil {
				continue
			}
			bit, err := HandleAction(action, req[field.Name], colValue, timestamp)
			if err != nil {
				return nil, fmt.Errorf("error handling action: %s, err: %s", action.ValueDestination, err)
			}
			if bit != nil {
				setBits[frame] = append(setBits[frame], bit)
			}
		}
	}
	return setBits, nil
}
