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

//go:generate stringer -type=apiMethod

package pilosa

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/pilosa/pilosa/internal"
	"github.com/pilosa/pilosa/pql"
	"github.com/pkg/errors"
)

// API provides the top level programmatic interface to Pilosa. It is usually
// wrapped by a handler which provides an external interface (e.g. HTTP).
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
	URI              URI
	RemoteClient     *http.Client
	Logger           Logger
}

// NewAPI returns a new API instance.
func NewAPI() *API {
	return &API{
		Broadcaster: NopBroadcaster,
		//BroadcastHandler: NopBroadcastHandler, // TODO: implement the nop
		//StatusHandler:    NopStatusHandler,    // TODO: implement the nop
		Logger: NopLogger,
	}
}

// validAPIMethods specifies the api methods that are valid for each
// cluster state.
var validAPIMethods = map[string]map[apiMethod]struct{}{
	ClusterStateStarting: methodsCommon,
	ClusterStateNormal:   appendMap(methodsCommon, methodsNormal),
	ClusterStateResizing: appendMap(methodsCommon, methodsResizing),
}

func appendMap(a, b map[apiMethod]struct{}) map[apiMethod]struct{} {
	r := make(map[apiMethod]struct{})
	for k, v := range a {
		r[k] = v
	}
	for k, v := range b {
		r[k] = v
	}
	return r
}

func (api *API) validate(f apiMethod) error {
	state := api.Cluster.State()
	if _, ok := validAPIMethods[state][f]; ok {
		return nil
	}
	return ApiMethodNotAllowedError{errors.Errorf("api method %s not allowed in state %s", f, state)}
}

// Query parses a PQL query out of the request and executes it.
func (api *API) Query(ctx context.Context, req *QueryRequest) (QueryResponse, error) {
	if err := api.validate(apiQuery); err != nil {
		return QueryResponse{}, errors.Wrap(err, "validate api method")
	}

	resp := QueryResponse{}

	q, err := pql.NewParser(strings.NewReader(req.Query)).Parse()
	if err != nil {
		return resp, err
	}
	execOpts := &ExecOptions{
		Remote:       req.Remote,
		ExcludeAttrs: req.ExcludeAttrs,
		ExcludeBits:  req.ExcludeBits,
	}
	results, err := api.Executor.Execute(ctx, req.Index, q, req.Slices, execOpts)
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
		columnAttrSets, err := api.readColumnAttrSets(api.Holder.Index(req.Index), columnIDs)
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

// CreateIndex makes a new Pilosa index.
func (api *API) CreateIndex(ctx context.Context, indexName string, options IndexOptions) (*Index, error) {
	if err := api.validate(apiCreateIndex); err != nil {
		return nil, errors.Wrap(err, "validate api method")
	}

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
		api.Logger.Printf("problem sending CreateIndex message: %s", err)
		return nil, err
	}
	api.Holder.Stats.Count("createIndex", 1, 1.0)
	return index, nil
}

// Index retrieves the named index.
func (api *API) Index(ctx context.Context, indexName string) (*Index, error) {
	if err := api.validate(apiIndex); err != nil {
		return nil, errors.Wrap(err, "validate api method")
	}

	index := api.Holder.Index(indexName)
	if index == nil {
		return nil, ErrIndexNotFound
	}
	return index, nil
}

// DeleteIndex removes the named index. If the index is not found it does
// nothing and returns no error.
func (api *API) DeleteIndex(ctx context.Context, indexName string) error {
	if err := api.validate(apiDeleteIndex); err != nil {
		return errors.Wrap(err, "validate api method")
	}

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
		api.Logger.Printf("problem sending DeleteIndex message: %s", err)
		return err
	}
	api.Holder.Stats.Count("deleteIndex", 1, 1.0)
	return nil
}

// CreateFrame makes the named frame in the named index with the given options.
func (api *API) CreateFrame(ctx context.Context, indexName string, frameName string, options FrameOptions) (*Frame, error) {
	if err := api.validate(apiCreateFrame); err != nil {
		return nil, errors.Wrap(err, "validate api method")
	}

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
		api.Logger.Printf("problem sending CreateFrame message: %s", err)
		return nil, err
	}
	api.Holder.Stats.CountWithCustomTags("createFrame", 1, 1.0, []string{fmt.Sprintf("index:%s", indexName)})
	return frame, nil
}

// DeleteFrame removes the named frame from the named index. If the index is not
// found, an error is returned. If the frame is not found, it is ignored and no
// action is taken.
func (api *API) DeleteFrame(ctx context.Context, indexName string, frameName string) error {
	if err := api.validate(apiDeleteFrame); err != nil {
		return errors.Wrap(err, "validate api method")
	}

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
		api.Logger.Printf("problem sending DeleteFrame message: %s", err)
		return err
	}
	api.Holder.Stats.CountWithCustomTags("deleteFrame", 1, 1.0, []string{fmt.Sprintf("index:%s", indexName)})
	return nil
}

// ExportCSV encodes the fragment designated by the index,frame,view,slice as
// CSV of the form <row>,<col>
func (api *API) ExportCSV(ctx context.Context, indexName string, frameName string, viewName string, slice uint64, w io.Writer) error {
	if err := api.validate(apiExportCSV); err != nil {
		return errors.Wrap(err, "validate api method")
	}

	// Validate that this handler owns the slice.
	if !api.Cluster.OwnsSlice(api.LocalID(), indexName, slice) {
		api.Logger.Printf("host does not own slice %s-%s slice:%d", api.URI, indexName, slice)
		return ErrClusterDoesNotOwnSlice
	}

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

// SliceNodes returns the node and all replicas which should contain a slice's data.
func (api *API) SliceNodes(ctx context.Context, indexName string, slice uint64) ([]*Node, error) {
	if err := api.validate(apiSliceNodes); err != nil {
		return nil, errors.Wrap(err, "validate api method")
	}

	return api.Cluster.SliceNodes(indexName, slice), nil
}

// MarshalFragment returns an object which can write the specified fragment's data
// to an io.Writer. The serialized data can be read back into a fragment with
// the UnmarshalFragment API call.
func (api *API) MarshalFragment(ctx context.Context, indexName string, frameName string, viewName string, slice uint64) (io.WriterTo, error) {
	if err := api.validate(apiMarshalFragment); err != nil {
		return nil, errors.Wrap(err, "validate api method")
	}

	// Retrieve fragment from holder.
	f := api.Holder.Fragment(indexName, frameName, viewName, slice)
	if f == nil {
		return nil, ErrFragmentNotFound
	}
	return f, nil
}

// UnmarshalFragment creates a new fragment (if necessary) and reads data from a
// Reader which was previously written by MarshalFragment to populate the
// fragment's data.
func (api *API) UnmarshalFragment(ctx context.Context, indexName string, frameName string, viewName string, slice uint64, reader io.ReadCloser) error {
	if err := api.validate(apiUnmarshalFragment); err != nil {
		return errors.Wrap(err, "validate api method")
	}

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

// FragmentBlockData is an endpoint for internal usage. It is not guaranteed to
// return anything useful. Currently it returns protobuf encoded row and column
// ids from a "block" which is a subdivision of a fragment.
func (api *API) FragmentBlockData(ctx context.Context, body io.Reader) ([]byte, error) {
	if err := api.validate(apiFragmentBlockData); err != nil {
		return nil, errors.Wrap(err, "validate api method")
	}

	reqBytes, err := ioutil.ReadAll(body)
	if err != nil {
		return nil, BadRequestError{errors.Wrap(err, "read body error")}
	}
	var req internal.BlockDataRequest
	if err := proto.Unmarshal(reqBytes, &req); err != nil {
		return nil, BadRequestError{errors.Wrap(err, "unmarshal body error")}
	}

	// Retrieve fragment from holder.
	f := api.Holder.Fragment(req.Index, req.Frame, req.View, req.Slice)
	if f == nil {
		return nil, ErrFragmentNotFound
	}

	var resp = internal.BlockDataResponse{}
	resp.RowIDs, resp.ColumnIDs = f.BlockData(int(req.Block))

	// Encode response.
	buf, err := proto.Marshal(&resp)
	if err != nil {
		return nil, errors.Wrap(err, "merge block response encoding error")
	}

	return buf, nil
}

// FragmentBlocks returns the checksums and block ids for all blocks in the specified fragment.
func (api *API) FragmentBlocks(ctx context.Context, indexName string, frameName string, viewName string, slice uint64) ([]FragmentBlock, error) {
	if err := api.validate(apiFragmentBlocks); err != nil {
		return nil, errors.Wrap(err, "validate api method")
	}

	// Retrieve fragment from holder.
	f := api.Holder.Fragment(indexName, frameName, viewName, slice)
	if f == nil {
		return nil, ErrFragmentNotFound
	}

	// Retrieve blocks.
	blocks := f.Blocks()
	return blocks, nil
}

// RestoreFrame reads all the data that this host should have for a given frame
// from replicas in the cluster and restores that data to it.
func (api *API) RestoreFrame(ctx context.Context, indexName string, frameName string, host *URI) error {
	if err := api.validate(apiRestoreFrame); err != nil {
		return errors.Wrap(err, "validate api method")
	}

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
		if !api.Cluster.OwnsSlice(api.LocalID(), indexName, slice) {
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

// Hosts returns a list of the hosts in the cluster including their ID,
// URL, and which is the coordinator.
func (api *API) Hosts(ctx context.Context) []*Node {
	return api.Cluster.Nodes
}

// CreateInputDefinition is deprecated and will be removed. Do not use it.
func (api *API) CreateInputDefinition(ctx context.Context, indexName string, inputDefName string, inputDef InputDefinitionInfo) error {
	if err := api.validate(apiCreateInputDefinition); err != nil {
		return errors.Wrap(err, "validate api method")
	}

	api.Logger.Printf(`CreateInputDefinition is deprecated and will be removed.
Please open an issue if you need to continue using it.`)
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
		api.Logger.Printf("problem sending CreateInputDefinition message: %s", err)
	}
	return nil
}

// InputDefinition is deprecated and will be removed.
func (api *API) InputDefinition(ctx context.Context, indexName string, inputDefName string) (*InputDefinition, error) {
	if err := api.validate(apiInputDefinition); err != nil {
		return nil, errors.Wrap(err, "validate api method")
	}

	api.Logger.Printf(`InputDefinition is deprecated and will be removed.`)
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

// DeleteInputDefinition is deprecated and will be removed.
func (api *API) DeleteInputDefinition(ctx context.Context, indexName string, inputDefName string) error {
	if err := api.validate(apiDeleteInputDefinition); err != nil {
		return errors.Wrap(err, "validate api method")
	}

	api.Logger.Printf("DeleteInputDefinition is deprecated and will be removed.")
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
		api.Logger.Printf("problem sending DeleteInputDefinition message: %s", err)
	}
	return nil
}

// WriteInput is deprecated and will be removed.
func (api *API) WriteInput(ctx context.Context, indexName string, inputDefName string, reqs []interface{}) error {
	if err := api.validate(apiWriteInput); err != nil {
		return errors.Wrap(err, "validate api method")
	}

	api.Logger.Printf("WriteInput is deprecated and will be removed.")
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

// RecalculateCaches forces all TopN caches to be updated. Used mainly for integration tests.
func (api *API) RecalculateCaches(ctx context.Context) error {
	if err := api.validate(apiRecalculateCaches); err != nil {
		return errors.Wrap(err, "validate api method")
	}

	err := api.Broadcaster.SendSync(&internal.RecalculateCaches{})
	if err != nil {
		return errors.Wrap(err, "broacasting message")
	}
	api.Holder.RecalculateCaches()
	return nil
}

// PostClusterMessage is for internal use. It decodes a protobuf message out of
// the body and forwards it to the BroadcastHandler.
func (api *API) ClusterMessage(ctx context.Context, reqBody io.Reader) error {
	if err := api.validate(apiClusterMessage); err != nil {
		return errors.Wrap(err, "validate api method")
	}

	// Read entire body.
	body, err := ioutil.ReadAll(reqBody)
	if err != nil {
		return errors.Wrap(err, "reading body")
	}

	// Marshal into request object.
	pb, err := UnmarshalMessage(body)
	if err != nil {
		return errors.Wrap(err, "unmarshaling message")
	}

	// Forward the error message.
	if err := api.BroadcastHandler.ReceiveMessage(pb); err != nil {
		return errors.Wrap(err, "receiving message")
	}
	return nil
}

// LocalID returns the current node's ID.
func (api *API) LocalID() string {
	return api.Cluster.Node.ID
}

// Schema returns information about each index in Pilosa including which frames
// and views they contain.
func (api *API) Schema(ctx context.Context) []*IndexInfo {
	return api.Holder.Schema()
}

// CreateField creates a new BSI field in the given index and frame.
func (api *API) CreateField(ctx context.Context, indexName string, frameName string, field *Field) error {
	if err := api.validate(apiCreateField); err != nil {
		return errors.Wrap(err, "validate api method")
	}

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
		api.Logger.Printf("problem sending CreateField message: %s", err)
	}
	return err
}

// DeleteField deletes the given field.
func (api *API) DeleteField(ctx context.Context, indexName string, frameName string, fieldName string) error {
	if err := api.validate(apiDeleteField); err != nil {
		return errors.Wrap(err, "validate api method")
	}

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
		api.Logger.Printf("problem sending DeleteField message: %s", err)
	}
	return err
}

// Fields returns the fields in the given frame.
func (api *API) Fields(ctx context.Context, indexName string, frameName string) ([]*Field, error) {
	if err := api.validate(apiFields); err != nil {
		return nil, errors.Wrap(err, "validate api method")
	}

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

// Views returns the views in the given frame.
func (api *API) Views(ctx context.Context, indexName string, frameName string) ([]*View, error) {
	if err := api.validate(apiViews); err != nil {
		return nil, errors.Wrap(err, "validate api method")
	}

	// Retrieve views.
	f := api.Holder.Frame(indexName, frameName)
	if f == nil {
		return nil, ErrFrameNotFound
	}

	// Fetch views.
	views := f.Views()
	return views, nil
}

// DeleteView removes the given view.
func (api *API) DeleteView(ctx context.Context, indexName string, frameName string, viewName string) error {
	if err := api.validate(apiDeleteView); err != nil {
		return errors.Wrap(err, "validate api method")
	}

	// Retrieve frame.
	f := api.Holder.Frame(indexName, frameName)
	if f == nil {
		return ErrFrameNotFound
	}

	// Delete the view.
	if err := f.DeleteView(viewName); err != nil {
		// Ignore this error because views do not exist on all nodes due to slice distribution.
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
		api.Logger.Printf("problem sending DeleteView message: %s", err)
	}

	return err
}

// IndexAttrDiff
func (api *API) IndexAttrDiff(ctx context.Context, indexName string, blocks []AttrBlock) (map[uint64]map[string]interface{}, error) {
	if err := api.validate(apiIndexAttrDiff); err != nil {
		return nil, errors.Wrap(err, "validate api method")
	}

	// Retrieve index from holder.
	index := api.Holder.Index(indexName)
	if index == nil {
		return nil, ErrIndexNotFound
	}

	// Retrieve local blocks.
	localBlocks, err := index.ColumnAttrStore().Blocks()
	if err != nil {
		return nil, err
	}

	// Read all attributes from all mismatched blocks.
	attrs := make(map[uint64]map[string]interface{})
	for _, blockID := range AttrBlocks(localBlocks).Diff(blocks) {
		// Retrieve block data.
		m, err := index.ColumnAttrStore().BlockData(blockID)
		if err != nil {
			return nil, err
		}

		// Copy to index-wide struct.
		for k, v := range m {
			attrs[k] = v
		}
	}
	return attrs, nil
}

func (api *API) FrameAttrDiff(ctx context.Context, indexName string, frameName string, blocks []AttrBlock) (map[uint64]map[string]interface{}, error) {
	if err := api.validate(apiFrameAttrDiff); err != nil {
		return nil, errors.Wrap(err, "validate api method")
	}

	// Retrieve index from holder.
	f := api.Holder.Frame(indexName, frameName)
	if f == nil {
		return nil, ErrFrameNotFound
	}

	// Retrieve local blocks.
	localBlocks, err := f.RowAttrStore().Blocks()
	if err != nil {
		return nil, err
	}

	// Read all attributes from all mismatched blocks.
	attrs := make(map[uint64]map[string]interface{})
	for _, blockID := range AttrBlocks(localBlocks).Diff(blocks) {
		// Retrieve block data.
		m, err := f.RowAttrStore().BlockData(blockID)
		if err != nil {
			return nil, err
		}

		// Copy to index-wide struct.
		for k, v := range m {
			attrs[k] = v
		}
	}
	return attrs, nil
}

// Import bulk imports data into a particular index,frame,slice.
func (api *API) Import(ctx context.Context, req internal.ImportRequest) error {
	if err := api.validate(apiImport); err != nil {
		return errors.Wrap(err, "validate api method")
	}

	_, frame, err := api.indexFrame(req.Index, req.Frame, req.Slice)
	if err != nil {
		return err
	}

	// Convert timestamps to time.Time.
	timestamps := make([]*time.Time, len(req.Timestamps))
	for i, ts := range req.Timestamps {
		if ts == 0 {
			continue
		}
		t := time.Unix(0, ts)
		timestamps[i] = &t
	}

	// Import into fragment.
	err = frame.Import(req.RowIDs, req.ColumnIDs, timestamps)
	if err != nil {
		api.Logger.Printf("import error: index=%s, frame=%s, slice=%d, bits=%d, err=%s", req.Index, req.Frame, req.Slice, len(req.ColumnIDs), err)
	}
	return err
}

// ImportValue bulk imports values into a particular field.
func (api *API) ImportValue(ctx context.Context, req internal.ImportValueRequest) error {
	if err := api.validate(apiImportValue); err != nil {
		return errors.Wrap(err, "validate api method")
	}

	_, frame, err := api.indexFrame(req.Index, req.Frame, req.Slice)
	if err != nil {
		return err
	}

	// Import into fragment.
	err = frame.ImportValue(req.Field, req.ColumnIDs, req.Values)
	if err != nil {
		api.Logger.Printf("import error: index=%s, frame=%s, slice=%d, field=%s, bits=%d, err=%s", req.Index, req.Frame, req.Slice, req.Field, len(req.ColumnIDs), err)
	}
	return err
}

// MaxSlices returns the maximum slice number for each index in a map.
func (api *API) MaxSlices(ctx context.Context) map[string]uint64 {
	return api.Holder.MaxSlices()
}

// MaxInverseSlices returns the maximum inverse slice number for each index in a
// map.
func (api *API) MaxInverseSlices(ctx context.Context) map[string]uint64 {
	return api.Holder.MaxInverseSlices()
}

// StatsWithTags returns an instance of whatever implementation of StatsClient
// pilosa is using with the given tags.
func (api *API) StatsWithTags(tags []string) StatsClient {
	if api.Holder == nil || api.Cluster == nil {
		return nil
	}
	return api.Holder.Stats.WithTags(tags...)
}

// LongQueryTime returns the configured threshold for logging/statting
// long running queries.
func (api *API) LongQueryTime() time.Duration {
	if api.Cluster == nil {
		return 0
	}
	return api.Cluster.LongQueryTime
}

func (api *API) indexFrame(indexName string, frameName string, slice uint64) (*Index, *Frame, error) {
	// Validate that this handler owns the slice.
	if !api.Cluster.OwnsSlice(api.LocalID(), indexName, slice) {
		api.Logger.Printf("host does not own slice %s-%s slice:%d", api.URI, indexName, slice)
		return nil, nil, ErrClusterDoesNotOwnSlice
	}

	// Find the Index.
	api.Logger.Printf("importing: %v %v %v", indexName, frameName, slice)
	index := api.Holder.Index(indexName)
	if index == nil {
		api.Logger.Printf("fragment error: index=%s, frame=%s, slice=%d, err=%s", indexName, frameName, slice, ErrIndexNotFound.Error())
		return nil, nil, ErrIndexNotFound
	}

	// Retrieve frame.
	frame := index.Frame(frameName)
	if frame == nil {
		api.Logger.Printf("frame error: index=%s, frame=%s, slice=%d, err=%s", indexName, frameName, slice, ErrFrameNotFound.Error())
		return nil, nil, ErrFrameNotFound
	}
	return index, frame, nil
}

// inputJSONDataParser validates input json file and executes SetBit. Deprecated - remove with input definition stuff.
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

// SetCoordinator makes a new Node the cluster coordinator.
func (api *API) SetCoordinator(ctx context.Context, id string) (oldNode, newNode *Node, err error) {
	if err := api.validate(apiSetCoordinator); err != nil {
		return nil, nil, errors.Wrap(err, "validate api method")
	}

	oldNode = api.Cluster.NodeByID(api.Cluster.Coordinator)
	newNode = api.Cluster.NodeByID(id)
	if newNode == nil {
		return nil, nil, errors.Wrap(ErrNodeIDNotExists, "getting new node")
	}

	// If the new coordinator is this node, do the SetCoordinator directly.
	if newNode.ID == api.LocalID() {
		return oldNode, newNode, api.Cluster.SetCoordinator(newNode)
	}

	// Send the set-coordinator message to new node.
	err = api.Broadcaster.SendTo(
		newNode,
		&internal.SetCoordinatorMessage{
			New: EncodeNode(newNode),
		})
	if err != nil {
		return nil, nil, fmt.Errorf("problem sending SetCoordinator message: %s", err)
	}
	return oldNode, newNode, nil
}

// RemoveNode puts the cluster into the "RESIZING" state and begins the job of
// removing the given node.
func (api *API) RemoveNode(id string) (*Node, error) {
	if err := api.validate(apiRemoveNode); err != nil {
		return nil, errors.Wrap(err, "validate api method")
	}

	removeNode := api.Cluster.nodeByID(id)
	if removeNode == nil {
		return nil, errors.Wrap(ErrNodeIDNotExists, "finding node to remove")
	}

	// Start the resize process (similar to NodeJoin)
	err := api.Cluster.NodeLeave(removeNode)
	if err != nil {
		return removeNode, errors.Wrap(err, "calling node leave")
	}
	return removeNode, nil
}

// ResizeAbort stops the current resize job.
func (api *API) ResizeAbort() error {
	if err := api.validate(apiResizeAbort); err != nil {
		return errors.Wrap(err, "validate api method")
	}

	err := api.Cluster.CompleteCurrentJob(ResizeJobStateAborted)
	return errors.Wrap(err, "complete current job")
}

// State returns the cluster state which is usually "NORMAL", but could be
// "STARTING", "RESIZING", or potentially others. See cluster.go for more
// details.
func (api *API) State() string {
	return api.Cluster.State()
}

// Version returns the Pilosa version.
func (api *API) Version() string {
	return strings.TrimPrefix(Version, "v")
}

type apiMethod int

// API validation constants.
const (
	apiClusterMessage apiMethod = iota
	apiCreateField
	apiCreateFrame
	apiCreateIndex
	apiCreateInputDefinition
	apiDeleteField
	apiDeleteFrame
	apiDeleteIndex
	apiDeleteInputDefinition
	apiDeleteView
	apiExportCSV
	apiFields
	apiFragmentBlockData
	apiFragmentBlocks
	apiFrameAttrDiff
	//apiHosts // not implemented
	apiImport
	apiImportValue
	apiIndex
	apiIndexAttrDiff
	apiInputDefinition
	//apiLocalID // not implemented
	//apiLongQueryTime // not implemented
	apiMarshalFragment
	//apiMaxInverseSlices // not implemented
	//apiMaxSlices // not implemented
	apiQuery
	apiRecalculateCaches
	apiRemoveNode
	apiResizeAbort
	apiRestoreFrame
	//apiSchema // not implemented
	apiSetCoordinator
	apiSliceNodes
	//apiState // not implemented
	//apiStatsWithTags // not implemented
	apiUnmarshalFragment
	//apiVersion // not implemented
	apiViews
	apiWriteInput
)

var methodsCommon = map[apiMethod]struct{}{
	apiClusterMessage:  struct{}{},
	apiMarshalFragment: struct{}{},
	apiSetCoordinator:  struct{}{},
}

var methodsResizing = map[apiMethod]struct{}{
	apiResizeAbort: struct{}{},
}

var methodsNormal = map[apiMethod]struct{}{
	apiCreateField:           struct{}{},
	apiCreateFrame:           struct{}{},
	apiCreateIndex:           struct{}{},
	apiCreateInputDefinition: struct{}{},
	apiDeleteField:           struct{}{},
	apiDeleteFrame:           struct{}{},
	apiDeleteIndex:           struct{}{},
	apiDeleteInputDefinition: struct{}{},
	apiDeleteView:            struct{}{},
	apiExportCSV:             struct{}{},
	apiFields:                struct{}{},
	apiFragmentBlockData:     struct{}{},
	apiFragmentBlocks:        struct{}{},
	apiFrameAttrDiff:         struct{}{},
	apiImport:                struct{}{},
	apiImportValue:           struct{}{},
	apiIndex:                 struct{}{},
	apiIndexAttrDiff:         struct{}{},
	apiInputDefinition:       struct{}{},
	apiQuery:                 struct{}{},
	apiRecalculateCaches:     struct{}{},
	apiRemoveNode:            struct{}{},
	apiRestoreFrame:          struct{}{},
	apiSliceNodes:            struct{}{},
	apiUnmarshalFragment:     struct{}{},
	apiViews:                 struct{}{},
	apiWriteInput:            struct{}{},
}
