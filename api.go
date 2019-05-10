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
	"encoding/binary"
	"encoding/csv"
	"fmt"
	"io"
	"io/ioutil"
	"strconv"
	"strings"
	"time"

	"github.com/pilosa/pilosa/pql"
	"github.com/pilosa/pilosa/roaring"
	"github.com/pilosa/pilosa/stats"
	"github.com/pilosa/pilosa/tracing"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

// API provides the top level programmatic interface to Pilosa. It is usually
// wrapped by a handler which provides an external interface (e.g. HTTP).
type API struct {
	holder  *Holder
	cluster *cluster
	server  *Server

	Serializer Serializer
}

// apiOption is a functional option type for pilosa.API
type apiOption func(*API) error

func OptAPIServer(s *Server) apiOption {
	return func(a *API) error {
		a.server = s
		a.holder = s.holder
		a.cluster = s.cluster
		a.Serializer = s.serializer
		return nil
	}
}

// NewAPI returns a new API instance.
func NewAPI(opts ...apiOption) (*API, error) {
	api := &API{}

	for _, opt := range opts {
		err := opt(api)
		if err != nil {
			return nil, errors.Wrap(err, "applying option")
		}
	}
	return api, nil
}

// validAPIMethods specifies the api methods that are valid for each
// cluster state.
var validAPIMethods = map[string]map[apiMethod]struct{}{
	ClusterStateStarting: methodsCommon,
	ClusterStateNormal:   appendMap(methodsCommon, methodsNormal),
	ClusterStateDegraded: appendMap(methodsCommon, methodsNormal),
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
	state := api.cluster.State()
	if _, ok := validAPIMethods[state][f]; ok {
		return nil
	}
	return newAPIMethodNotAllowedError(errors.Errorf("api method %s not allowed in state %s", f, state))
}

// Query parses a PQL query out of the request and executes it.
func (api *API) Query(ctx context.Context, req *QueryRequest) (QueryResponse, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "API.Query")
	defer span.Finish()

	if err := api.validate(apiQuery); err != nil {
		return QueryResponse{}, errors.Wrap(err, "validating api method")
	}

	q, err := pql.NewParser(strings.NewReader(req.Query)).Parse()
	if err != nil {
		return QueryResponse{}, errors.Wrap(err, "parsing")
	}
	execOpts := &execOptions{
		Remote:          req.Remote,
		ExcludeRowAttrs: req.ExcludeRowAttrs, // NOTE: Kept for Pilosa 1.x compat.
		ExcludeColumns:  req.ExcludeColumns,  // NOTE: Kept for Pilosa 1.x compat.
		ColumnAttrs:     req.ColumnAttrs,     // NOTE: Kept for Pilosa 1.x compat.
	}
	resp, err := api.server.executor.Execute(ctx, req.Index, q, req.Shards, execOpts)
	if err != nil {
		return QueryResponse{}, errors.Wrap(err, "executing")
	}

	return resp, nil
}

// CreateIndex makes a new Pilosa index.
func (api *API) CreateIndex(ctx context.Context, indexName string, options IndexOptions) (*Index, error) {
	span, _ := tracing.StartSpanFromContext(ctx, "API.CreateIndex")
	defer span.Finish()

	if err := api.validate(apiCreateIndex); err != nil {
		return nil, errors.Wrap(err, "validating api method")
	}

	// Create index.
	index, err := api.holder.CreateIndex(indexName, options)
	if err != nil {
		return nil, errors.Wrap(err, "creating index")
	}
	// Send the create index message to all nodes.
	err = api.server.SendSync(
		&CreateIndexMessage{
			Index: indexName,
			Meta:  &options,
		})
	if err != nil {
		return nil, errors.Wrap(err, "sending CreateIndex message")
	}
	api.holder.Stats.Count("createIndex", 1, 1.0)
	return index, nil
}

// Index retrieves the named index.
func (api *API) Index(ctx context.Context, indexName string) (*Index, error) {
	span, _ := tracing.StartSpanFromContext(ctx, "API.Index")
	defer span.Finish()

	if err := api.validate(apiIndex); err != nil {
		return nil, errors.Wrap(err, "validating api method")
	}

	index := api.holder.Index(indexName)
	if index == nil {
		return nil, newNotFoundError(ErrIndexNotFound)
	}
	return index, nil
}

// DeleteIndex removes the named index. If the index is not found it does
// nothing and returns no error.
func (api *API) DeleteIndex(ctx context.Context, indexName string) error {
	span, _ := tracing.StartSpanFromContext(ctx, "API.DeleteIndex")
	defer span.Finish()

	if err := api.validate(apiDeleteIndex); err != nil {
		return errors.Wrap(err, "validating api method")
	}

	// Delete index from the holder.
	err := api.holder.DeleteIndex(indexName)
	if err != nil {
		return errors.Wrap(err, "deleting index")
	}
	// Send the delete index message to all nodes.
	err = api.server.SendSync(
		&DeleteIndexMessage{
			Index: indexName,
		})
	if err != nil {
		api.server.logger.Printf("problem sending DeleteIndex message: %s", err)
		return errors.Wrap(err, "sending DeleteIndex message")
	}
	api.holder.Stats.Count("deleteIndex", 1, 1.0)
	return nil
}

// CreateField makes the named field in the named index with the given options.
// This method currently only takes a single functional option, but that may be
// changed in the future to support multiple options.
func (api *API) CreateField(ctx context.Context, indexName string, fieldName string, opts ...FieldOption) (*Field, error) {
	span, _ := tracing.StartSpanFromContext(ctx, "API.CreateField")
	defer span.Finish()

	if err := api.validate(apiCreateField); err != nil {
		return nil, errors.Wrap(err, "validating api method")
	}

	// Apply functional options.
	fo := FieldOptions{}
	for _, opt := range opts {
		err := opt(&fo)
		if err != nil {
			return nil, errors.Wrap(err, "applying option")
		}
	}

	// Find index.
	index := api.holder.Index(indexName)
	if index == nil {
		return nil, newNotFoundError(ErrIndexNotFound)
	}

	// Create field.
	field, err := index.CreateField(fieldName, opts...)
	if err != nil {
		return nil, errors.Wrap(err, "creating field")
	}

	// Send the create field message to all nodes.
	err = api.server.SendSync(
		&CreateFieldMessage{
			Index: indexName,
			Field: fieldName,
			Meta:  &fo,
		})
	if err != nil {
		api.server.logger.Printf("problem sending CreateField message: %s", err)
		return nil, errors.Wrap(err, "sending CreateField message")
	}
	api.holder.Stats.CountWithCustomTags("createField", 1, 1.0, []string{fmt.Sprintf("index:%s", indexName)})
	return field, nil
}

// Field retrieves the named field.
func (api *API) Field(ctx context.Context, indexName, fieldName string) (*Field, error) {
	span, _ := tracing.StartSpanFromContext(ctx, "API.Field")
	defer span.Finish()

	if err := api.validate(apiField); err != nil {
		return nil, errors.Wrap(err, "validating api method")
	}

	field := api.holder.Field(indexName, fieldName)
	if field == nil {
		return nil, newNotFoundError(ErrFieldNotFound)
	}
	return field, nil
}

func setUpImportOptions(opts ...ImportOption) (*ImportOptions, error) {
	options := &ImportOptions{}
	for _, opt := range opts {
		err := opt(options)
		if err != nil {
			return nil, errors.Wrap(err, "applying option")
		}
	}
	return options, nil
}

// ImportRoaring is a low level interface for importing data to Pilosa when
// extremely high throughput is desired. The data must be encoded in a
// particular way which may be unintuitive (discussed below). The data is merged
// with existing data.
//
// It takes as input a roaring bitmap which it uses as the data for the
// indicated index, field, and shard. The bitmap may be encoded according to the
// official roaring spec (https://github.com/RoaringBitmap/RoaringFormatSpec),
// or to the pilosa roaring spec which supports 64 bit integers
// (https://www.pilosa.com/docs/latest/architecture/#roaring-bitmap-storage-format).
//
// The data should be encoded the same way that Pilosa stores fragments
// internally. A bit "i" being set in the input bitmap indicates that the bit is
// set in Pilosa row "i/ShardWidth", and in column
// (shard*ShardWidth)+(i%ShardWidth). That is to say that "data" represents all
// of the rows in this shard of this field concatenated together in one long
// bitmap.
func (api *API) ImportRoaring(ctx context.Context, indexName, fieldName string, shard uint64, remote bool, req *ImportRoaringRequest) (err error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "API.ImportRoaring")
	span.LogKV("index", indexName, "field", fieldName)
	defer span.Finish()

	if err = api.validate(apiField); err != nil {
		return errors.Wrap(err, "validating api method")
	}

	nodes := api.cluster.shardNodes(indexName, shard)
	var eg errgroup.Group

	field := api.holder.Field(indexName, fieldName)
	if field == nil {
		return newNotFoundError(ErrFieldNotFound)
	}

	// only set and time fields are supported
	if field.Type() != FieldTypeSet && field.Type() != FieldTypeTime {
		return NewBadRequestError(errors.New("roaring import is only supported for set and time fields"))
	}

	for _, node := range nodes {
		node := node
		if node.ID == api.server.nodeID {
			eg.Go(func() error {
				var err error
				for viewName, viewData := range req.Views {
					if viewName == "" {
						viewName = viewStandard
					} else {
						viewName = fmt.Sprintf("%s_%s", viewStandard, viewName)
					}
					if len(viewData) == 0 {
						return fmt.Errorf("no data to import for view: %s", viewName)
					}
					fileMagic := uint32(binary.LittleEndian.Uint16(viewData[0:2]))
					if fileMagic == roaring.MagicNumber { // if pilosa roaring format
						err = field.importRoaring(ctx, viewData, shard, viewName, req.Clear)
						if err != nil {
							return errors.Wrap(err, "importing pilosa roaring")
						}

					} else {
						// must make a copy of data to operate on locally on standard roaring format.
						// field.importRoaring changes the standard roaring run format to pilosa roaring
						data := make([]byte, len(viewData))
						copy(data, viewData)
						err = field.importRoaring(ctx, data, shard, viewName, req.Clear)
						if err != nil {
							return errors.Wrap(err, "importing standard roaring")
						}
					}
				}
				return err
			})
		} else if !remote { // if remote == true we don't forward to other nodes
			// forward it on
			eg.Go(func() error {
				return api.server.defaultClient.ImportRoaring(ctx, &node.URI, indexName, fieldName, shard, true, req)
			})
		}
	}
	return eg.Wait()
}

// DeleteField removes the named field from the named index. If the index is not
// found, an error is returned. If the field is not found, it is ignored and no
// action is taken.
func (api *API) DeleteField(ctx context.Context, indexName string, fieldName string) error {
	span, _ := tracing.StartSpanFromContext(ctx, "API.DeleteField")
	defer span.Finish()

	if err := api.validate(apiDeleteField); err != nil {
		return errors.Wrap(err, "validating api method")
	}

	// Find index.
	index := api.holder.Index(indexName)
	if index == nil {
		return newNotFoundError(ErrIndexNotFound)
	}

	// Delete field from the index.
	if err := index.DeleteField(fieldName); err != nil {
		return errors.Wrap(err, "deleting field")
	}

	// Send the delete field message to all nodes.
	err := api.server.SendSync(
		&DeleteFieldMessage{
			Index: indexName,
			Field: fieldName,
		})
	if err != nil {
		api.server.logger.Printf("problem sending DeleteField message: %s", err)
		return errors.Wrap(err, "sending DeleteField message")
	}
	api.holder.Stats.CountWithCustomTags("deleteField", 1, 1.0, []string{fmt.Sprintf("index:%s", indexName)})
	return nil
}

// DeleteAvailableShard a shard ID from the available shard set cache.
func (api *API) DeleteAvailableShard(_ context.Context, indexName, fieldName string, shardID uint64) error {
	if err := api.validate(apiDeleteAvailableShard); err != nil {
		return errors.Wrap(err, "validating api method")
	}

	// Find field.
	field := api.holder.Field(indexName, fieldName)
	if field == nil {
		return newNotFoundError(ErrFieldNotFound)
	}

	// Delete shard from the cache.
	if err := field.RemoveAvailableShard(shardID); err != nil {
		return errors.Wrap(err, "deleting available shard")
	}

	// Send the delete shard message to all nodes.
	err := api.server.SendSync(
		&DeleteAvailableShardMessage{
			Index:   indexName,
			Field:   fieldName,
			ShardID: shardID,
		})
	if err != nil {
		api.server.logger.Printf("problem sending DeleteAvailableShard message: %s", err)
		return errors.Wrap(err, "sending DeleteAvailableShard message")
	}
	api.holder.Stats.CountWithCustomTags("deleteAvailableShard", 1, 1.0, []string{fmt.Sprintf("index:%s", indexName), fmt.Sprintf("field:%s", fieldName)})
	return nil
}

// ExportCSV encodes the fragment designated by the index,field,shard as
// CSV of the form <row>,<col>
func (api *API) ExportCSV(ctx context.Context, indexName string, fieldName string, shard uint64, w io.Writer) error {
	span, _ := tracing.StartSpanFromContext(ctx, "API.ExportCSV")
	defer span.Finish()

	if err := api.validate(apiExportCSV); err != nil {
		return errors.Wrap(err, "validating api method")
	}

	// Validate that this handler owns the shard.
	if !api.cluster.ownsShard(api.Node().ID, indexName, shard) {
		api.server.logger.Printf("node %s does not own shard %d of index %s", api.Node().ID, shard, indexName)
		return ErrClusterDoesNotOwnShard
	}

	// Find index.
	index := api.holder.Index(indexName)
	if index == nil {
		return newNotFoundError(ErrIndexNotFound)
	}

	// Find field from the index.
	field := index.Field(fieldName)
	if field == nil {
		return newNotFoundError(ErrFieldNotFound)
	}

	// Find the fragment.
	f := api.holder.fragment(indexName, fieldName, viewStandard, shard)
	if f == nil {
		return ErrFragmentNotFound
	}

	// Wrap writer with a CSV writer.
	cw := csv.NewWriter(w)

	// Define the function to write each bit as a string,
	// translating to keys where necessary.
	var n int
	fn := func(rowID, columnID uint64) error {
		var rowStr string
		var colStr string
		var err error

		if field.keys() {
			if rowStr, err = api.holder.translateFile.TranslateRowToString(index.Name(), field.Name(), rowID); err != nil {
				return errors.Wrap(err, "translating row")
			}
		} else {
			rowStr = strconv.FormatUint(rowID, 10)
		}

		if index.Keys() {
			if colStr, err = api.holder.translateFile.TranslateColumnToString(index.Name(), columnID); err != nil {
				return errors.Wrap(err, "translating column")
			}
		} else {
			colStr = strconv.FormatUint(columnID, 10)
		}

		n++
		return cw.Write([]string{rowStr, colStr})
	}

	// Iterate over each column.
	if err := f.forEachBit(fn); err != nil {
		return errors.Wrap(err, "writing CSV")
	}

	// Ensure data is flushed.
	cw.Flush()

	span.LogKV("n", n)

	return nil
}

// ShardNodes returns the node and all replicas which should contain a shard's data.
func (api *API) ShardNodes(ctx context.Context, indexName string, shard uint64) ([]*Node, error) {
	span, _ := tracing.StartSpanFromContext(ctx, "API.ShardNodes")
	defer span.Finish()

	if err := api.validate(apiShardNodes); err != nil {
		return nil, errors.Wrap(err, "validating api method")
	}

	return api.cluster.shardNodes(indexName, shard), nil
}

// FragmentBlockData is an endpoint for internal usage. It is not guaranteed to
// return anything useful. Currently it returns protobuf encoded row and column
// ids from a "block" which is a subdivision of a fragment.
func (api *API) FragmentBlockData(ctx context.Context, body io.Reader) ([]byte, error) {
	span, _ := tracing.StartSpanFromContext(ctx, "API.FragmentBlockData")
	defer span.Finish()

	if err := api.validate(apiFragmentBlockData); err != nil {
		return nil, errors.Wrap(err, "validating api method")
	}

	reqBytes, err := ioutil.ReadAll(body)
	if err != nil {
		return nil, NewBadRequestError(errors.Wrap(err, "read body error"))
	}
	var req BlockDataRequest
	if err := api.Serializer.Unmarshal(reqBytes, &req); err != nil {
		return nil, NewBadRequestError(errors.Wrap(err, "unmarshal body error"))
	}

	// Retrieve fragment from holder.
	f := api.holder.fragment(req.Index, req.Field, req.View, req.Shard)
	if f == nil {
		return nil, ErrFragmentNotFound
	}

	var resp = BlockDataResponse{}
	resp.RowIDs, resp.ColumnIDs = f.blockData(int(req.Block))

	// Encode response.
	buf, err := api.Serializer.Marshal(&resp)
	if err != nil {
		return nil, errors.Wrap(err, "merge block response encoding error")
	}

	return buf, nil
}

// FragmentBlocks returns the checksums and block ids for all blocks in the specified fragment.
func (api *API) FragmentBlocks(ctx context.Context, indexName, fieldName, viewName string, shard uint64) ([]FragmentBlock, error) {
	span, _ := tracing.StartSpanFromContext(ctx, "API.FragmentBlocks")
	defer span.Finish()

	if err := api.validate(apiFragmentBlocks); err != nil {
		return nil, errors.Wrap(err, "validating api method")
	}

	// Retrieve fragment from holder.
	f := api.holder.fragment(indexName, fieldName, viewName, shard)
	if f == nil {
		return nil, ErrFragmentNotFound
	}

	// Retrieve blocks.
	blocks := f.Blocks()
	return blocks, nil
}

// FragmentData returns all data in the specified fragment.
func (api *API) FragmentData(ctx context.Context, indexName, fieldName, viewName string, shard uint64) (io.WriterTo, error) {
	span, _ := tracing.StartSpanFromContext(ctx, "API.FragmentData")
	defer span.Finish()

	if err := api.validate(apiFragmentData); err != nil {
		return nil, errors.Wrap(err, "validating api method")
	}

	// Retrieve fragment from holder.
	f := api.holder.fragment(indexName, fieldName, viewName, shard)
	if f == nil {
		return nil, ErrFragmentNotFound
	}
	return f, nil
}

// Hosts returns a list of the hosts in the cluster including their ID,
// URL, and which is the coordinator.
func (api *API) Hosts(ctx context.Context) []*Node {
	span, _ := tracing.StartSpanFromContext(ctx, "API.Hosts")
	defer span.Finish()
	return api.cluster.Nodes()
}

// Node gets the ID, URI and coordinator status for this particular node.
func (api *API) Node() *Node {
	node := api.server.node()
	return &node
}

// RecalculateCaches forces all TopN caches to be updated. Used mainly for integration tests.
func (api *API) RecalculateCaches(ctx context.Context) error {
	span, _ := tracing.StartSpanFromContext(ctx, "API.RecalculateCaches")
	defer span.Finish()

	if err := api.validate(apiRecalculateCaches); err != nil {
		return errors.Wrap(err, "validating api method")
	}

	err := api.server.SendSync(&RecalculateCaches{})
	if err != nil {
		return errors.Wrap(err, "broacasting message")
	}
	api.holder.recalculateCaches()
	return nil
}

// ClusterMessage is for internal use. It decodes a protobuf message out of
// the body and forwards it to the BroadcastHandler.
func (api *API) ClusterMessage(ctx context.Context, reqBody io.Reader) error {
	span, _ := tracing.StartSpanFromContext(ctx, "API.ClusterMessage")
	defer span.Finish()

	if err := api.validate(apiClusterMessage); err != nil {
		return errors.Wrap(err, "validating api method")
	}

	// Read entire body.
	body, err := ioutil.ReadAll(reqBody)
	if err != nil {
		return errors.Wrap(err, "reading body")
	}

	typ := body[0]
	msg := getMessage(typ)
	err = api.server.serializer.Unmarshal(body[1:], msg)
	if err != nil {
		return errors.Wrap(err, "deserializing cluster message")
	}

	// Forward the error message.
	if err := api.server.receiveMessage(msg); err != nil {
		return errors.Wrap(err, "receiving message")
	}
	return nil
}

// Schema returns information about each index in Pilosa including which fields
// they contain.
func (api *API) Schema(ctx context.Context) []*IndexInfo {
	span, _ := tracing.StartSpanFromContext(ctx, "API.Schema")
	defer span.Finish()
	return api.holder.limitedSchema()
}

// ApplySchema takes the given schema and applies it across the
// cluster (if remote is false), or just to this node (if remote is
// true). This is designed for the use case of replicating a schema
// from one Pilosa cluster to another which is initially empty. It is
// not officially supported in other scenarios and may produce
// surprising results.
func (api *API) ApplySchema(ctx context.Context, s *Schema, remote bool) error {
	span, _ := tracing.StartSpanFromContext(ctx, "API.ApplySchema")
	defer span.Finish()

	if err := api.validate(apiApplySchema); err != nil {
		return errors.Wrap(err, "validating api method")
	}

	if !remote {
		nodes := api.cluster.Nodes()
		for i, node := range nodes {
			err := api.server.defaultClient.PostSchema(ctx, &node.URI, s, true)
			if err != nil {
				return errors.Wrapf(err, "forwarding post schema to node %d of %d", i+1, len(nodes))
			}
		}
	}

	return api.holder.applySchema(s)
}

// Views returns the views in the given field.
func (api *API) Views(ctx context.Context, indexName string, fieldName string) ([]*view, error) {
	span, _ := tracing.StartSpanFromContext(ctx, "API.Views")
	defer span.Finish()

	if err := api.validate(apiViews); err != nil {
		return nil, errors.Wrap(err, "validating api method")
	}

	// Retrieve views.
	f := api.holder.Field(indexName, fieldName)
	if f == nil {
		return nil, ErrFieldNotFound
	}

	// Fetch views.
	views := f.views()
	return views, nil
}

// DeleteView removes the given view.
func (api *API) DeleteView(ctx context.Context, indexName string, fieldName string, viewName string) error {
	span, _ := tracing.StartSpanFromContext(ctx, "API.DeleteView")
	defer span.Finish()

	if err := api.validate(apiDeleteView); err != nil {
		return errors.Wrap(err, "validating api method")
	}

	// Retrieve field.
	f := api.holder.Field(indexName, fieldName)
	if f == nil {
		return ErrFieldNotFound
	}

	// Delete the view.
	if err := f.deleteView(viewName); err != nil {
		// Ignore this error because views do not exist on all nodes due to shard distribution.
		if err != ErrInvalidView {
			return errors.Wrap(err, "deleting view")
		}
	}

	// Send the delete view message to all nodes.
	err := api.server.SendSync(
		&DeleteViewMessage{
			Index: indexName,
			Field: fieldName,
			View:  viewName,
		})
	if err != nil {
		api.server.logger.Printf("problem sending DeleteView message: %s", err)
	}

	return errors.Wrap(err, "sending DeleteView message")
}

// IndexAttrDiff determines the local column attribute data blocks which differ from those provided.
func (api *API) IndexAttrDiff(ctx context.Context, indexName string, blocks []AttrBlock) (map[uint64]map[string]interface{}, error) {
	span, _ := tracing.StartSpanFromContext(ctx, "API.IndexAttrDiff")
	defer span.Finish()

	if err := api.validate(apiIndexAttrDiff); err != nil {
		return nil, errors.Wrap(err, "validating api method")
	}

	// Retrieve index from holder.
	index := api.holder.Index(indexName)
	if index == nil {
		return nil, newNotFoundError(ErrIndexNotFound)
	}

	// Retrieve local blocks.
	localBlocks, err := index.ColumnAttrStore().Blocks()
	if err != nil {
		return nil, errors.Wrap(err, "getting blocks")
	}

	// Read all attributes from all mismatched blocks.
	attrs := make(map[uint64]map[string]interface{})
	for _, blockID := range attrBlocks(localBlocks).Diff(blocks) {
		// Retrieve block data.
		m, err := index.ColumnAttrStore().BlockData(blockID)
		if err != nil {
			return nil, errors.Wrap(err, "getting block")
		}

		// Copy to index-wide struct.
		for k, v := range m {
			attrs[k] = v
		}
	}
	return attrs, nil
}

// FieldAttrDiff determines the local row attribute data blocks which differ from those provided.
func (api *API) FieldAttrDiff(ctx context.Context, indexName string, fieldName string, blocks []AttrBlock) (map[uint64]map[string]interface{}, error) {
	span, _ := tracing.StartSpanFromContext(ctx, "API.FieldAttrDiff")
	defer span.Finish()

	if err := api.validate(apiFieldAttrDiff); err != nil {
		return nil, errors.Wrap(err, "validating api method")
	}

	// Retrieve index from holder.
	f := api.holder.Field(indexName, fieldName)
	if f == nil {
		return nil, ErrFieldNotFound
	}

	// Retrieve local blocks.
	localBlocks, err := f.RowAttrStore().Blocks()
	if err != nil {
		return nil, errors.Wrap(err, "getting blocks")
	}

	// Read all attributes from all mismatched blocks.
	attrs := make(map[uint64]map[string]interface{})
	for _, blockID := range attrBlocks(localBlocks).Diff(blocks) {
		// Retrieve block data.
		m, err := f.RowAttrStore().BlockData(blockID)
		if err != nil {
			return nil, errors.Wrap(err, "getting block")
		}

		// Copy to index-wide struct.
		for k, v := range m {
			attrs[k] = v
		}
	}
	return attrs, nil
}

// ImportOptions holds the options for the API.Import method.
type ImportOptions struct {
	Clear          bool
	IgnoreKeyCheck bool
}

// ImportOption is a functional option type for API.Import.
type ImportOption func(*ImportOptions) error

// OptImportOptionsClear is a functional option on ImportOption
// used to specify whether the import is a set or clear operation.
func OptImportOptionsClear(c bool) ImportOption {
	return func(o *ImportOptions) error {
		o.Clear = c
		return nil
	}
}

// OptImportOptionsIgnoreKeyCheck is a functional option on ImportOption
// used to specify whether key check should be ignored.
func OptImportOptionsIgnoreKeyCheck(b bool) ImportOption {
	return func(o *ImportOptions) error {
		o.IgnoreKeyCheck = b
		return nil
	}
}

// Import bulk imports data into a particular index,field,shard.
func (api *API) Import(ctx context.Context, req *ImportRequest, opts ...ImportOption) error {
	span, _ := tracing.StartSpanFromContext(ctx, "API.Import")
	defer span.Finish()

	if err := api.validate(apiImport); err != nil {
		return errors.Wrap(err, "validating api method")
	}

	// Set up import options.
	options, err := setUpImportOptions(opts...)
	if err != nil {
		return errors.Wrap(err, "setting up import options")
	}

	index, field, err := api.indexField(req.Index, req.Field, req.Shard)
	if err != nil {
		return errors.Wrap(err, "getting index and field")
	}

	// Unless explicitly ignoring key validation (meaning keys have been
	// translated to ids in a previous step at the coordinator node), then
	// check to see if keys need translation.
	if !options.IgnoreKeyCheck {
		// Translate row keys.
		if field.keys() {
			if len(req.RowIDs) != 0 {
				return errors.New("row ids cannot be used because field uses string keys")
			}
			if req.RowIDs, err = api.holder.translateFile.TranslateRowsToUint64(index.Name(), field.Name(), req.RowKeys); err != nil {
				return errors.Wrap(err, "translating rows")
			}
		}

		// Translate column keys.
		if index.Keys() {
			if len(req.ColumnIDs) != 0 {
				return errors.New("column ids cannot be used because index uses string keys")
			}
			if req.ColumnIDs, err = api.holder.translateFile.TranslateColumnsToUint64(index.Name(), req.ColumnKeys); err != nil {
				return errors.Wrap(err, "translating columns")
			}
		}

		// For translated data, map the columnIDs to shards. If
		// this node does not own the shard, forward to the node that does.
		if index.Keys() || field.keys() {
			m := make(map[uint64][]Bit)

			for i, colID := range req.ColumnIDs {
				shard := colID / ShardWidth
				if _, ok := m[shard]; !ok {
					m[shard] = make([]Bit, 0)
				}
				bit := Bit{
					RowID:    req.RowIDs[i],
					ColumnID: colID,
				}
				if len(req.Timestamps) > 0 {
					bit.Timestamp = req.Timestamps[i]
				}
				m[shard] = append(m[shard], bit)
			}

			// Signal to the receiving nodes to ignore checking for key translation.
			opts = append(opts, OptImportOptionsIgnoreKeyCheck(true))

			var eg errgroup.Group
			for shard, bits := range m {
				// TODO: if local node owns this shard we don't need to go through the client
				shard := shard
				bits := bits
				eg.Go(func() error {
					return api.server.defaultClient.Import(ctx, req.Index, req.Field, shard, bits, opts...)
				})
			}
			return eg.Wait()
		}
	}

	// Validate shard ownership.
	if err := api.validateShardOwnership(req.Index, req.Shard); err != nil {
		return errors.Wrap(err, "validating shard ownership")
	}

	// Convert timestamps to time.Time.
	timestamps := make([]*time.Time, len(req.Timestamps))
	for i, ts := range req.Timestamps {
		if ts == 0 {
			continue
		}
		t := time.Unix(0, ts).UTC()
		timestamps[i] = &t
	}

	// Import columnIDs into existence field.
	if !options.Clear {
		if err := importExistenceColumns(index, req.ColumnIDs); err != nil {
			api.server.logger.Printf("import existence error: index=%s, field=%s, shard=%d, columns=%d, err=%s", req.Index, req.Field, req.Shard, len(req.ColumnIDs), err)
			return errors.Wrap(err, "importing existence columns")
		}
	}

	// Import into fragment.
	err = field.Import(req.RowIDs, req.ColumnIDs, timestamps, opts...)
	if err != nil {
		api.server.logger.Printf("import error: index=%s, field=%s, shard=%d, columns=%d, err=%s", req.Index, req.Field, req.Shard, len(req.ColumnIDs), err)
	}
	return errors.Wrap(err, "importing")
}

// ImportValue bulk imports values into a particular field.
func (api *API) ImportValue(ctx context.Context, req *ImportValueRequest, opts ...ImportOption) error {
	span, _ := tracing.StartSpanFromContext(ctx, "API.ImportValue")
	defer span.Finish()

	if err := api.validate(apiImportValue); err != nil {
		return errors.Wrap(err, "validating api method")
	}

	// Set up import options.
	options, err := setUpImportOptions(opts...)
	if err != nil {
		return errors.Wrap(err, "setting up import options")
	}

	index, field, err := api.indexField(req.Index, req.Field, req.Shard)
	if err != nil {
		return errors.Wrap(err, "getting index and field")
	}

	// Unless explicitly ignoring key validation (meaning keys have been
	// translate to ids in a previous step at the coordinator node), then
	// check to see if keys need translation.
	if !options.IgnoreKeyCheck {
		// Translate column keys.
		if index.Keys() {
			if len(req.ColumnIDs) != 0 {
				return errors.New("column ids cannot be used because index uses string keys")
			}
			if req.ColumnIDs, err = api.holder.translateFile.TranslateColumnsToUint64(index.Name(), req.ColumnKeys); err != nil {
				return errors.Wrap(err, "translating columns")
			}

			// For translated data, map the columnIDs to shards. If
			// this node does not own the shard, forward to the node that does.
			m := make(map[uint64][]FieldValue)

			for i, colID := range req.ColumnIDs {
				shard := colID / ShardWidth
				if _, ok := m[shard]; !ok {
					m[shard] = make([]FieldValue, 0)
				}
				m[shard] = append(m[shard], FieldValue{
					Value:    req.Values[i],
					ColumnID: colID,
				})
			}

			// Signal to the receiving nodes to ignore checking for key translation.
			opts = append(opts, OptImportOptionsIgnoreKeyCheck(true))

			var eg errgroup.Group
			for shard, vals := range m {
				// TODO: if local node owns this shard we don't need to go through the client
				shard := shard
				vals := vals
				eg.Go(func() error {
					return api.server.defaultClient.ImportValue(ctx, req.Index, req.Field, shard, vals, opts...)
				})
			}
			return eg.Wait()
		}
	}

	// Validate shard ownership.
	if err := api.validateShardOwnership(req.Index, req.Shard); err != nil {
		return errors.Wrap(err, "validating shard ownership")
	}

	// Import columnIDs into existence field.
	if !options.Clear {
		if err := importExistenceColumns(index, req.ColumnIDs); err != nil {
			api.server.logger.Printf("import existence error: index=%s, field=%s, shard=%d, columns=%d, err=%s", req.Index, req.Field, req.Shard, len(req.ColumnIDs), err)
			return errors.Wrap(err, "importing existence columns")
		}
	}

	// Import into fragment.
	err = field.importValue(req.ColumnIDs, req.Values, options)
	if err != nil {
		api.server.logger.Printf("import error: index=%s, field=%s, shard=%d, columns=%d, err=%s", req.Index, req.Field, req.Shard, len(req.ColumnIDs), err)
	}
	return errors.Wrap(err, "importing")
}

func importExistenceColumns(index *Index, columnIDs []uint64) error {
	ef := index.existenceField()
	if ef == nil {
		return nil
	}

	existenceRowIDs := make([]uint64, len(columnIDs))
	return ef.Import(existenceRowIDs, columnIDs, nil)
}

// MaxShards returns the maximum shard number for each index in a map.
// TODO (2.0): This method has been deprecated. Instead, use
// AvailableShardsByIndex.
func (api *API) MaxShards(ctx context.Context) map[string]uint64 {
	span, _ := tracing.StartSpanFromContext(ctx, "API.MaxShards")
	defer span.Finish()

	m := make(map[string]uint64)
	for k, v := range api.holder.availableShardsByIndex() {
		m[k] = v.Max()
	}
	return m
}

// AvailableShardsByIndex returns bitmaps of shards with available by index name.
func (api *API) AvailableShardsByIndex(ctx context.Context) map[string]*roaring.Bitmap {
	span, _ := tracing.StartSpanFromContext(ctx, "API.AvailableShardsByIndex")
	defer span.Finish()
	return api.holder.availableShardsByIndex()
}

// StatsWithTags returns an instance of whatever implementation of StatsClient
// pilosa is using with the given tags.
func (api *API) StatsWithTags(tags []string) stats.StatsClient {
	if api.holder == nil || api.cluster == nil {
		return nil
	}
	return api.holder.Stats.WithTags(tags...)
}

// LongQueryTime returns the configured threshold for logging/statting
// long running queries.
func (api *API) LongQueryTime() time.Duration {
	if api.cluster == nil {
		return 0
	}
	return api.cluster.longQueryTime
}

func (api *API) validateShardOwnership(indexName string, shard uint64) error {
	// Validate that this handler owns the shard.
	if !api.cluster.ownsShard(api.Node().ID, indexName, shard) {
		api.server.logger.Printf("node %s does not own shard %d of index %s", api.Node().ID, shard, indexName)
		return ErrClusterDoesNotOwnShard
	}
	return nil
}

func (api *API) indexField(indexName string, fieldName string, shard uint64) (*Index, *Field, error) {
	api.server.logger.Debugf("importing: %v %v %v", indexName, fieldName, shard)

	// Find the Index.
	index := api.holder.Index(indexName)
	if index == nil {
		api.server.logger.Printf("fragment error: index=%s, field=%s, shard=%d, err=%s", indexName, fieldName, shard, ErrIndexNotFound.Error())
		return nil, nil, newNotFoundError(ErrIndexNotFound)
	}

	// Retrieve field.
	field := index.Field(fieldName)
	if field == nil {
		api.server.logger.Printf("field error: index=%s, field=%s, shard=%d, err=%s", indexName, fieldName, shard, ErrFieldNotFound.Error())
		return nil, nil, ErrFieldNotFound
	}
	return index, field, nil
}

// SetCoordinator makes a new Node the cluster coordinator.
func (api *API) SetCoordinator(ctx context.Context, id string) (oldNode, newNode *Node, err error) {
	span, _ := tracing.StartSpanFromContext(ctx, "API.SetCoordinator")
	defer span.Finish()

	if err := api.validate(apiSetCoordinator); err != nil {
		return nil, nil, errors.Wrap(err, "validating api method")
	}

	oldNode = api.cluster.nodeByID(api.cluster.Coordinator)
	newNode = api.cluster.nodeByID(id)
	if newNode == nil {
		return nil, nil, errors.Wrap(ErrNodeIDNotExists, "getting new node")
	}

	// If the new coordinator is this node, do the SetCoordinator directly.
	if newNode.ID == api.Node().ID {
		return oldNode, newNode, api.cluster.setCoordinator(newNode)
	}

	// Send the set-coordinator message to new node.
	err = api.server.SendTo(
		newNode,
		&SetCoordinatorMessage{
			New: newNode,
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
		return nil, errors.Wrap(err, "validating api method")
	}

	removeNode := api.cluster.nodeByID(id)
	if removeNode == nil {
		if !api.cluster.topologyContainsNode(id) {
			return nil, errors.Wrap(ErrNodeIDNotExists, "finding node to remove")
		}
		removeNode = &Node{
			ID: id,
		}
	}

	// Start the resize process (similar to NodeJoin)
	err := api.cluster.nodeLeave(id)
	if err != nil {
		return removeNode, errors.Wrap(err, "calling node leave")
	}
	return removeNode, nil
}

// ResizeAbort stops the current resize job.
func (api *API) ResizeAbort() error {
	if err := api.validate(apiResizeAbort); err != nil {
		return errors.Wrap(err, "validating api method")
	}

	err := api.cluster.completeCurrentJob(resizeJobStateAborted)
	return errors.Wrap(err, "complete current job")
}

// GetTranslateData provides a reader for key translation logs starting at offset.
func (api *API) GetTranslateData(ctx context.Context, offset int64) (io.ReadCloser, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "API.GetTranslateData")
	defer span.Finish()

	rc, err := api.holder.translateFile.Reader(ctx, offset)
	if err != nil {
		return nil, errors.Wrap(err, "read from translate store")
	}

	// Ensure reader is closed when the client disconnects.
	go func() { <-ctx.Done(); rc.Close() }()

	return rc, nil
}

// State returns the cluster state which is usually "NORMAL", but could be
// "STARTING", "RESIZING", or potentially others. See cluster.go for more
// details.
func (api *API) State() string {
	return api.cluster.State()
}

// Version returns the Pilosa version.
func (api *API) Version() string {
	return strings.TrimPrefix(Version, "v")
}

// Info returns information about this server instance.
func (api *API) Info() serverInfo {
	si := api.server.systemInfo
	// we don't report errors on failures to get this information
	physicalCores, logicalCores, _ := si.CPUCores()
	mhz, _ := si.CPUMHz()
	mem, _ := si.MemTotal()
	return serverInfo{
		ShardWidth:       ShardWidth,
		CPUPhysicalCores: physicalCores,
		CPULogicalCores:  logicalCores,
		CPUMHz:           mhz,
		CPUType:          si.CPUModel(),
		Memory:           mem,
	}
}

// TranslateKeys handles a TranslateKeyRequest.
func (api *API) TranslateKeys(body io.Reader) ([]byte, error) {
	reqBytes, err := ioutil.ReadAll(body)
	if err != nil {
		return nil, NewBadRequestError(errors.Wrap(err, "read body error"))
	}
	var req TranslateKeysRequest
	if err := api.Serializer.Unmarshal(reqBytes, &req); err != nil {
		return nil, NewBadRequestError(errors.Wrap(err, "unmarshal body error"))
	}
	var ids []uint64
	if req.Field == "" {
		ids, err = api.holder.translateFile.TranslateColumnsToUint64(req.Index, req.Keys)
	} else {
		ids, err = api.holder.translateFile.TranslateRowsToUint64(req.Index, req.Field, req.Keys)
	}
	if err != nil {
		return nil, err
	}

	resp := TranslateKeysResponse{
		IDs: ids,
	}
	// Encode response.
	buf, err := api.Serializer.Marshal(&resp)
	if err != nil {
		return nil, errors.Wrap(err, "translate keys response encoding error")
	}
	return buf, nil
}

type serverInfo struct {
	ShardWidth       uint64 `json:"shardWidth"`
	Memory           uint64 `json:"memory"`
	CPUType          string `json:"cpuType"`
	CPUPhysicalCores int    `json:"cpuPhysicalCores"`
	CPULogicalCores  int    `json:"cpuLogicalCores"`
	CPUMHz           int    `json:"cpuMHz"`
}

type apiMethod int

// API validation constants.
const (
	apiClusterMessage apiMethod = iota
	apiCreateField
	apiCreateIndex
	apiDeleteField
	apiDeleteAvailableShard
	apiDeleteIndex
	apiDeleteView
	apiExportCSV
	apiFragmentBlockData
	apiFragmentBlocks
	apiFragmentData
	apiField
	apiFieldAttrDiff
	//apiHosts // not implemented
	apiImport
	apiImportValue
	apiIndex
	apiIndexAttrDiff
	//apiLocalID // not implemented
	//apiLongQueryTime // not implemented
	//apiMaxShards // not implemented
	apiQuery
	apiRecalculateCaches
	apiRemoveNode
	apiResizeAbort
	//apiSchema // not implemented
	apiSetCoordinator
	apiShardNodes
	//apiState // not implemented
	//apiStatsWithTags // not implemented
	//apiVersion // not implemented
	apiViews
	apiApplySchema
)

var methodsCommon = map[apiMethod]struct{}{
	apiClusterMessage: {},
	apiSetCoordinator: {},
}

var methodsResizing = map[apiMethod]struct{}{
	apiFragmentData: {},
	apiResizeAbort:  {},
}

var methodsNormal = map[apiMethod]struct{}{
	apiCreateField:          {},
	apiCreateIndex:          {},
	apiDeleteField:          {},
	apiDeleteAvailableShard: {},
	apiDeleteIndex:          {},
	apiDeleteView:           {},
	apiExportCSV:            {},
	apiFragmentBlockData:    {},
	apiFragmentBlocks:       {},
	apiField:                {},
	apiFieldAttrDiff:        {},
	apiImport:               {},
	apiImportValue:          {},
	apiIndex:                {},
	apiIndexAttrDiff:        {},
	apiQuery:                {},
	apiRecalculateCaches:    {},
	apiRemoveNode:           {},
	apiShardNodes:           {},
	apiViews:                {},
	apiApplySchema:          {},
}
