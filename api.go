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
	"strconv"
	"strings"
	"time"

	"github.com/pilosa/pilosa/pql"
	"github.com/pkg/errors"
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
	return newApiMethodNotAllowedError(errors.Errorf("api method %s not allowed in state %s", f, state))
}

// Query parses a PQL query out of the request and executes it.
func (api *API) Query(ctx context.Context, req *QueryRequest) (QueryResponse, error) {
	if err := api.validate(apiQuery); err != nil {
		return QueryResponse{}, errors.Wrap(err, "validating api method")
	}

	resp := QueryResponse{}

	q, err := pql.NewParser(strings.NewReader(req.Query)).Parse()
	if err != nil {
		return resp, errors.Wrap(err, "parsing")
	}
	execOpts := &execOptions{
		Remote:          req.Remote,
		ExcludeRowAttrs: req.ExcludeRowAttrs,
		ExcludeColumns:  req.ExcludeColumns,
	}
	results, err := api.server.executor.Execute(ctx, req.Index, q, req.Shards, execOpts)
	if err != nil {
		return resp, errors.Wrap(err, "executing")
	}
	resp.Results = results

	// Fill column attributes if requested.
	if req.ColumnAttrs && !req.ExcludeColumns {
		// Consolidate all column ids across all calls.
		var columnIDs []uint64
		for _, result := range results {
			bm, ok := result.(*Row)
			if !ok {
				continue
			}
			columnIDs = uint64Slice(columnIDs).merge(bm.Columns())
		}

		// Retrieve column attributes across all calls.
		columnAttrSets, err := api.readColumnAttrSets(api.holder.Index(req.Index), columnIDs)
		if err != nil {
			return resp, errors.Wrap(err, "reading column attrs")
		}

		// Translate column attributes, if necessary.
		if api.holder.translateFile != nil {
			for _, col := range resp.ColumnAttrSets {
				v, err := api.holder.translateFile.TranslateColumnToString(req.Index, col.ID)
				if err != nil {
					return resp, err
				}
				col.Key, col.ID = v, 0
			}
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
			return nil, errors.Wrap(err, "getting attrs")
		} else if len(attrs) == 0 {
			continue
		}

		// Append column with attributes.
		ax = append(ax, &ColumnAttrSet{ID: id, Attrs: attrs})
	}

	return ax, nil
}

// CreateIndex makes a new Pilosa index.
func (api *API) CreateIndex(_ context.Context, indexName string, options IndexOptions) (*Index, error) {
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
func (api *API) Index(_ context.Context, indexName string) (*Index, error) {
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
func (api *API) DeleteIndex(_ context.Context, indexName string) error {
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
func (api *API) CreateField(_ context.Context, indexName string, fieldName string, opts ...FieldOption) (*Field, error) {
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
func (api *API) Field(_ context.Context, indexName, fieldName string) (*Field, error) {
	if err := api.validate(apiField); err != nil {
		return nil, errors.Wrap(err, "validating api method")
	}

	field := api.holder.Field(indexName, fieldName)
	if field == nil {
		return nil, newNotFoundError(ErrFieldNotFound)
	}
	return field, nil
}

// DeleteField removes the named field from the named index. If the index is not
// found, an error is returned. If the field is not found, it is ignored and no
// action is taken.
func (api *API) DeleteField(_ context.Context, indexName string, fieldName string) error {
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

// ExportCSV encodes the fragment designated by the index,field,shard as
// CSV of the form <row>,<col>
func (api *API) ExportCSV(_ context.Context, indexName string, fieldName string, shard uint64, w io.Writer) error {
	if err := api.validate(apiExportCSV); err != nil {
		return errors.Wrap(err, "validating api method")
	}

	// Validate that this handler owns the shard.
	if !api.cluster.ownsShard(api.Node().ID, indexName, shard) {
		api.server.logger.Printf("node %s does not own shard %d of index %s", api.Node().ID, shard, indexName)
		return ErrClusterDoesNotOwnShard
	}

	// Find the fragment.
	f := api.holder.fragment(indexName, fieldName, viewStandard, shard)
	if f == nil {
		return ErrFragmentNotFound
	}

	// Wrap writer with a CSV writer.
	cw := csv.NewWriter(w)

	// Iterate over each column.
	if err := f.forEachBit(func(rowID, columnID uint64) error {
		return cw.Write([]string{
			strconv.FormatUint(rowID, 10),
			strconv.FormatUint(columnID, 10),
		})
	}); err != nil {
		return errors.Wrap(err, "writing CSV")
	}

	// Ensure data is flushed.
	cw.Flush()

	return nil
}

// ShardNodes returns the node and all replicas which should contain a shard's data.
func (api *API) ShardNodes(_ context.Context, indexName string, shard uint64) ([]*Node, error) {
	if err := api.validate(apiShardNodes); err != nil {
		return nil, errors.Wrap(err, "validating api method")
	}

	return api.cluster.shardNodes(indexName, shard), nil
}

// FragmentBlockData is an endpoint for internal usage. It is not guaranteed to
// return anything useful. Currently it returns protobuf encoded row and column
// ids from a "block" which is a subdivision of a fragment.
func (api *API) FragmentBlockData(_ context.Context, body io.Reader) ([]byte, error) {
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
	f := api.holder.fragment(req.Index, req.Field, viewStandard, req.Shard)
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
func (api *API) FragmentBlocks(_ context.Context, indexName string, fieldName string, shard uint64) ([]FragmentBlock, error) {
	if err := api.validate(apiFragmentBlocks); err != nil {
		return nil, errors.Wrap(err, "validating api method")
	}

	// Retrieve fragment from holder.
	f := api.holder.fragment(indexName, fieldName, viewStandard, shard)
	if f == nil {
		return nil, ErrFragmentNotFound
	}

	// Retrieve blocks.
	blocks := f.Blocks()
	return blocks, nil
}

// Hosts returns a list of the hosts in the cluster including their ID,
// URL, and which is the coordinator.
func (api *API) Hosts(_ context.Context) []*Node {
	return api.cluster.Nodes
}

// Node gets the ID, URI and coordinator status for this particular node.
func (api *API) Node() *Node {
	node := api.server.node()
	return &node
}

// RecalculateCaches forces all TopN caches to be updated. Used mainly for integration tests.
func (api *API) RecalculateCaches(_ context.Context) error {
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

// PostClusterMessage is for internal use. It decodes a protobuf message out of
// the body and forwards it to the BroadcastHandler.
func (api *API) ClusterMessage(_ context.Context, reqBody io.Reader) error {
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
func (api *API) Schema(_ context.Context) []*IndexInfo {
	return api.holder.limitedSchema()
}

// Views returns the views in the given field.
func (api *API) Views(_ context.Context, indexName string, fieldName string) ([]*view, error) {
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
func (api *API) DeleteView(_ context.Context, indexName string, fieldName string, viewName string) error {
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

// IndexAttrDiff
func (api *API) IndexAttrDiff(_ context.Context, indexName string, blocks []AttrBlock) (map[uint64]map[string]interface{}, error) {
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

func (api *API) FieldAttrDiff(_ context.Context, indexName string, fieldName string, blocks []AttrBlock) (map[uint64]map[string]interface{}, error) {
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

// Import bulk imports data into a particular index,field,shard.
func (api *API) Import(_ context.Context, req *ImportRequest) error {
	if err := api.validate(apiImport); err != nil {
		return errors.Wrap(err, "validating api method")
	}

	index := api.holder.Index(req.Index)
	if index == nil {
		return newNotFoundError(ErrIndexNotFound)
	}

	field, err := api.indexField(req.Index, req.Field, req.Shard)
	if err != nil {
		return errors.Wrap(err, "getting field")
	}

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
	err = field.Import(req.RowIDs, req.ColumnIDs, timestamps)
	if err != nil {
		api.server.logger.Printf("import error: index=%s, field=%s, shard=%d, columns=%d, err=%s", req.Index, req.Field, req.Shard, len(req.ColumnIDs), err)
	}
	return errors.Wrap(err, "importing")
}

// ImportValue bulk imports values into a particular field.
func (api *API) ImportValue(_ context.Context, req *ImportValueRequest) error {
	if err := api.validate(apiImportValue); err != nil {
		return errors.Wrap(err, "validating api method")
	}

	index := api.holder.Index(req.Index)
	if index == nil {
		return newNotFoundError(ErrIndexNotFound)
	}

	field, err := api.indexField(req.Index, req.Field, req.Shard)
	if err != nil {
		return errors.Wrap(err, "getting field")
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

	// Import into fragment.
	err = field.importValue(req.ColumnIDs, req.Values)
	if err != nil {
		api.server.logger.Printf("import error: index=%s, field=%s, shard=%d, columns=%d, err=%s", req.Index, req.Field, req.Shard, len(req.ColumnIDs), err)
	}
	return errors.Wrap(err, "importing")
}

// MaxShards returns the maximum shard number for each index in a map.
func (api *API) MaxShards(_ context.Context) map[string]uint64 {
	return api.holder.maxShards()
}

// StatsWithTags returns an instance of whatever implementation of StatsClient
// pilosa is using with the given tags.
func (api *API) StatsWithTags(tags []string) StatsClient {
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

func (api *API) indexField(indexName string, fieldName string, shard uint64) (*Field, error) {
	// Validate that this handler owns the shard.
	if !api.cluster.ownsShard(api.Node().ID, indexName, shard) {
		api.server.logger.Printf("node %s does not own shard %d of index %s", api.Node().ID, shard, indexName)
		return nil, ErrClusterDoesNotOwnShard
	}

	// Find the Index.
	api.server.logger.Printf("importing: %v %v %v", indexName, fieldName, shard)
	index := api.holder.Index(indexName)
	if index == nil {
		api.server.logger.Printf("fragment error: index=%s, field=%s, shard=%d, err=%s", indexName, fieldName, shard, ErrIndexNotFound.Error())
		return nil, newNotFoundError(ErrIndexNotFound)
	}

	// Retrieve field.
	field := index.Field(fieldName)
	if field == nil {
		api.server.logger.Printf("field error: index=%s, field=%s, shard=%d, err=%s", indexName, fieldName, shard, ErrFieldNotFound.Error())
		return nil, ErrFieldNotFound
	}
	return field, nil
}

// SetCoordinator makes a new Node the cluster coordinator.
func (api *API) SetCoordinator(_ context.Context, id string) (oldNode, newNode *Node, err error) {
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

// Info returns information about this server instance
func (api *API) Info() serverInfo {
	return serverInfo{
		ShardWidth: ShardWidth,
	}
}

type serverInfo struct {
	ShardWidth uint64 `json:"shardWidth"`
}

type apiMethod int

// API validation constants.
const (
	apiClusterMessage apiMethod = iota
	apiCreateField
	apiCreateIndex
	apiDeleteField
	apiDeleteIndex
	apiDeleteView
	apiExportCSV
	apiFragmentBlockData
	apiFragmentBlocks
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
)

var methodsCommon = map[apiMethod]struct{}{
	apiClusterMessage: {},
	apiSetCoordinator: {},
}

var methodsResizing = map[apiMethod]struct{}{
	apiResizeAbort: {},
}

var methodsNormal = map[apiMethod]struct{}{
	apiCreateField:       {},
	apiCreateIndex:       {},
	apiDeleteField:       {},
	apiDeleteIndex:       {},
	apiDeleteView:        {},
	apiExportCSV:         {},
	apiFragmentBlockData: {},
	apiFragmentBlocks:    {},
	apiField:             {},
	apiFieldAttrDiff:     {},
	apiImport:            {},
	apiImportValue:       {},
	apiIndex:             {},
	apiIndexAttrDiff:     {},
	apiQuery:             {},
	apiRecalculateCaches: {},
	apiRemoveNode:        {},
	apiShardNodes:        {},
	apiViews:             {},
}
