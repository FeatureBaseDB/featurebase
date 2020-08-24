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
	"math"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pilosa/pilosa/v2/pql"
	"github.com/pilosa/pilosa/v2/roaring"
	"github.com/pilosa/pilosa/v2/stats"
	"github.com/pilosa/pilosa/v2/tracing"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

// API provides the top level programmatic interface to Pilosa. It is usually
// wrapped by a handler which provides an external interface (e.g. HTTP).
type API struct {
	holder  *Holder
	cluster *cluster
	server  *Server
	tracker *queryTracker

	importWorkersWG      sync.WaitGroup
	importWorkerPoolSize int
	importWork           chan importJob

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

func OptAPIImportWorkerPoolSize(size int) apiOption {
	return func(a *API) error {
		a.importWorkerPoolSize = size
		return nil
	}
}

// NewAPI returns a new API instance.
func NewAPI(opts ...apiOption) (*API, error) {
	api := &API{
		importWorkerPoolSize: 2,
	}

	for _, opt := range opts {
		err := opt(api)
		if err != nil {
			return nil, errors.Wrap(err, "applying option")
		}
	}

	api.importWork = make(chan importJob, api.importWorkerPoolSize)
	for i := 0; i < api.importWorkerPoolSize; i++ {
		api.importWorkersWG.Add(1)
		go func() {
			importWorker(api.importWork)
			defer api.importWorkersWG.Done()
		}()
	}

	api.tracker = newQueryTracker()

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

// Close closes the api and waits for it to shutdown.
func (api *API) Close() error {
	close(api.importWork)
	api.importWorkersWG.Wait()
	api.tracker.Stop()
	return nil
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
	defer api.tracker.Finish(api.tracker.Start(req.Query))
	execOpts := &execOptions{
		Remote:          req.Remote,
		Profile:         req.Profile,
		ExcludeRowAttrs: req.ExcludeRowAttrs, // NOTE: Kept for Pilosa 1.x compat.
		ExcludeColumns:  req.ExcludeColumns,  // NOTE: Kept for Pilosa 1.x compat.
		ColumnAttrs:     req.ColumnAttrs,     // NOTE: Kept for Pilosa 1.x compat.
		EmbeddedData:    req.EmbeddedData,    // precomputed values that needed to be passed with the request
	}
	resp, err := api.server.executor.Execute(ctx, req.Index, q, req.Shards, execOpts)
	if err != nil {
		return QueryResponse{}, errors.Wrap(err, "executing")
	}

	// Check for an error embedded in the response.
	if resp.Err != nil {
		err = errors.Wrap(resp.Err, "executing")
	}

	return resp, err
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

	createdAt := timestamp()
	index.mu.Lock()
	index.createdAt = createdAt
	index.mu.Unlock()

	// Send the create index message to all nodes.
	err = api.server.SendSync(
		&CreateIndexMessage{
			Index:     indexName,
			CreatedAt: createdAt,
			Meta:      &options,
		})
	if err != nil {
		return nil, errors.Wrap(err, "sending CreateIndex message")
	}
	api.holder.Stats.Count(MetricCreateIndex, 1, 1.0)
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
	api.holder.Stats.Count(MetricDeleteIndex, 1, 1.0)
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
			return nil, NewBadRequestError(errors.Wrap(err, "applying option"))
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
	createdAt := timestamp()
	field.mu.Lock()
	field.createdAt = createdAt
	field.mu.Unlock()

	// Send the create field message to all nodes.
	err = api.server.SendSync(&CreateFieldMessage{
		Index:     indexName,
		Field:     fieldName,
		CreatedAt: createdAt,
		Meta:      &fo,
	})
	if err != nil {
		api.server.logger.Printf("problem sending CreateField message: %s", err)
		return nil, errors.Wrap(err, "sending CreateField message")
	}
	api.holder.Stats.CountWithCustomTags(MetricCreateField, 1, 1.0, []string{fmt.Sprintf("index:%s", indexName)})
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

type importJob struct {
	ctx     context.Context
	tx      Tx
	req     *ImportRoaringRequest
	shard   uint64
	field   *Field
	errChan chan error
}

func importWorker(importWork chan importJob) {
	for j := range importWork {
		err := func() error {
			for viewName, viewData := range j.req.Views {
				// The logic here corresponds to the logic in fragment.cleanViewName().
				// Unfortunately, the logic in that method is not completely exclusive
				// (i.e. an "other" view named with format YYYYMMDD would be handled
				// incorrectly). One way to address this would be to change the logic
				// overall so there weren't conflicts. For now, we just
				// rely on the field type to inform the intended view name.
				if viewName == "" {
					viewName = viewStandard
				} else if j.field.Type() == FieldTypeTime {
					viewName = fmt.Sprintf("%s_%s", viewStandard, viewName)
				}
				if len(viewData) == 0 {
					return fmt.Errorf("no data to import for view: %s", viewName)
				}

				// TODO: deprecate ImportRoaringRequest.Clear, but
				// until we do, we need to check its value to provide
				// backward compatibility.
				doAction := j.req.Action
				if doAction == "" {
					if j.req.Clear {
						doAction = RequestActionClear
					} else {
						doAction = RequestActionSet
					}
				}

				var doClear bool
				switch doAction {
				case RequestActionOverwrite:
					if err := j.field.importRoaringOverwrite(j.ctx, j.tx, viewData, j.shard, viewName, j.req.Block); err != nil {
						return errors.Wrap(err, "importing roaring as overwrite")
					}
				case RequestActionClear:
					doClear = true
					fallthrough
				case RequestActionSet:
					fileMagic := uint32(binary.LittleEndian.Uint16(viewData[0:2]))
					if fileMagic == roaring.MagicNumber { // if pilosa roaring format
						if err := j.field.importRoaring(j.ctx, j.tx, viewData, j.shard, viewName, doClear); err != nil {
							return errors.Wrap(err, "importing pilosa roaring")
						}
					} else {
						// must make a copy of data to operate on locally on standard roaring format.
						// field.importRoaring changes the standard roaring run format to pilosa roaring
						data := make([]byte, len(viewData))
						copy(data, viewData)
						if err := j.field.importRoaring(j.ctx, j.tx, data, j.shard, viewName, doClear); err != nil {
							return errors.Wrap(err, "importing standard roaring")
						}
					}
				}
			}
			return nil
		}()

		select {
		case <-j.ctx.Done():
		case j.errChan <- err:
		}
	}
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

	index, field, err := api.indexField(indexName, fieldName, shard)
	if index == nil || field == nil {
		return newNotFoundError(ErrFieldNotFound)
	}

	if err = req.ValidateWithTimestamp(index.CreatedAt(), field.CreatedAt()); err != nil {
		return newPreconditionFailedError(err)
	}

	//  Obtain transaction.
	tx := index.Txf.NewTx(Txo{Write: true, Index: index})
	defer tx.Rollback()

	nodes := api.cluster.shardNodes(indexName, shard)
	errCh := make(chan error, len(nodes))
	for _, node := range nodes {
		node := node
		if node.ID == api.server.nodeID {
			api.importWork <- importJob{
				ctx:     ctx,
				tx:      tx,
				req:     req,
				shard:   shard,
				field:   field,
				errChan: errCh,
			}
		} else if !remote { // if remote == true we don't forward to other nodes
			// forward it on
			go func() {
				errCh <- api.server.defaultClient.ImportRoaring(ctx, &node.URI, indexName, fieldName, shard, true, req)
			}()
		} else {
			errCh <- nil
		}
	}

	var maxNode int
	for {
		select {
		case <-ctx.Done():
			// defered tx.Rollback() happens automatically here.
			return ctx.Err()
		case nodeErr := <-errCh:
			if nodeErr != nil {
				// defered tx.Rollback() happens automatically here.
				return nodeErr
			}
			maxNode++
		}

		// Exit once all nodes are processed.
		if maxNode == len(nodes) {
			return tx.Commit()
		}
	}
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
	api.holder.Stats.CountWithCustomTags(MetricDeleteField, 1, 1.0, []string{fmt.Sprintf("index:%s", indexName)})
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
	api.holder.Stats.CountWithCustomTags(MetricDeleteAvailableShard, 1, 1.0, []string{fmt.Sprintf("index:%s", indexName)})
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

	// Obtain transaction
	tx := index.Txf.NewTx(Txo{Write: !writable, Index: index})
	defer tx.Rollback()

	// Wrap writer with a CSV writer.
	cw := csv.NewWriter(w)

	// Define the function to write each bit as a string,
	// translating to keys where necessary.
	var n int
	fn := func(rowID, columnID uint64) error {
		var rowStr string
		var colStr string
		var err error

		if field.Keys() {
			// TODO: handle case: field.ForeignIndex
			if rowStr, err = field.TranslateStore().TranslateID(rowID); err != nil {
				return errors.Wrap(err, "translating row")
			}
		} else {
			rowStr = strconv.FormatUint(rowID, 10)
		}

		if index.Keys() {
			if store := index.TranslateStore(api.cluster.idPartition(indexName, columnID)); store == nil {
				return errors.Wrap(err, "partition does not exist")
			} else if colStr, err = store.TranslateID(columnID); err != nil {
				return errors.Wrap(err, "translating column")
			}
		} else {
			colStr = strconv.FormatUint(columnID, 10)
		}

		n++
		return cw.Write([]string{rowStr, colStr})
	}

	// Iterate over each column.
	if err := f.forEachBit(tx, fn); err != nil {
		return errors.Wrap(err, "writing CSV")
	}

	// Ensure data is flushed.
	cw.Flush()
	span.LogKV("n", n)
	return tx.Commit()
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
func (api *API) FragmentBlockData(ctx context.Context, body io.Reader) (_ []byte, err error) {
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
	resp.RowIDs, resp.ColumnIDs, err = f.blockData(int(req.Block))
	if err != nil {
		return nil, err
	}

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
	return f.Blocks()
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

// TranslateData returns all translation data in the specified partition.
func (api *API) TranslateData(ctx context.Context, indexName string, partition int) (io.WriterTo, error) {
	span, _ := tracing.StartSpanFromContext(ctx, "API.TranslateData")
	defer span.Finish()

	if err := api.validate(apiTranslateData); err != nil {
		return nil, errors.Wrap(err, "validating api method")
	}

	// Retrieve index from holder.
	idx := api.holder.Index(indexName)
	if idx == nil {
		return nil, ErrIndexNotFound
	}

	// Retrieve translatestore from holder.
	store := idx.TranslateStore(partition)
	if store == nil {
		return nil, ErrTranslateStoreNotFound
	}

	return store, nil
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

	// Forward the message.
	if err := api.server.receiveMessage(msg); err != nil {
		return MessageProcessingError{err}
	}
	return nil
}

// MessageProcessingError is an error indicating that a cluster message could not be processed.
type MessageProcessingError struct {
	Err error
}

func (err MessageProcessingError) Error() string {
	return "processing message: " + err.Err.Error()
}

// Cause allows the error to be unwrapped.
func (err MessageProcessingError) Cause() error {
	return err.Err
}

// Unwrap allows the error to be unwrapped.
func (err MessageProcessingError) Unwrap() error {
	return err.Err
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

	// set CreatedAt for indexes and fields (if empty), and then apply schema.
	for _, index := range s.Indexes {
		if index.CreatedAt == 0 {
			index.CreatedAt = timestamp()
		}
		for _, field := range index.Fields {
			if field.CreatedAt == 0 {
				field.CreatedAt = timestamp()
			}
		}
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

	return errors.Wrap(api.holder.applySchema(s), "applying schema")
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

// ImportOptions holds the options for the API.Import
// method.
//
// TODO(2.0) we have entirely missed the point of functional options
// by exporting this structure. If it needs to be exported for some
// reason, we should consider not using functional options here which
// just adds complexity.
type ImportOptions struct {
	Clear          bool
	IgnoreKeyCheck bool
	Presorted      bool

	// test Tx atomicity if > 0
	SimPowerLossAfter int
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

func OptImportOptionsPresorted(b bool) ImportOption {
	return func(o *ImportOptions) error {
		o.Presorted = b
		return nil
	}
}

var ErrAborted = fmt.Errorf("error: update was aborted")

func (api *API) ImportAtomicRecord(ctx context.Context, req *AtomicRecord, opts ...ImportOption) error {
	simPowerLoss := false
	lossAfter := -1
	var opt ImportOptions
	for _, setter := range opts {
		if setter != nil {
			err := setter(&opt)
			if err != nil {
				return errors.Wrap(err, "ImportAtomicRecord ImportOptions")
			}
		}
	}
	if opt.SimPowerLossAfter > 0 {
		simPowerLoss = true
		lossAfter = opt.SimPowerLossAfter
	}

	idx, err := api.Index(ctx, req.Index)
	if err != nil {
		return errors.Wrap(err, "getting index")
	}

	// the whole point is to run this part of the import atomically.
	// So make a Tx.
	tx := idx.Txf.NewTx(Txo{Write: writable, Index: idx})
	defer tx.Rollback()

	tot := 0

	// BSIs (Values)
	for _, ivr := range req.Ivr {
		tot++
		if simPowerLoss && tot > lossAfter {
			return ErrAborted
		}
		opts0 := append(opts, OptImportOptionsClear(ivr.Clear))
		err := api.ImportValueWithTx(ctx, tx, ivr, opts0...)
		if err != nil {
			return errors.Wrap(err, "ImportAtomicRecord ImportValueWithTx")
		}
	}

	// other bits, non-BSI
	for _, ir := range req.Ir {
		tot++
		if simPowerLoss && tot > lossAfter {
			return ErrAborted
		}
		opts0 := append(opts, OptImportOptionsClear(ir.Clear))
		err := api.ImportWithTx(ctx, tx, ir, opts0...)
		if err != nil {
			return errors.Wrap(err, "ImportAtomicRecord ImportWithTx")
		}
	}
	return tx.Commit()
}

func addClearToImportOptions(opts []ImportOption) []ImportOption {
	var opt ImportOptions
	for _, o := range opts {
		// check for side-effect of setting io.Clear; that is
		// how we know it is present.
		_ = o(&opt)
		if opt.Clear {
			// we already have the clear flag set, so nothing more to do.
			return opts
		}
	}
	// no clear flag being set, add that option now.
	return append(opts, OptImportOptionsClear(true))
}

func (api *API) Import(ctx context.Context, req *ImportRequest, opts ...ImportOption) error {
	if req.Clear {
		opts = addClearToImportOptions(opts)
	}
	return api.ImportWithTx(ctx, nil, req, opts...)
}

// Import bulk imports data into a particular index,field,shard.
func (api *API) ImportWithTx(ctx context.Context, tx Tx, req *ImportRequest, opts ...ImportOption) error {
	span, _ := tracing.StartSpanFromContext(ctx, "API.Import")
	defer span.Finish()

	if err := api.validate(apiImport); err != nil {
		return errors.Wrap(err, "validating api method")
	}

	index, field, err := api.indexField(req.Index, req.Field, req.Shard)
	if err != nil {
		return errors.Wrap(err, "getting index and field")
	}

	if err := req.ValidateWithTimestamp(index.CreatedAt(), field.CreatedAt()); err != nil {
		return errors.Wrap(err, "validating import value request")
	}

	// Set up import options.
	options, err := setUpImportOptions(opts...)
	if err != nil {
		return errors.Wrap(err, "setting up import options")
	}
	span.LogKV(
		"index", req.Index,
		"field", req.Field)

	// Unless explicitly ignoring key validation (meaning keys have been
	// translated to ids in a previous step at the coordinator node), then
	// check to see if keys need translation.
	if !options.IgnoreKeyCheck {
		// Translate row keys.
		if field.Keys() {
			span.LogKV("rowKeys", true)
			if len(req.RowIDs) != 0 {
				return errors.New("row ids cannot be used because field uses string keys")
			}
			if req.RowIDs, err = api.cluster.translateFieldKeys(ctx, field, req.RowKeys...); err != nil {
				return errors.Wrapf(err, "translating field keys")
			}
		}

		// Translate column keys.
		if index.Keys() {
			span.LogKV("columnKeys", true)
			if len(req.ColumnIDs) != 0 {
				return errors.New("column ids cannot be used because index uses string keys")
			}
			if req.ColumnIDs, err = api.cluster.translateIndexKeys(ctx, req.Index, req.ColumnKeys); err != nil {
				return errors.Wrap(err, "translating columns")
			}
		}

		// For translated data, map the columnIDs to shards. If
		// this node does not own the shard, forward to the node that does.
		if index.Keys() || field.Keys() {
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

	isLocalTx := false
	if tx == nil {
		isLocalTx = true
		tx = index.Txf.NewTx(Txo{Write: true, Index: index})
		defer tx.Rollback()
	}

	// Import columnIDs into existence field.
	if !options.Clear {
		if err := importExistenceColumns(tx, index, req.ColumnIDs); err != nil {
			api.server.logger.Printf("import existence error: index=%s, field=%s, shard=%d, columns=%d, err=%s", req.Index, req.Field, req.Shard, len(req.ColumnIDs), err)
			return err
		}
		if err != nil {
			return errors.Wrap(err, "importing existence columns")
		}
	}

	// Import into fragment.
	err = field.Import(tx, req.RowIDs, req.ColumnIDs, timestamps, opts...)
	if err != nil {
		api.server.logger.Printf("import error: index=%s, field=%s, shard=%d, columns=%d, err=%s", req.Index, req.Field, req.Shard, len(req.ColumnIDs), err)
		return errors.Wrap(err, "importing")
	}

	if isLocalTx {
		err = tx.Commit()
	}
	return errors.Wrap(err, "committing")
}

func (api *API) ImportValue(ctx context.Context, req *ImportValueRequest, opts ...ImportOption) error {
	if req.Clear {
		opts = addClearToImportOptions(opts)
	}
	return api.ImportValueWithTx(ctx, nil, req, opts...)
}

// ImportValue bulk imports values into a particular field.
func (api *API) ImportValueWithTx(ctx context.Context, tx Tx, req *ImportValueRequest, opts ...ImportOption) error {
	span, _ := tracing.StartSpanFromContext(ctx, "API.ImportValue")
	defer span.Finish()

	if err := api.validate(apiImportValue); err != nil {
		return errors.Wrap(err, "validating api method")
	}

	index, field, err := api.indexField(req.Index, req.Field, req.Shard)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("getting index '%v' and field '%v'; shard=%v", req.Index, req.Field, req.Shard))
	}

	if err := req.ValidateWithTimestamp(index.CreatedAt(), field.CreatedAt()); err != nil {
		return errors.Wrap(err, "validating import value request")
	}

	// Set up import options.
	options, err := setUpImportOptions(opts...)
	if err != nil {
		return errors.Wrap(err, "setting up import options")
	}

	index, field, err = api.indexField(req.Index, req.Field, req.Shard)
	if err != nil {
		return errors.Wrap(err, "getting index and field")
	}
	span.LogKV(
		"index", req.Index,
		"field", req.Field)
	// Unless explicitly ignoring key validation (meaning keys have been
	// translate to ids in a previous step at the coordinator node), then
	// check to see if keys need translation.
	if !options.IgnoreKeyCheck {
		// Translate column keys.
		if index.Keys() {
			span.LogKV("columnKeys", true)
			if len(req.ColumnIDs) != 0 {
				return errors.New("column ids cannot be used because index uses string keys")
			}
			if req.ColumnIDs, err = api.cluster.translateIndexKeys(ctx, req.Index, req.ColumnKeys); err != nil {
				return errors.Wrap(err, "translating columns")
			}
			req.Shard = math.MaxUint64
		}

		// Translate values when the field uses keys (for example, when
		// the field has a ForeignIndex with keys).
		if field.Keys() {
			// Perform translation.
			span.LogKV("rowKeys", true)
			uints, err := api.cluster.translateIndexKeys(ctx, field.ForeignIndex(), req.StringValues)
			if err != nil {
				return err
			}

			// Because the BSI field supports negative values, we have to
			// convert the uint64 keys to a slice of int64.
			ints := make([]int64, len(uints))
			for i := range uints {
				ints[i] = int64(uints[i])
			}
			req.Values = ints
		}
	}

	if !options.Presorted {
		sort.Sort(req)
	}

	isLocalTx := false
	// if we're importing into a specific shard
	if req.Shard != math.MaxUint64 {
		// Obtain transaction.
		if tx == nil {
			isLocalTx = true
			tx = index.Txf.NewTx(Txo{Write: true, Index: index})
			defer tx.Rollback()
		}

		// Check that column IDs match the stated shard.
		if s1, s2 := req.ColumnIDs[0]/ShardWidth, req.ColumnIDs[len(req.ColumnIDs)-1]/ShardWidth; s1 != s2 && s2 != req.Shard {
			return errors.Errorf("shard %d specified, but import spans shards %d to %d", req.Shard, s1, s2)
		}
		// Validate shard ownership. TODO - we should forward to the
		// correct node rather than barfing here.
		if err := api.validateShardOwnership(req.Index, req.Shard); err != nil {
			return errors.Wrap(err, "validating shard ownership")
		}
		// Import columnIDs into existence field.
		if !options.Clear {
			if err := importExistenceColumns(tx, index, req.ColumnIDs); err != nil {
				api.server.logger.Printf("import existence error: index=%s, field=%s, shard=%d, columns=%d, err=%s", req.Index, req.Field, req.Shard, len(req.ColumnIDs), err)
				return errors.Wrap(err, "importing existence columns")
			}
		}

		// Import into fragment.
		if len(req.Values) > 0 {
			err = field.importValue(tx, req.ColumnIDs, req.Values, options)
			if err != nil {
				api.server.logger.Printf("import error: index=%s, field=%s, shard=%d, columns=%d, err=%s", req.Index, req.Field, req.Shard, len(req.ColumnIDs), err)
			}
		} else if len(req.FloatValues) > 0 {
			err = field.importFloatValue(tx, req.ColumnIDs, req.FloatValues, options)
			if err != nil {
				api.server.logger.Printf("import error: index=%s, field=%s, shard=%d, columns=%d, err=%s", req.Index, req.Field, req.Shard, len(req.ColumnIDs), err)
			}
		}
		if err == nil && isLocalTx {
			err = tx.Commit()
		}
		return errors.Wrap(err, "importing value")
	}

	options.IgnoreKeyCheck = true
	start := 0
	shard := req.ColumnIDs[0] / ShardWidth
	var eg errgroup.Group // TODO make this a pooled errgroup
	for i, colID := range req.ColumnIDs {
		if colID/ShardWidth != shard {
			subreq := &ImportValueRequest{
				Index:     req.Index,
				Field:     req.Field,
				Shard:     shard,
				ColumnIDs: req.ColumnIDs[start:i],
			}
			if req.Values != nil {
				subreq.Values = req.Values[start:i]
			} else if req.FloatValues != nil {
				subreq.FloatValues = req.FloatValues[start:i]
			}

			eg.Go(func() error {
				return api.server.defaultClient.ImportValue2(ctx, subreq, options)
			})
			start = i
			shard = colID / ShardWidth
		}
	}
	subreq := &ImportValueRequest{
		Index:     req.Index,
		Field:     req.Field,
		Shard:     shard,
		ColumnIDs: req.ColumnIDs[start:],
	}
	if req.Values != nil {
		subreq.Values = req.Values[start:]
	} else if req.FloatValues != nil {
		subreq.FloatValues = req.FloatValues[start:]
	}
	eg.Go(func() error {
		// TODO we should elevate the logic for figuring out which
		// node(s) to send to into API instead of having those details
		// in the client implementation.
		return api.server.defaultClient.ImportValue2(ctx, subreq, options)
	})
	return eg.Wait()

}

func (api *API) ImportColumnAttrs(ctx context.Context, req *ImportColumnAttrsRequest, opts ...ImportOption) error {
	span, _ := tracing.StartSpanFromContext(ctx, "API.ImportColumnAttrs")
	defer span.Finish()

	index, err := api.Index(ctx, req.Index)
	if err != nil {
		return errors.Wrap(err, "getting index")
	}

	if err := api.validateShardOwnership(req.Index, uint64(req.Shard)); err != nil {
		return errors.Wrap(err, "validating shard ownership")
	}

	if req.IndexCreatedAt != 0 {
		if index.CreatedAt() != req.IndexCreatedAt {
			return ErrPreconditionFailed
		}
	}

	bulkAttrs := make(map[uint64]map[string]interface{})
	for n := 0; n < len(req.ColumnIDs); n++ {
		bulkAttrs[uint64(req.ColumnIDs[n])] = map[string]interface{}{req.AttrKey: req.AttrVals[n]}
	}
	if err := index.ColumnAttrStore().SetBulkAttrs(bulkAttrs); err != nil {
		api.server.logger.Printf("import error: index=%s, shard=%d, len(columns)=%d, err=%s", req.Index, req.Shard, len(req.ColumnIDs), err)
		return errors.Wrap(err, "importing column attrs")
	}
	return nil
}

func importExistenceColumns(tx Tx, index *Index, columnIDs []uint64) error {
	ef := index.existenceField()
	if ef == nil {
		return nil
	}

	existenceRowIDs := make([]uint64, len(columnIDs))
	return ef.Import(tx, existenceRowIDs, columnIDs, nil)
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

func (api *API) Inspect(ctx context.Context, req *InspectRequest) (*HolderInfo, error) {
	return api.holder.Inspect(ctx, req)
}

// GetTranslateEntryReader provides an entry reader for key translation logs starting at offset.
func (api *API) GetTranslateEntryReader(ctx context.Context, offsets TranslateOffsetMap) (_ TranslateEntryReader, err error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "API.GetTranslateEntryReader")
	defer span.Finish()

	// Ensure all readers are cleaned up if any error.
	var a []TranslateEntryReader
	defer func() {
		if err != nil {
			for i := range a {
				a[i].Close()
			}
		}
	}()

	// Fetch all index partition readers.
	for indexName, indexMap := range offsets {
		index := api.holder.Index(indexName)
		if index == nil {
			return nil, ErrIndexNotFound
		}

		for partitionID, offset := range indexMap.Partitions {
			store := index.TranslateStore(partitionID)
			if store == nil {
				return nil, ErrTranslateStoreNotFound
			}

			r, err := store.EntryReader(ctx, uint64(offset))
			if err != nil {
				return nil, errors.Wrap(err, "index partition translate reader")
			}
			a = append(a, r)
		}
	}

	// Fetch all field readers.
	for indexName, indexMap := range offsets {
		index := api.holder.Index(indexName)
		if index == nil {
			return nil, ErrIndexNotFound
		}

		for fieldName, offset := range indexMap.Fields {
			field := index.Field(fieldName)
			if field == nil {
				return nil, ErrFieldNotFound
			}

			r, err := field.TranslateStore().EntryReader(ctx, uint64(offset))
			if err != nil {
				return nil, errors.Wrap(err, "field translate reader")
			}
			a = append(a, r)
		}
	}

	return NewMultiTranslateEntryReader(ctx, a), nil
}

func (api *API) TranslateIndexKey(ctx context.Context, indexName string, key string) (uint64, error) {
	return api.cluster.translateIndexKey(ctx, indexName, key)
}

func (api *API) TranslateIndexIDs(ctx context.Context, indexName string, ids []uint64) ([]string, error) {
	return api.cluster.translateIndexIDs(ctx, indexName, ids)
}

// TranslateKeys handles a TranslateKeyRequest.
func (api *API) TranslateKeys(ctx context.Context, r io.Reader) (_ []byte, err error) {
	var req TranslateKeysRequest
	if buf, err := ioutil.ReadAll(r); err != nil {
		return nil, NewBadRequestError(errors.Wrap(err, "read translate keys request error"))
	} else if err := api.Serializer.Unmarshal(buf, &req); err != nil {
		return nil, NewBadRequestError(errors.Wrap(err, "unmarshal translate keys request error"))
	}

	// Lookup store for either index or field and translate keys.
	var ids []uint64
	if req.Field == "" {
		if ids, err = api.cluster.translateIndexKeys(ctx, req.Index, req.Keys); err != nil {
			return nil, err
		}
	} else {
		if field := api.holder.Field(req.Index, req.Field); field == nil {
			return nil, ErrFieldNotFound
		} else if fi := field.ForeignIndex(); fi != "" {
			ids, err = api.cluster.translateIndexKeys(ctx, fi, req.Keys)
			if err != nil {
				return nil, err
			}
		} else if ids, err = api.cluster.translateFieldKeys(ctx, field, req.Keys...); err != nil {
			return nil, errors.Wrapf(err, "translating field keys")
		}
	}

	// Encode response.
	buf, err := api.Serializer.Marshal(&TranslateKeysResponse{IDs: ids})
	if err != nil {
		return nil, errors.Wrap(err, "translate keys response encoding error")
	}
	return buf, nil
}

// TranslateIDs handles a TranslateIDRequest.
func (api *API) TranslateIDs(ctx context.Context, r io.Reader) (_ []byte, err error) {
	var req TranslateIDsRequest
	if buf, err := ioutil.ReadAll(r); err != nil {
		return nil, NewBadRequestError(errors.Wrap(err, "read translate ids request error"))
	} else if err := api.Serializer.Unmarshal(buf, &req); err != nil {
		return nil, NewBadRequestError(errors.Wrap(err, "unmarshal translate ids request error"))
	}

	// Lookup store for either index or field and translate ids.
	var keys []string
	if req.Field == "" {
		if keys, err = api.cluster.translateIndexIDs(ctx, req.Index, req.IDs); err != nil {
			return nil, err
		}
	} else {
		if field := api.holder.Field(req.Index, req.Field); field == nil {
			return nil, ErrFieldNotFound
		} else if fi := field.ForeignIndex(); fi != "" {
			keys, err = api.cluster.translateIndexIDs(ctx, fi, req.IDs)
			if err != nil {
				return nil, err
			}
		} else if keys, err = field.TranslateStore().TranslateIDs(req.IDs); err != nil {
			return nil, err
		}
	}

	// Encode response.
	buf, err := api.Serializer.Marshal(&TranslateIDsResponse{Keys: keys})
	if err != nil {
		return nil, errors.Wrap(err, "translate ids response encoding error")
	}
	return buf, nil
}

// PrimaryReplicaNodeURL returns the URL of the cluster's primary replica.
func (api *API) PrimaryReplicaNodeURL() url.URL {
	node := api.cluster.PrimaryReplicaNode()
	if node == nil {
		return url.URL{}
	}
	return node.URI.URL()
}

func (api *API) StartTransaction(ctx context.Context, id string, timeout time.Duration, exclusive bool, remote bool) (*Transaction, error) {
	if err := api.validate(apiStartTransaction); err != nil {
		return nil, errors.Wrap(err, "validating api method")
	}
	t, err := api.server.StartTransaction(ctx, id, timeout, exclusive, remote)
	if exclusive {
		switch err {
		case nil:
			api.holder.Stats.Count(MetricExclusiveTransactionRequest, 1, 1.0)
		case ErrTransactionExclusive:
			api.holder.Stats.Count(MetricExclusiveTransactionBlocked, 1, 1.0)
		}
		if t.Active {
			api.holder.Stats.Count(MetricExclusiveTransactionActive, 1, 1.0)
		}
	} else {
		switch err {
		case nil:
			api.holder.Stats.Count(MetricTransactionStart, 1, 1.0)
		case ErrTransactionExclusive:
			api.holder.Stats.Count(MetricTransactionBlocked, 1, 1.0)
		}
	}
	return t, err
}

func (api *API) FinishTransaction(ctx context.Context, id string, remote bool) (*Transaction, error) {
	if err := api.validate(apiFinishTransaction); err != nil {
		return nil, errors.Wrap(err, "validating api method")
	}
	t, err := api.server.FinishTransaction(ctx, id, remote)
	if err == nil {
		if t.Exclusive {
			api.holder.Stats.Count(MetricExclusiveTransactionEnd, 1, 1.0)
		} else {
			api.holder.Stats.Count(MetricTransactionEnd, 1, 1.0)
		}
	}
	return t, err
}

func (api *API) Transactions(ctx context.Context) (map[string]*Transaction, error) {
	if err := api.validate(apiTransactions); err != nil {
		return nil, errors.Wrap(err, "validating api method")
	}
	return api.server.Transactions(ctx)
}

func (api *API) GetTransaction(ctx context.Context, id string, remote bool) (*Transaction, error) {
	if err := api.validate(apiGetTransaction); err != nil {
		return nil, errors.Wrap(err, "validating api method")
	}
	t, err := api.server.GetTransaction(ctx, id, remote)
	if err == nil {
		if t.Exclusive && t.Active {
			api.holder.Stats.Count(MetricExclusiveTransactionActive, 1, 1.0)
		}
	}
	return t, err
}

func (api *API) ActiveQueries(ctx context.Context) ([]ActiveQueryStatus, error) {
	if err := api.validate(apiActiveQueries); err != nil {
		return nil, errors.Wrap(err, "validating api method")
	}
	return api.tracker.ActiveQueries(), nil
}

// TranslateIndexDB is an internal function to load the index keys database
func (api *API) TranslateIndexDB(ctx context.Context, indexName string, partitionID int, rd io.Reader) error {
	idx := api.holder.Index(indexName)
	store := idx.TranslateStore(partitionID)
	_, err := store.ReadFrom(rd)
	return err
}

// TranslateFieldDB is an internal function to load the field keys database
func (api *API) TranslateFieldDB(ctx context.Context, indexName, fieldName string, rd io.Reader) error {
	idx := api.holder.Index(indexName)
	field := idx.Field(fieldName)
	store := field.TranslateStore()
	_, err := store.ReadFrom(rd)
	return err
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
	apiTranslateData
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
	apiStartTransaction
	apiFinishTransaction
	apiTransactions
	apiGetTransaction
	apiActiveQueries
)

var methodsCommon = map[apiMethod]struct{}{
	apiClusterMessage: {},
	apiSetCoordinator: {},
}

var methodsResizing = map[apiMethod]struct{}{
	apiFragmentData:  {},
	apiTranslateData: {},
	apiResizeAbort:   {},
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
	apiStartTransaction:     {},
	apiFinishTransaction:    {},
	apiTransactions:         {},
	apiGetTransaction:       {},
	apiActiveQueries:        {},
}
