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
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/csv"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pilosa/pilosa/v2/disco"
	"github.com/pilosa/pilosa/v2/pql"
	"github.com/pilosa/pilosa/v2/roaring"
	"github.com/pilosa/pilosa/v2/stats"
	"github.com/pilosa/pilosa/v2/topology"
	"github.com/pilosa/pilosa/v2/tracing"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

// API provides the top level programmatic interface to Pilosa. It is usually
// wrapped by a handler which provides an external interface (e.g. HTTP).
type API struct {
	mu     sync.Mutex
	closed bool // protected by mu

	holder  *Holder
	cluster *cluster
	server  *Server
	tracker *queryTracker

	importWorkersWG      sync.WaitGroup
	importWorkerPoolSize int
	importWork           chan importJob

	usageCache *usageCache

	Serializer Serializer
}

func (api *API) Holder() *Holder {
	return api.holder
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

	api.tracker = newQueryTracker(api.server.queryHistoryLength)

	return api, nil
}

// validAPIMethods specifies the api methods that are valid for each
// cluster state.
var validAPIMethods = map[disco.ClusterState]map[apiMethod]struct{}{
	disco.ClusterStateStarting: methodsCommon,
	disco.ClusterStateNormal:   appendMap(methodsCommon, methodsNormal),
	disco.ClusterStateDegraded: appendMap(methodsCommon, methodsDegraded),
	disco.ClusterStateResizing: appendMap(methodsCommon, methodsResizing),
	disco.ClusterStateDown:     methodsCommon,
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
	state, err := api.cluster.State()
	if err != nil {
		return errors.Wrap(err, "getting cluster state")
	}
	if _, ok := validAPIMethods[state][f]; ok {
		return nil
	}
	return newAPIMethodNotAllowedError(errors.Errorf("api method %s not allowed in state %s", f, state))
}

// Close closes the api and waits for it to shutdown.
func (api *API) Close() error {
	// only close once
	api.mu.Lock()
	defer api.mu.Unlock()
	if api.closed {
		return nil
	}
	api.closed = true

	close(api.importWork)
	api.importWorkersWG.Wait()
	api.tracker.Stop()
	return nil
}

func (api *API) Txf() *TxFactory {
	return api.holder.Txf()
}

// Query parses a PQL query out of the request and executes it.
func (api *API) Query(ctx context.Context, req *QueryRequest) (QueryResponse, error) {
	start := time.Now()
	span, ctx := tracing.StartSpanFromContext(ctx, "API.Query")
	defer span.Finish()

	if err := api.validate(apiQuery); err != nil {
		return QueryResponse{}, errors.Wrap(err, "validating api method")
	}

	if !req.Remote {
		defer api.tracker.Finish(api.tracker.Start(req.Query, req.SQLQuery, api.server.nodeID, req.Index, start))
	}

	return api.query(ctx, req)
}

// query provides query functionality for internal use, without tracing, validation, or tracking
func (api *API) query(ctx context.Context, req *QueryRequest) (QueryResponse, error) {
	q, err := pql.NewParser(strings.NewReader(req.Query)).Parse()
	if err != nil {
		return QueryResponse{}, errors.Wrap(err, "parsing")
	}

	// TODO can we get rid of exec options and pass the QueryRequest directly to executor?
	execOpts := &execOptions{
		Remote:        req.Remote,
		Profile:       req.Profile,
		PreTranslated: req.PreTranslated,
		EmbeddedData:  req.EmbeddedData, // precomputed values that needed to be passed with the request
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

	// Populate the create index message.
	cim := &CreateIndexMessage{
		Index:     indexName,
		CreatedAt: timestamp(),
		Meta:      options,
	}

	// Create index.
	index, err := api.holder.CreateIndexAndBroadcast(cim)
	if err != nil {
		return nil, errors.Wrap(err, "creating index")
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
		return nil, newNotFoundError(ErrIndexNotFound, indexName)
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
		api.server.logger.Errorf("problem sending DeleteIndex message: %s", err)
		return errors.Wrap(err, "sending DeleteIndex message")
	}
	// Delete ids allocated for index if any present
	snap := topology.NewClusterSnapshot(api.cluster.noder, api.cluster.Hasher, api.cluster.ReplicaN)
	if snap.IsPrimaryFieldTranslationNode(api.NodeID()) {
		if err := api.holder.ida.reset(indexName); err != nil {
			return errors.Wrap(err, "deleting id allocation for index")
		}
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

	// Apply and validate functional options.
	fo, err := newFieldOptions(opts...)
	if err != nil {
		return nil, NewBadRequestError(errors.Wrap(err, "applying option"))
	}

	// Find index.
	index := api.holder.Index(indexName)
	if index == nil {
		return nil, newNotFoundError(ErrIndexNotFound, indexName)
	}

	// Populate the create field message.
	cfm := &CreateFieldMessage{
		Index:     indexName,
		Field:     fieldName,
		CreatedAt: timestamp(),
		Meta:      fo,
	}

	// Create field.
	field, err := index.CreateFieldAndBroadcast(cfm)
	if err != nil {
		return nil, errors.Wrap(err, "creating field")
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
		return nil, newNotFoundError(ErrFieldNotFound, fieldName)
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
	qcx     *Qcx
	req     *ImportRoaringRequest
	shard   uint64
	field   *Field
	errChan chan error
}

func importWorker(importWork chan importJob) {
	for j := range importWork {
		err := func() (err0 error) {
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

				if err := func() (err1 error) {
					tx, finisher, err := j.qcx.GetTx(Txo{Write: writable, Index: j.field.idx, Shard: j.shard})
					if err != nil {
						return err
					}
					defer finisher(&err1)

					var doClear bool
					switch doAction {
					case RequestActionOverwrite:
						err := j.field.importRoaringOverwrite(j.ctx, tx, viewData, j.shard, viewName, j.req.Block)
						if err != nil {
							return errors.Wrap(err, "importing roaring as overwrite")
						}
					case RequestActionClear:
						doClear = true
						fallthrough
					case RequestActionSet:
						fileMagic := uint32(binary.LittleEndian.Uint16(viewData[0:2]))
						data := viewData
						if fileMagic != roaring.MagicNumber {
							// if the view data arrives is in the "standard" roaring format, we must
							// make a copy of data in order allow for the conversion to the pilosa roaring run format
							// in field.importRoaring
							data = make([]byte, len(viewData))
							copy(data, viewData)
						}
						if j.req.UpdateExistence {
							if ef := j.field.idx.existenceField(); ef != nil {
								existence, err := combineForExistence(data)
								if err != nil {
									return errors.Wrap(err, "merging existence on roaring import")
								}

								err = ef.importRoaring(j.ctx, tx, existence, j.shard, "standard", false)
								if err != nil {
									return errors.Wrap(err, "updating existence on roaring import")
								}
							}
						}

						err := j.field.importRoaring(j.ctx, tx, data, j.shard, viewName, doClear)

						if err != nil {
							return errors.Wrap(err, "importing standard roaring")
						}
					}
					return nil
				}(); err != nil {
					return err
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

// combineForExistence unions all rows in the fragment to be imported into a single row to update the existence field. TODO: It would probably be more efficient to only unmarshal the input data once, and use the calculated existence Bitmap directly rather than returning it to bytes, but most of our ingest paths update existence separately, so it's more important that this just be obviously correct at the moment.
func combineForExistence(inputRoaringData []byte) ([]byte, error) {
	rowSize := uint64(1 << shardVsContainerExponent)
	rit, err := roaring.NewRoaringIterator(inputRoaringData)
	if err != nil {
		return nil, err
	}
	bm := roaring.NewBitmap()
	err = bm.MergeRoaringRawIteratorIntoExists(rit, rowSize)
	if err != nil {
		return nil, err
	}
	buf := new(bytes.Buffer)

	_, err = bm.WriteTo(buf)
	return buf.Bytes(), err
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
func (api *API) ImportRoaring(ctx context.Context, indexName, fieldName string, shard uint64, remote bool, req *ImportRoaringRequest) (err0 error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "API.ImportRoaring")
	span.LogKV("index", indexName, "field", fieldName)
	defer span.Finish()

	if err := api.validate(apiField); err != nil {
		return errors.Wrap(err, "validating api method")
	}

	index, field, err := api.indexField(indexName, fieldName, shard)
	if index == nil || field == nil {
		return err
	}

	if err = req.ValidateWithTimestamp(index.CreatedAt(), field.CreatedAt()); err != nil {
		return newPreconditionFailedError(err)
	}

	qcx := api.Txf().NewQcx()
	defer qcx.Abort()

	// Create a snapshot of the cluster to use for node/partition calculations.
	snap := topology.NewClusterSnapshot(api.cluster.noder, api.cluster.Hasher, api.cluster.ReplicaN)

	nodes := snap.ShardNodes(indexName, shard)
	errCh := make(chan error, len(nodes))
	for _, node := range nodes {
		node := node
		if node.ID == api.server.nodeID {
			api.importWork <- importJob{
				ctx:     ctx,
				qcx:     qcx,
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
			return qcx.Finish()
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
		return newNotFoundError(ErrIndexNotFound, indexName)
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
		api.server.logger.Errorf("problem sending DeleteField message: %s", err)
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
		return newNotFoundError(ErrFieldNotFound, fieldName)
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
		api.server.logger.Errorf("problem sending DeleteAvailableShard message: %s", err)
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

	// Create a snapshot of the cluster to use for node/partition calculations.
	snap := topology.NewClusterSnapshot(api.cluster.noder, api.cluster.Hasher, api.cluster.ReplicaN)

	// Validate that this handler owns the shard.
	if !snap.OwnsShard(api.NodeID(), indexName, shard) {
		api.server.logger.Errorf("node %s does not own shard %d of index %s", api.NodeID(), shard, indexName)
		return ErrClusterDoesNotOwnShard
	}

	// Find index.
	index := api.holder.Index(indexName)
	if index == nil {
		return newNotFoundError(ErrIndexNotFound, indexName)
	}

	// Find field from the index.
	field := index.Field(fieldName)
	if field == nil {
		return newNotFoundError(ErrFieldNotFound, fieldName)
	}

	// Find the fragment.
	f := api.holder.fragment(indexName, fieldName, viewStandard, shard)
	if f == nil {
		return ErrFragmentNotFound
	}

	// Obtain transaction
	tx := index.holder.txf.NewTx(Txo{Write: !writable, Index: index, Shard: shard})
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
			if store := index.TranslateStore(snap.IDToShardPartition(indexName, columnID)); store == nil {
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
	tx.Rollback()
	return nil
}

// ShardNodes returns the node and all replicas which should contain a shard's data.
func (api *API) ShardNodes(ctx context.Context, indexName string, shard uint64) ([]*topology.Node, error) {
	span, _ := tracing.StartSpanFromContext(ctx, "API.ShardNodes")
	defer span.Finish()

	if err := api.validate(apiShardNodes); err != nil {
		return nil, errors.Wrap(err, "validating api method")
	}

	// Create a snapshot of the cluster to use for node/partition calculations.
	snap := topology.NewClusterSnapshot(api.cluster.noder, api.cluster.Hasher, api.cluster.ReplicaN)

	return snap.ShardNodes(indexName, shard), nil
}

// PartitionNodes returns the node and all replicas which should contain a partition key data.
func (api *API) PartitionNodes(ctx context.Context, partitionID int) ([]*topology.Node, error) {
	span, _ := tracing.StartSpanFromContext(ctx, "API.PartitionNodes")
	defer span.Finish()

	if err := api.validate(apiPartitionNodes); err != nil {
		return nil, errors.Wrap(err, "validating api method")
	}

	// Create a snapshot of the cluster to use for node/partition calculations.
	snap := topology.NewClusterSnapshot(api.cluster.noder, api.cluster.Hasher, api.cluster.ReplicaN)

	return snap.PartitionNodes(partitionID), nil
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
		return nil, newNotFoundError(ErrIndexNotFound, indexName)
	}

	// Retrieve translatestore from holder.
	store := idx.TranslateStore(partition)
	if store == nil {
		return nil, ErrTranslateStoreNotFound
	}

	return store, nil
}

// FieldTranslateData returns all translation data in the specified field.
func (api *API) FieldTranslateData(ctx context.Context, indexName, fieldName string) (io.WriterTo, error) {
	span, _ := tracing.StartSpanFromContext(ctx, "API.FieldTranslateData")
	defer span.Finish()
	if err := api.validate(apiFieldTranslateData); err != nil {
		return nil, errors.Wrap(err, "validating api method")
	}

	// Retrieve index from holder.
	idx := api.holder.Index(indexName)
	if idx == nil {
		return nil, newNotFoundError(ErrIndexNotFound, indexName)
	}

	// Retrieve field from index.
	field := idx.Field(fieldName)
	if field == nil {
		return nil, newNotFoundError(ErrFieldNotFound, fieldName)
	}

	// Retrieve translatestore from holder.
	store := field.TranslateStore()
	if store == nil {
		return nil, ErrTranslateStoreNotFound
	}
	return store, nil
}

// Hosts returns a list of the hosts in the cluster including their ID,
// URL, and which is the primary.
func (api *API) Hosts(ctx context.Context) []*topology.Node {
	span, _ := tracing.StartSpanFromContext(ctx, "API.Hosts")
	defer span.Finish()
	return api.cluster.Nodes()
}

// Node gets the ID, URI and primary status for this particular node.
func (api *API) Node() *topology.Node {
	return api.server.node()
}

// NodeID gets the ID alone, so it doesn't have to do a complete lookup
// of the node, searching by its ID, to return the ID it searched for.
func (api *API) NodeID() string {
	return api.server.nodeID
}

// PrimaryNode returns the primary node for the cluster.
func (api *API) PrimaryNode() *topology.Node {
	// Create a snapshot of the cluster to use for node/partition calculations.
	snap := topology.NewClusterSnapshot(api.cluster.noder, api.cluster.Hasher, api.cluster.ReplicaN)
	return snap.PrimaryFieldTranslationNode()
}

// Cache of disk usage statistics
type usageCache struct {
	data             map[string]NodeUsage
	refreshInterval  time.Duration
	lastUpdated      time.Time
	resetTrigger     chan bool
	lastCalcDuration time.Duration
	waitMultiplier   time.Duration

	muCalculate sync.Mutex
	muAssign    sync.Mutex
}

// NodeUsage represents all usage measurements for one node.
type NodeUsage struct {
	Disk        DiskUsage   `json:"diskUsage"`
	Memory      MemoryUsage `json:"memoryUsage"`
	LastUpdated time.Time   `json:"lastUpdated"`
}

// DiskUsage represents the storage space used on disk by one node.
type DiskUsage struct {
	Capacity   uint64                `json:"capacity,omitempty"`
	TotalUse   uint64                `json:"totalInUse"`
	IndexUsage map[string]IndexUsage `json:"indexes"`
}

// IndexUsage represents the storage space used on disk by one index, on one node.
type IndexUsage struct {
	Total          uint64                `json:"total"`
	IndexKeys      uint64                `json:"indexKeys"`
	FieldKeysTotal uint64                `json:"fieldKeysTotal"`
	Fragments      uint64                `json:"fragments"`
	Metadata       uint64                `json:"metadata"`
	Fields         map[string]FieldUsage `json:"fields"`
}

// FieldUsage represents the storage space used on disk by one field, on one node
type FieldUsage struct {
	Total     uint64 `json:"total"`
	Fragments uint64 `json:"fragments"`
	Keys      uint64 `json:"keys"`
	Metadata  uint64 `json:"metadata"`
}

// MemoryUsage represents the memory used by one node.
type MemoryUsage struct {
	Capacity uint64 `json:"capacity"`
	TotalUse uint64 `json:"totalInUse"`
}

// Returns disk usage from cache if cache is large. It will recalculate on the spot of the last cacluation was under 5 seconds.
func (api *API) Usage(ctx context.Context, remote bool) (map[string]NodeUsage, error) {
	span, _ := tracing.StartSpanFromContext(ctx, "API.Usage")
	defer span.Finish()

	if api.usageCache.lastCalcDuration < (time.Second * 5) {
		err := api.ResetUsageCache()
		if err != nil {
			api.server.logger.Infof("data detected but could not recalculate cache: %s", err)
		}
	}

	api.usageCache.muAssign.Lock()
	lastUpdated := api.usageCache.lastUpdated
	api.usageCache.muAssign.Unlock()
	if lastUpdated == (time.Time{}) {
		api.calculateUsage()
	}

	if !remote {
		api.requestUsageOfNodes()
	}

	return api.usageCache.data, nil
}

// Makes a ui/usage request for each node in cluster to calculates its usage and adds it to the cache
func (api *API) requestUsageOfNodes() {
	nodes := api.cluster.Nodes()
	for _, node := range nodes {
		if node.ID == api.server.nodeID {
			continue
		}

		nodeUsage, err := api.server.defaultClient.GetNodeUsage(context.Background(), &node.URI)
		if err != nil {
			api.server.logger.Infof("couldn't collect disk usage from %s: %s", node.URI, err)
		}

		api.usageCache.muAssign.Lock()
		api.usageCache.data[node.ID] = nodeUsage[node.ID]
		api.usageCache.muAssign.Unlock()
	}
}

// Calculates disk usage from scratch for each index and stores the results in the usage cache
func (api *API) calculateUsage() {
	api.usageCache.muCalculate.Lock()
	defer api.usageCache.muCalculate.Unlock()
	api.server.wg.Add(1)
	defer api.server.wg.Done()

	api.usageCache.muAssign.Lock()
	lastUpdated := api.usageCache.lastUpdated
	api.usageCache.muAssign.Unlock()

	if time.Since(lastUpdated) > api.usageCache.refreshInterval {
		indexDetails, nodeMetadataBytes, err := api.holder.Txf().IndexUsageDetails(api.isClosing)
		if err != nil {
			api.server.logger.Infof("couldn't get index usage details: %s", err)
		}
		if api.isClosing() {
			return
		}

		totalSize := nodeMetadataBytes
		for _, s := range indexDetails {
			totalSize += s.Total
		}

		// NOTE: these errors are ignored in api.Info(), but checked here
		si := api.server.systemInfo
		diskCapacity, err := si.DiskCapacity(api.holder.path)
		if err != nil {
			api.server.logger.Infof("couldn't read disk capacity: %s", err)
		}

		memoryCapacity, err := si.MemTotal()
		if err != nil {
			api.server.logger.Infof("couldn't read memory capacity: %s", err)
		}
		memoryUse, err := si.MemUsed()
		if err != nil {
			api.server.logger.Infof("couldn't read memory usage: %s", err)
		}

		lastUpdated = time.Now()
		// Insert into result.
		nodeUsage := NodeUsage{
			Disk: DiskUsage{
				Capacity:   diskCapacity,
				TotalUse:   totalSize,
				IndexUsage: indexDetails,
			},
			Memory: MemoryUsage{
				Capacity: memoryCapacity,
				TotalUse: memoryUse,
			},
			LastUpdated: lastUpdated,
		}
		api.usageCache.muAssign.Lock()
		api.usageCache.data = make(map[string]NodeUsage)
		api.usageCache.data[api.server.nodeID] = nodeUsage
		api.usageCache.lastUpdated = lastUpdated
		api.usageCache.muAssign.Unlock()
	}
}

// Periodically calculates disk usage
func (api *API) RefreshUsageCache() {
	trigger := make(chan bool)
	defer close(trigger)
	api.usageCache = &usageCache{
		data:             make(map[string]NodeUsage),
		refreshInterval:  time.Hour,
		resetTrigger:     trigger,
		lastCalcDuration: 0,
		waitMultiplier:   time.Duration(5),
	}
	for {
		start := time.Now()
		api.calculateUsage()
		api.setRefreshInterval(time.Since(start))
		select {
		case <-trigger:
			continue
		case <-api.server.closing:
			return
		case <-time.After(api.usageCache.refreshInterval):
			continue
		}
	}
}

func (api *API) setRefreshInterval(dur time.Duration) {
	refresh := dur * api.usageCache.waitMultiplier
	if refresh < time.Hour {
		refresh = time.Hour
	}
	api.usageCache.muAssign.Lock()
	api.usageCache.refreshInterval = refresh
	api.usageCache.lastCalcDuration = dur
	api.usageCache.muAssign.Unlock()
}

// Resets the lastUpdated time and awakens RefreshUsageCache()
func (api *API) ResetUsageCache() error {
	if api.usageCache != nil {
		api.usageCache.muAssign.Lock()
		api.usageCache.lastUpdated = time.Time{}
		api.usageCache.muAssign.Unlock()
	} else {
		return errors.New("invalidating cache: cache not initialized")
	}
	api.usageCache.resetTrigger <- true
	return nil
}

// isClosing returns true if the server is shutting down.
func (api *API) isClosing() bool {
	select {
	case <-api.server.closing:
		return true
	default:
		return false
	}
}

// RecalculateCaches forces all TopN caches to be updated.
// This is done internally within a TopN query, but a user may want to do it ahead of time?
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
func (api *API) Schema(ctx context.Context, withViews bool) ([]*IndexInfo, error) {
	if err := api.validate(apiSchema); err != nil {
		return nil, errors.Wrap(err, "validating api method")
	}

	span, _ := tracing.StartSpanFromContext(ctx, "API.Schema")
	defer span.Finish()

	if withViews {
		return api.holder.Schema()
	}

	return api.holder.limitedSchema()
}

// SchemaDetails returns information about each index in Pilosa including which
// fields they contain, and additional field information such as cardinality
func (api *API) SchemaDetails(ctx context.Context) ([]*IndexInfo, error) {
	span, _ := tracing.StartSpanFromContext(ctx, "API.Schema")
	defer span.Finish()
	schema, err := api.holder.Schema()
	if err != nil {
		return nil, errors.Wrap(err, "getting schema")
	}
	for _, index := range schema {
		for _, field := range index.Fields {
			q := fmt.Sprintf("Count(Distinct(field=%s))", field.Name)
			req := QueryRequest{Index: index.Name, Query: q}
			resp, err := api.query(ctx, &req)
			if err != nil {
				return schema, errors.Wrapf(err, "querying cardinality (%s/%s)", index.Name, field.Name)
			}
			if len(resp.Results) == 0 {
				continue
			}
			if card, ok := resp.Results[0].(uint64); ok {
				field.Cardinality = &card
			}
		}
	}
	return schema, nil
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

	err := api.holder.applySchema(s)
	if err != nil {
		return errors.Wrap(err, "applying schema")
	}

	return nil
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
		return nil, newNotFoundError(ErrFieldNotFound, fieldName)
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
		return newNotFoundError(ErrFieldNotFound, fieldName)
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
		api.server.logger.Errorf("problem sending DeleteView message: %s", err)
	}

	return errors.Wrap(err, "sending DeleteView message")
}

// IndexShardSnapshot returns a reader that contains the contents of an RBF snapshot for an index/shard.
func (api *API) IndexShardSnapshot(ctx context.Context, indexName string, shard uint64) (io.ReadCloser, error) {
	span, _ := tracing.StartSpanFromContext(ctx, "API.IndexShardSnapshot")
	defer span.Finish()

	// Find index.
	index := api.holder.Index(indexName)
	if index == nil {
		return nil, newNotFoundError(ErrIndexNotFound, indexName)
	}

	// Start transaction.
	tx := index.holder.txf.NewTx(Txo{Index: index, Shard: shard})

	// Ensure transaction is an RBF transaction.
	rtx, ok := tx.(*RBFTx)
	if !ok {
		tx.Rollback()
		return nil, fmt.Errorf("snapshot not available for %q storage", tx.Type())
	}

	r, err := rtx.SnapshotReader()
	if err != nil {
		tx.Rollback()
		return nil, err
	}
	return &txReadCloser{tx: tx, Reader: r}, nil
}

var _ io.ReadCloser = (*txReadCloser)(nil)

// txReadCloser wraps a reader to close a tx on close.
type txReadCloser struct {
	io.Reader
	tx Tx
}

func (r *txReadCloser) Close() error {
	r.tx.Rollback()
	return nil
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

func (api *API) ImportAtomicRecord(ctx context.Context, qcx *Qcx, req *AtomicRecord, opts ...ImportOption) error {

	// this is because some of the tests pass nil qcx for convenience.
	isLocalQcx := false
	if qcx == nil {
		isLocalQcx = true
		qcx = api.Txf().NewQcx()
		defer func() {
			qcx.Abort()
		}()
	}

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
	// Begin that Tx now!
	qcx.StartAtomicWriteTx(Txo{Write: writable, Index: idx, Shard: req.Shard})
	tot := 0

	// BSIs (Values)
	for _, ivr := range req.Ivr {
		tot++
		if simPowerLoss && tot > lossAfter {
			return ErrAborted
		}
		opts0 := append(opts, OptImportOptionsClear(ivr.Clear))
		err := api.ImportValueWithTx(ctx, qcx, ivr, opts0...)
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
		err := api.ImportWithTx(ctx, qcx, ir, opts0...)
		if err != nil {
			return errors.Wrap(err, "ImportAtomicRecord ImportWithTx")
		}
	}

	// got to the end succesfully, so commit if we made the qcx
	if isLocalQcx {
		return qcx.Finish()
	}
	return nil
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

// Import avoids re-writing a bajillion tests to be transaction-aware by allowing a nil pQcx.
// It is convenient for some tests, particularly those in loops, to pass a nil qcx and
// treat the Import as having been commited when we return without error. We make it so.
func (api *API) Import(ctx context.Context, qcx *Qcx, req *ImportRequest, opts ...ImportOption) (err error) {
	if req.Clear {
		opts = addClearToImportOptions(opts)
	}
	isLocalQcx := false
	if qcx == nil {
		isLocalQcx = true
		qcx = api.Txf().NewQcx()
		defer func() {
			qcx.Abort()
		}()
	}
	err = api.ImportWithTx(ctx, qcx, req, opts...)
	if err != nil {
		return err
	}
	if isLocalQcx {
		return qcx.Finish()
	}
	return nil
}

// ImportWithTx bulk imports data into a particular index,field,shard.
func (api *API) ImportWithTx(ctx context.Context, qcx *Qcx, req *ImportRequest, opts ...ImportOption) error {
	span, _ := tracing.StartSpanFromContext(ctx, "API.Import")
	defer span.Finish()

	if err := api.validate(apiImport); err != nil {
		return errors.Wrap(err, "validating api method")
	}

	idx, field, err := api.indexField(req.Index, req.Field, req.Shard)
	if err != nil {
		return errors.Wrap(err, "getting index and field")
	}

	if err := req.ValidateWithTimestamp(idx.CreatedAt(), field.CreatedAt()); err != nil {
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
	// translated to ids in a previous step at the primary node), then
	// check to see if keys need translation.
	if !options.IgnoreKeyCheck {
		// Translate row keys.
		if field.Keys() {
			span.LogKV("rowKeys", true)
			if len(req.RowIDs) != 0 {
				return errors.New("row ids cannot be used because field uses string keys")
			}
			if req.RowIDs, err = api.cluster.translateFieldKeys(ctx, field, req.RowKeys, true); err != nil {
				return errors.Wrapf(err, "translating field keys")
			}
		}

		// Translate column keys.
		if idx.Keys() {
			span.LogKV("columnKeys", true)
			if len(req.ColumnIDs) != 0 {
				return errors.New("column ids cannot be used because index uses string keys")
			}
			if req.ColumnIDs, err = api.cluster.translateIndexKeys(ctx, req.Index, req.ColumnKeys, true); err != nil {
				return errors.Wrap(err, "translating columns")
			}
		}

		// For translated data, map the columnIDs to shards. If
		// this node does not own the shard, forward to the node that does.
		if idx.Keys() || field.Keys() {
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
	// Note: req.Shard may not be the only shard imported into here,
	// so don't expect it to be invariant.
	if !options.Clear {
		if err := importExistenceColumns(qcx, idx, req.ColumnIDs); err != nil {
			api.server.logger.Errorf("import existence error: index=%s, field=%s, shard=%d, columns=%d, err=%s", req.Index, req.Field, req.Shard, len(req.ColumnIDs), err)
			return err
		}
		if err != nil {
			return errors.Wrap(err, "importing existence columns")
		}
	}

	// Import into fragment.
	err = field.Import(qcx, req.RowIDs, req.ColumnIDs, timestamps, opts...)
	if err != nil {
		api.server.logger.Errorf("import error: index=%s, field=%s, shard=%d, columns=%d, err=%s", req.Index, req.Field, req.Shard, len(req.ColumnIDs), err)
		return errors.Wrap(err, "importing")
	}
	return errors.Wrap(err, "committing")
}

// ImportValue avoids re-writing a bajillion tests by allowing a nil pQcx.
// Then we will commit before returning.
func (api *API) ImportValue(ctx context.Context, qcx *Qcx, req *ImportValueRequest, opts ...ImportOption) error {
	if req.Clear {
		opts = addClearToImportOptions(opts)
	}
	return api.ImportValueWithTx(ctx, qcx, req, opts...)
}

// ImportValueWithTx bulk imports values into a particular field.
func (api *API) ImportValueWithTx(ctx context.Context, qcx *Qcx, req *ImportValueRequest, opts ...ImportOption) (err0 error) {
	span, _ := tracing.StartSpanFromContext(ctx, "API.ImportValue")
	defer span.Finish()

	if err := api.validate(apiImportValue); err != nil {
		return errors.Wrap(err, "validating api method")
	}

	idx, field, err := api.indexField(req.Index, req.Field, req.Shard)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("getting index '%v' and field '%v'; shard=%v", req.Index, req.Field, req.Shard))
	}

	if err := req.ValidateWithTimestamp(idx.CreatedAt(), field.CreatedAt()); err != nil {
		return errors.Wrap(err, "validating import value request")
	}

	// Set up import options.
	options, err := setUpImportOptions(opts...)
	if err != nil {
		return errors.Wrap(err, "setting up import options")
	}

	idx, field, err = api.indexField(req.Index, req.Field, req.Shard)
	if err != nil {
		return errors.Wrap(err, "getting index and field")
	}
	span.LogKV(
		"index", req.Index,
		"field", req.Field)
	// Unless explicitly ignoring key validation (meaning keys have been
	// translate to ids in a previous step at the primary node), then
	// check to see if keys need translation.
	if !options.IgnoreKeyCheck {
		// Translate column keys.
		if idx.Keys() {
			span.LogKV("columnKeys", true)
			if len(req.ColumnIDs) != 0 {
				return errors.New("column ids cannot be used because index uses string keys")
			}
			if req.ColumnIDs, err = api.cluster.translateIndexKeys(ctx, req.Index, req.ColumnKeys, true); err != nil {
				return errors.Wrap(err, "translating columns")
			}
			req.Shard = math.MaxUint64
		}

		// Translate values when the field uses keys (for example, when
		// the field has a ForeignIndex with keys).
		if field.Keys() {
			// Perform translation.
			span.LogKV("rowKeys", true)
			uints, err := api.cluster.translateIndexKeys(ctx, field.ForeignIndex(), req.StringValues, true)
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
		// horrible hackery: we implement a secondary key so we can
		// get a stable sort without using sort.Stable
		req.scratch = make([]int, len(req.ColumnIDs))
		for i := range req.scratch {
			req.scratch[i] = i
		}
		sort.Sort(req)
		// don't keep that list around since we don't need it anymore
		req.scratch = nil
	}
	isLocalQcx := false
	if qcx == nil {
		isLocalQcx = true
		qcx = api.Txf().NewQcx()
		defer func() {
			qcx.Abort()
		}()
	}

	// if we're importing into a specific shard
	if req.Shard != math.MaxUint64 {
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
			if err := importExistenceColumns(qcx, idx, req.ColumnIDs); err != nil {
				api.server.logger.Errorf("import existence error: index=%s, field=%s, shard=%d, columns=%d, err=%s", req.Index, req.Field, req.Shard, len(req.ColumnIDs), err)
				return errors.Wrap(err, "importing existence columns")
			}
		}

		// Import into fragment.
		if len(req.Values) > 0 {
			err = field.importValue(qcx, req.ColumnIDs, req.Values, options)
			if err != nil {
				api.server.logger.Errorf("import error: index=%s, field=%s, shard=%d, columns=%d, err=%s", req.Index, req.Field, req.Shard, len(req.ColumnIDs), err)
			}
		} else if len(req.TimestampValues) > 0 {
			err = field.importTimestampValue(qcx, req.ColumnIDs, req.TimestampValues, options)
			if err != nil {
				api.server.logger.Errorf("import error: index=%s, field=%s, shard=%d, columns=%d, err=%s", req.Index, req.Field, req.Shard, len(req.ColumnIDs), err)
			}
		} else if len(req.FloatValues) > 0 {
			err = field.importFloatValue(qcx, req.ColumnIDs, req.FloatValues, options)
			if err != nil {
				api.server.logger.Errorf("import error: index=%s, field=%s, shard=%d, columns=%d, err=%s", req.Index, req.Field, req.Shard, len(req.ColumnIDs), err)
			}
		}
		return errors.Wrap(err, "importing value")

	} // end if req.Shard != math.MaxUint64

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
	err = eg.Wait()
	if err != nil {
		return err
	}
	if isLocalQcx {
		return qcx.Finish()
	}
	return nil
}

func importExistenceColumns(qcx *Qcx, index *Index, columnIDs []uint64) error {
	ef := index.existenceField()
	if ef == nil {
		return nil
	}

	existenceRowIDs := make([]uint64, len(columnIDs))
	return ef.Import(qcx, existenceRowIDs, columnIDs, nil)
}

// ShardDistribution returns an object representing the distribution of shards
// across nodes for each index, distinguishing between primary and replica.
// The structure of this information is [indexName][nodeID][primaryOrReplica][]uint64.
// This function supports a view in the UI.
func (api *API) ShardDistribution(ctx context.Context) map[string]interface{} {
	distByIndex := make(map[string]interface{})

	for idx := range api.holder.indexes {
		dist := api.cluster.shardDistributionByIndex(idx)
		distByIndex[idx] = dist
	}

	return distByIndex
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

// AvailableShards returns bitmap of available shards for a single index.
func (api *API) AvailableShards(ctx context.Context, indexName string) (*roaring.Bitmap, error) {
	span, _ := tracing.StartSpanFromContext(ctx, "API.AvailableShards")
	defer span.Finish()

	// Find the index.
	index := api.holder.Index(indexName)
	if index == nil {
		return nil, newNotFoundError(ErrIndexNotFound, indexName)
	}
	return index.AvailableShards(false), nil
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
	return api.server.longQueryTime
}

func (api *API) validateShardOwnership(indexName string, shard uint64) error {
	// Create a snapshot of the cluster to use for node/partition calculations.
	snap := topology.NewClusterSnapshot(api.cluster.noder, api.cluster.Hasher, api.cluster.ReplicaN)
	// Validate that this handler owns the shard.
	if !snap.OwnsShard(api.NodeID(), indexName, shard) {
		api.server.logger.Errorf("node %s does not own shard %d of index %s", api.NodeID(), shard, indexName)
		return ErrClusterDoesNotOwnShard
	}
	return nil
}

func (api *API) indexField(indexName string, fieldName string, shard uint64) (*Index, *Field, error) {
	api.server.logger.Debugf("importing: %v %v %v", indexName, fieldName, shard)

	// Find the Index.
	index := api.holder.Index(indexName)
	if index == nil {
		api.server.logger.Errorf("fragment error: index=%s, field=%s, shard=%d, err=%s", indexName, fieldName, shard, ErrIndexNotFound.Error())
		return nil, nil, newNotFoundError(ErrIndexNotFound, indexName)
	}

	// Retrieve field.
	field := index.Field(fieldName)
	if field == nil {
		api.server.logger.Errorf("field error: index=%s, field=%s, shard=%d, err=%s", indexName, fieldName, shard, ErrFieldNotFound.Error())
		return nil, nil, newNotFoundError(ErrFieldNotFound, fieldName)
	}
	return index, field, nil
}

// RemoveNode puts the cluster into the "RESIZING" state and begins the job of
// removing the given node.
func (api *API) RemoveNode(id string) (*topology.Node, error) {
	if err := api.validate(apiRemoveNode); err != nil {
		return nil, errors.Wrap(err, "validating api method")
	}

	if api.cluster.disCo.ID() == id {
		return nil, errors.Wrapf(ErrPreconditionFailed, "cannot issue node removal request to the node being removed, id=%s", id)
	}

	removeNode := api.cluster.nodeByID(id)
	if removeNode == nil {
		return nil, errors.Wrap(ErrNodeIDNotExists, "finding node to remove")
	}

	if err := api.cluster.removeNode(id); err != nil {
		return nil, errors.Wrapf(err, "removing node %s", id)
	}

	return removeNode, nil
}

// ResizeAbort stops the current resize job.
func (api *API) ResizeAbort() error {
	if err := api.validate(apiResizeAbort); err != nil {
		return errors.Wrap(err, "validating api method")
	}

	return api.cluster.resizeAbortAndBroadcast()
}

// State returns the cluster state which is usually "NORMAL", but could be
// "STARTING", "RESIZING", or potentially others. See disco.go for more
// details.
func (api *API) State() (disco.ClusterState, error) {
	if err := api.validate(apiState); err != nil {
		return "", errors.Wrap(err, "validating api method")
	}

	return api.cluster.State()
}

// ClusterName returns the cluster name.
func (api *API) ClusterName() string {
	if api.cluster.Name == "" {
		return api.cluster.id
	}
	return api.cluster.Name
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
		StorageBackend:   api.holder.txf.TxType(),
		ReplicaN:         api.cluster.ReplicaN,
		ShardHash:        api.cluster.Hasher.Name(),
		KeyHash:          api.cluster.Hasher.Name(),
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
				a[i].Close() // nolint: errcheck
			}
		}
	}()

	// Fetch all index partition readers.
	for indexName, indexMap := range offsets {
		index := api.holder.Index(indexName)
		if index == nil {
			return nil, newNotFoundError(ErrIndexNotFound, indexName)
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
			return nil, newNotFoundError(ErrIndexNotFound, indexName)
		}

		for fieldName, offset := range indexMap.Fields {
			field := index.Field(fieldName)
			if field == nil {
				return nil, newNotFoundError(ErrFieldNotFound, fieldName)
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

func (api *API) TranslateIndexKey(ctx context.Context, indexName string, key string, writable bool) (uint64, error) {
	return api.cluster.translateIndexKey(ctx, indexName, key, writable)
}

func (api *API) TranslateIndexIDs(ctx context.Context, indexName string, ids []uint64) ([]string, error) {
	return api.cluster.translateIndexIDs(ctx, indexName, ids)
}

// TranslateKeys handles a TranslateKeyRequest.
// ErrTranslatingKeyNotFound error will be swallowed here, so the empty response will be returned.
func (api *API) TranslateKeys(ctx context.Context, r io.Reader) (_ []byte, err error) {
	var req TranslateKeysRequest
	buf, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, NewBadRequestError(errors.Wrap(err, "read translate keys request error"))
	} else if err := api.Serializer.Unmarshal(buf, &req); err != nil {
		return nil, NewBadRequestError(errors.Wrap(err, "unmarshal translate keys request error"))
	}

	// Lookup store for either index or field and translate keys.
	var ids []uint64
	if req.Field == "" {
		ids, err = api.cluster.translateIndexKeys(ctx, req.Index, req.Keys, !req.NotWritable)
	} else {
		field := api.holder.Field(req.Index, req.Field)
		if field == nil {
			return nil, newNotFoundError(ErrFieldNotFound, req.Field)
		}

		if fi := field.ForeignIndex(); fi != "" {
			ids, err = api.cluster.translateIndexKeys(ctx, fi, req.Keys, !req.NotWritable)
		} else {
			ids, err = api.cluster.translateFieldKeys(ctx, field, req.Keys, !req.NotWritable)
		}
	}
	if err != nil && errors.Cause(err) != ErrTranslatingKeyNotFound {
		return nil, errors.WithMessage(err, "translating keys")
	}

	// Encode response.
	if buf, err = api.Serializer.Marshal(&TranslateKeysResponse{IDs: ids}); err != nil {
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
			return nil, newNotFoundError(ErrFieldNotFound, req.Field)
		} else if fi := field.ForeignIndex(); fi != "" {
			keys, err = api.cluster.translateIndexIDs(ctx, fi, req.IDs)
			if err != nil {
				return nil, err
			}
		} else if keys, err = api.cluster.translateFieldListIDs(field, req.IDs); err != nil {
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

// FindIndexKeys looks up column keys in the index, mapping them to IDs.
// If a key does not exist, it will be absent from the resulting map.
func (api *API) FindIndexKeys(ctx context.Context, index string, keys ...string) (map[string]uint64, error) {
	return api.cluster.findIndexKeys(ctx, index, keys...)
}

// FindFieldKeys looks up keys in a field, mapping them to IDs.
// If a key does not exist, it will be absent from the resulting map.
func (api *API) FindFieldKeys(ctx context.Context, index, field string, keys ...string) (map[string]uint64, error) {
	f := api.holder.Field(index, field)
	if f == nil {
		return nil, newNotFoundError(ErrFieldNotFound, field)
	}
	return api.cluster.findFieldKeys(ctx, f, keys...)
}

// CreateIndexKeys looks up column keys in the index, mapping them to IDs.
// If a key does not exist, it will be created.
func (api *API) CreateIndexKeys(ctx context.Context, index string, keys ...string) (map[string]uint64, error) {
	return api.cluster.createIndexKeys(ctx, index, keys...)
}

// CreateFieldKeys looks up keys in a field, mapping them to IDs.
// If a key does not exist, it will be created.
func (api *API) CreateFieldKeys(ctx context.Context, index, field string, keys ...string) (map[string]uint64, error) {
	f := api.holder.Field(index, field)
	if f == nil {
		return nil, newNotFoundError(ErrFieldNotFound, field)
	}
	return api.cluster.createFieldKeys(ctx, f, keys...)
}

// MatchField finds the IDs of all field keys matching a filter.
func (api *API) MatchField(ctx context.Context, index, field string, like string) ([]uint64, error) {
	f := api.holder.Field(index, field)
	if f == nil {
		return nil, newNotFoundError(ErrFieldNotFound, field)
	}
	return api.cluster.matchField(ctx, f, like)
}

// PrimaryReplicaNodeURL returns the URL of the cluster's primary replica.
func (api *API) PrimaryReplicaNodeURL() url.URL {
	// Create a snapshot of the cluster to use for node/partition calculations.
	snap := topology.NewClusterSnapshot(api.cluster.noder, api.cluster.Hasher, api.cluster.ReplicaN)

	node := snap.PrimaryReplicaNode(api.NodeID())
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

func (api *API) PastQueries(ctx context.Context, remote bool) ([]PastQueryStatus, error) {
	if err := api.validate(apiPastQueries); err != nil {
		return nil, errors.Wrap(err, "validating api method")
	}

	clusterQueries := api.tracker.PastQueries()

	if !remote {
		nodes := api.cluster.Nodes()
		for _, node := range nodes {
			if node.ID == api.server.nodeID {
				continue
			}
			nodeQueries, err := api.server.defaultClient.GetPastQueries(ctx, &node.URI)
			if err != nil {
				return nil, errors.Wrapf(err, "collecting query history from %s", node.URI)
			}
			clusterQueries = append(clusterQueries, nodeQueries...)
		}
	}

	sort.Slice(clusterQueries, func(i, j int) bool {
		return clusterQueries[i].Start.After(clusterQueries[j].Start)
	})

	return clusterQueries, nil
}

func (api *API) ReserveIDs(key IDAllocKey, session [32]byte, offset uint64, count uint64) ([]IDRange, error) {
	if err := api.validate(apiIDReserve); err != nil {
		return nil, errors.Wrap(err, "validating api method")
	}

	// Create a snapshot of the cluster to use for node/partition calculations.
	snap := topology.NewClusterSnapshot(api.cluster.noder, api.cluster.Hasher, api.cluster.ReplicaN)

	if !snap.IsPrimaryFieldTranslationNode(api.NodeID()) {
		return nil, errors.New("cannot reserve IDs on a non-primary node")
	}

	return api.holder.ida.reserve(key, session, offset, count)
}

func (api *API) CommitIDs(key IDAllocKey, session [32]byte, count uint64) error {
	if err := api.validate(apiIDCommit); err != nil {
		return errors.Wrap(err, "validating api method")
	}

	// Create a snapshot of the cluster to use for node/partition calculations.
	snap := topology.NewClusterSnapshot(api.cluster.noder, api.cluster.Hasher, api.cluster.ReplicaN)

	if !snap.IsPrimaryFieldTranslationNode(api.NodeID()) {
		return errors.New("cannot commit IDs on a non-primary node")
	}

	return api.holder.ida.commit(key, session, count)
}

func (api *API) ResetIDAlloc(index string) error {
	if err := api.validate(apiIDReset); err != nil {
		return errors.Wrap(err, "validating api method")
	}

	// Create a snapshot of the cluster to use for node/partition calculations.
	snap := topology.NewClusterSnapshot(api.cluster.noder, api.cluster.Hasher, api.cluster.ReplicaN)

	if !snap.IsPrimaryFieldTranslationNode(api.NodeID()) {
		return errors.New("cannot reset IDs on a non-primary node")
	}

	return api.holder.ida.reset(index)
}

func (api *API) WriteIDAllocDataTo(w io.Writer) error {
	_, err := api.holder.ida.WriteTo(w)
	return err
}
func (api *API) RestoreIDAlloc(r io.Reader) error {
	return api.holder.ida.Replace(r)
}

// TranslateIndexDB is an internal function to load the index keys database
// rd is a boltdb file.
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

// RestoreShard
func (api *API) RestoreShard(ctx context.Context, indexName string, shard uint64, rd io.Reader) error {
	snap := topology.NewClusterSnapshot(api.cluster.noder, api.cluster.Hasher, api.cluster.ReplicaN)
	if !snap.OwnsShard(api.server.nodeID, indexName, shard) {
		return ErrClusterDoesNotOwnShard // TODO (twg)really just node doesn't own shard but leave for now
	}

	idx := api.holder.Index(indexName)
	//need to get a dbShard
	dbs, err := idx.Txf().dbPerShard.GetDBShard(indexName, shard, idx)
	if err != nil {
		return err
	}
	//need to find the path to the db
	//will not work on blue green
	db := dbs.W[0]
	finalPath := db.Path() + "/data"
	tempPath := finalPath + ".tmp"
	o, err := os.OpenFile(tempPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	if err != nil {
		return err
	}
	defer o.Close()

	bw := bufio.NewWriter(o)
	if _, err = io.Copy(bw, rd); err != nil {
		return err
	} else if err := bw.Flush(); err != nil {
		return err
	} else if err := o.Sync(); err != nil {
		return err
	} else if err := o.Close(); err != nil {
		return err
	}

	if err != nil {
		_ = os.Remove(tempPath)
		return err
	}
	err = db.CloseDB()
	if err != nil {
		return err
	}
	err = os.Rename(tempPath, finalPath)
	if err != nil {
		_ = os.Remove(tempPath)
		return err
	}
	err = db.OpenDB()
	if err != nil {
		return err
	}
	tx, err := db.NewTx(false, idx.name, Txo{})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	//arguments idx,shard do not matter for rbf they
	//are ignored
	flvs, err := tx.GetSortedFieldViewList(idx, shard)
	if err != nil {
		return nil
	}

	for _, flv := range flvs {
		fld := idx.field(flv.Field)
		view, ok := fld.viewMap[flv.View]
		if !ok {
			view, err = fld.createViewIfNotExists(flv.View)
			if err != nil {
				return err
			}
		}
		frag, err := view.CreateFragmentIfNotExists(shard)
		if err != nil {
			return err
		}
		err = frag.RebuildRankCache(ctx)
		if err != nil {
			return err
		}
	}

	return nil
}

type serverInfo struct {
	ShardWidth       uint64 `json:"shardWidth"`
	ReplicaN         int    `json:"replicaN"`
	ShardHash        string `json:"shardHash"`
	KeyHash          string `json:"keyHash"`
	Memory           uint64 `json:"memory"`
	CPUType          string `json:"cpuType"`
	CPUPhysicalCores int    `json:"cpuPhysicalCores"`
	CPULogicalCores  int    `json:"cpuLogicalCores"`
	CPUMHz           int    `json:"cpuMHz"`
	StorageBackend   string `json:"storageBackend"`
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
	apiFieldTranslateData
	apiField
	//apiHosts // not implemented
	apiImport
	apiImportValue
	apiIndex
	//apiLocalID // not implemented
	//apiLongQueryTime // not implemented
	//apiMaxShards // not implemented
	apiQuery
	apiRecalculateCaches
	apiRemoveNode
	apiResizeAbort
	apiSchema
	apiShardNodes
	apiState
	//apiStatsWithTags // not implemented
	//apiVersion // not implemented
	apiViews
	apiApplySchema
	apiStartTransaction
	apiFinishTransaction
	apiTransactions
	apiGetTransaction
	apiActiveQueries
	apiPastQueries
	apiIDReserve
	apiIDCommit
	apiIDReset
	apiPartitionNodes
)

var methodsCommon = map[apiMethod]struct{}{
	apiClusterMessage: {},
	apiState:          {},
}

var methodsResizing = map[apiMethod]struct{}{
	apiFragmentData:       {},
	apiTranslateData:      {},
	apiFieldTranslateData: {},
	apiResizeAbort:        {},
	apiSchema:             {},
}

var methodsDegraded = map[apiMethod]struct{}{
	apiExportCSV:         {},
	apiFragmentBlockData: {},
	apiFragmentBlocks:    {},
	apiField:             {},
	apiIndex:             {},
	apiQuery:             {},
	apiRecalculateCaches: {},
	apiRemoveNode:        {},
	apiShardNodes:        {},
	apiSchema:            {},
	apiViews:             {},
	apiStartTransaction:  {},
	apiFinishTransaction: {},
	apiTransactions:      {},
	apiGetTransaction:    {},
	apiActiveQueries:     {},
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
	apiFieldTranslateData:   {},
	apiImport:               {},
	apiImportValue:          {},
	apiIndex:                {},
	apiQuery:                {},
	apiRecalculateCaches:    {},
	apiRemoveNode:           {},
	apiShardNodes:           {},
	apiSchema:               {},
	apiViews:                {},
	apiApplySchema:          {},
	apiStartTransaction:     {},
	apiFinishTransaction:    {},
	apiTransactions:         {},
	apiTranslateData:        {},
	apiGetTransaction:       {},
	apiActiveQueries:        {},
	apiPastQueries:          {},
	apiIDReserve:            {},
	apiIDCommit:             {},
	apiIDReset:              {},
	apiPartitionNodes:       {},
}
