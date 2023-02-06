// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
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
	"math"
	"net/url"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	fbcontext "github.com/featurebasedb/featurebase/v3/context"
	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/dax/computer"
	"github.com/featurebasedb/featurebase/v3/dax/storage"
	"github.com/featurebasedb/featurebase/v3/disco"
	"github.com/featurebasedb/featurebase/v3/logger"
	"github.com/featurebasedb/featurebase/v3/rbf"
	"github.com/prometheus/client_golang/prometheus"

	//"github.com/featurebasedb/featurebase/v3/pg"
	"github.com/featurebasedb/featurebase/v3/pql"
	"github.com/featurebasedb/featurebase/v3/roaring"
	planner_types "github.com/featurebasedb/featurebase/v3/sql3/planner/types"
	"github.com/featurebasedb/featurebase/v3/tracing"
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

	Serializer Serializer

	serverlessStorage *storage.ResourceManager

	directiveWorkerPoolSize int

	// isComputeNode is set to true if this node is running as a DAX compute
	// node.
	isComputeNode bool
}

func (api *API) Holder() *Holder {
	return api.holder
}

func (api *API) logger() logger.Logger {
	return api.server.logger
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

func OptAPIServerlessStorage(mm *storage.ResourceManager) apiOption {
	return func(a *API) error {
		a.serverlessStorage = mm
		return nil
	}
}

func OptAPIImportWorkerPoolSize(size int) apiOption {
	return func(a *API) error {
		a.importWorkerPoolSize = size
		return nil
	}
}

func OptAPIDirectiveWorkerPoolSize(size int) apiOption {
	return func(a *API) error {
		a.directiveWorkerPoolSize = size
		return nil
	}
}

func OptAPIIsComputeNode(is bool) apiOption {
	return func(a *API) error {
		a.isComputeNode = is
		return nil
	}
}

// NewAPI returns a new API instance.
func NewAPI(opts ...apiOption) (*API, error) {
	api := &API{
		importWorkerPoolSize: 2,

		directiveWorkerPoolSize: 2,
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

// SetAPIOptions applies the given functional options to the API.
func (api *API) SetAPIOptions(opts ...apiOption) error {
	for _, opt := range opts {
		err := opt(api)
		if err != nil {
			return errors.Wrap(err, "setting API option")
		}
	}
	return nil
}

// validAPIMethods specifies the api methods that are valid for each
// cluster state.
var validAPIMethods = map[disco.ClusterState]map[apiMethod]struct{}{
	disco.ClusterStateNormal:   appendMap(methodsCommon, methodsNormal),
	disco.ClusterStateDegraded: appendMap(methodsCommon, methodsDegraded),
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
	execOpts := &ExecOptions{
		Remote:        req.Remote,
		Profile:       req.Profile,
		PreTranslated: req.PreTranslated,
		EmbeddedData:  req.EmbeddedData, // precomputed values that needed to be passed with the request
		MaxMemory:     req.MaxMemory,
	}
	resp, err := api.server.executor.Execute(ctx, dax.StringTableKeyer(req.Index), q, req.Shards, execOpts)
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

	// get the requestUserID from the context -- assumes the http handler has populated this from
	// authN/Z info
	requestUserID, _ := fbcontext.UserID(ctx) // requestUserID is "" if not in ctx

	if err := api.validate(apiCreateIndex); err != nil {
		return nil, errors.Wrap(err, "validating api method")
	}

	// Populate the create index message.
	ts := timestamp()
	cim := &CreateIndexMessage{
		Index:     indexName,
		CreatedAt: ts,
		Owner:     requestUserID,
		Meta:      options,
	}

	// Create index.
	index, err := api.holder.CreateIndexAndBroadcast(ctx, cim)
	if err != nil {
		return nil, errors.Wrap(err, "creating index")
	}

	CounterCreateIndex.Inc()
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

func (api *API) DeleteDataframe(ctx context.Context, indexName string) error {
	span, _ := tracing.StartSpanFromContext(ctx, "API.DeleteDataframe")
	defer span.Finish()

	if err := api.validate(apiDeleteDataframe); err != nil {
		return errors.Wrap(err, "validating api method")
	}
	// Delete index from the holder.
	err := api.holder.DeleteDataframe(indexName)
	if err != nil {
		return errors.Wrap(err, "deleting index")
	}
	// Send the delete index message to all nodes.
	err = api.server.SendSync(
		&DeleteDataframeMessage{
			Index: indexName,
		})
	if err != nil {
		api.server.logger.Errorf("problem sending DeleteIndex message: %s", err)
		return errors.Wrap(err, "sending DeleteIndex message")
	}
	CounterDeleteDataframe.Inc()
	return nil
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

	// Remove from writelogger/snapshotter if serverless.
	if api.isComputeNode {
		if err := api.serverlessStorage.RemoveTable(dax.TableKey(indexName).QualifiedTableID()); err != nil {
			return errors.Wrapf(err, "removing table from serverless storage: %s", indexName)
		}
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
	snap := api.cluster.NewSnapshot()
	if snap.IsPrimaryFieldTranslationNode(api.NodeID()) {
		if err := api.holder.ida.reset(indexName); err != nil {
			return errors.Wrap(err, "deleting id allocation for index")
		}
	}
	CounterDeleteIndex.Inc()
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

	// get the requestUserID from the context -- assumes the http handler has populated this from
	// authN/Z info
	requestUserID, _ := fbcontext.UserID(ctx) // requestUserID is "" if not in ctx

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
		Owner:     requestUserID,
		Meta:      fo,
	}

	// Create field.
	field, err := index.CreateField(fieldName, requestUserID, opts...)
	if err != nil {
		return nil, errors.Wrap(err, "creating field")
	}

	// Send the create field message to all nodes. We do this *outside* the
	// CreateField logic so we're not blocking on it.
	if err := api.holder.sendOrSpool(cfm); err != nil {
		return nil, errors.Wrap(err, "sending CreateField message")
	}

	CounterCreateField.With(prometheus.Labels{"index": indexName})
	return field, nil
}

// FieldUpdate represents a change to a field. The thinking is to only
// support changing one field option at a time to keep the
// implementation sane. At time of writing, only TTL is supported.
type FieldUpdate struct {
	Option string `json:"option"`
	Value  string `json:"value"`
}

func (api *API) UpdateField(ctx context.Context, indexName, fieldName string, update FieldUpdate) error {
	// Find index.
	index := api.holder.Index(indexName)
	if index == nil {
		return newNotFoundError(ErrIndexNotFound, indexName)
	}

	// get the requestUserID from the context -- assumes the http handler has populated this from
	// authN/Z info
	requestUserID, _ := fbcontext.UserID(ctx)

	cfm, err := index.UpdateField(ctx, fieldName, requestUserID, update)
	if err != nil {
		return errors.Wrap(err, "updating field")
	}

	if err := index.UpdateFieldLocal(cfm, update); err != nil {
		return errors.Wrap(err, "updating field locally")
	}

	// broadcast field update
	err = api.holder.sendOrSpool(&UpdateFieldMessage{
		CreateFieldMessage: *cfm,
		Update:             update,
	})
	return errors.Wrap(err, "sending UpdateField message")
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

	api.server.logger.Debugf("ImportRoaring: %v %v %v", indexName, fieldName, shard)
	index, field, err := api.indexField(indexName, fieldName, shard)
	if index == nil || field == nil {
		return err
	}

	// This node only handles the shard(s) that it owns.
	if api.isComputeNode {
		directive := api.holder.Directive()
		if !shardInShards(dax.ShardNum(shard), directive.ComputeShards(dax.TableKey(index.Name()))) {
			return errors.Errorf("import request shard is not supported (roaring): %d", shard)
		}
	}

	if err = req.ValidateWithTimestamp(index.CreatedAt(), field.CreatedAt()); err != nil {
		return newPreconditionFailedError(err)
	}

	qcx := api.Txf().NewQcx()
	defer qcx.Abort()

	// Create a snapshot of the cluster to use for node/partition calculations.
	snap := api.cluster.NewSnapshot()

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
			if api.isComputeNode && !req.SuppressLog {
				// Write the request to the write logger.
				partition := disco.ShardToShardPartition(indexName, shard, disco.DefaultPartitionN)
				msg := &computer.ImportRoaringMessage{
					Table:           indexName,
					Field:           fieldName,
					Partition:       partition,
					Shard:           shard,
					Clear:           req.Clear,
					Action:          req.Action,
					Block:           req.Block,
					UpdateExistence: req.UpdateExistence,
					Views:           req.Views,
				}

				tkey := dax.TableKey(indexName)
				qtid := tkey.QualifiedTableID()
				partitionNum := dax.PartitionNum(partition)
				shardNum := dax.ShardNum(shard)

				b, err := computer.MarshalLogMessage(msg, computer.EncodeTypeJSON)
				if err != nil {
					return errors.Wrap(err, "marshalling log message")
				}

				resource := api.serverlessStorage.GetShardResource(qtid, partitionNum, shardNum)
				err = resource.Append(b)
				if err != nil {
					return errors.Wrap(err, "appending shard data") // TODO do we need to set err0 or something?
				}
			}

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
	CounterDeleteField.With(prometheus.Labels{"index": indexName})
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
	CounterDeleteAvailableShard.With(prometheus.Labels{"index": indexName}).Inc()
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
	snap := api.cluster.NewSnapshot()

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

	citer, _, err := tx.ContainerIterator(indexName, fieldName, viewStandard, shard, 0)
	if err != nil {
		return err
	}
	var row, hi uint64
	var failed error
	process := func(u uint16) {
		if err := fn(row, hi|uint64(u)); err != nil {
			failed = err
		}
	}
	for citer.Next() {
		key, c := citer.Value()
		hi = key << 16
		row, hi = (hi / ShardWidth), (shard*ShardWidth)+(hi%ShardWidth)
		roaring.ContainerCallback(c, process)
		if failed != nil {
			return errors.Wrap(err, "writing CSV")
		}
	}

	// Ensure data is flushed.
	cw.Flush()
	span.LogKV("n", n)
	tx.Rollback()
	return nil
}

// ShardNodes returns the node and all replicas which should contain a shard's data.
func (api *API) ShardNodes(ctx context.Context, indexName string, shard uint64) ([]*disco.Node, error) {
	span, _ := tracing.StartSpanFromContext(ctx, "API.ShardNodes")
	defer span.Finish()

	if err := api.validate(apiShardNodes); err != nil {
		return nil, errors.Wrap(err, "validating api method")
	}

	// Create a snapshot of the cluster to use for node/partition calculations.
	snap := api.cluster.NewSnapshot()

	return snap.ShardNodes(indexName, shard), nil
}

// PartitionNodes returns the node and all replicas which should contain a partition key data.
func (api *API) PartitionNodes(ctx context.Context, partitionID int) ([]*disco.Node, error) {
	span, _ := tracing.StartSpanFromContext(ctx, "API.PartitionNodes")
	defer span.Finish()

	if err := api.validate(apiPartitionNodes); err != nil {
		return nil, errors.Wrap(err, "validating api method")
	}

	// Create a snapshot of the cluster to use for node/partition calculations.
	snap := api.cluster.NewSnapshot()

	return snap.PartitionNodes(partitionID), nil
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

type RedirectError struct {
	HostPort string
	error    string
}

func (r RedirectError) Error() string {
	return r.error
}

// TranslateData returns all translation data in the specified partition.
func (api *API) TranslateData(ctx context.Context, indexName string, partition int) (TranslateStore, error) {
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

	// Find the node that can service the request.
	snap := api.cluster.NewSnapshot()
	nodes := snap.PartitionNodes(partition)
	var upNode *disco.Node
	for _, node := range nodes {
		// we all UNKNOWN state here because we often mistakenly think
		// a node is not up under heavy load, but prefer STARTED if we
		// find one.
		if node.State == disco.NodeStateStarted {
			upNode = node
			break
		} else if node.State == disco.NodeStateUnknown {
			if upNode != nil {
				upNode = node
			}
		}
	}

	// If there is no upNode, then we can't service the request.
	if upNode == nil {
		return nil, fmt.Errorf("can't get translate data, no nodes available for partition %d", partition)
	}

	// If we're not the upNode, we need to redirect to it.
	if upNode.ID != api.server.NodeID() {
		return nil, RedirectError{
			HostPort: upNode.URI.HostPort(),
			error:    fmt.Sprintf("can't translate data, this node(%s) does not partition %d", api.server.uri, partition),
		}
	}

	// We are the upNode!
	store := idx.TranslateStore(partition)
	if store == nil {
		return nil, ErrTranslateStoreNotFound
	}

	return store, nil
}

// FieldTranslateData returns all translation data in the specified field.
func (api *API) FieldTranslateData(ctx context.Context, indexName, fieldName string) (TranslateStore, error) {
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
func (api *API) Hosts(ctx context.Context) []*disco.Node {
	span, _ := tracing.StartSpanFromContext(ctx, "API.Hosts")
	defer span.Finish()
	return api.cluster.Nodes()
}

// Node gets the ID, URI and primary status for this particular node.
func (api *API) Node() *disco.Node {
	return api.server.node()
}

// NodeID gets the ID alone, so it doesn't have to do a complete lookup
// of the node, searching by its ID, to return the ID it searched for.
func (api *API) NodeID() string {
	return api.server.nodeID
}

// PrimaryNode returns the primary node for the cluster.
func (api *API) PrimaryNode() *disco.Node {
	// Create a snapshot of the cluster to use for node/partition calculations.
	snap := api.cluster.NewSnapshot()
	return snap.PrimaryFieldTranslationNode()
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
	body, err := io.ReadAll(reqBody)
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

// IndexInfo returns the same information as Schema(), but only for a single
// index.
func (api *API) IndexInfo(ctx context.Context, name string) (*IndexInfo, error) {
	schema, err := api.Schema(ctx, false)
	if err != nil {
		return nil, err
	}

	for _, idx := range schema {
		if idx.Name == name {
			return idx, nil
		}
	}

	return nil, ErrIndexNotFound
}

// FieldInfo returns the same information as Schema(), but only for a single
// field.
func (api *API) FieldInfo(ctx context.Context, indexName, fieldName string) (*FieldInfo, error) {
	idx, err := api.IndexInfo(ctx, indexName)
	if err != nil {
		return nil, err
	}

	fld := idx.Field(fieldName)
	if fld == nil {
		return nil, ErrFieldNotFound
	}

	return fld, nil
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

// IndexShardSnapshot returns a reader that contains the contents of
// an RBF snapshot for an index/shard. When snapshotting for
// serverless, we need to be able to transactionally move the write
// log to the new version, so we expose writeTx to allow the caller to
// request a write transaction for the snapshot even though we'll just
// be reading inside RBF.
func (api *API) IndexShardSnapshot(ctx context.Context, indexName string, shard uint64, writeTx bool) (io.ReadCloser, error) {
	span, _ := tracing.StartSpanFromContext(ctx, "API.IndexShardSnapshot")
	defer span.Finish()

	// Find index.
	index := api.holder.Index(indexName)
	if index == nil {
		return nil, newNotFoundError(ErrIndexNotFound, indexName)
	}

	// Start transaction.
	tx := index.holder.txf.NewTx(Txo{Index: index, Shard: shard, Write: writeTx})

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
	fullySorted    bool // format-aware sorting, internal use only please.
	suppressLog    bool

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

func OptImportOptionsSuppressLog(b bool) ImportOption {
	return func(o *ImportOptions) error {
		o.suppressLog = b
		return nil
	}
}

var ErrAborted = fmt.Errorf("error: update was aborted")

func (api *API) ImportAtomicRecord(ctx context.Context, qcx *Qcx, req *AtomicRecord, opts ...ImportOption) error {
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

	options, err := setUpImportOptions(opts...)
	if err != nil {
		return errors.Wrap(err, "setting up import options")
	}

	// BSIs (Values)
	for _, ivr := range req.Ivr {
		tot++
		if simPowerLoss && tot > lossAfter {
			return ErrAborted
		}
		subOpts := *options
		subOpts.Clear = ivr.Clear
		err = api.ImportValueWithTx(ctx, qcx, ivr, &subOpts)
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
		subOpts := *options
		subOpts.Clear = ir.Clear
		err := api.ImportWithTx(ctx, qcx, ir, &subOpts)
		if err != nil {
			return errors.Wrap(err, "ImportAtomicRecord ImportWithTx")
		}
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

// Import does the top-level importing.
func (api *API) Import(ctx context.Context, qcx *Qcx, req *ImportRequest, opts ...ImportOption) (err error) {
	if req.Clear {
		opts = addClearToImportOptions(opts)
	}
	// Set up import options.
	options, err := setUpImportOptions(opts...)
	if err != nil {
		return errors.Wrap(err, "setting up import options")
	}

	/////////////////////////////////////////////////////////////////////////////
	// We build the ImportMessage here BEFORE the call to api.ImportWithTx(),
	// because something in that method is modifying the values of req, so if we
	// build ImportMessage after the call to api.ImportWithTx(), then the values
	// that get logged are incorrect. An example I saw were RowIDS going from:
	// shard 0 [1, 2, 3]
	// shard 4 [0]
	//
	// to:
	// shard 0 [1048577, 2097154, 3145731]
	// shard 4 [1]
	//
	// which seem to be the offset in the field shard bitmap.
	var partition int
	var msg *computer.ImportMessage
	if api.isComputeNode && !options.suppressLog {
		partition = disco.ShardToShardPartition(req.Index, req.Shard, disco.DefaultPartitionN)
		msg = &computer.ImportMessage{
			Table:      req.Index,
			Field:      req.Field,
			Partition:  partition,
			Shard:      req.Shard,
			RowIDs:     make([]uint64, len(req.RowIDs)),
			ColumnIDs:  make([]uint64, len(req.ColumnIDs)),
			RowKeys:    make([]string, len(req.RowKeys)),
			ColumnKeys: make([]string, len(req.ColumnKeys)),
			Timestamps: make([]int64, len(req.Timestamps)),
			Clear:      req.Clear,

			IgnoreKeyCheck: options.IgnoreKeyCheck,
			Presorted:      options.Presorted,
		}
		copy(msg.RowIDs, req.RowIDs)
		copy(msg.ColumnIDs, req.ColumnIDs)
		copy(msg.RowKeys, req.RowKeys)
		copy(msg.ColumnKeys, req.ColumnKeys)
		copy(msg.Timestamps, req.Timestamps)
	}
	/////////////////////////////////////////////////////////////////////////////

	err = api.ImportWithTx(ctx, qcx, req, options)
	if err != nil {
		return err
	}

	if api.isComputeNode && !options.suppressLog {
		tkey := dax.TableKey(req.Index)
		qtid := tkey.QualifiedTableID()
		partitionNum := dax.PartitionNum(partition)
		shardNum := dax.ShardNum(req.Shard)

		b, err := computer.MarshalLogMessage(msg, computer.EncodeTypeJSON)
		if err != nil {
			return errors.Wrap(err, "marshalling log message")
		}

		resource := api.serverlessStorage.GetShardResource(qtid, partitionNum, shardNum)
		err = resource.Append(b)
		if err != nil {
			return errors.Wrap(err, "appending shard data") // TODO do we need to set err0 or something?
		}
	}

	return nil
}

// ImportWithTx bulk imports data into a particular index,field,shard.
func (api *API) ImportWithTx(ctx context.Context, qcx *Qcx, req *ImportRequest, options *ImportOptions) error {
	span, _ := tracing.StartSpanFromContext(ctx, "API.Import")
	defer span.Finish()

	if err := api.validate(apiImport); err != nil {
		return errors.Wrap(err, "validating api method")
	}

	api.server.logger.Debugf("ImportWithTx: %v %v %v", req.Index, req.Field, req.Shard)
	idx, field, err := api.indexField(req.Index, req.Field, req.Shard)
	if err != nil {
		return errors.Wrap(err, "getting index and field")
	}

	// This node only handles the shard(s) that it owns.
	if api.isComputeNode {
		directive := api.holder.Directive()
		if !shardInShards(dax.ShardNum(req.Shard), directive.ComputeShards(dax.TableKey(idx.Name()))) {
			return errors.Errorf("import request shard is not supported (with tx): %d", req.Shard)
		}
	}

	if err := req.ValidateWithTimestamp(idx.CreatedAt(), field.CreatedAt()); err != nil {
		return errors.Wrap(err, "validating import value request")
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
		} else if len(req.RowKeys) != 0 {
			return errors.New("value keys cannot be used because field uses integer IDs")
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
			// mark this request as having an unknown shard, meaning it will
			// be sorted and served out to multiple nodes.
			req.Shard = ^uint64(0)
		} else if len(req.ColumnKeys) != 0 {
			return errors.New("record keys cannot be used because field uses integer IDs")
		}
	}

	// if you specify a shard of ^0, we try to split this out. If we did any
	// key translation, we set it to ^0 already above.
	if req.Shard == ^uint64(0) {
		reqs := req.SortToShards()

		// Signal to the receiving nodes to ignore checking for key translation.
		options.IgnoreKeyCheck = true

		var eg errgroup.Group
		guard := make(chan struct{}, runtime.NumCPU()) // only run as many goroutines as CPUs available
		for _, subReq := range reqs {
			// TODO: if local node owns this shard we don't need to go through the client
			guard <- struct{}{} // would block if guard channel is already filled
			subReq := subReq
			eg.Go(func() error {
				err := api.server.defaultClient.Import(ctx, qcx, subReq, options)
				<-guard
				return err
			})
		}
		return eg.Wait()
	}

	// otherwise, this has to be a shard that we have, and everything has
	// to be for that shard.

	// Validate shard ownership.
	if err := api.validateShardOwnership(req.Index, req.Shard); err != nil {
		return errors.Wrap(err, "validating shard ownership")
	}

	var timestamps []int64
	for _, v := range req.Timestamps {
		if v != 0 {
			timestamps = req.Timestamps
			break
		}
	}

	// Import columnIDs into existence field.
	if !options.Clear {
		if err := importExistenceColumns(qcx, idx, req.ColumnIDs, req.Shard); err != nil {
			api.server.logger.Errorf("import existence error: index=%s, field=%s, shard=%d, columns=%d, err=%s", req.Index, req.Field, req.Shard, len(req.ColumnIDs), err)
			return err
		}
		if err != nil {
			return errors.Wrap(err, "importing existence columns")
		}
	}

	// Import into fragment.
	err = field.Import(qcx, req.RowIDs, req.ColumnIDs, timestamps, req.Shard, options)
	if err != nil {
		api.server.logger.Errorf("import error: index=%s, field=%s, shard=%d, columns=%d, err=%s", req.Index, req.Field, req.Shard, len(req.ColumnIDs), err)
		return errors.Wrap(err, "importing")
	}
	return errors.Wrap(err, "committing")
}

// ImportRoaringShard transactionally imports roaring-encoded data
// across many fields in a single shard. It can both set and clear
// bits and updates caches/bitDepth as appropriate, although only the
// bitmap parts happen truly transactionally.
func (api *API) ImportRoaringShard(ctx context.Context, indexName string, shard uint64, req *ImportRoaringShardRequest) error {
	index, err := api.Index(ctx, indexName)
	if err != nil {
		return errors.Wrap(err, "getting index")
	}

	// we really only need a Tx, but getting a Qcx so that there's only one path for getting a Tx
	qcx := api.Txf().NewQcx()
	qcx.write = true
	tx, finisher, err := qcx.GetTx(Txo{Write: true, Index: index, Shard: shard})
	if err != nil {
		return errors.Wrap(err, "getting Tx")
	}
	defer qcx.Finish()
	var err1 error
	defer finisher(&err1)

	if !req.Remote {
		err1 = errors.New("forwarding unimplemented on this endpoint")
		return err1
	}

	for _, viewUpdate := range req.Views {
		field := index.Field(viewUpdate.Field)
		if field == nil {
			err1 = errors.Errorf("no field named '%s' found.", viewUpdate.Field)
			return err1
		}

		fieldType := field.Options().Type
		if err1 = cleanupView(fieldType, &viewUpdate); err1 != nil {
			return err1
		}

		view, err := field.createViewIfNotExists(viewUpdate.View)
		if err != nil {
			err1 = errors.Wrap(err, "getting view")
			return err1
		}

		frag, err := view.CreateFragmentIfNotExists(shard)
		if err != nil {
			err1 = errors.Wrap(err, "getting fragment")
			return err1
		}

		switch fieldType {
		case FieldTypeSet, FieldTypeTime:
			if !viewUpdate.ClearRecords {
				err1 = frag.ImportRoaringClearAndSet(ctx, tx, viewUpdate.Clear, viewUpdate.Set)
			} else {
				err1 = frag.ImportRoaringSingleValued(ctx, tx, viewUpdate.Clear, viewUpdate.Set)
			}
		case FieldTypeInt, FieldTypeTimestamp, FieldTypeDecimal:
			err1 = frag.ImportRoaringBSI(ctx, tx, viewUpdate.Clear, viewUpdate.Set)
		case FieldTypeMutex, FieldTypeBool:
			err1 = frag.ImportRoaringSingleValued(ctx, tx, viewUpdate.Clear, viewUpdate.Set)
		default:
			err1 = errors.Errorf("field type %s is not supported", fieldType)
		}
		if err1 != nil {
			return err1
		}

		// need to update field/bsiGroup bitDepth value if this is an int-like field.
		//
		// TODO get rid of cached bitDepth entirely because the fact
		// that we have to do this is weird and since this state isn't
		// in RBF might have transactional issues.
		if len(field.bsiGroups) > 0 {
			maxRowID, _, err := frag.maxRow(tx, nil)
			if err != nil {
				err1 = errors.Wrapf(err, "getting fragment max row id")
				return err1
			}
			var bd uint64
			if maxRowID+1 > bsiOffsetBit {
				bd = maxRowID + 1 - bsiOffsetBit
			}
			field.cacheBitDepth(bd) // updating bitDepth shouldn't harm anything even if we roll back... only might make some ops slightly more inefficient
		}
	}

	if api.isComputeNode && !req.SuppressLog {
		partition := disco.ShardToShardPartition(indexName, shard, disco.DefaultPartitionN)
		msg := &computer.ImportRoaringShardMessage{
			Table:     indexName,
			Partition: partition,
			Shard:     shard,
			Views:     make([]computer.RoaringUpdate, len(req.Views)),
		}
		for i, view := range req.Views {
			msg.Views[i] = computer.RoaringUpdate{
				Field:        view.Field,
				View:         view.View,
				Clear:        view.Clear,
				Set:          view.Set,
				ClearRecords: view.ClearRecords,
			}
		}

		tkey := dax.TableKey(indexName)
		qtid := tkey.QualifiedTableID()
		partitionNum := dax.PartitionNum(partition)
		shardNum := dax.ShardNum(shard)

		b, err := computer.MarshalLogMessage(msg, computer.EncodeTypeJSON)
		if err != nil {
			err1 = errors.Wrap(err, "marshalling log message")
			return err1
		}

		resource := api.serverlessStorage.GetShardResource(qtid, partitionNum, shardNum)
		err1 = errors.Wrap(resource.Append(b), "appending shard data")
		if err1 != nil {
			return err1
		}
	}

	return nil
}

func cleanupView(fieldType string, viewUpdate *RoaringUpdate) error {
	// TODO wouldn't hurt to have consolidated logic somewhere for validating view names.
	switch fieldType {
	case FieldTypeSet, FieldTypeTime:
		if viewUpdate.View == "" {
			viewUpdate.View = "standard"
		}
		// add 'standard_' if we just have a time... this is how IDK works by default
		if fieldType == FieldTypeTime && !strings.HasPrefix(viewUpdate.View, viewStandard) {
			viewUpdate.View = fmt.Sprintf("%s_%s", viewStandard, viewUpdate.View)
		}
	case FieldTypeInt, FieldTypeDecimal, FieldTypeTimestamp:
		if viewUpdate.View == "" {
			viewUpdate.View = "bsig_" + viewUpdate.Field
		} else if viewUpdate.View != "bsig_"+viewUpdate.Field {
			return NewBadRequestError(errors.Errorf("invalid view name (%s) for field %s of type %s", viewUpdate.View, viewUpdate.Field, fieldType))
		}
	}
	return nil
}

// ImportValue is a wrapper around the common code in ImportValueWithTx, which
// currently just translates req.Clear into a clear ImportOption.
func (api *API) ImportValue(ctx context.Context, qcx *Qcx, req *ImportValueRequest, opts ...ImportOption) error {
	if req.Clear {
		opts = addClearToImportOptions(opts)
	}
	// Set up import options.
	options, err := setUpImportOptions(opts...)
	if err != nil {
		return errors.Wrap(err, "setting up import options")
	}

	/////////////////////////////////////////////////////////////////////////////
	// We build the ImportValueMessage here BEFORE the call to
	// api.ImportValueWithTx() because we don't trust that req doesn't get
	// changed out from under us. See the similar comment in the API.Import()
	// method above.
	var partition int
	var msg *computer.ImportValueMessage
	if api.isComputeNode && !options.suppressLog {
		partition = disco.ShardToShardPartition(req.Index, req.Shard, disco.DefaultPartitionN)
		msg = &computer.ImportValueMessage{
			Table:           req.Index,
			Field:           req.Field,
			Partition:       partition,
			Shard:           req.Shard,
			ColumnIDs:       make([]uint64, len(req.ColumnIDs)),
			ColumnKeys:      make([]string, len(req.ColumnKeys)),
			Values:          make([]int64, len(req.Values)),
			FloatValues:     make([]float64, len(req.FloatValues)),
			TimestampValues: make([]time.Time, len(req.TimestampValues)),
			StringValues:    make([]string, len(req.StringValues)),
			Clear:           req.Clear,

			IgnoreKeyCheck: options.IgnoreKeyCheck,
			Presorted:      options.Presorted,
		}
		copy(msg.ColumnIDs, req.ColumnIDs)
		copy(msg.ColumnKeys, req.ColumnKeys)
		copy(msg.Values, req.Values)
		copy(msg.FloatValues, req.FloatValues)
		copy(msg.TimestampValues, req.TimestampValues)
		copy(msg.StringValues, req.StringValues)
	}
	/////////////////////////////////////////////////////////////////////////////

	if err := api.ImportValueWithTx(ctx, qcx, req, options); err != nil {
		return errors.Wrap(err, "importing value with tx")
	}

	if api.isComputeNode && !options.suppressLog {
		// Get the current version for shard.
		tkey := dax.TableKey(req.Index)
		qtid := tkey.QualifiedTableID()
		partitionNum := dax.PartitionNum(partition)
		shardNum := dax.ShardNum(req.Shard)
		b, err := computer.MarshalLogMessage(msg, computer.EncodeTypeJSON)
		if err != nil {
			return errors.Wrap(err, "marshalling log message")
		}

		resource := api.serverlessStorage.GetShardResource(qtid, partitionNum, shardNum)
		err = resource.Append(b)
		if err != nil {
			return errors.Wrap(err, "appending shard data") // TODO do we need to set err0 or something?
		}

	}

	return nil
}

// ImportValueWithTx bulk imports values into a particular field.
func (api *API) ImportValueWithTx(ctx context.Context, qcx *Qcx, req *ImportValueRequest, options *ImportOptions) (err0 error) {
	span, _ := tracing.StartSpanFromContext(ctx, "API.ImportValue")
	defer span.Finish()

	if err := api.validate(apiImportValue); err != nil {
		return errors.Wrap(err, "validating api method")
	}

	numCols := len(req.ColumnIDs) + len(req.ColumnKeys)
	numVals := len(req.Values) + len(req.FloatValues) + len(req.TimestampValues) + len(req.StringValues)
	if numCols != numVals {
		return errors.New(fmt.Sprintf("number of columns (%v) and number of values (%v) do not match", numCols, numVals))
	}
	if numCols == 0 {
		return nil
	}

	api.server.logger.Debugf("ImportValueWithTx: %v %v %v", req.Index, req.Field, req.Shard)
	idx, field, err := api.indexField(req.Index, req.Field, req.Shard)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("getting index '%v' and field '%v'; shard=%v", req.Index, req.Field, req.Shard))
	}

	// This node only handles the shard(s) that it owns.
	if api.isComputeNode {
		directive := api.holder.Directive()
		if !shardInShards(dax.ShardNum(req.Shard), directive.ComputeShards(dax.TableKey(idx.Name()))) {
			return errors.Errorf("import request shard is not supported (value with tx): %d", req.Shard)
		}
	}

	if err := req.ValidateWithTimestamp(idx.CreatedAt(), field.CreatedAt()); err != nil {
		return errors.Wrap(err, "validating import value request")
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

	// if we're importing into a specific shard
	if req.Shard != math.MaxUint64 {
		// Check that column IDs match the stated shard.
		shard := req.ColumnIDs[0] / ShardWidth
		if s2 := req.ColumnIDs[len(req.ColumnIDs)-1] / ShardWidth; (shard != s2) || (shard != req.Shard) {
			return errors.Errorf("shard %d specified, but import spans shards %d to %d", req.Shard, shard, s2)
		}
		// Validate shard ownership. TODO - we should forward to the
		// correct node rather than barfing here.
		if err := api.validateShardOwnership(req.Index, req.Shard); err != nil {
			return errors.Wrap(err, "validating shard ownership")
		}
		// Import columnIDs into existence field.
		if !options.Clear {
			if err := importExistenceColumns(qcx, idx, req.ColumnIDs, shard); err != nil {
				api.server.logger.Errorf("import existence error: index=%s, field=%s, shard=%d, columns=%d, err=%s", req.Index, req.Field, req.Shard, len(req.ColumnIDs), err)
				return errors.Wrap(err, "importing existence columns")
			}
		}

		// Import into fragment.
		if len(req.Values) > 0 {
			err = field.importValue(qcx, req.ColumnIDs, req.Values, shard, options)
			if err != nil {
				api.server.logger.Errorf("import error: index=%s, field=%s, shard=%d, columns=%d, err=%s", req.Index, req.Field, req.Shard, len(req.ColumnIDs), err)
			}
		} else if len(req.TimestampValues) > 0 {
			err = field.importTimestampValue(qcx, req.ColumnIDs, req.TimestampValues, shard, options)
			if err != nil {
				api.server.logger.Errorf("import error: index=%s, field=%s, shard=%d, columns=%d, err=%s", req.Index, req.Field, req.Shard, len(req.ColumnIDs), err)
			}
		} else if len(req.FloatValues) > 0 {
			err = field.importFloatValue(qcx, req.ColumnIDs, req.FloatValues, shard, options)
			if err != nil {
				api.server.logger.Errorf("import error: index=%s, field=%s, shard=%d, columns=%d, err=%s", req.Index, req.Field, req.Shard, len(req.ColumnIDs), err)
			}
		}
		return errors.Wrap(err, "importing value")

	} // end if req.Shard != math.MaxUint64
	options.IgnoreKeyCheck = true
	start := 0
	shard := req.ColumnIDs[0] / ShardWidth
	var eg errgroup.Group
	guard := make(chan struct{}, runtime.NumCPU()) // only run as many goroutines as CPUs available
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
			} else if req.TimestampValues != nil {
				subreq.TimestampValues = req.TimestampValues[start:i]
			}
			guard <- struct{}{} // would block if guard channel is already filled
			eg.Go(func() error {
				err := api.server.defaultClient.ImportValue(ctx, qcx, subreq, options)
				<-guard
				return err
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
		return api.server.defaultClient.ImportValue(ctx, qcx, subreq, options)
	})
	err = eg.Wait()
	if err != nil {
		return err
	}
	return nil
}

func importExistenceColumns(qcx *Qcx, index *Index, columnIDs []uint64, shard uint64) error {
	ef := index.existenceField()
	if ef == nil {
		return nil
	}

	existenceRowIDs := make([]uint64, len(columnIDs))
	// If we don't gratuitously hand-duplicate things in field.Import,
	// the fact that fragment.bulkImport rewrites its row and column
	// lists can burn us if we don't make a copy before doing the
	// existence field write.
	columnCopy := make([]uint64, len(columnIDs))
	copy(columnCopy, columnIDs)
	options := ImportOptions{}
	return ef.Import(qcx, existenceRowIDs, columnCopy, nil, shard, &options)
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

// LongQueryTime returns the configured threshold for logging/statting
// long running queries.
func (api *API) LongQueryTime() time.Duration {
	return api.server.longQueryTime
}

func (api *API) validateShardOwnership(indexName string, shard uint64) error {
	// Create a snapshot of the cluster to use for node/partition calculations.
	snap := api.cluster.NewSnapshot()
	// Validate that this handler owns the shard.
	if !snap.OwnsShard(api.NodeID(), indexName, shard) {
		api.server.logger.Errorf("node %s does not own shard %d of index %s", api.NodeID(), shard, indexName)
		return ErrClusterDoesNotOwnShard
	}
	return nil
}

func (api *API) indexField(indexName string, fieldName string, shard uint64) (*Index, *Field, error) {
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

// State returns the cluster state which is usually "NORMAL", but could be
// "STARTING", or potentially others. See disco.go for more
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
			store := field.TranslateStore()
			if store == nil {
				return nil, ErrTranslateStoreNotFound
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
	buf, err := io.ReadAll(r)
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
	if buf, err := io.ReadAll(r); err != nil {
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
		} else if keys, err = api.cluster.translateFieldListIDs(ctx, field, req.IDs); err != nil {
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
	snap := api.cluster.NewSnapshot()

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

	switch err {
	case nil:
		if exclusive {
			CounterExclusiveTransactionRequest.Inc()
		} else {
			CounterTransactionStart.Inc()
		}
	case ErrTransactionExclusive:
		if exclusive {
			CounterExclusiveTransactionBlocked.Inc()
		} else {
			CounterTransactionBlocked.Inc()
		}
	}
	if exclusive && t != nil && t.Active {
		CounterExclusiveTransactionActive.Inc()
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
			CounterExclusiveTransactionEnd.Inc()
		} else {
			CounterTransactionEnd.Inc()
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
			CounterExclusiveTransactionActive.Inc()
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
	snap := api.cluster.NewSnapshot()

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
	snap := api.cluster.NewSnapshot()

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
	snap := api.cluster.NewSnapshot()

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
	if idx == nil {
		return fmt.Errorf("index %q not found", indexName)
	}
	store := idx.TranslateStore(partitionID)
	if store == nil {
		return fmt.Errorf("index %q has no translate store", indexName)
	}
	_, err := store.ReadFrom(rd)
	return err
}

// TranslateFieldDB is an internal function to load the field keys database
func (api *API) TranslateFieldDB(ctx context.Context, indexName, fieldName string, rd io.Reader) error {
	idx := api.holder.Index(indexName)
	if idx == nil {
		return fmt.Errorf("index %q not found", indexName)
	}
	field := idx.Field(fieldName)
	if field == nil {
		// Older versions used to accidentally provide an empty translation
		// data file for a nonexistent field called "_keys". To make migration
		// easier, we politely ignore that.
		if fieldName == "_keys" {
			return nil
		}
		return fmt.Errorf("field %q/%q not found", indexName, fieldName)
	}
	store := field.TranslateStore()
	if store == nil {
		return fmt.Errorf("field %q/%q has no translate store", indexName, fieldName)
	}
	_, err := store.ReadFrom(rd)
	return err
}

// RestoreShard is used by the restore tool to restore previously backed up data. This call is specific to RBF data for a shard.
func (api *API) RestoreShard(ctx context.Context, indexName string, shard uint64, rd io.Reader) error {
	snap := api.cluster.NewSnapshot()
	if !snap.OwnsShard(api.server.nodeID, indexName, shard) {
		return ErrClusterDoesNotOwnShard // TODO (twg)really just node doesn't own shard but leave for now
	}

	idx := api.holder.Index(indexName)
	// need to get a dbShard
	dbs, err := api.holder.Txf().dbPerShard.GetDBShard(indexName, shard, idx)
	if err != nil {
		return err
	}
	db := dbs.W
	finalPath := db.Path() + "/data"
	tempPath := finalPath + ".tmp"
	o, err := os.OpenFile(tempPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
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
	// arguments idx,shard do not matter for rbf they
	// are ignored
	flvs, err := tx.GetSortedFieldViewList(idx, shard)
	if err != nil {
		return nil
	}

	for _, flv := range flvs {
		fld := idx.field(flv.Field)
		view := fld.view(flv.View)
		if view == nil {
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
		bd, err := view.bitDepth([]uint64{shard})
		if err != nil {
			return err
		}
		err = fld.cacheBitDepth(bd)
		if err != nil {
			return err
		}
	}

	return nil
}

func (api *API) mutexCheckThisNode(ctx context.Context, qcx *Qcx, indexName string, fieldName string, details bool, limit int) (map[uint64]map[uint64][]uint64, error) {
	index := api.holder.Index(indexName)
	if index == nil {
		return nil, newNotFoundError(ErrIndexNotFound, indexName)
	}
	field := index.Field(fieldName)
	if field == nil {
		return nil, newNotFoundError(ErrFieldNotFound, fieldName)
	}
	results, err := field.MutexCheck(ctx, qcx, details, limit)
	if err != nil {
		return nil, err
	}
	if limit != 0 && len(results) > limit {
		toDel := len(results) - limit
		// yes, Go allows you to delete keys you've already seen while
		// iterating a map. The spec says that if a value not-yet-reached
		// is deleted during iteration, it may or may not appear; this
		// carries the implication that deleting things during map iteration
		// is safe.
		for k := range results {
			delete(results, k)
			toDel--
			if toDel == 0 {
				break
			}
		}
	}
	return results, err
}

// mergeIDLists merges a list of numeric IDs into another list, removing
// duplicates.
func mergeIDLists(dst []uint64, src []uint64) []uint64 {
	dst = append(dst, src...)
	sort.Slice(dst, func(i, j int) bool {
		return dst[i] < dst[j]
	})
	// dedup.
	n := 1
	prev := dst[0]
	for i := 1; i < len(dst); i++ {
		if dst[i] != prev {
			dst[n] = dst[i]
			n++
		}
		prev = dst[i]
	}
	return dst[:n]
}

// mergeKeyLists merges a list of string IDs into another list, removing
// duplicates.
func mergeKeyLists(dst []string, src []string) []string {
	dst = append(dst, src...)
	sort.Slice(dst, func(i, j int) bool {
		return dst[i] < dst[j]
	})
	// dedup.
	n := 1
	prev := dst[0]
	for i := 1; i < len(dst); i++ {
		if dst[i] != prev {
			dst[n] = dst[i]
			n++
		}
		prev = dst[i]
	}
	return dst[:n]
}

// MutexCheckNode checks for collisions in a given mutex field. The response is
// a map[shard]map[column]values, not translated.
func (api *API) MutexCheckNode(ctx context.Context, qcx *Qcx, indexName string, fieldName string, details bool, limit int) (map[uint64]map[uint64][]uint64, error) {
	if err := api.validate(apiMutexCheck); err != nil {
		return nil, errors.Wrap(err, "validating api method")
	}
	return api.mutexCheckThisNode(ctx, qcx, indexName, fieldName, details, limit)
}

// MutexCheck checks a named field for mutex violations, returning a
// map of record IDs to values for records that have multiple values in the
// field. The return will be one of:
//
//	details true:
//	map[uint64][]uint64 // unkeyed index, unkeyed field
//	map[uint64][]string // unkeyed index, keyed field
//	map[string][]uint64 // keyed index, unkeyed field
//	map[string][]string // keyed index, keyed field
//	details false:
//	[]uint64            // unkeyed index
//	[]string            // keyed index
func (api *API) MutexCheck(ctx context.Context, qcx *Qcx, indexName string, fieldName string, details bool, limit int) (result interface{}, err error) {
	if err = api.validate(apiMutexCheck); err != nil {
		return nil, errors.Wrap(err, "validating api method")
	}
	index, err := api.Index(ctx, indexName)
	if err != nil {
		return nil, err
	}
	field, err := api.Field(ctx, indexName, fieldName)
	if err != nil {
		return nil, err
	}
	if field.Type() != FieldTypeMutex {
		return nil, errors.New("can only check mutex state for mutex fields")
	}
	// request data from other nodes as well
	snap := api.cluster.NewSnapshot()
	eg, _ := errgroup.WithContext(ctx)
	myID := api.NodeID()
	results := make([]map[uint64]map[uint64][]uint64, len(snap.Nodes))
	for i, node := range snap.Nodes {
		i := i // loop variable shadowing is a war crime
		if node.ID != myID {
			node := node // loop variable shadowing again
			eg.Go(func() (err error) {
				results[i], err = api.server.defaultClient.MutexCheck(ctx, &node.URI, indexName, fieldName, details, limit)
				return err
			})
		} else {
			eg.Go(func() (err error) {
				results[i], err = api.mutexCheckThisNode(ctx, qcx, indexName, fieldName, details, limit)
				return err
			})
		}
	}
	err = eg.Wait()
	if err != nil {
		return nil, err
	}
	// Set this arbitrarily large so we don't have to be hand-checking for 0
	// throughout.
	if limit == 0 {
		limit = math.MaxInt32
	}
	// We now have a series of maps from shards to maps of record IDs to
	// values. But wait! Either the field, or the index, might be using keys,
	// and want those translated. So we have to translate those. We'll create
	// some tables.
	useIndexKeys := index.Keys()
	// If we're not doing details, we won't translate field keys even if we could.
	useFieldKeys := field.Keys() && details
	indexKeys := map[uint64]string{}
	fieldKeys := map[uint64]string{}
	var indexIDs []uint64
	var fieldIDs []uint64
	// We'll use the string "untranslated" as our default value and overwrite
	// it with translations. We do check for missing translation values in
	// our returns, but just in case, you know?
	untranslated := "untranslated"
	// We don't know which of four map types we want to be working with,
	// but what we can do is make a function which works with that map type
	// given the raw integer values, and is a closure with an already-created
	// map which has already been stashed in `result`. Because maps are
	// reference-y, this should actually work. This function returns true if
	// it's hit the limit for length of results.
	var process func(uint64, []uint64) bool
	if useIndexKeys || useFieldKeys {
		for _, nodeResults := range results {
			for _, shardResults := range nodeResults {
				for record, values := range shardResults {
					if useIndexKeys {
						if _, ok := indexKeys[record]; !ok {
							indexKeys[record] = untranslated
							indexIDs = append(indexIDs, record)
						}
					}
					if useFieldKeys {
						for _, value := range values {
							if _, ok := fieldKeys[value]; !ok {
								fieldKeys[value] = untranslated
								fieldIDs = append(fieldIDs, value)
							}
						}
					}
				}
			}
		}
		// if context is done, return early.
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		untranslatedKeys := 0
		// Obtain translation tables for the keys.
		if useIndexKeys {
			indexKeyList, err := api.cluster.translateIndexIDs(ctx, indexName, indexIDs)
			if err != nil {
				return nil, errors.Wrap(err, "translating index keys")
			}
			if len(indexKeyList) != len(indexIDs) {
				return nil, fmt.Errorf("translating %d record IDs, got %d keys", len(indexIDs), len(indexKeyList))
			}
			for i := range indexIDs {
				if indexKeyList[i] != "" {
					indexKeys[indexIDs[i]] = indexKeyList[i]
				} else {
					untranslatedKeys++
				}
			}
		}
		// if context is done, return early.
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if useFieldKeys {
			fieldKeyList, err := api.cluster.translateFieldListIDs(ctx, field, fieldIDs)
			if err != nil {
				return nil, errors.Wrap(err, "translating index keys")
			}
			if len(fieldKeyList) != len(fieldIDs) {
				return nil, fmt.Errorf("translating %d IDs, got %d keys", len(indexIDs), len(fieldKeyList))
			}
			for i := range fieldIDs {
				if fieldKeyList[i] != "" {
					fieldKeys[fieldIDs[i]] = fieldKeyList[i]
				} else {
					untranslatedKeys++
				}
			}
		}
		if untranslatedKeys > 0 {
			api.server.logger.Warnf("translating mutex check results: %d key(s) untranslated", untranslatedKeys)
		}
	}

	// if context is done, return early.
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// define the process functions. separated from above code just to make
	// it easier to follow/compare them.
	if useIndexKeys {
		if !details {
			outMap := make(map[uint64]struct{})
			outStrings := []string{}
			// unlike a map, the slice won't get updated-in-place, so we have
			// to assign to result after we're done
			defer func() {
				if err == nil {
					result = outStrings
				}
			}()
			process = func(recordID uint64, valueIDs []uint64) bool {
				if _, ok := outMap[recordID]; ok {
					return len(outMap) >= limit
				}
				outMap[recordID] = struct{}{}
				outStrings = append(outStrings, indexKeys[recordID])
				return len(outMap) >= limit
			}
		} else if useFieldKeys {
			outMap := make(map[string][]string)
			var valueKeys []string
			result = outMap
			process = func(recordID uint64, valueIDs []uint64) bool {
				valueKeys = valueKeys[:0]
				for _, id := range valueIDs {
					valueKeys = append(valueKeys, fieldKeys[id])
				}
				record := indexKeys[recordID]
				if existing, ok := outMap[record]; ok {
					outMap[record] = mergeKeyLists(existing, valueKeys)
				} else if len(outMap) < limit {
					// The append is so we can reuse this buffer safely,
					// which matters if there's replication, because many
					// cases won't need to copy the buffer, they'll just
					// copy individual things from it.
					outMap[record] = append([]string{}, valueKeys...)
				}
				return len(outMap) >= limit
			}
		} else {
			outMap := make(map[string][]uint64)
			result = outMap
			process = func(recordID uint64, values []uint64) bool {
				record := indexKeys[recordID]
				if existing, ok := outMap[record]; ok {
					outMap[record] = mergeIDLists(existing, values)
				} else if len(outMap) < limit {
					outMap[record] = values
				}
				return len(outMap) >= limit
			}
		}
	} else {
		if !details {
			outMap := make(map[uint64]struct{})
			outIDs := []uint64{}
			// unlike a map, the slice won't get updated-in-place, so we have
			// to assign to result after we're done
			defer func() {
				if err == nil {
					result = outIDs
				}
			}()
			process = func(recordID uint64, valueIDs []uint64) bool {
				if _, ok := outMap[recordID]; ok {
					return len(outMap) >= limit
				}
				outMap[recordID] = struct{}{}
				outIDs = append(outIDs, recordID)
				return len(outMap) >= limit
			}
		} else if useFieldKeys {
			outMap := make(map[uint64][]string)
			var valueKeys []string
			result = outMap
			process = func(record uint64, valueIDs []uint64) bool {
				valueKeys = valueKeys[:0]
				for _, id := range valueIDs {
					valueKeys = append(valueKeys, fieldKeys[id])
				}
				if existing, ok := outMap[record]; ok {
					outMap[record] = mergeKeyLists(existing, valueKeys)
				} else {
					// The append is so we can reuse this buffer safely,
					// which matters if there's replication, because many
					// cases won't need to copy the buffer, they'll just
					// copy individual things from it.
					outMap[record] = append([]string{}, valueKeys...)
				}
				return len(outMap) >= limit
			}
		} else {
			outMap := make(map[uint64][]uint64)
			result = outMap
			process = func(record uint64, values []uint64) bool {
				if existing, ok := outMap[record]; ok {
					outMap[record] = mergeIDLists(existing, values)
				} else {
					outMap[record] = values
				}
				return len(outMap) >= limit
			}
		}
	}

	// if you specify a limit, and you have *different* errors on different
	// nodes, we will not check all of the nodes. otherwise there's no practical
	// way to get the primary benefit of specifying a limit.
processing:
	for _, nodeResults := range results {
		if len(nodeResults) == 0 {
			continue
		}
		// if context is done, return early.
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		for _, v := range nodeResults {
			if len(v) == 0 {
				continue
			}
			counter := 0
			for record, values := range v {
				counter++
				if process(record, values) {
					break processing
				}
				// every 65k items or so, check the context for done-ness
				if counter%(1<<16) == 0 {
					if err := ctx.Err(); err != nil {
						return nil, err
					}
				}
			}
		}
	}
	return result, ctx.Err()
}

// CompilePlan takes a sql string and returns a PlanOperator. Note that this is
// different from the internal CompilePlan() method on the CompilePlanner
// interface, which takes a parser statement and returns a PlanOperator. In
// other words, this CompilePlan() both parses and plans the provided sql
// string; it's the equivalent of the CompileExecutionPlan() method on Server.
// TODO: consider renaming this to something with less conflict.
func (api *API) CompilePlan(ctx context.Context, q string) (planner_types.PlanOperator, error) {
	return api.server.CompileExecutionPlan(ctx, q)
}

func (api *API) RehydratePlanOperator(ctx context.Context, reader io.Reader) (planner_types.PlanOperator, error) {
	return api.server.RehydratePlanOperator(ctx, reader)
}

func (api *API) RBFDebugInfo() map[string]*rbf.DebugInfo {
	infos := make(map[string]*rbf.DebugInfo)

	for key, dbShard := range api.holder.Txf().dbPerShard.Flatmap {
		wrapper, ok := dbShard.W.(*RbfDBWrapper)
		if !ok {
			continue
		}

		skey := fmt.Sprintf("%s/%d", key.index, key.shard)
		infos[skey] = wrapper.db.DebugInfo()
	}
	return infos
}

// Directive applies the provided Directive to the local computer.
func (api *API) Directive(ctx context.Context, d *dax.Directive) error {
	return api.ApplyDirective(ctx, d)
}

// DirectiveApplied returns true if the computer's current Directive has been
// applied and is ready to be queried. This is temporary (primarily for tests)
// and needs to be refactored as we improve the logic around
// controller-to-computer communication.
func (api *API) DirectiveApplied(ctx context.Context) (bool, error) {
	return api.holder.DirectiveApplied(), nil
}

// SnapshotShardData triggers the node to perform a shard snapshot based on the
// provided SnapshotShardDataRequest.
func (api *API) SnapshotShardData(ctx context.Context, req *dax.SnapshotShardDataRequest) error {
	if !api.holder.DirectiveApplied() {
		return errors.New("don't have directive yet, can't snapshot shard")
	}
	// TODO(jaffee) confirm this node is actually responsible for the given
	// shard? Not sure we need to given that this request comes from
	// the Controller, but might be a belt&suspenders situation.

	qtid := req.TableKey.QualifiedTableID()

	partition := disco.ShardToShardPartition(string(req.TableKey), uint64(req.ShardNum), disco.DefaultPartitionN)
	partitionNum := dax.PartitionNum(partition)

	// Open a write Tx snapshotting current version.
	rc, err := api.IndexShardSnapshot(ctx, string(req.TableKey), uint64(req.ShardNum), true)
	if err != nil {
		return errors.Wrap(err, "getting index/shard readcloser")
	}
	defer rc.Close()

	resource := api.serverlessStorage.GetShardResource(qtid, partitionNum, req.ShardNum)
	// Bump writelog version while write Tx is held.
	if ok, err := resource.IncrementWLVersion(); err != nil {
		return errors.Wrap(err, "incrementing write log version")
	} else if !ok {
		return nil
	}
	// TODO(jaffee) look into downgrading Tx on RBF to read lock here now that WL version is incremented.
	err = resource.Snapshot(rc)
	return errors.Wrap(err, "snapshotting shard data")
}

// SnapshotTableKeys triggers the node to perform a table keys snapshot based on
// the provided SnapshotTableKeysRequest.
func (api *API) SnapshotTableKeys(ctx context.Context, req *dax.SnapshotTableKeysRequest) error {
	if !api.holder.DirectiveApplied() {
		return errors.New("don't have directive yet, can't snapshot table keys")
	}
	// If the index is not keyed, no-op on snapshotting its keys.
	if idx, err := api.Index(ctx, string(req.TableKey)); err != nil {
		return newNotFoundError(ErrIndexNotFound, string(req.TableKey))
	} else if !idx.Keys() {
		return nil
	}

	qtid := req.TableKey.QualifiedTableID()

	// Create the snapshot for the current version.
	trans, err := api.TranslateData(ctx, string(req.TableKey), int(req.PartitionNum))
	if err != nil {
		return errors.Wrapf(err, "getting index/partition translate store: %s/%d", req.TableKey, req.PartitionNum)
	}
	// get a write tx to ensure no other writes while incrementing WL version.
	wrTo, err := trans.Begin(true)
	if err != nil {
		return errors.Wrap(err, "beginning table translate write tx")
	}
	defer wrTo.Rollback()

	resource := api.serverlessStorage.GetTableKeyResource(qtid, req.PartitionNum)
	if ok, err := resource.IncrementWLVersion(); err != nil {
		return errors.Wrap(err, "incrementing write log version")
	} else if !ok {
		// no need to snapshot, no writes
		return nil
	}
	// TODO(jaffee) downgrade write tx to read-only
	err = resource.SnapshotTo(wrTo)
	return errors.Wrap(err, "snapshotting table keys")
}

// SnapshotFieldKeys triggers the node to perform a field keys snapshot based on
// the provided SnapshotFieldKeysRequest.
func (api *API) SnapshotFieldKeys(ctx context.Context, req *dax.SnapshotFieldKeysRequest) error {
	if !api.holder.DirectiveApplied() {
		return errors.New("don't have directive yet, can't snapshot field keys")
	}
	qtid := req.TableKey.QualifiedTableID()

	// Create the snapshot for the current version.
	trans, err := api.FieldTranslateData(ctx, string(req.TableKey), string(req.Field))
	if err != nil {
		return errors.Wrap(err, "getting index/field translator")
	}
	// get a write tx to ensure no other writes while incrementing WL version.
	wrTo, err := trans.Begin(true)
	if err != nil {
		return errors.Wrap(err, "beginning field translate write tx")
	}
	defer wrTo.Rollback()

	resource := api.serverlessStorage.GetFieldKeyResource(qtid, req.Field)
	if ok, err := resource.IncrementWLVersion(); err != nil {
		return errors.Wrap(err, "incrementing writelog version")
	} else if !ok {
		// no need to snapshot, no writes
		return nil
	}
	// TODO(jaffee) downgrade to read tx
	err = resource.SnapshotTo(wrTo)
	return errors.Wrap(err, "snapshotTo in FieldKeys")
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
	// apiHosts // not implemented
	apiImport
	apiImportValue
	apiIndex
	// apiLocalID // not implemented
	// apiLongQueryTime // not implemented
	// apiMaxShards // not implemented
	apiQuery
	apiRecalculateCaches
	apiSchema
	apiShardNodes
	apiState
	// apiStatsWithTags // not implemented
	// apiVersion // not implemented
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
	apiMutexCheck
	apiApplyChangeset
	apiDeleteDataframe
)

var methodsCommon = map[apiMethod]struct{}{
	apiClusterMessage: {},
	apiState:          {},
}

var methodsDegraded = map[apiMethod]struct{}{
	apiExportCSV:         {},
	apiFragmentBlockData: {},
	apiFragmentBlocks:    {},
	apiField:             {},
	apiIndex:             {},
	apiQuery:             {},
	apiRecalculateCaches: {},
	apiShardNodes:        {},
	apiSchema:            {},
	apiViews:             {},
	apiStartTransaction:  {},
	apiFinishTransaction: {},
	apiTransactions:      {},
	apiGetTransaction:    {},
	apiActiveQueries:     {},
	apiPastQueries:       {},
	apiPartitionNodes:    {},
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
	apiMutexCheck:           {},
	apiApplyChangeset:       {},
	apiDeleteDataframe:      {},
}

func shardInShards(i dax.ShardNum, s dax.ShardNums) bool {
	for _, o := range s {
		if i == o {
			return true
		}
	}
	return false
}

type SchemaAPI interface {
	CreateDatabase(context.Context, *dax.Database) error
	DropDatabase(context.Context, dax.DatabaseID) error

	DatabaseByName(ctx context.Context, dbname dax.DatabaseName) (*dax.Database, error)
	DatabaseByID(ctx context.Context, dbid dax.DatabaseID) (*dax.Database, error)
	SetDatabaseOption(ctx context.Context, dbid dax.DatabaseID, option string, value string) error
	Databases(context.Context, ...dax.DatabaseID) ([]*dax.Database, error)

	TableByName(ctx context.Context, tname dax.TableName) (*dax.Table, error)
	TableByID(ctx context.Context, tid dax.TableID) (*dax.Table, error)
	Tables(ctx context.Context) ([]*dax.Table, error)

	CreateTable(ctx context.Context, tbl *dax.Table) error
	CreateField(ctx context.Context, tname dax.TableName, fld *dax.Field) error

	DeleteTable(ctx context.Context, tname dax.TableName) error
	DeleteField(ctx context.Context, tname dax.TableName, fname dax.FieldName) error
}

type ClusterNode struct {
	ID        string
	State     string
	URI       string
	GRPCURI   string
	IsPrimary bool
}

type SystemAPI interface {
	ClusterName() string
	Version() string
	PlatformDescription() string
	PlatformVersion() string
	ClusterNodeCount() int
	ClusterReplicaCount() int
	ShardWidth() int
	ClusterState() string
	DataDir() string

	NodeID() string
	ClusterNodes() []ClusterNode
}

// CreateFieldObj is used to encapsulate the information required for creating a
// field in the SchemaAPI.CreateIndexAndFields interface method.
type CreateFieldObj struct {
	Name    string
	Options []FieldOption
}

// QueryAPI is a subset of the API methods which have to do with query.
type QueryAPI interface {
	Query(ctx context.Context, req *QueryRequest) (QueryResponse, error)
}

// Ensure type implements interface.
var _ SystemAPI = (*FeatureBaseSystemAPI)(nil)

// FeatureBaseSystemAPI is a wrapper around pilosa.API. It implements the
// SystemAPI interface
type FeatureBaseSystemAPI struct {
	*API
}

func (fsapi *FeatureBaseSystemAPI) ClusterName() string {
	return fsapi.API.ClusterName()
}

func (fsapi *FeatureBaseSystemAPI) Version() string {
	return fsapi.API.Version()
}

func (fsapi *FeatureBaseSystemAPI) PlatformDescription() string {
	si := fsapi.server.systemInfo
	platform, err := si.Platform()
	if err != nil {
		return "unknown"
	}
	return platform
}

func (fsapi *FeatureBaseSystemAPI) PlatformVersion() string {
	si := fsapi.server.systemInfo
	platformVersion, err := si.OSVersion()
	if err != nil {
		return "unknown"
	}
	return platformVersion
}

func (fsapi *FeatureBaseSystemAPI) ClusterNodeCount() int {
	return len(fsapi.cluster.noder.Nodes())
}

func (fsapi *FeatureBaseSystemAPI) ClusterReplicaCount() int {
	return fsapi.cluster.ReplicaN
}

func (fsapi *FeatureBaseSystemAPI) ShardWidth() int {
	return ShardWidth
}

func (fsapi *FeatureBaseSystemAPI) ClusterState() string {
	state, err := fsapi.State()
	if err != nil {
		return "UNKNOWN"
	}
	return string(state)
}

func (fsapi *FeatureBaseSystemAPI) DataDir() string {
	return fsapi.server.dataDir
}

func (fsapi *FeatureBaseSystemAPI) NodeID() string {
	return fsapi.cluster.Node.ID
}

func (fsapi *FeatureBaseSystemAPI) ClusterNodes() []ClusterNode {
	result := make([]ClusterNode, 0)

	nodes := fsapi.Hosts(context.Background())

	for _, n := range nodes {
		scn := ClusterNode{
			ID:        n.ID,
			State:     string(n.State),
			URI:       n.URI.String(),
			GRPCURI:   n.GRPCURI.String(),
			IsPrimary: n.IsPrimary,
		}
		result = append(result, scn)
	}

	return result
}

// Ensure type implements interface.
var _ SystemAPI = (*NopSystemAPI)(nil)

// NopSystemAPI is a no-op implementation of the SystemAPI.
type NopSystemAPI struct{}

func (napi *NopSystemAPI) ClusterName() string {
	return ""
}

func (napi *NopSystemAPI) Version() string {
	return ""
}

func (napi *NopSystemAPI) PlatformDescription() string {
	return ""
}

func (napi *NopSystemAPI) PlatformVersion() string {
	return ""
}

func (napi *NopSystemAPI) ClusterNodeCount() int {
	return 0
}

func (napi *NopSystemAPI) ClusterReplicaCount() int {
	return 0
}

func (napi *NopSystemAPI) ShardWidth() int {
	return 0
}

func (napi *NopSystemAPI) ClusterState() string {
	return ""
}

func (napi *NopSystemAPI) DataDir() string {
	return ""
}

func (napi *NopSystemAPI) NodeID() string {
	return ""
}

func (napi *NopSystemAPI) ClusterNodes() []ClusterNode {
	result := make([]ClusterNode, 0)
	return result
}
