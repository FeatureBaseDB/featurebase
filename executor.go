// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package pilosa

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"math/bits"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/disco"
	"github.com/featurebasedb/featurebase/v3/pql"
	"github.com/featurebasedb/featurebase/v3/proto"
	"github.com/featurebasedb/featurebase/v3/roaring"
	"github.com/featurebasedb/featurebase/v3/shardwidth"
	"github.com/featurebasedb/featurebase/v3/task"
	"github.com/featurebasedb/featurebase/v3/testhook"
	"github.com/featurebasedb/featurebase/v3/tracing"
	"github.com/gomem/gomem/pkg/dataframe"
	"github.com/lib/pq"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/errgroup"
)

// defaultField is the field used if one is not specified.
const (
	defaultField = "general"

	// defaultMinThreshold is the lowest count to use in a Top-N operation when
	// looking for additional id/count pairs.
	defaultMinThreshold = 1

	columnLabel = "col"
	rowLabel    = "row"

	errConnectionRefused = "connect: connection refused"
)

type Executor interface {
	Execute(context.Context, dax.TableKeyer, *pql.Query, []uint64, *ExecOptions) (QueryResponse, error)
}

// executor recursively executes calls in a PQL query across all shards.
type executor struct {
	Holder *Holder

	// Local hostname & cluster configuration.
	Node    *disco.Node
	Cluster *cluster

	// Client used for remote requests.
	client *InternalClient

	// Maximum number of Set() or Clear() commands per request.
	MaxWritesPerRequest int

	shutdown       chan struct{}
	workers        *task.Pool
	workerPoolSize int
	work           chan job

	// track active mapperLocal tasks so we can ensure we don't close
	// e.work while they're still waiting to send
	activeMappers uint64

	// Maximum per-request memory usage (Extract() only)
	maxMemory int64

	// Temporary flag to be removed when stablized
	dataframeEnabled   bool
	datafameUseParquet bool
}

// executorOption is a functional option type for pilosa.executor
type executorOption func(e *executor) error

func optExecutorInternalQueryClient(c *InternalClient) executorOption {
	return func(e *executor) error {
		e.client = c
		return nil
	}
}

func optExecutorWorkerPoolSize(size int) executorOption {
	return func(e *executor) error {
		e.workerPoolSize = size
		return nil
	}
}

func optExecutorMaxMemory(v int64) executorOption {
	return func(e *executor) error {
		e.maxMemory = v
		return nil
	}
}

func emptyResult(c *pql.Call) interface{} {
	switch c.Name {
	case "Clear", "ClearRow":
		return false

	case "Row":
		return &Row{Keys: []string{}}

	case "Rows":
		return RowIdentifiers{Keys: []string{}}

	case "IncludesColumn":
		return false
	}

	return nil
}

// newExecutor returns a new instance of executor.
func newExecutor(opts ...executorOption) *executor {
	e := &executor{
		workerPoolSize: 2,
		shutdown:       make(chan struct{}),
	}
	for _, opt := range opts {
		err := opt(e)
		if err != nil {
			panic(err)
		}
	}
	// this channel cap doesn't necessarily have to be the same as
	// workerPoolSize... any larger doesn't seem to have an effect in
	// the few tests we've done at scale with concurrent query
	// workloads. Possible that it could be smaller.
	e.work = make(chan job, e.workerPoolSize)
	_ = testhook.Opened(NewAuditor(), e, nil)
	e.workers = task.NewPool(e.workerPoolSize, e.doOneJob, e)
	return e
}

func (e *executor) Close() error {
	select {
	case <-e.shutdown:
		return nil
	default:
	}
	close(e.shutdown)
	_ = testhook.Closed(NewAuditor(), e, nil)
	// can't close e.work while a mapper is active.
	for atomic.LoadUint64(&e.activeMappers) > 0 {
		time.Sleep(1 * time.Millisecond)
	}
	close(e.work)
	e.workers.Close()
	return nil
}

// PoolSize is exported to let the task pool update us
func (e *executor) PoolSize(n int) {
	if e.Holder != nil {
		GaugeWorkerTotal.Set(float64(n))
	}
}

// InitStats initializes stats counters. Must be called after Holder set.
func (e *executor) InitStats() {
	if e.Holder != nil {
		CounterJobTotal.Add(0)
		l, _, _ := e.workers.Stats()
		GaugeWorkerTotal.Set(float64(l))
	}
}

// Execute executes a PQL query.
func (e *executor) Execute(ctx context.Context, tableKeyer dax.TableKeyer, q *pql.Query, shards []uint64, opt *ExecOptions) (QueryResponse, error) {
	index := string(tableKeyer.Key())

	span, ctx := tracing.StartSpanFromContext(ctx, "executor.Execute")
	span.LogKV("pql", q.String())
	defer span.Finish()

	resp := QueryResponse{}

	// Check for query cancellation.
	if err := validateQueryContext(ctx); err != nil {
		return resp, err
	}

	// Verify that an index is set.
	if index == "" {
		return resp, ErrIndexRequired
	}

	idx := e.Holder.Index(index)
	if idx == nil {
		return resp, newNotFoundError(ErrIndexNotFound, index)
	}

	needWriteTxn := false
	nw := q.WriteCallN()
	if nw > 0 {
		needWriteTxn = true
	}

	// Verify that the number of writes do not exceed the maximum.
	if e.MaxWritesPerRequest > 0 && nw > e.MaxWritesPerRequest {
		return resp, ErrTooManyWrites
	}

	// Default options.
	if opt == nil {
		opt = &ExecOptions{}
	}
	// Default maximum memory, if not passed in.
	if opt.MaxMemory == 0 && q.HasCall("Extract") {
		opt.MaxMemory = e.maxMemory
	}

	if opt.Profile {
		var prof tracing.ProfiledSpan
		prof, ctx = tracing.StartProfiledSpanFromContext(ctx, "Execute")
		defer prof.Finish()
		var ok bool
		resp.Profile, ok = prof.(*tracing.Profile)
		if !ok {
			return resp, fmt.Errorf("profiling execution failed: %T is not tracing.Profile", prof)
		}
	}

	// Can't do NewTx() this high up, because we need a specific shard.
	// So start a qcx with a TxGroup and pass it down.
	var qcx *Qcx
	if needWriteTxn {
		qcx = idx.holder.txf.NewWritableQcx()
	} else {
		qcx = idx.holder.txf.NewQcx()
	}
	defer qcx.Abort()

	results, err := e.execute(ctx, qcx, index, q, shards, opt)
	if err != nil {
		return resp, err
	} else if err := validateQueryContext(ctx); err != nil {
		return resp, err
	}
	resp.Results = results

	// Translate response objects from ids to keys, if necessary.
	// No need to translate a remote call.
	if !opt.Remote {
		// only translateResults if this local node is the final destination. only string/column keys.
		if err := e.translateResults(ctx, index, idx, q.Calls, results, opt.MaxMemory); err != nil {
			if errors.Cause(err) == ErrTranslatingKeyNotFound {
				// No error - return empty result
				resp.Results = make([]interface{}, len(q.Calls))
				for i, c := range q.Calls {
					resp.Results[i] = emptyResult(c)
				}
				return resp, nil
			}
			return resp, err
		} else if err := validateQueryContext(ctx); err != nil {
			return resp, err
		}
	}
	// Must copy out of Tx data before Commiting, because it will become invalid afterwards.
	respSafeNoTxData := safeCopy(resp)

	// Commit transactions if writing; else let the defer grp.Abort do the rollbacks.
	if needWriteTxn {
		if err := qcx.Finish(); err != nil {
			return respSafeNoTxData, err
		}
	}
	return respSafeNoTxData, nil
}

// safeCopy copies everything in resp that has Bitmap material,
// to avoid anything coming from the mmap-ed Tx storage.
func safeCopy(resp QueryResponse) (out QueryResponse) {
	out = QueryResponse{
		Err:     resp.Err,     //  error
		Profile: resp.Profile, //  *tracing.Profile
	}
	// Results can contain *roaring.Bitmap, so need to copy from Tx mmap-ed memory.
	for _, v := range resp.Results {
		switch x := v.(type) {
		case *Row:
			rowSafe := x.Clone()
			out.Results = append(out.Results, rowSafe)
		case bool:
			out.Results = append(out.Results, x)
		case nil:
			out.Results = append(out.Results, nil)
		case uint64:
			out.Results = append(out.Results, x) // for counts
		case *PairsField:
			// no bitmap material, so should be ok to skip Clone()
			out.Results = append(out.Results, x)
		case PairField: // not PairsField but PairField
			// no bitmap material, so should be ok to skip Clone()
			out.Results = append(out.Results, x)
		case ValCount:
			// no bitmap material, so should be ok to skip Clone()
			out.Results = append(out.Results, x)
		case SignedRow:
			// has *Row in it, so has Bitmap material, and very likely needs Clone.
			y := x.Clone()
			out.Results = append(out.Results, *y)
		case GroupCount:
			// no bitmap material, so should be ok to skip Clone()
			out.Results = append(out.Results, x)
		case []GroupCount:
			out.Results = append(out.Results, x)
		case *GroupCounts:
			out.Results = append(out.Results, x)
		case ExtractedTable:
			out.Results = append(out.Results, x)
		case ExtractedIDMatrix:
			out.Results = append(out.Results, x)
		case RowIdentifiers:
			// no bitmap material, so should be ok to skip Clone()
			out.Results = append(out.Results, x)
		case RowIDs:
			// defined as: type RowIDs []uint64
			// so does not contain bitmap material, and
			// should not need to be cloned.
			out.Results = append(out.Results, x)
		case []*Row:
			safe := make([]*Row, len(x))
			for i, v := range x {
				safe[i] = v.Clone()
			}
			out.Results = append(out.Results, safe)
		case DistinctTimestamp:
			out.Results = append(out.Results, x)
		case *SortedRow:
			out.Results = append(out.Results, x)
		case *dataframe.DataFrame:
			// dumpTable(x)
			out.Results = append(out.Results, x)
		case *BasicTable:
			// dumpTable(x)
			out.Results = append(out.Results, x)
		case ExtractedIDMatrixSorted:
			out.Results = append(out.Results, x)
		default:
			panic(fmt.Sprintf("handle %T here", v))
		}
	}
	return
}

// handlePreCalls traverses the call tree looking for calls that need
// precomputed values (e.g. Distinct, UnionRows, ConstRow...).
func (e *executor) handlePreCalls(ctx context.Context, qcx *Qcx, index string, c *pql.Call, shards []uint64, opt *ExecOptions) error {
	if c.Name == "Precomputed" {
		idx := c.Args["valueidx"].(int64)
		if idx >= 0 && idx < int64(len(opt.EmbeddedData)) {
			row := opt.EmbeddedData[idx]
			c.Precomputed = make(map[uint64]interface{}, len(row.Segments))
			for _, segment := range row.Segments {
				c.Precomputed[segment.shard] = &Row{Segments: []RowSegment{segment}}
			}
		} else {
			return fmt.Errorf("no precomputed data! index %d, len %d", idx, len(opt.EmbeddedData))
		}
		return nil
	}
	newIndex := c.CallIndex()
	// A cross-index query is handled by precall. This is inefficient,
	// but we have to do it for now because shards might be different and
	// we haven't implemented the local precalls that would be enough
	// in some cases.
	//
	// This makes simple cross-index queries noticably inefficient.
	//
	// If you're here because of that: We should be using PrecallLocal
	// in cases where the call isn't already PrecallGlobal, and
	// PrecallLocal should wait until we're running on a specific node
	// to do the farming-out of just the sub-queries it has to run
	// for its local shards.
	//
	// As is, we have one node querying every node, then sending out
	// all the data to every node, including the data that node already
	// has. We could reduce the actual copying around dramatically,
	// but only in the cases where local is good enough -- not something
	// like Distinct, where you can't predict output shard for a result
	// from the shard being queried.
	if newIndex != "" && newIndex != index {
		c.Type = pql.PrecallGlobal
		index = newIndex
		// we need to recompute shards, then
		shards = nil
	}
	if err := e.handlePreCallChildren(ctx, qcx, index, c, shards, opt); err != nil {
		return err
	}
	// child calls already handled, no precall for this, so we're done
	if c.Type == pql.PrecallNone {
		return nil
	}
	// We don't try to handle sub-calls from here. I'm not 100%
	// sure that's right, but I think the fact that they're happening
	// inside a precomputed call may mean they need different
	// handling. In any event, the sub-calls will get handled by
	// the executeCall when it gets to them...

	// We set c to look like a normal call, and actually execute it:
	c.Type = pql.PrecallNone
	// possibly override call index.
	v, err := e.executeCall(ctx, qcx, index, c, shards, opt)
	if err != nil {
		return err
	}
	var row *Row
	switch r := v.(type) {
	case *Row:
		row = r
	case SignedRow:
		row = r.Pos
	default:
		return fmt.Errorf("precomputed call %s returned unexpected non-Row data: %T", c.Name, v)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	c.Children = []*pql.Call{}
	c.Name = "Precomputed"
	c.Args = map[string]interface{}{"valueidx": len(opt.EmbeddedData)}
	// stash a copy of the full results, which can be forwarded to other
	// shards if the query has to go to them
	opt.EmbeddedData = append(opt.EmbeddedData, row)
	// and stash a copy locally, so local calls can use it
	if row != nil {
		c.Precomputed = make(map[uint64]interface{}, len(row.Segments))
		for _, segment := range row.Segments {
			c.Precomputed[segment.shard] = &Row{Segments: []RowSegment{segment}}
		}
	}
	return nil
}

// dumpPrecomputedCalls throws away precomputed call data. this is used so we
// can drop any large data associated with a call once we've processed
// the call.
func (e *executor) dumpPrecomputedCalls(ctx context.Context, c *pql.Call) {
	for _, call := range c.Children {
		e.dumpPrecomputedCalls(ctx, call)
	}
	c.Precomputed = nil
}

// handlePreCallChildren handles any pre-calls in the children of a given call.
func (e *executor) handlePreCallChildren(ctx context.Context, qcx *Qcx, index string, c *pql.Call, shards []uint64, opt *ExecOptions) error {
	for i := range c.Children {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := e.handlePreCalls(ctx, qcx, index, c.Children[i], shards, opt); err != nil {
			return err
		}
	}
	for key, val := range c.Args {
		// Do not precompute GroupBy aggregates
		if key == "aggregate" {
			continue
		}
		// Handle Call() operations which exist inside named arguments, too.
		if call, ok := val.(*pql.Call); ok {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := e.handlePreCalls(ctx, qcx, index, call, shards, opt); err != nil {
				return err
			}
		}
	}
	return nil
}

func (e *executor) execute(ctx context.Context, qcx *Qcx, index string, q *pql.Query, shards []uint64, opt *ExecOptions) ([]interface{}, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "executor.execute")
	defer span.Finish()

	// Apply translations if necessary.
	var colTranslations map[string]map[string]uint64            // colID := colTranslations[index][key]
	var rowTranslations map[string]map[string]map[string]uint64 // rowID := rowTranslations[index][field][key]
	if !opt.Remote {
		cols, rows, err := e.preTranslate(ctx, index, q.Calls...)
		if err != nil {
			return nil, err
		}
		colTranslations, rowTranslations = cols, rows
	}

	needShards := false
	if len(shards) == 0 {
		for _, call := range q.Calls {
			if needsShards(call) {
				needShards = true
				break
			}
		}
	}

	if needShards {
		// Round up the number of shards.
		idx := e.Holder.Index(index)
		if idx == nil {
			return nil, newNotFoundError(ErrIndexNotFound, index)
		}
		shards = idx.AvailableShards(includeRemote).Slice()
		if len(shards) == 0 {
			shards = []uint64{0}
		}
	}

	lastWasWrite := false
	// Execute each call serially.
	results := make([]interface{}, 0, len(q.Calls))
	for i, call := range q.Calls {
		if lastWasWrite && needsShards(call) && needShards {
			// Round up the number of shards.
			idx := e.Holder.Index(index)
			if idx == nil {
				return nil, newNotFoundError(ErrIndexNotFound, index)
			}
			shards = idx.AvailableShards(includeRemote).Slice()
			if len(shards) == 0 {
				shards = []uint64{0}
			}
		}

		lastWasWrite = call.IsWrite()

		if err := validateQueryContext(ctx); err != nil {
			return nil, err
		}

		// Apply call translation.
		if !opt.Remote && !opt.PreTranslated {
			translated, err := e.translateCall(call, index, colTranslations, rowTranslations)
			if err != nil {
				return nil, errors.Wrap(err, "translating call")
			}
			if translated == nil {
				results = append(results, emptyResult(call))
				continue
			}

			call = translated
		}

		// If you actually make a top-level Distinct call, you
		// want a SignedRow back. Otherwise, it's something else
		// that will be using it as a row, and we only care
		// about the positive values, because only positive values
		// are valid column IDs. So we don't actually eat top-level
		// pre calls.
		if call.Name == "Count" {
			// Handle count specially, skipping the level directly underneath it.
			for _, child := range call.Children {
				err := e.handlePreCallChildren(ctx, qcx, index, child, shards, opt)
				if err != nil {
					return nil, err
				}
			}
		} else {
			err := e.handlePreCallChildren(ctx, qcx, index, call, shards, opt)
			if err != nil {
				return nil, err
			}
		}
		var v interface{}
		var err error
		// Top-level calls don't need to precompute cross-index things,
		// because we can just pick whatever index we want, but we
		// still need to handle them. Since everything else was
		// already precomputed by handlePreCallChildren, though,
		// we don't need this logic in executeCall.
		newIndex := call.CallIndex()
		if newIndex != "" && newIndex != index {
			v, err = e.executeCall(ctx, qcx, newIndex, call, nil, opt)
		} else {
			v, err = e.executeCall(ctx, qcx, index, call, shards, opt)
		}
		if err != nil {
			return nil, err
		}

		if vc, ok := v.(ValCount); ok {
			vc.Cleanup()
			v = vc
		}

		results = append(results, v)
		// Some Calls can have significant data associated with them
		// that gets generated during processing, such as Precomputed
		// values. Dumping the precomputed data, if any, lets the GC
		// free the memory before we get there.
		e.dumpPrecomputedCalls(ctx, q.Calls[i])
	}
	return results, nil
}

// cleanup removes the integer value (Val) from the ValCount if one of
// the other fields is in use.
//
// ValCounts are normally holding data which is stored as a BSI
// (integer) under the hood. Sometimes it's convenient to be able to
// compare the underlying integer values rather than their
// interpretation as decimal, timestamp, etc, so the lower level
// functions may return both integer and the interpreted value, but we
// don't want to pass that all the way back to the client, so we
// remove it here.
func (vc *ValCount) Cleanup() {
	if vc.Val != 0 && (vc.FloatVal != 0 || !vc.TimestampVal.IsZero() || vc.DecimalVal != nil) {
		vc.Val = 0
	}
}

// preprocessQuery expands any calls that need preprocessing.
func (e *executor) preprocessQuery(ctx context.Context, qcx *Qcx, index string, c *pql.Call, shards []uint64, opt *ExecOptions) (*pql.Call, error) {
	switch c.Name {
	case "All":
		_, hasLimit, err := c.UintArg("limit")
		if err != nil {
			return nil, err
		}
		_, hasOffset, err := c.UintArg("offset")
		if err != nil {
			return nil, err
		}
		if !hasLimit && !hasOffset {
			return c, nil
		}

		// Rewrite the All() w/ limit to Limit(All()).
		c.Children = []*pql.Call{
			{
				Name: "All",
			},
		}
		c.Name = "Limit"
		return c, nil

	default:
		// Recurse through child calls.
		out := make([]*pql.Call, len(c.Children))
		var changed bool
		for i, child := range c.Children {
			res, err := e.preprocessQuery(ctx, qcx, index, child, shards, opt)
			if err != nil {
				return nil, err
			}
			if res != child {
				changed = true
			}
			out[i] = res
		}
		if changed {
			c = c.Clone()
			c.Children = out
		}
		return c, nil
	}
}

// executeCall executes a call.
func (e *executor) executeCall(ctx context.Context, qcx *Qcx, index string, c *pql.Call, shards []uint64, opt *ExecOptions) (interface{}, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "executor.executeCall")
	defer span.Finish()

	if err := validateQueryContext(ctx); err != nil {
		return nil, err
	} else if err := e.validateCallArgs(c); err != nil {
		return nil, errors.Wrap(err, "validating args")
	}

	labels := prometheus.Labels{"index": index}
	statFn := func(ctr *prometheus.CounterVec) {
		if !opt.Remote {
			ctr.With(labels).Inc()
		}
	}

	// Fixes #2009
	// See: https://github.com/featurebasedb/featurebase/issues/2009
	// TODO: Remove at version 2.0
	if e.detectRangeCall(c) {
		e.Holder.Logger.Infof("DEPRECATED: Range() is deprecated, please use Row() instead.")
	}

	// If shards are specified, then use that value for shards. If shards aren't
	// specified, then include all of them.
	if shards == nil && needsShards(c) {
		// Round up the number of shards.
		idx := e.Holder.Index(index)
		if idx == nil {
			return nil, newNotFoundError(ErrIndexNotFound, index)
		}
		shards = idx.AvailableShards(includeRemote).Slice()
		if len(shards) == 0 {
			shards = []uint64{0}
		}
	}
	// Preprocess the query.
	c, err := e.preprocessQuery(ctx, qcx, index, c, shards, opt)
	if err != nil {
		return nil, err
	}

	switch c.Name {
	case "Sum":
		statFn(CounterQuerySumTotal)
		res, err := e.executeSum(ctx, qcx, index, c, shards, opt)
		return res, errors.Wrap(err, "executeSum")
	case "Min":
		statFn(CounterQueryMinTotal)
		res, err := e.executeMin(ctx, qcx, index, c, shards, opt)
		return res, errors.Wrap(err, "executeMin")
	case "Max":
		statFn(CounterQueryMaxTotal)
		res, err := e.executeMax(ctx, qcx, index, c, shards, opt)
		return res, errors.Wrap(err, "executeMax")
	case "MinRow":
		statFn(CounterQueryMinRowTotal)
		res, err := e.executeMinRow(ctx, qcx, index, c, shards, opt)
		return res, errors.Wrap(err, "executeMinRow")
	case "MaxRow":
		statFn(CounterQueryMaxRowTotal)
		res, err := e.executeMaxRow(ctx, qcx, index, c, shards, opt)
		return res, errors.Wrap(err, "executeMaxRow")
	case "Clear":
		statFn(CounterQueryClearTotal)
		res, err := e.executeClearBit(ctx, qcx, index, c, opt)
		return res, errors.Wrap(err, "executeClearBit")
	case "ClearRow":
		statFn(CounterQueryClearRowTotal)
		res, err := e.executeClearRow(ctx, qcx, index, c, shards, opt)
		return res, errors.Wrap(err, "executeClearRow")
	case "Distinct":
		statFn(CounterQueryDistinctTotal)
		res, err := e.executeDistinct(ctx, qcx, index, c, shards, opt)
		return res, errors.Wrap(err, "executeDistinct")
	case "Store":
		statFn(CounterQueryStoreTotal)
		res, err := e.executeSetRow(ctx, qcx, index, c, shards, opt)
		return res, errors.Wrap(err, "executeSetRow")
	case "Count":
		statFn(CounterQueryCountTotal)
		res, err := e.executeCount(ctx, qcx, index, c, shards, opt)
		return res, errors.Wrap(err, "executeCount")
	case "Set":
		statFn(CounterQuerySetTotal)
		res, err := e.executeSet(ctx, qcx, index, c, opt)
		return res, errors.Wrap(err, "executeSet")
	case "TopK":
		statFn(CounterQueryTopKTotal)
		res, err := e.executeTopK(ctx, qcx, index, c, shards, opt)
		return res, errors.Wrap(err, "executeTopK")
	case "TopN":
		statFn(CounterQueryTopNTotal)
		res, err := e.executeTopN(ctx, qcx, index, c, shards, opt)
		return res, errors.Wrap(err, "executeTopN")
	case "Rows":
		statFn(CounterQueryRowsTotal)
		res, err := e.executeRows(ctx, qcx, index, c, shards, opt)
		return res, errors.Wrap(err, "executeRows")
	case "ExternalLookup":
		statFn(CounterQueryExternalLookupTotal)
		res, err := e.executeExternalLookup(ctx, qcx, index, c, shards, opt)
		return res, errors.Wrap(err, "executeExternalLookup")
	case "Extract":
		statFn(CounterQueryExtractTotal)
		res, err := e.executeExtract(ctx, qcx, index, c, shards, opt)
		return res, errors.Wrap(err, "executeExtract")
	case "GroupBy":
		statFn(CounterQueryGroupByTotal)
		res, err := e.executeGroupBy(ctx, qcx, index, c, shards, opt)
		return res, errors.Wrap(err, "executeGroupBy")
	case "Options":
		statFn(CounterQueryOptionsTotal)
		res, err := e.executeOptionsCall(ctx, qcx, index, c, shards, opt)
		return res, errors.Wrap(err, "executeOptionsCall")
	case "IncludesColumn":
		statFn(CounterQueryIncludesColumnTotal)
		res, err := e.executeIncludesColumnCall(ctx, qcx, index, c, shards, opt)
		return res, errors.Wrap(err, "executeIncludesColumnCall")
	case "FieldValue":
		statFn(CounterQueryFieldValueTotal)
		res, err := e.executeFieldValueCall(ctx, qcx, index, c, shards, opt)
		return res, errors.Wrap(err, "executeFieldValueCall")
	case "Precomputed":
		statFn(CounterQueryPrecomputedTotal)
		res, err := e.executePrecomputedCall(ctx, qcx, index, c, shards, opt)
		return res, errors.Wrap(err, "executePrecomputedCall")
	case "UnionRows":
		statFn(CounterQueryUnionRowsTotal)
		res, err := e.executeUnionRows(ctx, qcx, index, c, shards, opt)
		return res, errors.Wrap(err, "executeUnionRows")
	case "ConstRow":
		statFn(CounterQueryConstRowTotal)
		res, err := e.executeConstRow(ctx, index, c)
		return res, errors.Wrap(err, "executeConstRow")
	case "Limit":
		statFn(CounterQueryLimitTotal)
		res, err := e.executeLimitCall(ctx, qcx, index, c, shards, opt)
		return res, errors.Wrap(err, "executeLimitCall")
	case "Percentile":
		statFn(CounterQueryPercentileTotal)
		res, err := e.executePercentile(ctx, qcx, index, c, shards, opt)
		return res, errors.Wrap(err, "executePercentile")
	case "Delete":
		statFn(CounterQueryDeleteTotal)
		res, err := e.executeDeleteRecords(ctx, qcx, index, c, shards, opt)
		return res, errors.Wrap(err, "executeDelete")
	case "Sort":
		statFn(CounterQuerySortTotal)
		res, err := e.executeSort(ctx, qcx, index, c, shards, opt)
		return res, errors.Wrap(err, "executeSort")
	case "Apply":
		statFn(CounterQueryApplyTotal)
		res, err := e.executeApply(ctx, qcx, index, c, shards, opt)
		return res, errors.Wrap(err, "executeApply")
	case "Arrow":
		statFn(CounterQueryArrowTotal)
		res, err := e.executeArrow(ctx, qcx, index, c, shards, opt)
		return res, errors.Wrap(err, "executeArrow")
	default: // e.g. "Row", "Union", "Intersect" or anything that returns a bitmap.
		res, err := e.executeBitmapCall(ctx, qcx, index, c, shards, opt)
		return res, errors.Wrap(err, "executeBitmapCall")
	}
}

// validateCallArgs ensures that the value types in call.Args are expected.
func (e *executor) validateCallArgs(c *pql.Call) error {
	if _, ok := c.Args["ids"]; ok {
		switch v := c.Args["ids"].(type) {
		case []int64, []uint64:
			// noop
		case []interface{}:
			b := make([]int64, len(v))
			for i := range v {
				b[i] = v[i].(int64)
			}
			c.Args["ids"] = b
		default:
			return fmt.Errorf("invalid call.Args[ids]: %s", v)
		}
	}

	return nil
}

// if call args include time args ("from"/"to")
// check if field is a time field
func (e *executor) validateTimeCallArgs(c *pql.Call, indexName string) error {
	f, err := c.FieldArg()
	if err != nil {
		return err
	}
	_, from := c.Args["from"]
	_, to := c.Args["to"]
	field := e.Holder.Field(indexName, f)
	tq := field.TimeQuantum()
	if (from || to) && tq == "" {
		return fmt.Errorf("field %s is not a time-field, 'from' and 'to' are not valid options for this field type", f)
	}

	return nil
}

func (e *executor) executeOptionsCall(ctx context.Context, qcx *Qcx, index string, c *pql.Call, shards []uint64, opt *ExecOptions) (interface{}, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "executor.executeOptionsCall")
	defer span.Finish()

	optCopy := &ExecOptions{}
	*optCopy = *opt
	if arg, ok := c.Args["shards"]; ok {
		if optShards, ok := arg.([]interface{}); ok {
			shards = []uint64{}
			for _, s := range optShards {
				if shard, ok := s.(int64); ok {
					shards = append(shards, uint64(shard))
				} else {
					return nil, errors.New("Query(): shards must be a list of unsigned integers")
				}
			}
		} else {
			return nil, errors.New("Query(): shards must be a list of unsigned integers")
		}
	}
	return e.executeCall(ctx, qcx, index, c.Children[0], shards, optCopy)
}

// executeIncludesColumnCall executes an IncludesColumn() call.
func (e *executor) executeIncludesColumnCall(ctx context.Context, qcx *Qcx, index string, c *pql.Call, shards []uint64, opt *ExecOptions) (bool, error) {
	// Get the shard containing the column, since that's the only
	// shard that needs to execute this query.
	var shard uint64
	col, ok, err := c.UintArg("column")
	if err != nil {
		return false, errors.Wrap(err, "getting column from args")
	} else if !ok {
		return false, errors.New("IncludesColumn call must specify a column")
	}
	shard = col / ShardWidth

	// If shard is not in shards, bail early.
	if !uint64InSlice(shard, shards) {
		return false, nil
	}

	// Execute calls in bulk on each remote node and merge.
	mapFn := func(ctx context.Context, shard uint64, mopt *mapOptions) (_ interface{}, err error) {
		return e.executeIncludesColumnCallShard(ctx, qcx, index, c, shard, col)
	}

	// Merge returned results at coordinating node.
	reduceFn := func(ctx context.Context, prev, v interface{}) interface{} {
		other, _ := prev.(bool)
		return other || v.(bool)
	}

	result, err := e.mapReduce(ctx, index, []uint64{shard}, c, opt, mapFn, reduceFn)
	if err != nil {
		return false, err
	}
	return result.(bool), nil
}

// executeFieldValueCall executes a FieldValue() call.
func (e *executor) executeFieldValueCall(ctx context.Context, qcx *Qcx, index string, c *pql.Call, shards []uint64, opt *ExecOptions) (_ ValCount, err error) {
	fieldName, ok := c.Args["field"].(string)
	if !ok || fieldName == "" {
		return ValCount{}, ErrFieldRequired
	}

	colKey, ok := c.Args["column"]
	if !ok || colKey == "" {
		return ValCount{}, ErrColumnRequired
	}

	// Fetch index.
	idx := e.Holder.Index(index)
	if idx == nil {
		return ValCount{}, newNotFoundError(ErrIndexNotFound, index)
	}

	// Fetch field.
	field := idx.Field(fieldName)
	if field == nil {
		return ValCount{}, newNotFoundError(ErrFieldNotFound, fieldName)
	}

	colID, ok, err := c.UintArg("column")
	if !ok || err != nil {
		return ValCount{}, errors.Wrap(err, "getting column argument")
	}

	shard := colID / ShardWidth

	// Execute calls in bulk on each remote node and merge.
	mapFn := func(ctx context.Context, shard uint64, mopt *mapOptions) (_ interface{}, err error) {
		return e.executeFieldValueCallShard(ctx, qcx, field, colID, shard)
	}

	// Select single returned result at coordinating node.
	reduceFn := func(ctx context.Context, prev, v interface{}) interface{} {
		other, _ := prev.(ValCount)
		if other.Count == 1 {
			return other
		}
		return v
	}

	result, err := e.mapReduce(ctx, index, []uint64{shard}, c, opt, mapFn, reduceFn)
	if err != nil {
		return ValCount{}, errors.Wrap(err, "map reduce")
	}
	other, _ := result.(ValCount)

	return other, nil
}

func (e *executor) executeFieldValueCallShard(ctx context.Context, qcx *Qcx, field *Field, col uint64, shard uint64) (_ ValCount, err0 error) {
	value, exists, err := field.Value(qcx, col)
	if err != nil {
		return ValCount{}, errors.Wrap(err, "getting field value")
	} else if !exists {
		return ValCount{}, nil
	}

	other := ValCount{
		Count: 1,
	}

	if field.Type() == FieldTypeInt {
		other.Val = value
	} else if field.Type() == FieldTypeDecimal {
		dec := pql.NewDecimal(value, field.Options().Scale)
		other.DecimalVal = &dec
		other.FloatVal = 0
		other.Val = 0
	} else if field.Type() == FieldTypeTimestamp {
		ts, err := ValToTimestamp(field.Options().TimeUnit, value)
		if err != nil {
			return ValCount{}, err
		}
		other.TimestampVal = ts
	}

	return other, nil
}

// executeLimitCall executes a Limit() call.
func (e *executor) executeLimitCall(ctx context.Context, qcx *Qcx, index string, c *pql.Call, shards []uint64, opt *ExecOptions) (*Row, error) {
	bitmapCall := c.Children[0]

	limit, hasLimit, err := c.UintArg("limit")
	if err != nil {
		return nil, errors.Wrap(err, "getting limit")
	}
	offset, _, err := c.UintArg("offset")
	if err != nil {
		return nil, errors.Wrap(err, "getting offset")
	}

	if !hasLimit {
		limit = math.MaxUint64
	}

	// Execute bitmap call, storing the full result on this node.
	res, err := e.executeCall(ctx, qcx, index, bitmapCall, shards, opt)
	if err != nil {
		return nil, errors.Wrap(err, "limit map reduce")
	}
	if res == nil {
		res = NewRow()
	}

	result, ok := res.(*Row)
	if !ok {
		return nil, errors.Errorf("expected Row but got %T", result)
	}

	if offset != 0 {
		i := 0
		var leadingBits []uint64
		for i < len(result.Segments) && offset > 0 {
			seg := result.Segments[i]
			count := seg.Count()
			if count > offset {
				data := seg.Columns()
				data = data[offset:]
				leadingBits = data
				i++
				break
			}

			offset -= count
			i++
		}
		row := NewRow(leadingBits...)
		row.Merge(&Row{Segments: result.Segments[i:]})
		result = row
	}
	if limit < result.Count() {
		i := 0
		var trailingBits []uint64
		for i < len(result.Segments) && limit > 0 {
			seg := result.Segments[i]
			count := seg.Count()
			if count > limit {
				data := seg.Columns()
				data = data[:limit]
				trailingBits = data
				break
			}

			limit -= count
			i++
		}
		row := NewRow(trailingBits...)
		row.Merge(&Row{Segments: result.Segments[:i]})
		result = row
	}

	return result, nil
}

// executeIncludesColumnCallShard
func (e *executor) executeIncludesColumnCallShard(ctx context.Context, qcx *Qcx, index string, c *pql.Call, shard uint64, column uint64) (_ bool, err error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "executor.executeIncludesColumnCallShard")
	defer span.Finish()

	if len(c.Children) == 1 {
		row, err := e.executeBitmapCallShard(ctx, qcx, index, c.Children[0], shard)
		if err != nil {
			return false, errors.Wrap(err, "executing bitmap call")
		}
		return row.Includes(column), nil
	}

	return false, errors.New("IncludesColumn call must specify a row query")
}

// executeSum executes a Sum() call.
func (e *executor) executeSum(ctx context.Context, qcx *Qcx, index string, c *pql.Call, shards []uint64, opt *ExecOptions) (_ ValCount, err error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "executor.executeSum")
	defer span.Finish()

	fieldName, err := c.FirstStringArg("field", "_field")
	if err != nil {
		return ValCount{}, errors.Wrap(err, "Sum(): field required")
	}

	if len(c.Children) > 1 {
		return ValCount{}, errors.New("Sum() only accepts a single bitmap input")
	}

	// Execute calls in bulk on each remote node and merge.
	mapFn := func(ctx context.Context, shard uint64, mopt *mapOptions) (_ interface{}, err error) {
		return e.executeSumCountShard(ctx, qcx, index, c, nil, shard)
	}

	// Merge returned results at coordinating node.
	reduceFn := func(ctx context.Context, prev, v interface{}) interface{} {
		other, _ := prev.(ValCount)
		return other.Add(v.(ValCount))
	}

	result, err := e.mapReduce(ctx, index, shards, c, opt, mapFn, reduceFn)
	if err != nil {
		return ValCount{}, err
	}
	other, _ := result.(ValCount)

	if other.Count == 0 {
		return ValCount{}, nil
	}

	// scale summed response if it's a decimal field and this is
	// not a remote query (we're about to return to original client).
	if !opt.Remote {
		field := e.Holder.Field(index, fieldName)
		if field == nil {
			return ValCount{}, newNotFoundError(ErrFieldNotFound, fieldName)
		}
		if field.Type() == FieldTypeDecimal {
			dec := pql.NewDecimal(other.Val, field.Options().Scale)
			other.DecimalVal = &dec
			other.FloatVal = 0
			other.Val = 0
		}
	}

	return other, nil
}

// executeDistinct executes a Distinct call on a field. It returns a
// SignedRow for int fields and a *Row for set/mutex/time fields.
func (e *executor) executeDistinct(ctx context.Context, qcx *Qcx, index string, c *pql.Call, shards []uint64, opt *ExecOptions) (interface{}, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "executor.executeDistinct")
	defer span.Finish()

	field, hasField, err := c.StringArg("field")
	if err != nil {
		return SignedRow{}, errors.Wrap(err, "loading field option in Distinct query")
	} else if !hasField {
		return SignedRow{}, fmt.Errorf("missing field option in Distinct query")
	}

	// Execute calls in bulk on each remote node and merge.
	mapFn := func(ctx context.Context, shard uint64, mopt *mapOptions) (_ interface{}, err error) {
		return e.executeDistinctShard(ctx, qcx, index, field, c, shard)
	}

	// Merge returned results at coordinating node.
	reduceFn := func(ctx context.Context, prev, v interface{}) interface{} {
		if err := ctx.Err(); err != nil {
			return err
		}
		switch other := prev.(type) {
		case SignedRow:
			return other.Union(v.(SignedRow))
		case *Row:
			if other == nil {
				return v
			} else if v.(*Row) == nil {
				return other
			}
			return other.Union(v.(*Row))
		case nil:
			return v
		case DistinctTimestamp:
			return other.Union(v.(DistinctTimestamp))
		default:
			return errors.Errorf("unexpected return type from executeDistinctShard: %+v %T", other, other)
		}
	}

	result, err := e.mapReduce(ctx, index, shards, c, opt, mapFn, reduceFn)
	if err != nil {
		return nil, errors.Wrap(err, "mapReduce")
	}

	if other, ok := result.(SignedRow); ok {
		other.Field = field
	}
	return result, nil
}

// executeMin executes a Min() call.
func (e *executor) executeMin(ctx context.Context, qcx *Qcx, index string, c *pql.Call, shards []uint64, opt *ExecOptions) (_ ValCount, err error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "executor.executeMin")
	defer span.Finish()

	if _, err := c.FirstStringArg("field", "_field"); err != nil {
		return ValCount{}, errors.Wrap(err, "Min(): field required")
	}

	if len(c.Children) > 1 {
		return ValCount{}, errors.New("Min() only accepts a single bitmap input")
	}

	// Execute calls in bulk on each remote node and merge.
	mapFn := func(ctx context.Context, shard uint64, mopt *mapOptions) (_ interface{}, err error) {
		return e.executeMinShard(ctx, qcx, index, c, shard)
	}

	// Merge returned results at coordinating node.
	reduceFn := func(ctx context.Context, prev, v interface{}) interface{} {
		other, _ := prev.(ValCount)
		return other.Smaller(v.(ValCount))
	}

	result, err := e.mapReduce(ctx, index, shards, c, opt, mapFn, reduceFn)
	if err != nil {
		return ValCount{}, err
	}
	other, _ := result.(ValCount)

	if other.Count == 0 {
		return ValCount{}, nil
	}
	return other, nil
}

// executeMax executes a Max() call.
func (e *executor) executeMax(ctx context.Context, qcx *Qcx, index string, c *pql.Call, shards []uint64, opt *ExecOptions) (_ ValCount, err error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "executor.executeMax")
	defer span.Finish()

	if _, err := c.FirstStringArg("field", "_field"); err != nil {
		return ValCount{}, errors.Wrap(err, "Max(): field required")
	}

	if len(c.Children) > 1 {
		return ValCount{}, errors.New("Max() only accepts a single bitmap input")
	}

	// Execute calls in bulk on each remote node and merge.
	mapFn := func(ctx context.Context, shard uint64, mopt *mapOptions) (_ interface{}, err error) {
		return e.executeMaxShard(ctx, qcx, index, c, shard)
	}

	// Merge returned results at coordinating node.
	reduceFn := func(ctx context.Context, prev, v interface{}) interface{} {
		other, _ := prev.(ValCount)
		return other.Larger(v.(ValCount))
	}

	result, err := e.mapReduce(ctx, index, shards, c, opt, mapFn, reduceFn)
	if err != nil {
		return ValCount{}, err
	}
	other, _ := result.(ValCount)

	if other.Count == 0 {
		return ValCount{}, nil
	}
	return other, nil
}

// executePercentile executes a Percentile() call.
func (e *executor) executePercentile(ctx context.Context, qcx *Qcx, index string, c *pql.Call, shards []uint64, opt *ExecOptions) (_ ValCount, err error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "executor.executePercentile")
	defer span.Finish()

	// get nth
	var nthFloat float64
	nthArg, ok := c.Args["nth"]
	if !ok {
		return ValCount{}, errors.New("Percentile(): nth required")
	}
	switch nthArg := nthArg.(type) {
	case pql.Decimal:
		nthFloat = nthArg.Float64()
	case int64:
		nthFloat = float64(nthArg)
	default:
		return ValCount{}, errors.Errorf("Percentile(): invalid nth='%v' of type (%[1]T), should be a number between 0 and 100 inclusive", c.Args["nth"])
	}
	if nthFloat < 0 || nthFloat > 100.0 {
		return ValCount{}, errors.Errorf("Percentile(): invalid nth value (%f), should be a number between 0 and 100 inclusive", nthFloat)
	}

	// get field
	fieldName, err := c.FirstStringArg("field", "_field")
	if err != nil {
		return ValCount{}, errors.New("Percentile(): field required")
	}
	field := e.Holder.Field(index, fieldName)
	if field == nil {
		return ValCount{}, ErrFieldNotFound
	}

	// filter call for min & max
	var filterCall *pql.Call

	// check if filter provided
	if filterArg, ok := c.Args["filter"].(*pql.Call); ok && filterArg != nil {
		filterCall = filterArg
	}

	// get min
	q, _ := pql.ParseString(fmt.Sprintf(`Min(field="%s")`, fieldName))
	minCall := q.Calls[0]
	if filterCall != nil {
		minCall.Children = append(minCall.Children, filterCall)
	}
	minVal, err := e.executeMin(ctx, qcx, index, minCall, shards, opt)
	if err != nil {
		return ValCount{}, errors.Wrap(err, "executing Min call for Percentile")
	}
	if nthFloat == 0.0 {
		return minVal, nil
	}

	// get max
	q, _ = pql.ParseString(fmt.Sprintf(`Max(field="%s")`, fieldName))
	maxCall := q.Calls[0]
	if filterCall != nil {
		maxCall.Children = append(maxCall.Children, filterCall)
	}
	maxVal, err := e.executeMax(ctx, qcx, index, maxCall, shards, opt)
	if err != nil {
		return ValCount{}, errors.Wrap(err, "executing Max call for Percentile")
	}
	// set up reusables
	var countCall, rangeCall *pql.Call
	if filterCall == nil {
		countQuery, _ := pql.ParseString(fmt.Sprintf("Count(Row(%s < 0))", fieldName))
		countCall = countQuery.Calls[0]
		rangeCall = countCall.Children[0]
	} else {
		countQuery, _ := pql.ParseString(fmt.Sprintf(`Count(Intersect(Row(%s < 0)))`, fieldName))
		countCall = countQuery.Calls[0]
		intersectCall := countCall.Children[0]
		intersectCall.Children = append(intersectCall.Children, filterCall)
		rangeCall = intersectCall.Children[0]
	}

	k := (100 - nthFloat) / nthFloat

	min, max := minVal.Val, maxVal.Val
	// estimate nth val, eg median when nth=0.5
	for min < max {
		// compute average without integer overflow, then correct for division of
		// odd numbers by 2
		possibleNthVal := ((max / 2) + (min / 2)) + (((max % 2) + (min % 2)) / 2)
		// possibleNthVal = (max + min) / 2
		// get left count
		rangeCall.Args[fieldName] = &pql.Condition{
			Op:    pql.Token(pql.LT),
			Value: possibleNthVal,
		}
		leftCountUint64, err := e.executeCount(ctx, qcx, index, countCall, shards, opt)
		if err != nil {
			return ValCount{}, errors.Wrap(err, "executing Count call L for Percentile")
		}
		leftCount := int64(leftCountUint64)

		// get right count
		rangeCall.Args[fieldName] = &pql.Condition{
			Op:    pql.Token(pql.GT),
			Value: possibleNthVal,
		}
		rightCountUint64, err := e.executeCount(ctx, qcx, index, countCall, shards, opt)
		if err != nil {
			return ValCount{}, errors.Wrap(err, "executing Count call R for Percentile")
		}
		rightCount := int64(rightCountUint64)

		// 'weight' the left count as per k
		leftCountWeighted := int64(math.Round(k * float64(leftCount)))

		// binary search
		if leftCountWeighted > rightCount {
			max = possibleNthVal - 1
		} else if leftCountWeighted < rightCount {
			min = possibleNthVal + 1
		} else {
			return field.valCountize(possibleNthVal, 1, nil)
		}
	}

	return field.valCountize(min, 1, nil)
}

// executeMinRow executes a MinRow() call.
func (e *executor) executeMinRow(ctx context.Context, qcx *Qcx, index string, c *pql.Call, shards []uint64, opt *ExecOptions) (_ interface{}, err error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "executor.executeMinRow")
	defer span.Finish()

	if field := c.Args["field"]; field == "" {
		return ValCount{}, errors.New("MinRow(): field required")
	}

	// Execute calls in bulk on each remote node and merge.
	mapFn := func(ctx context.Context, shard uint64, mopt *mapOptions) (_ interface{}, err error) {
		return e.executeMinRowShard(ctx, qcx, index, c, shard)
	}

	// Merge returned results at coordinating node.
	reduceFn := func(ctx context.Context, prev, v interface{}) interface{} {
		// if minRowID exists, and if it is smaller than the other one return it.
		// otherwise return the minRowID of the one which exists.
		if prev == nil {
			return v
		} else if v == nil {
			return prev
		}
		prevp, _ := prev.(PairField)
		vp, _ := v.(PairField)
		if prevp.Pair.Count > 0 && vp.Pair.Count > 0 {
			if prevp.Pair.ID < vp.Pair.ID {
				return prevp
			}
			return vp
		} else if prevp.Pair.Count > 0 {
			return prevp
		}
		return vp
	}

	return e.mapReduce(ctx, index, shards, c, opt, mapFn, reduceFn)
}

// executeMaxRow executes a MaxRow() call.
func (e *executor) executeMaxRow(ctx context.Context, qcx *Qcx, index string, c *pql.Call, shards []uint64, opt *ExecOptions) (_ interface{}, err error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "executor.executeMaxRow")
	defer span.Finish()

	if field := c.Args["field"]; field == "" {
		return ValCount{}, errors.New("MaxRow(): field required")
	}

	// Execute calls in bulk on each remote node and merge.
	mapFn := func(ctx context.Context, shard uint64, mopt *mapOptions) (_ interface{}, err error) {
		return e.executeMaxRowShard(ctx, qcx, index, c, shard)
	}

	// Merge returned results at coordinating node.
	reduceFn := func(ctx context.Context, prev, v interface{}) interface{} {
		// if minRowID exists, and if it is smaller than the other one return it.
		// otherwise return the minRowID of the one which exists.
		if prev == nil {
			return v
		} else if v == nil {
			return prev
		}
		prevp, _ := prev.(PairField)
		vp, _ := v.(PairField)
		if prevp.Pair.Count > 0 && vp.Pair.Count > 0 {
			if prevp.Pair.ID > vp.Pair.ID {
				return prevp
			}
			return vp
		} else if prevp.Pair.Count > 0 {
			return prevp
		}
		return vp
	}

	return e.mapReduce(ctx, index, shards, c, opt, mapFn, reduceFn)
}

// executePrecomputedCall pretends to execute a call that we have a precomputed value for.
func (e *executor) executePrecomputedCall(ctx context.Context, qcx *Qcx, index string, c *pql.Call, shards []uint64, opt *ExecOptions) (_ *Row, err error) {
	span, _ := tracing.StartSpanFromContext(ctx, "executor.executePrecomputedCall")
	defer span.Finish()
	result := NewRow()

	for _, row := range c.Precomputed {
		result.Merge(row.(*Row))
	}
	return result, nil
}

// executeBitmapCall executes a call that returns a bitmap.
func (e *executor) executeBitmapCall(ctx context.Context, qcx *Qcx, index string, c *pql.Call, shards []uint64, opt *ExecOptions) (_ *Row, err error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "executor.executeBitmapCall")
	span.LogKV("pqlCallName", c.Name)
	defer span.Finish()

	labels := prometheus.Labels{"index": index}
	statFn := func(ctr *prometheus.CounterVec) {
		if !opt.Remote {
			ctr.With(labels).Inc()
		}
	}

	if !opt.Remote {
		switch c.Name {
		case "Row":
			if c.HasConditionArg() {
				statFn(CounterQueryRowBSITotal)
			} else {
				statFn(CounterQueryRowTotal)
			}
		case "Range":
			statFn(CounterQueryRangeTotal)
		case "Difference":
			statFn(CounterQueryBitmapTotal)
		case "Intersect":
			statFn(CounterQueryIntersectTotal)
		case "Union":
			statFn(CounterQueryUnionTotal)
		case "InnerUnionRows":
			statFn(CounterQueryInnerUnionRowsTotal)
		case "Xor":
			statFn(CounterQueryXorTotal)
		case "Not":
			statFn(CounterQueryNotTotal)
		case "Shift":
			statFn(CounterQueryShiftTotal)
		case "All":
			statFn(CounterQueryAllTotal)
		default:
			statFn(CounterQueryBitmapTotal)
		}
	}

	// Execute calls in bulk on each remote node and merge.
	mapFn := func(ctx context.Context, shard uint64, mopt *mapOptions) (_ interface{}, err error) {
		return e.executeBitmapCallShard(ctx, qcx, index, c, shard)
	}

	// Merge returned results at coordinating node.
	reduceFn := func(ctx context.Context, prev, v interface{}) interface{} {
		other, _ := prev.(*Row)
		if other == nil {
			// TODO... what's going on on the following line
			other = NewRow() // bug! this row ends up containing Badger Txn data that should be accessed outside the Txn.
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		other.Merge(v.(*Row))
		return other
	}

	other, err := e.mapReduce(ctx, index, shards, c, opt, mapFn, reduceFn)
	if err != nil {
		return nil, errors.Wrap(err, "map reduce")
	}

	row, _ := other.(*Row)

	return row, nil
}

// executeBitmapCallShard executes a bitmap call for a single shard.
func (e *executor) executeBitmapCallShard(ctx context.Context, qcx *Qcx, index string, c *pql.Call, shard uint64) (_ *Row, err error) {
	if err := validateQueryContext(ctx); err != nil {
		return nil, err
	}

	span, ctx := tracing.StartSpanFromContext(ctx, "executor.executeBitmapCallShard")
	defer span.Finish()

	switch c.Name {
	case "Row", "Range":
		return e.executeRowShard(ctx, qcx, index, c, shard)
	case "Difference":
		return e.executeDifferenceShard(ctx, qcx, index, c, shard)
	case "Intersect":
		return e.executeIntersectShard(ctx, qcx, index, c, shard)
	case "Union":
		return e.executeUnionShard(ctx, qcx, index, c, shard)
	case "InnerUnionRows":
		return e.executeInnerUnionRowsShard(ctx, qcx, index, c, shard)
	case "Xor":
		return e.executeXorShard(ctx, qcx, index, c, shard)
	case "Not":
		return e.executeNotShard(ctx, qcx, index, c, shard)
	case "Shift":
		return e.executeShiftShard(ctx, qcx, index, c, shard)
	case "All": // Allow a shard computation to use All()
		return e.executeAllCallShard(ctx, qcx, index, c, shard)
	case "Distinct":
		return nil, errors.New("Distinct shouldn't be hit as a bitmap call")
	case "Precomputed":
		return e.executePrecomputedCallShard(ctx, qcx, index, c, shard)
	default:
		return nil, fmt.Errorf("unknown call: %s", c.Name)
	}
}

// executeDistinctShard executes a Distinct call on a single shard, yielding
// a SignedRow of the values found.
func (e *executor) executeDistinctShard(ctx context.Context, qcx *Qcx, index string, fieldName string, c *pql.Call, shard uint64) (result interface{}, err error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "executor.executeDistinctShard")
	defer span.Finish()

	idx := e.Holder.Index(index)
	field := e.Holder.Field(index, fieldName)
	if field == nil {
		return nil, ErrFieldNotFound
	}

	bsig := field.bsiGroup(fieldName)
	if bsig == nil {
		result = &Row{
			Index: index,
			Field: fieldName,
		}
	} else if field.Options().Type == FieldTypeTimestamp {
		result = DistinctTimestamp{Name: fieldName}
	} else {
		result = SignedRow{}
	}

	var filter *Row
	var filterBitmap *roaring.Bitmap
	// If a filter *is* specified, an empty filter means nothing, and any
	// filter at all means there's filtering to do. If a filter is *not*
	// specified, then we don't need to do any filtering. So a nil
	// filterBitmap (which we get if there's no children) means no filter.
	if len(c.Children) == 1 {
		row, err := e.executeBitmapCallShard(ctx, qcx, index, c.Children[0], shard)
		if err != nil {
			return result, errors.Wrap(err, "executing bitmap call")
		}
		filter = row
		if filter != nil && len(filter.Segments) > 0 {
			filterBitmap = filter.Segments[0].data
		}
		// if we had a filter to consider, but it came back empty, we
		// can go ahead and save time by returning the empty results,
		// because the filter excluded everything.
		if filterBitmap == nil || !filterBitmap.Any() {
			return result, nil
		}
	}

	if bsig == nil {
		return executeDistinctShardSet(ctx, qcx, idx, fieldName, shard, filterBitmap)
	}
	if field.Options().Type == FieldTypeTimestamp {
		r, err := executeDistinctShardBSI(ctx, qcx, idx, fieldName, shard, bsig, filterBitmap)
		if err != nil {
			return nil, err
		}
		// If we have a filter, or there's just no content for this shard, we
		// can end up with empty results. Rather than trying to synthesize
		// a result from this empty set, we just go ahead and use that.
		if r.Pos == nil {
			return result, nil
		}
		cols := r.Pos.Columns()
		results := make([]string, len(cols))
		for i, val := range cols {
			t, err := ValToTimestamp(field.options.TimeUnit, int64(val)+bsig.Base)
			if err != nil {
				return nil, errors.Wrap(err, "translating value to timestamp")
			}
			results[i] = t.Format(time.RFC3339Nano)
		}
		result = DistinctTimestamp{Name: fieldName, Values: results}
		return result, nil
	}
	return executeDistinctShardBSI(ctx, qcx, idx, fieldName, shard, bsig, filterBitmap)
}

type DistinctTimestamp struct {
	Values []string
	Name   string
}

var _ proto.ToRowser = DistinctTimestamp{}

// ToRows implements the ToRowser interface.
func (d DistinctTimestamp) ToRows(callback func(*proto.RowResponse) error) error {
	for _, ts := range d.Values {
		row := &proto.RowResponse{
			Headers: []*proto.ColumnInfo{
				{
					Name:     d.Name,
					Datatype: "timestamp",
				},
			},
			Columns: []*proto.ColumnResponse{
				{
					ColumnVal: &proto.ColumnResponse_TimestampVal{
						TimestampVal: ts,
					},
				},
			},
		}
		if err := callback(row); err != nil {
			return errors.Wrap(err, "calling callback")
		}
	}

	return nil
}

// ToTable implements the ToTabler interface for DistinctTimestamp
func (d DistinctTimestamp) ToTable() (*proto.TableResponse, error) {
	return proto.RowsToTable(&d, len(d.Values))
}

// Union returns the union of the values of `d` and `other`
func (d *DistinctTimestamp) Union(other DistinctTimestamp) DistinctTimestamp {
	both := map[string]struct{}{}
	for _, val := range d.Values {
		both[val] = struct{}{}
	}
	for _, val := range other.Values {
		both[val] = struct{}{}
	}
	vals := []string{}
	for key := range both {
		vals = append(vals, key)
	}
	return DistinctTimestamp{Name: d.Name, Values: vals}
}

const (
	ViewNotFound     = Error("view not found")
	FragmentNotFound = Error("fragment not found")
)

func executeDistinctShardSet(ctx context.Context, qcx *Qcx, idx *Index, fieldName string, shard uint64, filterBitmap *roaring.Bitmap) (result *Row, err0 error) {
	index := idx.Name()
	tx, finisher, err := qcx.GetTx(Txo{Write: !writable, Index: idx, Shard: shard})
	if err != nil {
		return nil, err
	}
	defer finisher(&err0)

	fragData, _, err := tx.ContainerIterator(index, fieldName, "standard", shard, 0)
	switch errors.Cause(err) {
	case ViewNotFound, FragmentNotFound:
		// It may seem reasonable to return `nil` here in the case where the
		// fragment for this shard does not exist. The problem with doing that
		// is that if this operation is being performed on a remote node, then
		// this result is going to get serialized as a QueryResponse and sent
		// back to the original, non-remote node. When this happens, the
		// encodeRow/decodeRow logic replaces `nil` with an empty Row. An empty
		// Row will cause problems during the union step of the reduce phase if
		// it is the "left" side of the union, because then the resulting Row
		// after the union will have blank Index and Field values. Here, we
		// ensure that we send a non-nil Row with valid Index and Field values
		// so that the union step doesn't cause problems.
		return &Row{Index: index, Field: fieldName}, nil
	case nil:
	default:
		return nil, errors.Wrap(err, "getting fragment data")
	}
	defer fragData.Close()

	// We can't grab the containers "for each row" from the set-type field,
	// because we don't know how many rows there are, and some of them
	// might be empty, so really, we're going to iterate through the
	// containers, and then intersect them with the filter if present.
	var filter []*roaring.Container
	if filterBitmap != nil {
		filter = make([]*roaring.Container, 1<<shardVsContainerExponent)
		filterIterator, _ := filterBitmap.Containers.Iterator(0)
		// So let's get these all with a nice convenient 0 offset...
		for filterIterator.Next() {
			k, c := filterIterator.Value()
			if c.N() == 0 {
				continue
			}
			filter[k%(1<<shardVsContainerExponent)] = c
		}
	}
	rows := roaring.NewSliceBitmap()
	prevRow := ^uint64(0)
	seenThisRow := false
	for fragData.Next() {
		k, c := fragData.Value()
		row := k >> shardVsContainerExponent
		if row == prevRow {
			if seenThisRow {
				continue
			}
		} else {
			seenThisRow = false
			prevRow = row
		}
		if filterBitmap != nil {
			if roaring.IntersectionAny(c, filter[k%(1<<shardVsContainerExponent)]) {
				_, err = rows.Add(row)
				if err != nil {
					return nil, errors.Wrap(err, "collecting results")
				}
				seenThisRow = true
			}
		} else if c.N() != 0 {
			_, err = rows.Add(row)
			if err != nil {
				return nil, errors.Wrap(err, "recording results")
			}
			seenThisRow = true
		}
	}
	result = NewRowFromBitmap(rows)
	result.Index = idx.Name()
	result.Field = fieldName
	return result, nil
}

func executeDistinctShardBSI(ctx context.Context, qcx *Qcx, idx *Index, fieldName string, shard uint64, bsig *bsiGroup, filterBitmap *roaring.Bitmap) (result SignedRow, err0 error) {
	view := viewBSIGroupPrefix + fieldName
	index := idx.Name()
	depth := uint64(bsig.BitDepth)
	offset := bsig.Base

	tx, finisher, err := qcx.GetTx(Txo{Write: !writable, Index: idx, Shard: shard})
	if err != nil {
		return SignedRow{}, err
	}
	defer finisher(&err0)

	existsBitmap, err := tx.OffsetRange(index, fieldName, view, shard, ShardWidth*shard, ShardWidth*0, ShardWidth*1)
	if err != nil {
		switch errors.Cause(err) {
		case ViewNotFound, FragmentNotFound:
			return result, nil
		}
		return result, errors.Wrap(err, "getting exists bitmap")
	}
	if filterBitmap != nil {
		existsBitmap = existsBitmap.Intersect(filterBitmap)
	}
	if !existsBitmap.Any() {
		return result, nil
	}

	signBitmap, err := tx.OffsetRange(index, fieldName, view, shard, ShardWidth*shard, ShardWidth*1, ShardWidth*2)
	if err != nil {
		return result, errors.Wrap(err, "getting sign bitmap")
	}

	dataBitmaps := make([]*roaring.Bitmap, depth)

	for i := uint64(0); i < depth; i++ {
		dataBitmaps[i], err = tx.OffsetRange(index, fieldName, view, shard, ShardWidth*shard, ShardWidth*(i+2), ShardWidth*(i+3))
		if err != nil {
			return result, err
		}
	}

	// we need spaces for sign bit, existence/filter bit, and data
	// row bits, which we'll be grabbing 64K bits at a time
	stashWords := make([]uint64, 1024*(depth+2))
	bitStashes := make([][]uint64, depth)
	for i := uint64(0); i < depth; i++ {
		start := i * 1024
		last := start + 1024
		bitStashes[i] = stashWords[start:last]
		i++
	}
	stashOffset := depth * 1024
	existStash := stashWords[stashOffset : stashOffset+1024]
	signStash := stashWords[stashOffset+1024 : stashOffset+2048]
	dataBits := make([][]uint64, depth)

	posValues := make([]uint64, 0, 64)
	negValues := make([]uint64, 0, 64)

	posBitmap := roaring.NewFileBitmap()
	negBitmap := roaring.NewFileBitmap()

	existIterator, _ := existsBitmap.Containers.Iterator(0)
	for existIterator.Next() {
		key, value := existIterator.Value()
		if value.N() == 0 {
			continue
		}
		exists := value.AsBitmap(existStash)
		sign := signBitmap.Containers.Get(key).AsBitmap(signStash)
		for i := uint64(0); i < depth; i++ {
			dataBits[i] = dataBitmaps[i].Containers.Get(key).AsBitmap(bitStashes[i])
		}
		for idx, word := range exists {
			// mask holds a mask we can test the other words against.
			mask := uint64(1)
			for word != 0 {
				shift := uint(bits.TrailingZeros64(word))
				// we shift one *more* than that, to move the
				// actual one bit off.
				word >>= shift + 1
				mask <<= shift
				value := int64(0)
				for b := uint64(0); b < depth; b++ {
					if dataBits[b][idx]&mask != 0 {
						value += (1 << b)
					}
				}
				if sign[idx]&mask != 0 {
					value *= -1
				}
				value += int64(offset)
				if value < 0 {
					negValues = append(negValues, uint64(-value))
				} else {
					posValues = append(posValues, uint64(value))
				}
				// and now we processed that bit, so we move the mask over one.
				mask <<= 1
			}
			if len(negValues) > 0 {
				_, _ = negBitmap.AddN(negValues...)
				negValues = negValues[:0]
			}
			if len(posValues) > 0 {
				_, _ = posBitmap.AddN(posValues...)
				posValues = posValues[:0]
			}
		}
	}

	result = SignedRow{
		Neg: NewRowFromBitmap(negBitmap),
		Pos: NewRowFromBitmap(posBitmap),
	}
	result.Neg.Index, result.Pos.Index = idx.Name(), idx.Name()
	result.Neg.Field, result.Pos.Field = fieldName, fieldName
	return result, nil
}

// executeSumCountShard calculates the sum and count for bsiGroups on a shard.
func (e *executor) executeSumCountShard(ctx context.Context, qcx *Qcx, index string, c *pql.Call, filter *Row, shard uint64) (_ ValCount, err0 error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "executor.executeSumCountShard")
	defer span.Finish()

	// use tx to keep consistency between
	// the filter and the later count.
	idx := e.Holder.Index(index)

	// Only calculate the filter if it doesn't exist and a child call as been passed in.
	if filter == nil && len(c.Children) == 1 {

		row, err := e.executeBitmapCallShard(ctx, qcx, index, c.Children[0], shard)
		if err != nil {
			return ValCount{}, errors.Wrap(err, "executing bitmap call")
		}
		filter = row
	}

	fieldName, err := c.FirstStringArg("field", "_field")
	if err != nil {
		return ValCount{}, errors.Wrap(err, "Sum(): field required")
	}

	field := e.Holder.Field(index, fieldName)
	if field == nil {
		return ValCount{}, ErrFieldNotFound
	}

	bsig := field.bsiGroup(fieldName)
	if bsig == nil {
		return ValCount{}, nil
	}

	fragment := e.Holder.fragment(index, fieldName, viewBSIGroupPrefix+fieldName, shard)
	if fragment == nil {
		return ValCount{}, nil
	}

	tx, finisher, err := qcx.GetTx(Txo{Write: !writable, Index: idx, Fragment: fragment, Shard: shard})
	if err != nil {
		return ValCount{}, err
	}
	defer finisher(&err0)

	sumspan, _ := tracing.StartSpanFromContext(ctx, "executor.executeSumCountShard_fragment.sum")
	defer sumspan.Finish()
	vsum, vcount, err := fragment.sum(tx, filter, bsig.BitDepth)
	if err != nil {
		return ValCount{}, errors.Wrap(err, "computing sum")
	}
	out := ValCount{
		Val:   int64(vsum) + (int64(vcount) * bsig.Base),
		Count: int64(vcount),
	}
	if field.Type() == FieldTypeDecimal {
		out.FloatVal = float64(int64(vsum)+(int64(vcount)*bsig.Base)) / math.Pow(10, float64(bsig.Scale))
		dec := pql.NewDecimal((int64(vsum) + (int64(vcount) * bsig.Base)), bsig.Scale)
		out.DecimalVal = &dec
	}
	return out, nil
}

// executeMinShard calculates the min for bsiGroups on a shard.
func (e *executor) executeMinShard(ctx context.Context, qcx *Qcx, index string, c *pql.Call, shard uint64) (_ ValCount, err0 error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "executor.executeMinShard")
	defer span.Finish()

	var filter *Row
	if len(c.Children) == 1 {
		row, err := e.executeBitmapCallShard(ctx, qcx, index, c.Children[0], shard)
		if err != nil {
			return ValCount{}, err
		}
		filter = row
	}

	fieldName, err := c.FirstStringArg("field", "_field")
	if err != nil {
		return ValCount{}, errors.Wrap(err, "Min(): field required")
	}

	field := e.Holder.Field(index, fieldName)
	if field == nil {
		return ValCount{}, ErrFieldNotFound
	}
	return field.MinForShard(qcx, shard, filter)
}

// executeMaxShard calculates the max for bsiGroups on a shard.
func (e *executor) executeMaxShard(ctx context.Context, qcx *Qcx, index string, c *pql.Call, shard uint64) (_ ValCount, err0 error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "Executor.executeMaxShard")
	defer span.Finish()

	var filter *Row
	if len(c.Children) == 1 {
		row, err := e.executeBitmapCallShard(ctx, qcx, index, c.Children[0], shard)
		if err != nil {
			return ValCount{}, err
		}
		filter = row
	}

	fieldName, err := c.FirstStringArg("field", "_field")
	if err != nil {
		return ValCount{}, errors.Wrap(err, "Max(): field required")
	}

	field := e.Holder.Field(index, fieldName)
	if field == nil {
		return ValCount{}, ErrFieldNotFound
	}
	return field.MaxForShard(qcx, shard, filter)
}

// executeMinRowShard returns the minimum row ID for a shard.
func (e *executor) executeMinRowShard(ctx context.Context, qcx *Qcx, index string, c *pql.Call, shard uint64) (_ PairField, err0 error) {
	var filter *Row

	if len(c.Children) == 1 {
		row, err := e.executeBitmapCallShard(ctx, qcx, index, c.Children[0], shard)
		if err != nil {
			return PairField{}, err
		}
		filter = row
	}

	fieldName, _ := c.Args["field"].(string)
	field := e.Holder.Field(index, fieldName)
	if field == nil {
		return PairField{}, ErrFieldNotFound
	}

	fragment := e.Holder.fragment(index, fieldName, viewStandard, shard)
	if fragment == nil {
		return PairField{}, nil
	}

	idx := e.Holder.Index(index)
	tx, finisher, err := qcx.GetTx(Txo{Write: !writable, Index: idx, Fragment: fragment, Shard: fragment.shard})
	if err != nil {
		return PairField{}, err
	}

	defer finisher(&err0)

	minRowID, count, err := fragment.minRow(tx, filter)
	if err != nil {
		return PairField{}, err
	}

	return PairField{
		Pair: Pair{
			ID:    minRowID,
			Count: count,
		},
		Field: fieldName,
	}, nil
}

// executeMaxRowShard returns the maximum row ID for a shard.
func (e *executor) executeMaxRowShard(ctx context.Context, qcx *Qcx, index string, c *pql.Call, shard uint64) (_ PairField, err0 error) {
	var filter *Row
	if len(c.Children) == 1 {
		row, err := e.executeBitmapCallShard(ctx, qcx, index, c.Children[0], shard)
		if err != nil {
			return PairField{}, err
		}
		filter = row
	}

	fieldName, _ := c.Args["field"].(string)
	field := e.Holder.Field(index, fieldName)
	if field == nil {
		return PairField{}, ErrFieldNotFound
	}

	fragment := e.Holder.fragment(index, fieldName, viewStandard, shard)
	if fragment == nil {
		return PairField{}, nil
	}

	idx := e.Holder.Index(index)
	tx, finisher, err := qcx.GetTx(Txo{Write: !writable, Index: idx, Shard: shard})
	if err != nil {
		return PairField{}, ErrQcxDone
	}
	defer finisher(&err0)

	maxRowID, count, err := fragment.maxRow(tx, filter)
	if err != nil {
		return PairField{}, nil
	}

	return PairField{
		Pair: Pair{
			ID:    maxRowID,
			Count: count,
		},
		Field: fieldName,
	}, nil
}

func (e *executor) executeTopK(ctx context.Context, qcx *Qcx, index string, c *pql.Call, shards []uint64, opt *ExecOptions) (interface{}, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "executor.executeTopK")
	defer span.Finish()

	mapFn := func(ctx context.Context, shard uint64, mopt *mapOptions) (_ interface{}, err error) {
		return e.executeTopKShard(ctx, qcx, index, c, shard)
	}

	reduceFn := func(ctx context.Context, prev, v interface{}) interface{} {
		x, _ := prev.([]*Row)
		y, _ := v.([]*Row)
		return ([]*Row)(AddBSI(x, y))
	}

	other, err := e.mapReduce(ctx, index, shards, c, opt, mapFn, reduceFn)
	if err != nil {
		return nil, err
	}
	results, _ := other.([]*Row)

	if opt.Remote {
		return results, nil
	}

	k, hasK, err := c.UintArg("k")
	if err != nil {
		return nil, errors.Wrap(err, "fetching k")
	}

	var limit *uint64
	if hasK {
		limit = &k
	}

	var dst []Pair
	BSIData(results).PivotDescending(NewRow().Union(results...), 0, limit, nil, func(count uint64, ids ...uint64) {
		for _, id := range ids {
			dst = append(dst, Pair{
				ID:    id,
				Count: count,
			})
		}
	})

	fieldName, hasFieldName, err := c.StringArg("_field")
	if err != nil {
		return nil, errors.Wrap(err, "fetching TopK field")
	} else if !hasFieldName {
		return nil, errors.New("missing field in TopK")
	}

	return &PairsField{
		Pairs: dst,
		Field: fieldName,
	}, nil
}

// executeTopKShard builds a perpendicular BSI bitmap of a shard for TopK.
func (e *executor) executeTopKShard(ctx context.Context, qcx *Qcx, index string, c *pql.Call, shard uint64) (_ []*Row, err0 error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "executor.executeTopKShard")
	defer span.Finish()

	// Look up the index.
	idx := e.Holder.Index(index)
	if idx == nil {
		return nil, ErrIndexNotFound
	}

	// Look up the field.
	fieldName, hasFieldName, err := c.StringArg("_field")
	if err != nil {
		return nil, errors.Wrap(err, "fetching TopK field")
	} else if !hasFieldName {
		return nil, errors.New("missing field in TopK")
	}
	f := idx.Field(fieldName)
	if f == nil {
		return nil, ErrFieldNotFound
	}

	// Parse "from" time, if set.
	var fromTime time.Time
	if v, ok := c.Args["from"]; ok {
		if fromTime, err = parseTime(v); err != nil {
			return nil, errors.Wrap(err, "parsing from time")
		}
	}

	// Parse "to" time, if set.
	var toTime time.Time
	if v, ok := c.Args["to"]; ok {
		if toTime, err = parseTime(v); err != nil {
			return nil, errors.Wrap(err, "parsing to time")
		}
	}

	// Fetch the filter.
	var filterBitmap *Row
	if filter, hasFilter, err := c.CallArg("filter"); err != nil {
		return nil, err
	} else if hasFilter {
		filterBitmap, err = e.executeBitmapCallShard(ctx, qcx, index, filter, shard)
		if err != nil {
			return nil, err
		}
		if !filterBitmap.Any() {
			return []*Row(nil), nil
		}
	}

	tx, finisher, err := qcx.GetTx(Txo{Write: !writable, Index: idx, Shard: shard})
	if err != nil {
		return nil, err
	}
	defer finisher(&err0)

	ftype := f.Type()
	switch ftype {
	case FieldTypeTime:
		if !(fromTime.IsZero() && toTime.IsZero()) {
			return e.executeTopKShardTime(ctx, tx, filterBitmap, index, fieldName, shard, fromTime, toTime)
		}
		fallthrough
	case FieldTypeSet, FieldTypeMutex:
		return e.executeTopKShardSet(ctx, tx, filterBitmap, index, fieldName, shard)
	default:
		return nil, errors.Errorf("field type %q is not yet supported by TopK", ftype)
	}
}

// executeTopKShardSet builds a perpendicular BSI bitmap of a set field within a shard.
func (e *executor) executeTopKShardSet(ctx context.Context, tx Tx, filter *Row, index, field string, shard uint64) ([]*Row, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "executor.executeTopKShardSet")
	defer span.Finish()

	f := e.Holder.fragment(index, field, viewStandard, shard)
	if f == nil {
		return nil, nil
	}

	return topKFragments(ctx, tx, filter, f)
}

// executeTopKShardTime builds a perpendicular BSI bitmap of a time field within a shard.
func (e *executor) executeTopKShardTime(ctx context.Context, tx Tx, filter *Row, index, field string, shard uint64, from, to time.Time) ([]*Row, error) {
	// Fetch index.
	idx := e.Holder.Index(index)
	if idx == nil {
		return nil, newNotFoundError(ErrIndexNotFound, index)
	}

	// Fetch field.
	f := idx.Field(field)
	if f == nil {
		return nil, newNotFoundError(ErrFieldNotFound, field)
	}

	views, err := f.viewsByTimeRange(from, to)
	if err != nil {
		return nil, err
	}

	// Fetch fragments.
	var fragments []*fragment
	for _, view := range views {
		f := e.Holder.fragment(index, field, view, shard)
		if f == nil {
			continue
		}

		fragments = append(fragments, f)
	}

	return topKFragments(ctx, tx, filter, fragments...)
}

// topKFragments builds a perpendicular BSI bitmap from fragments.
// The fragments are expected to be from set fields.
func topKFragments(ctx context.Context, tx Tx, filter *Row, fragments ...*fragment) (BSIData, error) {
	// Acquire fragment container iterators.
	iters := make([]roaring.ContainerIterator, len(fragments))
	for i, f := range fragments {
		f.mu.RLock()
		defer f.mu.RUnlock()

		iter, _, err := tx.ContainerIterator(f.index(), f.field(), f.view(), f.shard, 0)
		if err != nil {
			return nil, err
		}

		iters[i] = iter
	}

	// Merge to a single container iterator.
	var it roaring.ContainerIterator
	if len(iters) == 1 {
		it = iters[0]
	} else {
		it = mergerate(iters...)
	}

	// Extract filter data if a filter was provided.
	var filterData *topKFilter
	if filter != nil {
		var f topKFilter
		f.fill(filter)
		filterData = &f
	}

	return doTopK(ctx, it, filterData)
}

// mergerate returns a container iterator that unions many container iterators.
func mergerate(iters ...roaring.ContainerIterator) *mergerator {
	iterStates := make([]mergeState, len(iters))
	for i, s := range iters {
		iterStates[i].iter = s
	}
	m := mergerator{
		iters: iterStates,
		heap:  make(mergeratorHeap, 0, len(iters)),
	}
	for i := range iterStates {
		m.pusherate(uint64(i))
	}
	return &m
}

// mergerator is a container iterator that merges container iterators (via unioning).
type mergerator struct {
	iters     []mergeState
	heap      mergeratorHeap
	container *roaring.Container
	key       uint64
}

// pusherate pushes the iterator at the given index back onto the heap.
func (m *mergerator) pusherate(idx uint64) {
	state := &m.iters[idx]
	it := state.iter
	if !it.Next() {
		it.Close()
		return
	}
	key, c := it.Value()
	state.c = c
	m.heap.push(mergeNode{
		key: key,
		idx: idx,
	})
}

func (m *mergerator) Next() bool {
	nodes := m.heap.pop()
	if len(nodes) == 0 {
		return false
	}
	key := nodes[0].key
	var container *roaring.Container
	for _, n := range nodes {
		c := m.iters[n.idx].c
		if container != nil {
			container = roaring.Union(container, c)
		} else {
			container = c
		}
		m.pusherate(n.idx)
	}
	m.key, m.container = key, container
	return true
}

func (m *mergerator) Value() (uint64, *roaring.Container) {
	return m.key, m.container
}

func (m *mergerator) Close() {
	for _, n := range m.heap {
		m.iters[n.idx].iter.Close()
	}
	m.heap = nil
}

type mergeState struct {
	c    *roaring.Container
	iter roaring.ContainerIterator
}

// mergeratorHeap is a binary min-heap over keys.
// This is used to find the next iterator to hit.
type mergeratorHeap []mergeNode

type mergeNode struct {
	key, idx uint64
}

// push a node onto the heap.
func (h *mergeratorHeap) push(node mergeNode) {
	s := *h
	i := len(s)
	s = append(s, node)
	for i != 0 && s[(i-1)/2].key > s[i].key {
		s[(i-1)/2], s[i] = s[i], s[(i-1)/2]
		i = (i - 1) / 2
	}
	*h = s
}

// pop the minimum key off of the heap.
// If there are multiple iterators with this keys, this returns all of them.
func (h *mergeratorHeap) pop() []mergeNode {
	s := *h
	if len(s) == 0 {
		return nil
	}

	n := 0
	for key := s[0].key; len(s) > n && s[0].key == key; n++ {
		s[0], s[len(s)-n-1] = s[len(s)-n-1], s[0]
		s[:len(s)-n-1].minHeapify()
	}

	*h = s[:len(s)-n]
	return s[len(s)-n:]
}

// minHeapify fixes the heap invariant after updating the heap's root.
func (h mergeratorHeap) minHeapify() {
	i := 0
	for {
		l, r := 2*i+1, 2*i+2
		min := i
		if l < len(h) && h[l].key < h[min].key {
			min = l
		}
		if r < len(h) && h[r].key < h[min].key {
			min = r
		}
		if min == i {
			return
		}
		h[min], h[i] = h[i], h[min]
		i = min
	}
}

// doTopK uses a raw Pilosa matrix to produce a perpendicular BSI bitmap.
// It will apply a row filter if one is provided.
func doTopK(ctx context.Context, it roaring.ContainerIterator, filter *topKFilter) (BSIData, error) {
	row := ^uint64(0)
	var count uint64

	var builder bsiBuilder
	var i uint16
	for it.Next() {
		if i == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		i++

		// Fetch the next container.
		key, container := it.Value()
		keyrow, subkey := key/(ShardWidth>>16), key%(ShardWidth>>16)
		if keyrow != row {
			// The previous row has ended.
			// Flush the count to the BSI data.
			builder.Insert(row, count)
			row, count = keyrow, 0
		}

		// Add the selected bits to the count.
		if filter != nil {
			fc := filter[subkey]
			if fc == nil {
				continue
			}
			count += uint64(roaring.IntersectionCount(container, fc))
		} else {
			count += uint64(container.N())
		}
	}

	// Add the final count to the BSI data.
	builder.Insert(row, count)

	// Construct the result.
	return builder.Build(), nil
}

// topKFilter is a row filter for a TopK query.
// It is represented as a contiguous array of containers.
type topKFilter [ShardWidth >> 16]*roaring.Container

// fill the filter with the contents of a Row.
func (f *topKFilter) fill(row *Row) {
	for _, s := range row.Segments {
		it, _ := s.data.Containers.Iterator(0)
		f.fillIt(it)
	}
	// I don't think multiple segments make sense here?
}

func (f *topKFilter) fillIt(it roaring.ContainerIterator) {
	defer it.Close()

	for it.Next() {
		key, c := it.Value()

		key %= uint64(len(f))

		if f[key] != nil {
			panic("duplicate container in topk filter")
		}
		f[key] = c
	}
}

// executeTopN executes a TopN() call.
// This first performs the TopN() to determine the top results and then
// requeries to retrieve the full counts for each of the top results.
func (e *executor) executeTopN(ctx context.Context, qcx *Qcx, index string, c *pql.Call, shards []uint64, opt *ExecOptions) (*PairsField, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "executor.executeTopN")
	defer span.Finish()

	idsArg, _, err := c.UintSliceArg("ids")
	if err != nil {
		return nil, fmt.Errorf("executeTopN: %v", err)
	}

	fieldName, _ := c.Args["_field"].(string)
	n, _, err := c.UintArg("n")
	if err != nil {
		return nil, fmt.Errorf("executeTopN: %v", err)
	}

	// Execute original query.
	pairs, err := e.executeTopNShards(ctx, qcx, index, c, shards, opt)
	if err != nil {
		return nil, errors.Wrap(err, "finding top results")
	}

	// If this call is against specific ids, or we didn't get results,
	// or we are part of a larger distributed query then don't refetch.
	if len(pairs.Pairs) == 0 || len(idsArg) > 0 || opt.Remote {
		return &PairsField{
			Pairs: pairs.Pairs,
			Field: fieldName,
		}, nil
	}
	// Only the original caller should refetch the full counts.
	// TODO(@kuba--): ...but do we really need `Clone` here?
	other := c.Clone()

	ids := Pairs(pairs.Pairs).Keys()
	sort.Sort(uint64Slice(ids))
	other.Args["ids"] = ids

	trimmedList, err := e.executeTopNShards(ctx, qcx, index, other, shards, opt)
	if err != nil {
		return nil, errors.Wrap(err, "retrieving full counts")
	}

	if n != 0 && int(n) < len(trimmedList.Pairs) {
		trimmedList.Pairs = trimmedList.Pairs[0:n]
	}

	return &PairsField{
		Pairs: trimmedList.Pairs,
		Field: fieldName,
	}, nil
}

func (e *executor) executeTopNShards(ctx context.Context, qcx *Qcx, index string, c *pql.Call, shards []uint64, opt *ExecOptions) (*PairsField, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "executor.executeTopNShards")
	defer span.Finish()

	// Execute calls in bulk on each remote node and merge.
	mapFn := func(ctx context.Context, shard uint64, mopt *mapOptions) (_ interface{}, err error) {
		return e.executeTopNShard(ctx, qcx, index, c, shard)
	}

	// Merge returned results at coordinating node.
	reduceFn := func(ctx context.Context, prev, v interface{}) interface{} {
		other, _ := prev.(*PairsField)
		vpf, _ := v.(*PairsField)
		if other == nil {
			return vpf
		} else if vpf == nil {
			return other
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		other.Pairs = Pairs(other.Pairs).Add(vpf.Pairs)
		return other
	}

	other, err := e.mapReduce(ctx, index, shards, c, opt, mapFn, reduceFn)
	if err != nil {
		return nil, err
	}
	results, _ := other.(*PairsField)

	// Sort final merged results.
	sort.Sort(Pairs(results.Pairs))

	return results, nil
}

// executeTopNShard executes a TopN call for a single shard.
func (e *executor) executeTopNShard(ctx context.Context, qcx *Qcx, index string, c *pql.Call, shard uint64) (_ *PairsField, err0 error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "executor.executeTopNShard")
	defer span.Finish()

	fieldName, _ := c.Args["_field"].(string)
	n, _, err := c.UintArg("n")
	if err != nil {
		return nil, fmt.Errorf("executeTopNShard: %v", err)
	} else if f := e.Holder.Field(index, fieldName); f != nil && (f.Type() == FieldTypeInt || f.Type() == FieldTypeDecimal || f.Type() == FieldTypeTimestamp) {
		return nil, fmt.Errorf("cannot compute TopN() on integer, decimal, or timestamp field: %q", fieldName)
	}

	rowIDs, _, err := c.UintSliceArg("ids")
	if err != nil {
		return nil, fmt.Errorf("executeTopNShard: %v", err)
	}
	minThreshold, _, err := c.UintArg("threshold")
	if err != nil {
		return nil, fmt.Errorf("executeTopNShard: %v", err)
	}
	tanimotoThreshold, _, err := c.UintArg("tanimotoThreshold")
	if err != nil {
		return nil, fmt.Errorf("executeTopNShard: %v", err)
	}

	// Retrieve bitmap used to intersect.
	var src *Row
	if len(c.Children) == 1 {
		row, err := e.executeBitmapCallShard(ctx, qcx, index, c.Children[0], shard)
		if err != nil {
			return nil, err
		}
		src = row
	} else if len(c.Children) > 1 {
		return nil, errors.New("TopN() can only have one input bitmap")
	}

	// Set default field.
	if fieldName == "" {
		fieldName = defaultField
	}

	f := e.Holder.fragment(index, fieldName, viewStandard, shard)
	if f == nil {
		return &PairsField{}, nil
	} else if f.CacheType == CacheTypeNone {
		return nil, fmt.Errorf("cannot compute TopN(), field has no cache: %q", fieldName)
	}

	if minThreshold == 0 {
		minThreshold = defaultMinThreshold
	}

	if tanimotoThreshold > 100 {
		return nil, errors.New("Tanimoto Threshold is from 1 to 100 only")
	}

	idx := e.Holder.Index(index)
	tx, finisher, err := qcx.GetTx(Txo{Write: !writable, Fragment: f, Index: idx, Shard: shard})
	if err != nil {
		return nil, err
	}
	defer finisher(&err0)

	pairs, err := f.top(tx, topOptions{
		N:                 int(n),
		Src:               src,
		RowIDs:            rowIDs,
		MinThreshold:      minThreshold,
		TanimotoThreshold: tanimotoThreshold,
	})
	if err != nil {
		return nil, errors.Wrap(err, "getting top")
	}

	return &PairsField{
		Pairs: pairs,
	}, nil
}

// executeDifferenceShard executes a difference() call for a local shard.
func (e *executor) executeDifferenceShard(ctx context.Context, qcx *Qcx, index string, c *pql.Call, shard uint64) (_ *Row, err error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "executor.executeDifferenceShard")
	defer span.Finish()

	var other *Row
	if len(c.Children) == 0 {
		return nil, fmt.Errorf("empty Difference query is currently not supported")
	}
	for i, input := range c.Children {
		row, err := e.executeBitmapCallShard(ctx, qcx, index, input, shard)
		if err != nil {
			return nil, err
		}

		if i == 0 {
			other = row
		} else {
			other = other.Difference(row)
		}
	}
	other.invalidateCount()
	return other, nil
}

// RowIdentifiers is a return type for a list of
// row ids or row keys. The names `Rows` and `Keys`
// are meant to follow the same convention as the
// Row query which returns `Columns` and `Keys`.
// TODO: Rename this to something better. Anything.
type RowIdentifiers struct {
	Rows  []uint64 `json:"rows"`
	Keys  []string `json:"keys,omitempty"`
	Field string
}

func (r *RowIdentifiers) Clone() (clone *RowIdentifiers) {
	clone = &RowIdentifiers{
		Field: r.Field,
	}
	if r.Rows != nil {
		clone.Rows = make([]uint64, len(r.Rows))
		copy(clone.Rows, r.Rows)
	}
	if r.Keys != nil {
		clone.Keys = make([]string, len(r.Keys))
		copy(clone.Keys, r.Keys)
	}
	return
}

// ToTable implements the ToTabler interface.
func (r RowIdentifiers) ToTable() (*proto.TableResponse, error) {
	var n int
	if len(r.Keys) > 0 {
		n = len(r.Keys)
	} else {
		n = len(r.Rows)
	}
	return proto.RowsToTable(&r, n)
}

// ToRows implements the ToRowser interface.
func (r RowIdentifiers) ToRows(callback func(*proto.RowResponse) error) error {
	if len(r.Keys) > 0 {
		ci := []*proto.ColumnInfo{{Name: r.Field, Datatype: "string"}}
		for _, key := range r.Keys {
			if err := callback(&proto.RowResponse{
				Headers: ci,
				Columns: []*proto.ColumnResponse{
					{ColumnVal: &proto.ColumnResponse_StringVal{StringVal: key}},
				},
			}); err != nil {
				return errors.Wrap(err, "calling callback")
			}
			ci = nil
		}
	} else {
		ci := []*proto.ColumnInfo{{Name: r.Field, Datatype: "uint64"}}
		for _, id := range r.Rows {
			if err := callback(&proto.RowResponse{
				Headers: ci,
				Columns: []*proto.ColumnResponse{
					{ColumnVal: &proto.ColumnResponse_Uint64Val{Uint64Val: uint64(id)}},
				},
			}); err != nil {
				return errors.Wrap(err, "calling callback")
			}
			ci = nil
		}
	}
	return nil
}

// RowIDs is a query return type for just uint64 row ids.
// It should only be used internally (since RowIdentifiers
// is the external return type), but it is exported because
// the proto package needs access to it.
type RowIDs []uint64

func (r RowIDs) Merge(other RowIDs, limit int) RowIDs {
	i, j := 0, 0
	result := make(RowIDs, 0)
	for i < len(r) && j < len(other) && len(result) < limit {
		av, bv := r[i], other[j]
		if av < bv {
			result = append(result, av)
			i++
		} else if av > bv {
			result = append(result, bv)
			j++
		} else {
			result = append(result, bv)
			i++
			j++
		}
	}
	for i < len(r) && len(result) < limit {
		result = append(result, r[i])
		i++
	}
	for j < len(other) && len(result) < limit {
		result = append(result, other[j])
		j++
	}
	return result
}

// order denotes sort order—can be asc or desc (see constants below).
type order bool

const (
	asc  order = true
	desc order = false
)

// groupCountSorter sorts the output of a GroupBy request (a
// []GroupCount) according to sorting instructions encoded in "fields"
// and "order".
//
// Each field in "fields" is an integer which can be -1 to denote
// sorting on the Count and -2 to denote sorting on the
// sum/aggregate. Currently nothing else is supported, but the idea
// was that if there were positive integers they would be indexes into
// GroupCount.FieldRow and allowing sorting on the values of different
// fields in the group. Each item in "order" corresponds to the same
// index in "fields" and denotes the order of the sort.
type groupCountSorter struct {
	fields []int
	order  []order
	data   []GroupCount
}

func (g *groupCountSorter) Len() int      { return len(g.data) }
func (g *groupCountSorter) Swap(i, j int) { g.data[i], g.data[j] = g.data[j], g.data[i] }
func (g *groupCountSorter) Less(i, j int) bool {
	gci, gcj := g.data[i], g.data[j]
	for idx, fieldIndex := range g.fields {
		fieldOrder := g.order[idx]
		switch fieldIndex {
		case -1: // Count
			if gci.Count < gcj.Count {
				return fieldOrder == asc
			} else if gci.Count > gcj.Count {
				return fieldOrder == desc
			}
		case -2: // Aggregate
			if gci.Agg < gcj.Agg {
				return fieldOrder == asc
			} else if gci.Agg > gcj.Agg {
				return fieldOrder == desc
			}
		default:
			panic("impossible")
		}
	}
	return false
}

// getSorter hackily parses the sortSpec and figures out how to sort
// the GroupBy results.
func getSorter(sortSpec string) (*groupCountSorter, error) {
	gcs := &groupCountSorter{
		fields: []int{},
		order:  []order{},
	}
	sortOn := strings.Split(sortSpec, ",")
	for _, sortField := range sortOn {
		sortField = strings.TrimSpace(sortField)
		fieldDir := strings.Fields(sortField)
		if len(fieldDir) == 0 {
			return nil, errors.Errorf("invalid sorting directive: '%s'", sortField)
		} else if fieldDir[0] == "count" {
			gcs.fields = append(gcs.fields, -1)
		} else if fieldDir[0] == "aggregate" || fieldDir[0] == "sum" {
			gcs.fields = append(gcs.fields, -2)
		} else {
			return nil, errors.Errorf("sorting is only supported on count, aggregate, or sum, not '%s'", fieldDir[0])
		}

		if len(fieldDir) == 1 {
			gcs.order = append(gcs.order, desc)
		} else if len(fieldDir) > 2 {
			return nil, errors.Errorf("parsing sort directive: '%s': too many elements", sortField)
		} else if fieldDir[1] == "asc" {
			gcs.order = append(gcs.order, asc)
		} else if fieldDir[1] == "desc" {
			gcs.order = append(gcs.order, desc)
		} else {
			return nil, errors.Errorf("unknown sort direction '%s'", fieldDir[1])
		}
	}
	return gcs, nil
}

// findGroupCounts gets a safe-to-use but possibly empty []GroupCount from
// an interface which might be a *GroupCounts or a []GroupCount.
func findGroupCounts(v interface{}) []GroupCount {
	switch gc := v.(type) {
	case []GroupCount:
		return gc
	case *GroupCounts:
		return gc.Groups()
	}
	return nil
}

func (e *executor) executeGroupBy(ctx context.Context, qcx *Qcx, index string, c *pql.Call, shards []uint64, opt *ExecOptions) (*GroupCounts, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "executor.executeGroupBy")
	defer span.Finish()
	// validate call
	if len(c.Children) == 0 {
		return nil, errors.New("need at least one child call")
	}
	limit := int(^uint(0) >> 1)
	if lim, hasLimit, err := c.UintArg("limit"); err != nil {
		return nil, err
	} else if hasLimit {
		limit = int(lim)
	}
	filter, _, err := c.CallArg("filter")
	if err != nil {
		return nil, err
	}

	var sorter *groupCountSorter
	if sortSpec, found, err := c.StringArg("sort"); err != nil {
		return nil, errors.Wrap(err, "getting sort arg")
	} else if found {
		sorter, err = getSorter(sortSpec)
		if err != nil {
			return nil, errors.Wrap(err, "parsing sort spec")
		}
		// don't want to prematurely limit the results if we're sorting
		limit = int(^uint(0) >> 1)
	}
	having, hasHaving, err := c.CallArg("having")
	if err != nil {
		return nil, errors.Wrap(err, "getting 'having' argument")
	} else if hasHaving {
		// don't want to prematurely limit the results if we're filtering some out
		limit = int(^uint(0) >> 1)
	}

	idx := e.Holder.Index(index)
	if idx == nil {
		return nil, newNotFoundError(ErrIndexNotFound, index)
	}

	// perform necessary Rows queries (any that have limit or columns args) -
	// TODO, call async? would only help if multiple Rows queries had a column
	// or limit arg.
	// TODO support TopN in here would be really cool - and pretty easy I think.
	bases := make(map[int]int64)
	childRows := make([]RowIDs, len(c.Children))
	for i, child := range c.Children {
		// Check "field" first for backwards compatibility, then set _field.
		// TODO: remove at Pilosa 2.0
		if fieldName, ok := child.Args["field"].(string); ok {
			child.Args["_field"] = fieldName
		}

		if child.Name != "Rows" {
			return nil, errors.Errorf("'%s' is not a valid child query for GroupBy, must be 'Rows'", child.Name)
		}
		_, hasLimit, err := child.UintArg("limit")
		if err != nil {
			return nil, errors.Wrap(err, "getting limit")
		}
		_, hasCol, err := child.UintArg("column")
		if err != nil {
			return nil, errors.Wrap(err, "getting column")
		}
		_, hasLike, err := child.StringArg("like")
		if err != nil {
			return nil, errors.Wrap(err, "getting like")
		}
		_, hasIn, err := child.UintSliceArg("in")
		if err != nil {
			return nil, errors.Wrap(err, "getting 'in'")
		}
		fieldName, ok := child.Args["_field"].(string)
		if !ok {
			return nil, errors.Errorf("%s call must have field with valid (string) field name. Got %v of type %[2]T", child.Name, child.Args["_field"])
		}
		f := idx.Field(fieldName)
		if f == nil {
			return nil, newNotFoundError(ErrFieldNotFound, fieldName)
		}
		switch f.Type() {
		case FieldTypeInt, FieldTypeTimestamp:
			bases[i] = f.bsiGroup(f.name).Base
		}

		if hasLimit || hasCol || hasLike || hasIn { // we need to perform this query cluster-wide ahead of executeGroupByShard
			if idx, ok := child.Args["valueidx"].(int64); ok {
				// The rows query was already completed on the initiating node.
				childRows[i] = opt.EmbeddedData[idx].Columns()
				continue
			}

			r, er := e.executeRows(ctx, qcx, index, child, shards, opt)
			if er != nil {
				return nil, errors.Wrap(er, "getting rows for ")
			}
			// need to sort because filters assume ordering
			sort.Slice(r, func(x, y int) bool { return r[x] < r[y] })
			childRows[i] = r
			if len(childRows[i]) == 0 { // there are no results because this field has no values.
				return &GroupCounts{}, nil
			}

			// Stuff the result into opt.EmbeddedData so that it gets sent to other nodes in the map-reduce.
			// This is flagged as "NoSplit" to ensure that the entire row gets sent out.
			rowsRow := NewRow(childRows[i]...)
			rowsRow.NoSplit = true
			child.Args["valueidx"] = int64(len(opt.EmbeddedData))
			opt.EmbeddedData = append(opt.EmbeddedData, rowsRow)
		}
	}

	ignoreLimit := sorter != nil
	// Execute calls in bulk on each remote node and merge.
	mapFn := func(ctx context.Context, shard uint64, mopt *mapOptions) (_ interface{}, err error) {
		return e.executeGroupByShard(ctx, qcx, index, c, filter, shard, childRows, bases, ignoreLimit)
	}

	// Merge returned results at coordinating node.
	reduceFn := func(ctx context.Context, prev, v interface{}) interface{} {
		other := findGroupCounts(prev)
		if err := ctx.Err(); err != nil {
			return err
		}
		x := mergeGroupCounts(other, findGroupCounts(v), limit)
		for i := range x {
			gc := &x[i]
			for j := range gc.Group {
				fr := &gc.Group[j]
				if fr.FieldOptions == nil {
					// oops, options were omitted possibly by a remote. try to
					// guess them from our local options
					field := e.Holder.Field(index, fr.Field)
					if field != nil {
						options := field.Options()
						fr.FieldOptions = &options
					}
				}
			}
		}
		return x
	}
	// Get full result set.
	other, err := e.mapReduce(ctx, index, shards, c, opt, mapFn, reduceFn)
	if err != nil {
		return nil, errors.Wrap(err, "mapReduce")
	}
	results, _ := other.([]GroupCount)

	// If there's no sorting, we want to apply limits before
	// calculating the Distinct aggregate which is expensive on a
	// per-result basis.
	if sorter == nil && !hasHaving {
		results, err = applyLimitAndOffsetToGroupByResult(c, results)
		if err != nil {
			return nil, errors.Wrap(err, "applying limit/offset")
		}
	}

	// TODO as an optimization, we could apply some "having"
	// conditions here long as they aren't on the Count(Distinct)
	// aggregate
	// Calculate Count(Distinct) aggregate if requested.
	aggregate, _, err := c.CallArg("aggregate")
	if err == nil && aggregate != nil && aggregate.Name == "Count" && len(aggregate.Children) > 0 && aggregate.Children[0].Name == "Distinct" && !opt.Remote {
		for n, gc := range results {
			intersectRows := make([]*pql.Call, 0, len(gc.Group))
			for _, fr := range gc.Group {
				var value interface{} = fr.RowID
				// use fr.Value instead of fr.RowID if set (from int fields)
				if fr.Value != nil {
					value = &pql.Condition{Op: pql.EQ, Value: *fr.Value}
				}
				intersectRows = append(intersectRows, &pql.Call{Name: "Row", Args: map[string]interface{}{fr.Field: value}})
			}
			// apply any filter, if present
			if filter != nil {
				intersectRows = append(intersectRows, filter)
			}
			// also intersect with any children of Distinct
			if len(aggregate.Children[0].Children) > 0 {
				intersectRows = append(intersectRows, aggregate.Children[0].Children[0])
			}

			countDistinctIntersect := &pql.Call{
				Name: "Count",
				Children: []*pql.Call{
					{
						Name: "Distinct",
						Children: []*pql.Call{
							{
								Name:     "Intersect",
								Children: intersectRows,
							},
						},
						Args: aggregate.Children[0].Args,
						Type: pql.PrecallGlobal,
					},
				},
			}

			opt.PreTranslated = true
			aggregateCount, err := e.execute(ctx, qcx, index, &pql.Query{Calls: []*pql.Call{countDistinctIntersect}}, []uint64{}, opt)
			if err != nil {
				return nil, err
			}
			results[n].Agg = int64(aggregateCount[0].(uint64))
		}
	}

	// Apply having.
	if hasHaving && !opt.Remote {
		// parse the condition as PQL
		if having.Name != "Condition" {
			return nil, errors.New("the only supported having call is Condition()")
		}
		if len(having.Args) != 1 {
			return nil, errors.New("Condition() must contain a single condition")
		}
		for subj, cond := range having.Args {
			switch subj {
			case "count", "sum":
				results = ApplyConditionToGroupCounts(results, subj, cond.(*pql.Condition))
			default:
				return nil, errors.New("Condition() only supports count or sum")
			}
		}
	}

	if sorter != nil && !opt.Remote {
		sorter.data = results
		sort.Stable(sorter)
		results, err = applyLimitAndOffsetToGroupByResult(c, results)
		if err != nil {
			return nil, errors.Wrap(err, "applying limit/offset")
		}
	} else if hasHaving && !opt.Remote {
		results, err = applyLimitAndOffsetToGroupByResult(c, results)
		if err != nil {
			return nil, errors.Wrap(err, "applying limit/offset")
		}

	}

	aggType := ""
	if aggregate != nil {
		switch aggregate.Name {
		case "Sum":
			aggType = "sum"
		case "Count":
			aggType = "aggregate"
		}
	}
	for _, res := range results {
		if res.DecimalAgg != nil && aggType == "sum" {
			aggType = "decimalSum"
			break
		}
	}

	return NewGroupCounts(aggType, results...), nil
}

func applyLimitAndOffsetToGroupByResult(c *pql.Call, results []GroupCount) ([]GroupCount, error) {
	// Apply offset.
	if offset, hasOffset, err := c.UintArg("offset"); err != nil {
		return nil, err
	} else if hasOffset {
		if int(offset) < len(results) {
			results = results[offset:]
		}
	}
	// Apply limit.
	if limit, hasLimit, err := c.UintArg("limit"); err != nil {
		return nil, err
	} else if hasLimit {
		if int(limit) < len(results) {
			results = results[:limit]
		}
	}
	return results, nil
}

// FieldRow is used to distinguish rows in a group by result.
type FieldRow struct {
	Field        string        `json:"field"`
	RowID        uint64        `json:"rowID"`
	RowKey       string        `json:"rowKey,omitempty"`
	Value        *int64        `json:"value,omitempty"`
	FieldOptions *FieldOptions `json:"-"`
}

func (fr *FieldRow) Clone() (clone *FieldRow) {
	clone = &FieldRow{
		Field:  fr.Field,
		RowID:  fr.RowID,
		RowKey: fr.RowKey,
	}
	if fr.Value != nil {
		// deep copy, for safety.
		v := *fr.Value
		clone.Value = &v
	}
	if fr.FieldOptions != nil {
		// deep copy, for Extra Safety
		v := *fr.FieldOptions
		clone.FieldOptions = &v
	}
	return
}

// MarshalJSON marshals FieldRow to JSON such that
// either a Key or an ID is included.
func (fr FieldRow) MarshalJSON() ([]byte, error) {
	if fr.Value != nil {
		if fr.FieldOptions.Type == FieldTypeTimestamp {
			ts, err := ValToTimestamp(fr.FieldOptions.TimeUnit, int64(*fr.Value)+fr.FieldOptions.Base)
			if err != nil {
				return nil, errors.Wrap(err, "translating value to timestamp")
			}
			return json.Marshal(struct {
				Field string `json:"field"`
				Value string `json:"value"`
			}{
				Field: fr.Field,
				Value: ts.Format(time.RFC3339Nano),
			})
		} else {
			return json.Marshal(struct {
				Field string `json:"field"`
				Value int64  `json:"value"`
			}{
				Field: fr.Field,
				Value: *fr.Value,
			})
		}
	}

	if fr.RowKey != "" {
		return json.Marshal(struct {
			Field  string `json:"field"`
			RowKey string `json:"rowKey"`
		}{
			Field:  fr.Field,
			RowKey: fr.RowKey,
		})
	}

	return json.Marshal(struct {
		Field string `json:"field"`
		RowID uint64 `json:"rowID"`
	}{
		Field: fr.Field,
		RowID: fr.RowID,
	})
}

// String is the FieldRow stringer.
func (fr FieldRow) String() string {
	if fr.Value != nil {
		return fmt.Sprintf("%s.%d.%d.%s", fr.Field, fr.RowID, *fr.Value, fr.RowKey)
	}
	return fmt.Sprintf("%s.%d.%s", fr.Field, fr.RowID, fr.RowKey)
}

type aggregateType int

const (
	nilAggregate        aggregateType = 0
	sumAggregate        aggregateType = 1
	distinctAggregate   aggregateType = 2
	decimalSumAggregate aggregateType = 3
)

// GroupCounts is a list of GroupCount.
type GroupCounts struct {
	groups        []GroupCount
	aggregateType aggregateType
}

// AggregateColumn gives the likely column name to use for aggregates, because
// for historical reasons we used "sum" when it was a sum, but don't want to
// use that when it's something else. This will likely get revisited.
func (g *GroupCounts) AggregateColumn() string {
	switch g.aggregateType {
	case sumAggregate:
		return "sum"
	case distinctAggregate:
		return "aggregate"
	case decimalSumAggregate:
		return "decimalSum"
	default:
		return ""
	}
}

// Groups is a convenience method to let us not worry as much about the
// potentially-nil nature of a *GroupCounts.
func (g *GroupCounts) Groups() []GroupCount {
	if g == nil {
		return nil
	}
	return g.groups
}

// NewGroupCounts creates a GroupCounts with the given type and slice
// of GroupCount objects. There's intentionally no externally-accessible way
// to change the []GroupCount after creation.
func NewGroupCounts(agg string, groups ...GroupCount) *GroupCounts {
	var aggType aggregateType
	switch agg {
	case "sum":
		aggType = sumAggregate
	case "aggregate":
		aggType = distinctAggregate
	case "decimalSum":
		aggType = decimalSumAggregate
	case "":
		aggType = nilAggregate
	default:
		panic(fmt.Sprintf("invalid aggregate type %q", agg))
	}
	return &GroupCounts{aggregateType: aggType, groups: groups}
}

// ToTable implements the ToTabler interface.
func (g *GroupCounts) ToTable() (*proto.TableResponse, error) {
	return proto.RowsToTable(g, len(g.Groups()))
}

// ToRows implements the ToRowser interface.
func (g *GroupCounts) ToRows(callback func(*proto.RowResponse) error) error {
	agg := g.AggregateColumn()
	for i, gc := range g.Groups() {
		var ci []*proto.ColumnInfo
		if i == 0 {
			for _, fieldRow := range gc.Group {
				if fieldRow.RowKey != "" {
					ci = append(ci, &proto.ColumnInfo{Name: fieldRow.Field, Datatype: "string"})
				} else if fieldRow.Value != nil {
					ci = append(ci, &proto.ColumnInfo{Name: fieldRow.Field, Datatype: "int64"})
				} else {
					ci = append(ci, &proto.ColumnInfo{Name: fieldRow.Field, Datatype: "uint64"})
				}
			}
			ci = append(ci, &proto.ColumnInfo{Name: "count", Datatype: "uint64"})
			if agg != "" {
				ci = append(ci, &proto.ColumnInfo{Name: agg, Datatype: "int64"})
			}

		}
		rowResp := &proto.RowResponse{
			Headers: ci,
			Columns: []*proto.ColumnResponse{},
		}

		for _, fieldRow := range gc.Group {
			if fieldRow.RowKey != "" {
				rowResp.Columns = append(rowResp.Columns, &proto.ColumnResponse{ColumnVal: &proto.ColumnResponse_StringVal{StringVal: fieldRow.RowKey}})
			} else if fieldRow.Value != nil {
				rowResp.Columns = append(rowResp.Columns, &proto.ColumnResponse{ColumnVal: &proto.ColumnResponse_Int64Val{Int64Val: *fieldRow.Value}})
			} else {
				rowResp.Columns = append(rowResp.Columns, &proto.ColumnResponse{ColumnVal: &proto.ColumnResponse_Uint64Val{Uint64Val: fieldRow.RowID}})
			}
		}
		rowResp.Columns = append(rowResp.Columns,
			&proto.ColumnResponse{ColumnVal: &proto.ColumnResponse_Uint64Val{Uint64Val: gc.Count}})
		if agg != "" {
			rowResp.Columns = append(rowResp.Columns,
				&proto.ColumnResponse{ColumnVal: &proto.ColumnResponse_Int64Val{Int64Val: gc.Agg}})
		}
		if err := callback(rowResp); err != nil {
			return errors.Wrap(err, "calling callback")
		}
	}
	return nil
}

// MarshalJSON makes GroupCounts satisfy interface json.Marshaler and
// customizes the JSON output of the aggregate field label.
func (g *GroupCounts) MarshalJSON() ([]byte, error) {
	groups := g.Groups()
	var counts interface{} = groups

	if len(groups) == 0 {
		return []byte("[]"), nil
	}

	switch g.aggregateType {
	case sumAggregate:
		counts = *(*[]groupCountSum)(unsafe.Pointer(&groups))
	case distinctAggregate:
		counts = *(*[]groupCountAggregate)(unsafe.Pointer(&groups))
	case decimalSumAggregate:
		counts = *(*[]groupCountDecimalSum)(unsafe.Pointer(&groups))
	}
	return json.Marshal(counts)
}

// GroupCount represents a result item for a group by query.
type GroupCount struct {
	Group      []FieldRow   `json:"group"`
	Count      uint64       `json:"count"`
	Agg        int64        `json:"-"`
	DecimalAgg *pql.Decimal `json:"-"`
}

type groupCountSum struct {
	Group      []FieldRow   `json:"group"`
	Count      uint64       `json:"count"`
	Agg        int64        `json:"sum"`
	DecimalAgg *pql.Decimal `json:"-"`
}

type groupCountAggregate struct {
	Group      []FieldRow   `json:"group"`
	Count      uint64       `json:"count"`
	Agg        int64        `json:"aggregate"`
	DecimalAgg *pql.Decimal `json:"-"`
}

type groupCountDecimalSum struct {
	Group      []FieldRow   `json:"group"`
	Count      uint64       `json:"count"`
	Agg        int64        `json:"-"`
	DecimalAgg *pql.Decimal `json:"sum"`
}

var (
	_ GroupCount = GroupCount(groupCountSum{})
	_ GroupCount = GroupCount(groupCountAggregate{})
	_ GroupCount = GroupCount(groupCountDecimalSum{})
)

func (g *GroupCount) Clone() (r *GroupCount) {
	r = &GroupCount{
		Group:      make([]FieldRow, len(g.Group)),
		Count:      g.Count,
		Agg:        g.Agg,
		DecimalAgg: g.DecimalAgg,
	}
	for i := range g.Group {
		r.Group[i] = *(g.Group[i].Clone())
	}
	return
}

// mergeGroupCounts merges two slices of GroupCounts throwing away any that go
// beyond the limit. It assume that the two slices are sorted by the row ids in
// the fields of the group counts. It may modify its arguments.
func mergeGroupCounts(a, b []GroupCount, limit int) []GroupCount {
	if limit > len(a)+len(b) {
		limit = len(a) + len(b)
	}
	ret := make([]GroupCount, 0, limit)
	i, j := 0, 0
	for i < len(a) && j < len(b) && len(ret) < limit {
		switch a[i].Compare(b[j]) {
		case -1:
			ret = append(ret, a[i])
			i++
		case 0:
			a[i].Count += b[j].Count
			a[i].Agg += b[j].Agg
			if a[i].DecimalAgg != nil && b[j].DecimalAgg != nil {
				sum := pql.AddDecimal(*a[i].DecimalAgg, *b[j].DecimalAgg)
				a[i].DecimalAgg = &sum
			}
			ret = append(ret, a[i])
			i++
			j++
		case 1:
			ret = append(ret, b[j])
			j++
		}
	}
	for ; i < len(a) && len(ret) < limit; i++ {
		ret = append(ret, a[i])
	}
	for ; j < len(b) && len(ret) < limit; j++ {
		ret = append(ret, b[j])
	}
	return ret
}

// Compare is used in ordering two GroupCount objects.
func (g GroupCount) Compare(o GroupCount) int {
	for i, g1 := range g.Group {
		g2 := o.Group[i]

		if g1.Value != nil && g2.Value != nil {
			if *g1.Value < *g2.Value {
				return -1
			}
			if *g1.Value > *g2.Value {
				return 1
			}
		} else {
			if g1.RowID < g2.RowID {
				return -1
			}
			if g1.RowID > g2.RowID {
				return 1
			}
		}
	}
	return 0
}

func (g GroupCount) satisfiesCondition(subj string, cond *pql.Condition) bool {
	switch subj {
	case "count":
		switch cond.Op {
		case pql.EQ, pql.NEQ, pql.LT, pql.LTE, pql.GT, pql.GTE:
			val, ok := cond.Uint64Value()
			if !ok {
				return false
			}
			if cond.Op == pql.EQ {
				if g.Count == val {
					return true
				}
			} else if cond.Op == pql.NEQ {
				if g.Count != val {
					return true
				}
			} else if cond.Op == pql.LT {
				if g.Count < val {
					return true
				}
			} else if cond.Op == pql.LTE {
				if g.Count <= val {
					return true
				}
			} else if cond.Op == pql.GT {
				if g.Count > val {
					return true
				}
			} else if cond.Op == pql.GTE {
				if g.Count >= val {
					return true
				}
			}
		case pql.BETWEEN, pql.BTWN_LT_LTE, pql.BTWN_LTE_LT, pql.BTWN_LT_LT:
			val, ok := cond.Uint64SliceValue()
			if !ok {
				return false
			}
			if cond.Op == pql.BETWEEN {
				if val[0] <= g.Count && g.Count <= val[1] {
					return true
				}
			} else if cond.Op == pql.BTWN_LT_LTE {
				if val[0] < g.Count && g.Count <= val[1] {
					return true
				}
			} else if cond.Op == pql.BTWN_LTE_LT {
				if val[0] <= g.Count && g.Count < val[1] {
					return true
				}
			} else if cond.Op == pql.BTWN_LT_LT {
				if val[0] < g.Count && g.Count < val[1] {
					return true
				}
			}
		}
	case "sum":
		switch cond.Op {
		case pql.EQ, pql.NEQ, pql.LT, pql.LTE, pql.GT, pql.GTE:
			val, ok := cond.Int64Value()
			if !ok {
				return false
			}
			if cond.Op == pql.EQ {
				if g.Agg == val {
					return true
				}
			} else if cond.Op == pql.NEQ {
				if g.Agg != val {
					return true
				}
			} else if cond.Op == pql.LT {
				if g.Agg < val {
					return true
				}
			} else if cond.Op == pql.LTE {
				if g.Agg <= val {
					return true
				}
			} else if cond.Op == pql.GT {
				if g.Agg > val {
					return true
				}
			} else if cond.Op == pql.GTE {
				if g.Agg >= val {
					return true
				}
			}
		case pql.BETWEEN, pql.BTWN_LT_LTE, pql.BTWN_LTE_LT, pql.BTWN_LT_LT:
			val, ok := cond.Int64SliceValue()
			if !ok {
				return false
			}
			if cond.Op == pql.BETWEEN {
				if val[0] <= g.Agg && g.Agg <= val[1] {
					return true
				}
			} else if cond.Op == pql.BTWN_LT_LTE {
				if val[0] < g.Agg && g.Agg <= val[1] {
					return true
				}
			} else if cond.Op == pql.BTWN_LTE_LT {
				if val[0] <= g.Agg && g.Agg < val[1] {
					return true
				}
			} else if cond.Op == pql.BTWN_LT_LT {
				if val[0] < g.Agg && g.Agg < val[1] {
					return true
				}
			}
		}
	}
	return false
}

// ApplyConditionToGroupCounts filters the contents of gcs according
// to the condition. Currently, `count` and `sum` are the only
// fields supported.
func ApplyConditionToGroupCounts(gcs []GroupCount, subj string, cond *pql.Condition) []GroupCount {
	var i int
	for _, gc := range gcs {
		if !gc.satisfiesCondition(subj, cond) {
			continue // drop this GroupCount
		}
		gcs[i] = gc
		i++
	}
	return gcs[:i]
}

func (e *executor) executeGroupByShard(ctx context.Context, qcx *Qcx, index string, c *pql.Call, filter *pql.Call, shard uint64, childRows []RowIDs, bases map[int]int64, ignoreLimit bool) (_ []GroupCount, err error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "executor.executeGroupByShard")
	defer span.Finish()

	var filterRow *Row
	if filter != nil {
		if filterRow, err = e.executeBitmapCallShard(ctx, qcx, index, filter, shard); err != nil {
			return nil, errors.Wrapf(err, "executing group by filter for shard %d", shard)
		}
	}

	aggregate, _, err := c.CallArg("aggregate")
	if err != nil {
		return nil, err
	}

	newspan, ctx := tracing.StartSpanFromContext(ctx, "executor.executeGroupByShard_newGroupByIterator")
	iter, err := newGroupByIterator(e, qcx, childRows, c.Children, aggregate, filterRow, index, shard, e.Holder)
	newspan.Finish()

	if err != nil {
		return nil, errors.Wrapf(err, "getting group by iterator for shard %d", shard)
	}
	if iter == nil {
		return []GroupCount{}, nil
	}

	limit := int(^uint(0) >> 1)
	if lim, hasLimit, err := c.UintArg("limit"); err != nil {
		return nil, err
	} else if !ignoreLimit && hasLimit {
		limit = int(lim)
	}

	results := make([]GroupCount, 0)

	num := 0
	for gc, done, err := iter.Next(ctx); !done && num < limit; gc, done, err = iter.Next(ctx) {
		if err != nil {
			return nil, err
		}

		if gc.Count > 0 {
			num++
			results = append(results, gc)
		}
	}

	// Apply bases.
	//
	// SUP-139: The group value is shared across multiple groups so we can't
	// add the base to each one. Instead, we need to track which ones have been
	// seen already and avoid adding to those again in the future.
	for i, base := range bases {
		m := make(map[*int64]struct{})

		for _, r := range results {
			if _, ok := m[r.Group[i].Value]; ok {
				continue
			}

			*r.Group[i].Value += base
			m[r.Group[i].Value] = struct{}{}
		}
	}

	return results, nil
}

func (e *executor) executeRows(ctx context.Context, qcx *Qcx, index string, c *pql.Call, shards []uint64, opt *ExecOptions) (RowIDs, error) {
	// Fetch field name from argument.
	// Check "field" first for backwards compatibility.
	// TODO: remove at Pilosa 2.0
	var fieldName string
	var ok bool
	if fieldName, ok = c.Args["field"].(string); ok {
		c.Args["_field"] = fieldName
	}
	if fieldName, ok = c.Args["_field"].(string); !ok {
		return nil, errors.New("Rows() field required")
	}
	if columnID, ok, err := c.UintArg("column"); err != nil {
		return nil, errors.Wrap(err, "getting column")
	} else if ok {
		shards = []uint64{columnID / ShardWidth}
	}

	// TODO, support "in" in conjunction w/ other args... or at least error if they're present together
	if ids, found, err := c.UintSliceArg("in"); err != nil {
		return nil, errors.Wrapf(err, "'in' argument of Rows must be a slice")
	} else if found {
		// "in" not supported with other args, so check here
		for arg := range c.Args {
			if arg != "field" && arg != "_field" && arg != "in" {
				return nil, errors.Errorf("Rows call with 'in' does not support other arguments, but found '%s'", arg)
			}
		}
		return ids, nil
	}

	// Execute calls in bulk on each remote node and merge.
	mapFn := func(ctx context.Context, shard uint64, mopt *mapOptions) (_ interface{}, err error) {
		return e.executeRowsShard(ctx, qcx, index, fieldName, c, shard)
	}

	// Determine limit so we can use it when reducing.
	limit := int(^uint(0) >> 1)
	if lim, hasLimit, err := c.UintArg("limit"); err != nil {
		return nil, err
	} else if hasLimit {
		limit = int(lim)
	}

	// Merge returned results at coordinating node.
	reduceFn := func(ctx context.Context, prev, v interface{}) interface{} {
		other, _ := prev.(RowIDs)
		if err := ctx.Err(); err != nil {
			return err
		}
		return other.Merge(v.(RowIDs), limit)
	}
	// Get full result set.
	other, err := e.mapReduce(ctx, index, shards, c, opt, mapFn, reduceFn)
	if err != nil {
		return nil, err
	}
	results, _ := other.(RowIDs)

	if !opt.Remote {
		if like, hasLike, err := c.StringArg("like"); err != nil {
			return nil, errors.Wrap(err, "getting like pattern")
		} else if hasLike {
			matches, err := e.Cluster.matchField(ctx, e.Holder.Field(index, fieldName), like)
			if err != nil {
				return nil, errors.Wrap(err, "matching like pattern")
			}

			i, j, k := 0, 0, 0
			for i < len(results) && j < len(matches) {
				x, y := results[i], matches[j]
				switch {
				case x < y:
					i++
				case y < x:
					j++
				default:
					results[k] = x
					i++
					j++
					k++
				}
			}
			results = results[:k]
		}
	}

	return results, nil
}

func (e *executor) executeRowsShard(ctx context.Context, qcx *Qcx, index string, fieldName string, c *pql.Call, shard uint64) (_ RowIDs, err0 error) {
	// Fetch index.
	idx := e.Holder.Index(index)
	if idx == nil {
		return nil, newNotFoundError(ErrIndexNotFound, index)
	}
	// Fetch field.
	f := e.Holder.Field(index, fieldName)
	if f == nil {
		return nil, newNotFoundError(ErrFieldNotFound, fieldName)
	}

	// rowIDs is the result set.
	var rowIDs RowIDs

	// TODO, support "in" in conjunction w/ other args... or at least error if they're present together
	if ids, found, err := c.UintSliceArg("in"); err != nil {
		return nil, errors.Wrapf(err, "'in' argument of Rows must be a slice")
	} else if found {
		return ids, nil
	}

	// views contains the list of views to inspect (and merge)
	// in order to represent `Rows` for the field.
	views := []string{viewStandard}

	switch f.Type() {
	case FieldTypeSet, FieldTypeMutex:
	case FieldTypeTime:
		var err error

		// Parse "from" time, if set.
		var fromTime time.Time
		if v, ok := c.Args["from"]; ok {
			if fromTime, err = parseTime(v); err != nil {
				return nil, errors.Wrap(err, "parsing from time")
			}
		}

		// Parse "to" time, if set.
		var toTime time.Time
		if v, ok := c.Args["to"]; ok {
			if toTime, err = parseTime(v); err != nil {
				return nil, errors.Wrap(err, "parsing to time")
			}
		}
		views, err = f.viewsByTimeRange(fromTime, toTime)
		if err != nil {
			return nil, err
		}
	default:
		return nil, errors.Errorf("%s fields not supported by Rows() query", f.Type())
	}

	start := uint64(0)
	if previous, ok, err := c.UintArg("previous"); err != nil {
		return nil, errors.Wrap(err, "getting previous")
	} else if ok {
		start = previous + 1
	}

	filters := []roaring.BitmapFilter{}
	if columnID, ok, err := c.UintArg("column"); err != nil {
		return nil, err
	} else if ok {
		colShard := columnID >> shardwidth.Exponent
		if colShard != shard {
			return rowIDs, nil
		}
		filters = append(filters, roaring.NewBitmapColumnFilter(columnID))
	}

	limit := int(^uint(0) >> 1)
	if lim, hasLimit, err := c.UintArg("limit"); err != nil {
		return nil, errors.Wrap(err, "getting limit")
	} else if hasLimit {
		filters = append(filters, roaring.NewBitmapRowLimitFilter(lim))
		limit = int(lim)
	}

	tx, finisher, err := qcx.GetTx(Txo{Write: !writable, Index: idx, Shard: shard})
	if err != nil {
		return nil, err
	}
	defer finisher(&err0)
	for _, view := range views {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		frag := e.Holder.fragment(index, fieldName, view, shard)
		if frag == nil {
			continue
		}

		viewRows, err := frag.rows(ctx, tx, start, filters...)
		if err != nil {
			return nil, err
		}
		rowIDs = rowIDs.Merge(viewRows, limit)
	}

	return rowIDs, nil
}

type ExtractedTableField struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type KeyOrID struct {
	ID    uint64
	Key   string
	Keyed bool
}

func (kid KeyOrID) MarshalJSON() ([]byte, error) {
	if kid.Keyed {
		return json.Marshal(kid.Key)
	}

	return json.Marshal(kid.ID)
}

type ExtractedTableColumn struct {
	Column KeyOrID       `json:"column"`
	Rows   []interface{} `json:"rows"`
}

type ExtractedTable struct {
	Fields  []ExtractedTableField  `json:"fields"`
	Columns []ExtractedTableColumn `json:"columns"`
}

// ToRows implements the ToRowser interface.
func (t ExtractedTable) ToRows(callback func(*proto.RowResponse) error) error {
	if len(t.Columns) == 0 {
		return nil
	}

	headers := make([]*proto.ColumnInfo, len(t.Fields)+1)
	colType := "uint64"
	if t.Columns[0].Column.Keyed {
		colType = "string"
	}
	headers[0] = &proto.ColumnInfo{
		Name:     "_id",
		Datatype: colType,
	}
	dataHeaders := headers[1:]
	for i, f := range t.Fields {
		dataHeaders[i] = &proto.ColumnInfo{
			Name:     f.Name,
			Datatype: f.Type,
		}
	}

	for _, c := range t.Columns {
		cols := make([]*proto.ColumnResponse, len(c.Rows)+1)
		if c.Column.Keyed {
			cols[0] = &proto.ColumnResponse{
				ColumnVal: &proto.ColumnResponse_StringVal{
					StringVal: c.Column.Key,
				},
			}
		} else {
			cols[0] = &proto.ColumnResponse{
				ColumnVal: &proto.ColumnResponse_Uint64Val{
					Uint64Val: c.Column.ID,
				},
			}
		}
		valCols := cols[1:]
		for i, r := range c.Rows {
			var col *proto.ColumnResponse
			switch r := r.(type) {
			case nil:
				col = &proto.ColumnResponse{}
			case bool:
				col = &proto.ColumnResponse{
					ColumnVal: &proto.ColumnResponse_BoolVal{
						BoolVal: r,
					},
				}
			case int64:
				col = &proto.ColumnResponse{
					ColumnVal: &proto.ColumnResponse_Int64Val{
						Int64Val: r,
					},
				}
			case uint64:
				col = &proto.ColumnResponse{
					ColumnVal: &proto.ColumnResponse_Uint64Val{
						Uint64Val: r,
					},
				}
			case string:
				col = &proto.ColumnResponse{
					ColumnVal: &proto.ColumnResponse_StringVal{
						StringVal: r,
					},
				}
			case []uint64:
				col = &proto.ColumnResponse{
					ColumnVal: &proto.ColumnResponse_Uint64ArrayVal{
						Uint64ArrayVal: &proto.Uint64Array{
							Vals: r,
						},
					},
				}
			case []string:
				col = &proto.ColumnResponse{
					ColumnVal: &proto.ColumnResponse_StringArrayVal{
						StringArrayVal: &proto.StringArray{
							Vals: r,
						},
					},
				}
			case pql.Decimal:
				rValue := r.Value()
				rValuePtr := &rValue
				col = &proto.ColumnResponse{
					ColumnVal: &proto.ColumnResponse_DecimalVal{
						DecimalVal: &proto.Decimal{
							Value: rValuePtr.Int64(),
							Scale: r.Scale,
						},
					},
				}
			case time.Time:
				col = &proto.ColumnResponse{
					ColumnVal: &proto.ColumnResponse_TimestampVal{
						TimestampVal: r.UTC().Format(time.RFC3339Nano),
					},
				}
			default:
				return errors.Errorf("unsupported field value: %v (type: %T)", r, r)
			}
			valCols[i] = col
		}
		err := callback(&proto.RowResponse{
			Headers: headers,
			Columns: cols,
		})
		if err != nil {
			return err
		}
	}

	return nil
}

// ToTable converts the table to protobuf format.
func (t ExtractedTable) ToTable() (*proto.TableResponse, error) {
	return proto.RowsToTable(t, len(t.Columns))
}

type ExtractedIDColumn struct {
	ColumnID uint64
	Rows     [][]uint64
}

type ExtractedIDMatrix struct {
	Fields  []string
	Columns []ExtractedIDColumn
}

func (e *ExtractedIDMatrix) Append(m ExtractedIDMatrix) {
	e.Columns = append(e.Columns, m.Columns...)
	if e.Fields == nil {
		e.Fields = m.Fields
	}
}

var (
	typeSQLNullString = reflect.TypeOf(sql.NullString{})
	typeSQLNullBool   = reflect.TypeOf(sql.NullBool{})
	typeSQLNullInt32  = reflect.TypeOf(sql.NullInt32{})
	typeSQLNullInt64  = reflect.TypeOf(sql.NullInt64{})
)

func (e *executor) executeExternalLookup(ctx context.Context, qcx *Qcx, index string, c *pql.Call, shards []uint64, opt *ExecOptions) (res ExtractedTable, err error) {
	if e.Holder.lookupDB == nil {
		return ExtractedTable{}, errors.New("external DB connection is not configured")
	}

	idx := e.Holder.Index(index)
	if idx == nil {
		return ExtractedTable{}, errors.Errorf("index not found: %q", index)
	}

	query, ok, err := c.StringArg("query")
	if err != nil {
		return ExtractedTable{}, errors.Wrap(err, "looking up query")
	}
	if !ok {
		return ExtractedTable{}, errors.New("missing query")
	}

	switch len(c.Children) {
	case 0:
		return ExtractedTable{}, errors.New("missing lookup input")
	case 1:
	default:
		return ExtractedTable{}, errors.New("too many inputs to lookup query")
	}

	write, _, err := c.BoolArg("write")
	if err != nil {
		return ExtractedTable{}, errors.Wrap(err, "parsing write argument")
	}

	rawArg, err := e.executeCall(ctx, qcx, index, c.Children[0], shards, opt)
	if err != nil {
		return ExtractedTable{}, errors.Wrapf(err, "evaluating SQL argument call %q", c.String())
	}

	qr := []interface{}{rawArg}
	err = e.translateResults(ctx, index, idx, c.Children, qr, e.maxMemory)
	if err != nil {
		return ExtractedTable{}, errors.Wrap(err, "translating query result")
	}
	argRow, ok := qr[0].(*Row)
	if !ok {
		return ExtractedTable{}, errors.Errorf("argument call result is a %T but expected a row", qr[0])
	}
	if !argRow.Any() {
		return ExtractedTable{}, nil
	}

	var arg interface{}
	if argRow.Keys != nil {
		arg = argRow.Keys
	} else {
		arg = argRow.Columns()
	}

	if write {
		tx, err := e.Holder.lookupDB.BeginTx(ctx, nil)
		if err != nil {
			return ExtractedTable{}, errors.Wrap(err, "creating postgres transaction")
		}
		defer tx.Rollback() //nolint:errcheck

		_, err = tx.ExecContext(ctx, query, pq.Array(arg))
		if err != nil {
			return ExtractedTable{}, errors.Wrap(err, "executing postgres write")
		}

		return ExtractedTable{}, errors.Wrap(tx.Commit(), "committing postgres transaction")
	}

	result, err := e.Holder.lookupDB.QueryContext(ctx, query, pq.Array(arg))
	if err != nil {
		return ExtractedTable{}, errors.Wrapf(err, "SQL query failed")
	}
	defer func() {
		cerr := result.Close()
		if cerr != nil && err == nil {
			err = cerr
		}
	}()
	if !result.Next() {
		return ExtractedTable{}, errors.Wrap(result.Err(), "reading SQL query result")
	}

	colTypes, err := result.ColumnTypes()
	if err != nil {
		return ExtractedTable{}, errors.Wrapf(err, "fetching SQL query result types")
	}

	scanSlots := make([]interface{}, len(colTypes))
	scanMapper := make([]func() interface{}, len(colTypes))
	header := make([]ExtractedTableField, len(colTypes))
	for i, colType := range colTypes {
		scanType := colType.ScanType()
	setupScan:
		if scanType.PkgPath() != "" {
			// This is a named type.
			switch scanType {
			case typeSQLNullString:
				var dst sql.NullString
				scanSlots[i] = &dst
				scanMapper[i] = func() interface{} {
					if !dst.Valid {
						return nil
					}

					return dst.String
				}
				header[i] = ExtractedTableField{
					Name: colType.Name(),
					Type: "string",
				}

			case typeSQLNullBool:
				var dst sql.NullBool
				scanSlots[i] = &dst
				scanMapper[i] = func() interface{} {
					if !dst.Valid {
						return nil
					}

					return dst.Bool
				}
				header[i] = ExtractedTableField{
					Name: colType.Name(),
					Type: "bool",
				}

			case typeSQLNullInt32:
				var dst sql.NullInt32
				scanSlots[i] = &dst
				scanMapper[i] = func() interface{} {
					if !dst.Valid {
						return nil
					}

					return int64(dst.Int32)
				}
				header[i] = ExtractedTableField{
					Name: colType.Name(),
					Type: "int64",
				}

			case typeSQLNullInt64:
				var dst sql.NullInt64
				scanSlots[i] = &dst
				scanMapper[i] = func() interface{} {
					if !dst.Valid {
						return nil
					}

					return dst.Int64
				}
				header[i] = ExtractedTableField{
					Name: colType.Name(),
					Type: "int64",
				}

			default:
				return ExtractedTable{}, errors.Errorf("unable to process result type %v from SQL query %q", scanType, query)
			}
			continue
		}

		switch scanType.Kind() {
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			var dst uint64
			scanSlots[i] = &dst
			scanMapper[i] = func() interface{} { return dst }
			header[i] = ExtractedTableField{
				Name: colType.Name(),
				Type: "uint64",
			}

		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			// The database may have lied and this may be nullable.
			scanType = typeSQLNullInt64
			goto setupScan

		case reflect.String:
			// The database may have lied and this may be nullable.
			scanType = typeSQLNullString
			goto setupScan

		case reflect.Bool:
			// The database may have lied and this may be nullable.
			scanType = typeSQLNullBool
			goto setupScan

		default:
			return ExtractedTable{}, errors.Errorf("unable to process result type %v from SQL query %q", scanType, query)
		}
	}

	var columns []ExtractedTableColumn
	for {
		err := result.Scan(scanSlots...)
		if err != nil {
			return ExtractedTable{}, errors.Wrap(err, "scanning SQL result")
		}

		var col KeyOrID
		switch v := scanMapper[0]().(type) {
		case nil:
			return ExtractedTable{}, errors.Errorf("missing primary key in result")
		case uint64:
			col.ID = v
		case int64:
			col.ID = uint64(v)
		case string:
			col.Keyed = true
			col.Key = v
		default:
			return ExtractedTable{}, errors.Wrap(err, "cannot use %v of type %T as primary key for result table")
		}

		data := make([]interface{}, len(scanMapper)-1)
		for i, m := range scanMapper[1:] {
			data[i] = m()
		}

		columns = append(columns, ExtractedTableColumn{
			Column: col,
			Rows:   data,
		})

		if !result.Next() {
			break
		}
	}

	err = result.Err()
	if err != nil {
		return ExtractedTable{}, errors.Wrap(err, "reading SQL result")
	}

	return ExtractedTable{
		Fields:  header[1:],
		Columns: columns,
	}, nil
}

type TimeArgs struct {
	From time.Time
	To   time.Time
}

func extractFieldsFromRowsCalls(c *pql.Call) ([]string, []TimeArgs, error) {
	fields := make([]string, len(c.Children)-1)
	timeArgs := make([]TimeArgs, len(c.Children)-1)
	for i, rows := range c.Children[1:] {
		if rows.Name != "Rows" {
			return fields, timeArgs, errors.Errorf("child call of Extract is %q but expected Rows", rows.Name)
		}
		var fieldName string
		var ok bool
		var timeArg TimeArgs
		for k, v := range rows.Args {
			switch k {
			case "field", "_field":
				fieldName = v.(string)
				ok = true
			case "from":
				fromTime, err := parseTime(v)
				if err != nil {
					return fields, timeArgs, errors.Wrap(err, "parsing from time")
				}
				timeArg.From = fromTime
			case "to":
				toTime, err := parseTime(v)
				if err != nil {
					return fields, timeArgs, errors.Wrap(err, "parsing from time")
				}
				timeArg.To = toTime
			default:
				return fields, timeArgs, errors.Errorf("unsupported Rows argument for Extract: %q", k)
			}
		}
		if !ok {
			return fields, timeArgs, errors.New("missing field specification in Rows")
		}
		fields[i] = fieldName
		timeArgs[i] = timeArg
	}
	return fields, timeArgs, nil
}

func makeReduceFunc(sort_desc bool) func(ctx context.Context, prev, v interface{}) interface{} {
	return func(ctx context.Context, prev, v interface{}) interface{} {
		if err := ctx.Err(); err != nil {
			return err
		}
		switch other := v.(type) {
		case ExtractedIDMatrixSorted:
			if prev == nil {
				return other
			}
			if _, ok := prev.(ExtractedIDMatrixSorted); !ok {
				return other
			}
			if out, err := MergeExtractedIDMatrixSorted(other, prev.(ExtractedIDMatrixSorted), sort_desc); err != nil {
				return err
			} else {
				return out
			}

		case ExtractedIDMatrix:
			if prev != nil {
				if p, ok := prev.(ExtractedIDMatrix); ok {
					other.Append(p)
				}
			}
			return other
		case nil:
			return prev
		default:
			return ExtractedIDMatrix{}
		}
	}
}

func handleExtractResults(other interface{}, filter *pql.Call, opt *ExecOptions) (interface{}, error) {
	switch results := other.(type) {
	case ExtractedIDMatrix:
		sort.Slice(results.Columns, func(i, j int) bool {
			return results.Columns[i].ColumnID < results.Columns[j].ColumnID
		})
		return results, nil
	case ExtractedIDMatrixSorted:
		offset, hasOffset, err := filter.UintArg("offset")
		if err != nil {
			return ExtractedIDMatrix{}, err
		}
		limit, hasLimit, err := filter.UintArg("limit")
		if err != nil {
			return ExtractedIDMatrix{}, err
		}
		if hasOffset {
			results.ExtractedIDMatrix.Columns = results.ExtractedIDMatrix.Columns[offset:]
		}
		if hasLimit && limit < uint64(len(results.RowKVs)) {
			results.ExtractedIDMatrix.Columns = results.ExtractedIDMatrix.Columns[:limit]
		}
		// need to reture sorted to originating node in order to be able to sort properly
		if opt.Remote {
			return results, nil
		}
		return *results.ExtractedIDMatrix, nil
	default:
		return ExtractedIDMatrix{}, errors.New("Extract, unexpected result type found")
	}
}

func (e *executor) executeExtract(ctx context.Context, qcx *Qcx, index string, c *pql.Call, shards []uint64, opt *ExecOptions) (interface{}, error) {
	// Extract the column filter call.
	if len(c.Children) < 1 {
		return ExtractedIDMatrix{}, errors.New("missing column filter in Extract")
	}
	filter := c.Children[0]
	var sort_desc bool
	if filter.Name == "Sort" {
		sd, _, err := filter.BoolArg("sort-desc")
		if err != nil {
			return ExtractedIDMatrix{}, errors.Wrap(err, "sort field error")
		}
		sort_desc = sd
	}

	// Extract fields from rows calls.
	fields, timeArgs, err := extractFieldsFromRowsCalls(c)
	if err != nil {
		return ExtractedIDMatrix{}, errors.Wrap(err, "sort field error")
	}

	// Execute calls in bulk on each remote node and merge.
	mapFn := func(ctx context.Context, shard uint64, mopt *mapOptions) (_ interface{}, err error) {
		return e.executeExtractShard(ctx, qcx, index, fields, filter, shard, mopt, timeArgs)
	}

	// Merge returned results at coordinating node.
	reduceFn := makeReduceFunc(sort_desc)
	// Get full result set.
	other, err := e.mapReduce(ctx, index, shards, c, opt, mapFn, reduceFn)
	if err != nil {
		return ExtractedIDMatrix{}, err
	}
	return handleExtractResults(other, filter, opt)
}

func mergeBits(bits *Row, mask uint64, out map[uint64]uint64) {
	for _, v := range bits.Columns() {
		out[v] |= mask
	}
}

var (
	trueRowFakeID  = []uint64{1}
	falseRowFakeID = []uint64{0}
)

func (e *executor) executeExtractShard(ctx context.Context, qcx *Qcx, index string, fields []string, filter *pql.Call, shard uint64, mopt *mapOptions, timeArgs []TimeArgs) (_ interface{}, err0 error) {
	var colsBitmap *Row
	var cols []uint64
	var sortedResult *SortedRow
	if filter.Name == "Sort" {
		res, err := e.executeSortShard(ctx, qcx, index, filter, shard)
		if err != nil {
			return ExtractedIDMatrix{}, errors.Wrap(err, "failed to get extraction sort column filter")
		}
		cols = res.Columns()
		colsBitmap = res.Row
		sortedResult = res
	} else {
		// Execute filter.
		res, err := e.executeBitmapCallShard(ctx, qcx, index, filter, shard)
		if err != nil {
			return ExtractedIDMatrix{}, errors.Wrap(err, "failed to get extraction column filter")
		}
		// Decompress columns bitmap.
		colsBitmap = res
		cols = colsBitmap.Columns()
	}

	// Fetch index.
	idx := e.Holder.Index(index)
	if idx == nil {
		return ExtractedIDMatrix{}, newNotFoundError(ErrIndexNotFound, index)
	}

	tx, finisher, err := qcx.GetTx(Txo{Write: !writable, Index: idx, Shard: shard})
	if err != nil {
		return ExtractedIDMatrix{}, err
	}
	defer finisher(&err0)

	// Generate a matrix to stuff the results into.
	m := make([]ExtractedIDColumn, len(cols))
	{
		rowsBuf := make([][]uint64, len(m)*len(fields))
		for i, c := range cols {
			m[i] = ExtractedIDColumn{
				ColumnID: c,
				Rows:     rowsBuf[i*len(fields) : (i+1)*len(fields) : (i+1)*len(fields)],
			}
		}
	}
	if len(m) == 0 {
		return ExtractedIDMatrix{
			Fields:  fields,
			Columns: m,
		}, nil
	}
	mLookup := make(map[uint64]int)
	for i, j := range cols {
		mLookup[j] = i
	}

	// Process fields.
	for i, name := range fields {
		// Look up the field.
		field := idx.Field(name)
		if field == nil {
			return ExtractedIDMatrix{}, newNotFoundError(ErrFieldNotFound, name)
		}

		switch field.Type() {
		case FieldTypeSet, FieldTypeMutex:
			// Handle a set field by listing the rows and then intersecting them with the filter.

			// Extract the standard view fragment.
			fragment := e.Holder.fragment(index, name, viewStandard, shard)
			if fragment == nil {
				// There is nothing here.
				continue
			}

			// List all rows in the standard view.
			rows, err := fragment.rows(ctx, tx, 0)
			if err != nil {
				return ExtractedIDMatrix{}, errors.Wrap(err, "listing rows in set field")
			}

			// Loop over each row and scan the intersection with the filter.
			for _, rowID := range rows {
				// Load row from fragment.
				row, err := fragment.row(tx, rowID)
				if err != nil {
					return ExtractedIDMatrix{}, errors.Wrap(err, "loading row from fragment")
				}

				// Apply column filter to row.
				row = row.Intersect(colsBitmap)

				// Rotate vector into the matrix.
				for _, columnID := range row.Columns() {
					fieldSlot := &m[mLookup[columnID]].Rows[i]
					*fieldSlot = append(*fieldSlot, rowID)
				}
			}
		case FieldTypeTime:
			// Handle a set field by listing the rows and then intersecting them with the filter.
			timeArg := timeArgs[i]

			views, err := field.viewsByTimeRange(timeArg.From, timeArg.To)
			if err != nil {
				return ExtractedIDMatrix{}, errors.Wrap(err, "problem with from/to")
			}

			dedup := make(map[uint64]map[uint64]struct{})
			for _, view := range views {
				fragment := e.Holder.fragment(index, name, view, shard)
				if fragment == nil {
					// There is nothing here.
					continue
				}

				rows, err := fragment.rows(ctx, tx, 0)
				if err != nil {
					return ExtractedIDMatrix{}, errors.Wrap(err, "listing rows in set field")
				}

				// Loop over each row and scan the intersection with the filter.
				for _, rowID := range rows {
					// Load row from fragment.
					row, err := fragment.row(tx, rowID)
					if err != nil {
						return ExtractedIDMatrix{}, errors.Wrap(err, "loading row from fragment")
					}

					// Apply column filter to row.
					row = row.Intersect(colsBitmap)
					if len(views) == 1 {
						for _, columnID := range row.Columns() {
							fieldSlot := &m[mLookup[columnID]].Rows[i]
							*fieldSlot = append(*fieldSlot, rowID)
						}
					} else {
						// Rotate vector into the matrix.
						for _, columnID := range row.Columns() {
							if _m, ok := dedup[columnID]; ok {
								_m[rowID] = struct{}{}
								dedup[columnID] = _m
							} else {
								dedup[columnID] = map[uint64]struct{}{rowID: {}}
							}
						}
					}
				}
			}
			if len(views) > 1 {
				for columnID, rowsMap := range dedup {
					rowids := make([]uint64, 0, len(rowsMap))
					for k := range rowsMap {
						rowids = append(rowids, k)
					}
					sort.Slice(rowids, func(i, j int) bool { return rowids[i] < rowids[j] })
					m[mLookup[columnID]].Rows[i] = rowids
				}
			}
		case FieldTypeBool:
			// Handle bool fields by scanning the true and false rows and assigning an integer.

			// Extract the standard view fragment.
			fragment := e.Holder.fragment(index, name, viewStandard, shard)
			if fragment == nil {
				// There is nothing here.
				continue
			}

			// Fetch true and false rows.
			trueRow, err := fragment.row(tx, trueRowID)
			if err != nil {
				return ExtractedIDMatrix{}, errors.Wrap(err, "loading true row from fragment")
			}
			falseRow, err := fragment.row(tx, falseRowID)
			if err != nil {
				return ExtractedIDMatrix{}, errors.Wrap(err, "loading true row from fragment")
			}

			// Fetch values by column.
			for j := range m {
				col := m[j].ColumnID
				switch {
				case trueRow.Includes(col):
					m[j].Rows[i] = trueRowFakeID
				case falseRow.Includes(col):
					m[j].Rows[i] = falseRowFakeID
				}
			}

		case FieldTypeInt, FieldTypeDecimal, FieldTypeTimestamp:
			// Handle an int/decimal field by rotating a BSI matrix.

			// Extract the BSI view fragment.
			fragment := e.Holder.fragment(index, name, viewBSIGroupPrefix+name, shard)
			if fragment == nil {
				// There is nothing here.
				continue
			}

			// Load the BSI group.
			bsig := field.bsiGroup(name)
			if bsig == nil {
				return ExtractedIDMatrix{}, ErrBSIGroupNotFound
			}

			// Load the BSI exists bit.
			exists, err := fragment.row(tx, bsiExistsBit)
			if err != nil {
				return ExtractedIDMatrix{}, errors.Wrap(err, "loading BSI exists bit from fragment")
			}

			// Filter BSI exists bit by selected columns.
			exists = exists.Intersect(colsBitmap)
			if !exists.Any() {
				// No relevant BSI values are present in this fragment.
				continue
			}

			// Populate a map with the BSI data.
			data := make(map[uint64]uint64)
			mergeBits(exists, 0, data)

			// Copy in the sign bit.
			sign, err := fragment.row(tx, bsiSignBit)
			if err != nil {
				return ExtractedIDMatrix{}, errors.Wrap(err, "loading BSI sign bit from fragment")
			}
			sign = sign.Intersect(exists)
			mergeBits(sign, 1<<63, data)

			// Copy in the significand.
			for i := uint64(0); i < bsig.BitDepth; i++ {
				bits, err := fragment.row(tx, bsiOffsetBit+uint64(i))
				if err != nil {
					return ExtractedIDMatrix{}, errors.Wrap(err, "loading BSI significand bit from fragment")
				}
				bits = bits.Intersect(exists)
				mergeBits(bits, 1<<i, data)
			}

			// Store the results back into the matrix.
			for columnID, val := range data {
				// Convert to two's complement and add base back to value.
				val = uint64((2*(int64(val)>>63)+1)*int64(val&^(1<<63)) + bsig.Base)
				m[mLookup[columnID]].Rows[i] = []uint64{val}
			}
		}
	}

	// Emit the final matrix.
	// Like RowIDs, this is an internal type and will need to be converted.
	matrix := ExtractedIDMatrix{
		Fields:  fields,
		Columns: m,
	}
	if v := atomic.AddInt64(mopt.memoryAvailable, -calcResultMemory(matrix)); v < 0 {
		return ExtractedIDMatrix{}, fmt.Errorf("result exceeds available memory")
	}

	if sortedResult != nil {
		return ExtractedIDMatrixSorted{
			ExtractedIDMatrix: &matrix,
			RowKVs:            sortedResult.RowKVs,
		}, nil
	}
	return matrix, nil
}

func (e *executor) executeRowShard(ctx context.Context, qcx *Qcx, index string, c *pql.Call, shard uint64) (_ *Row, err0 error) {
	span, _ := tracing.StartSpanFromContext(ctx, "executor.executeRowShard")
	defer span.Finish()

	// Handle bsiGroup ranges differently.
	if c.HasConditionArg() {
		return e.executeRowBSIGroupShard(ctx, qcx, index, c, shard)
	}

	// Fetch index.
	idx := e.Holder.Index(index)
	if idx == nil {
		return nil, newNotFoundError(ErrIndexNotFound, index)
	}

	// Fetch field name from argument.
	fieldName, err := c.FieldArg()
	if err != nil {
		return nil, errors.New("Row() argument required: field")
	}
	f := idx.Field(fieldName)
	if f == nil {
		return nil, newNotFoundError(ErrFieldNotFound, fieldName)
	}

	err = e.validateTimeCallArgs(c, index)
	if err != nil {
		return nil, err
	}

	// Parse "from" time, if set.
	var fromTime time.Time
	if v, ok := c.Args["from"]; ok {
		if fromTime, err = parseTime(v); err != nil {
			return nil, errors.Wrap(err, "parsing from time")
		}
	}

	// Parse "to" time, if set.
	var toTime time.Time
	if v, ok := c.Args["to"]; ok {
		if toTime, err = parseTime(v); err != nil {
			return nil, errors.Wrap(err, "parsing to time")
		}
	}

	rowID, rowOK, rowErr := c.UintArg(fieldName)
	if rowErr != nil {
		return nil, fmt.Errorf("Row() error with arg for row: %v", rowErr)
	} else if !rowOK {
		return nil, fmt.Errorf("Row() must specify %v", rowLabel)
	}

	// Return row if times are not set and standard view exists.
	timeNotSet := fromTime.IsZero() && toTime.IsZero()
	if c.Name == "Row" && timeNotSet && !f.options.NoStandardView {
		frag := e.Holder.fragment(index, fieldName, viewStandard, shard)
		if frag == nil {
			return NewRow(), nil
		}

		tx, finisher, err := qcx.GetTx(Txo{Write: !writable, Fragment: frag, Index: idx, Shard: shard})
		if err != nil {
			return nil, err
		}
		defer finisher(&err0)
		row, err := frag.row(tx, rowID)
		if qcx.write && err == nil {
			row = row.Clone()
		}
		return row, err
	}

	views, err := f.viewsByTimeRange(fromTime, toTime)
	if err != nil {
		return nil, err
	}

	// Union bitmaps across all time-based views.
	rows := make([]*Row, 0, len(views))
	tx, finisher, err := qcx.GetTx(Txo{Write: !writable, Index: idx, Shard: shard})
	defer finisher(&err0)
	for _, view := range views {
		f := e.Holder.fragment(index, fieldName, view, shard)
		if f == nil {
			continue
		}
		if err != nil {
			return nil, err
		}

		row, err := f.row(tx, rowID)
		if err != nil {
			return nil, err
		}
		rows = append(rows, row)
	}
	if len(rows) == 0 {
		return &Row{}, nil
	} else if len(rows) == 1 {
		if qcx.write {
			return rows[0].Clone(), nil
		}
		return rows[0], nil
	}
	row := rows[0].Union(rows[1:]...)
	if qcx.write {
		row = row.Clone()
	}
	return row, nil
}

// executeRowBSIGroupShard executes a range(bsiGroup) call for a local shard.
func (e *executor) executeRowBSIGroupShard(ctx context.Context, qcx *Qcx, index string, c *pql.Call, shard uint64) (cloneable *Row, err0 error) {
	span, _ := tracing.StartSpanFromContext(ctx, "executor.executeRowBSIGroupShard")
	defer span.Finish()

	// Only one conditional should be present.
	if len(c.Args) == 0 {
		return nil, errors.New("Row(): condition required")
	} else if len(c.Args) > 1 {
		return nil, errors.New("Row(): too many arguments")
	}

	// Extract conditional.
	var fieldName string
	var cond *pql.Condition
	for k, v := range c.Args {
		vv, ok := v.(*pql.Condition)
		if !ok {
			return nil, fmt.Errorf("Row(): %q: expected condition argument, got %v", k, v)
		}
		fieldName, cond = k, vv
	}

	f := e.Holder.Field(index, fieldName)
	if f == nil {
		return nil, newNotFoundError(ErrFieldNotFound, fieldName)
	}

	tx, finisher, err := qcx.GetTx(Txo{Write: !writable, Index: f.idx, Shard: shard})
	if err != nil {
		return nil, err
	}
	defer finisher(&err0)
	defer func() {
		if qcx.write && cloneable != nil {
			cloneable = cloneable.Clone()
		}
	}()

	// EQ null           _exists - frag.NotNull()
	// NEQ null          frag.NotNull()
	// BETWEEN a,b(in)   BETWEEN/frag.RowBetween()
	// BETWEEN a,b(out)  BETWEEN/frag.NotNull()
	// EQ <int>          frag.RangeOp
	// NEQ <int>         frag.RangeOp

	// Handle `!= null` and `== null`.
	if cond.Op == pql.NEQ && cond.Value == nil {
		// Retrieve fragment.
		frag := e.Holder.fragment(index, fieldName, viewBSIGroupPrefix+fieldName, shard)
		if frag == nil {
			return NewRow(), nil
		}
		return frag.notNull(tx)

	} else if cond.Op == pql.EQ && cond.Value == nil {
		// Make sure the index supports existence tracking.
		idx := e.Holder.Index(index)
		if idx == nil {
			return nil, newNotFoundError(ErrIndexNotFound, index)
		} else if idx.existenceField() == nil {
			return nil, errors.Errorf("index does not support existence tracking: %s", index)
		}

		var existenceRow *Row
		existenceFrag := e.Holder.fragment(index, existenceFieldName, viewStandard, shard)
		if existenceFrag == nil {
			existenceRow = NewRow()
		} else {
			if existenceRow, err0 = existenceFrag.row(tx, 0); err0 != nil {
				return nil, err0
			}
		}

		var notNull *Row
		var err error

		// Retrieve notNull from fragment if it exists.
		if frag := e.Holder.fragment(index, fieldName, viewBSIGroupPrefix+fieldName, shard); frag != nil {
			if notNull, err = frag.notNull(tx); err != nil {
				return nil, errors.Wrap(err, "getting fragment not null")
			}
		} else {
			notNull = NewRow()
		}

		return existenceRow.Difference(notNull), nil

	} else if cond.Op == pql.BETWEEN || cond.Op == pql.BTWN_LT_LT ||
		cond.Op == pql.BTWN_LTE_LT || cond.Op == pql.BTWN_LT_LTE {
		predicates, err := getCondIntSlice(f, cond)
		if err != nil {
			return nil, errors.Wrap(err, "getting condition value")
		}

		// Only support two integers for the between operation.
		if len(predicates) != 2 {
			return nil, errors.New("Row(): BETWEEN condition requires exactly two integer values")
		}

		// The reason we don't just call:
		//     return f.RowBetween(fieldName, predicates[0], predicates[1])
		// here is because we need the call to be shard-specific.

		// Find bsiGroup.
		bsig := f.bsiGroup(fieldName)
		if bsig == nil {
			return nil, ErrBSIGroupNotFound
		}

		baseValueMin, baseValueMax, outOfRange := bsig.baseValueBetween(predicates[0], predicates[1])
		if outOfRange {
			return NewRow(), nil
		}

		// Retrieve fragment.
		frag := e.Holder.fragment(index, fieldName, viewBSIGroupPrefix+fieldName, shard)
		if frag == nil {
			return NewRow(), nil
		}

		// If the query is asking for the entire valid range, just return
		// the not-null bitmap for the bsiGroup.
		if predicates[0] <= bsig.Min && predicates[1] >= bsig.Max {
			return frag.notNull(tx)
		}

		return frag.rangeBetween(tx, bsig.BitDepth, baseValueMin, baseValueMax)

	} else {
		value, err := getScaledInt(f, cond.Value)
		if err != nil {
			return nil, errors.Wrap(err, "getting scaled integer")
		}

		// Find bsiGroup.
		bsig := f.bsiGroup(fieldName)
		if bsig == nil {
			return nil, ErrBSIGroupNotFound
		}

		baseValue, outOfRange := bsig.baseValue(cond.Op, value)
		if outOfRange && cond.Op != pql.NEQ {
			return NewRow(), nil
		}

		// Retrieve fragment.
		frag := e.Holder.fragment(index, fieldName, viewBSIGroupPrefix+fieldName, shard)
		if frag == nil {
			return NewRow(), nil
		}

		// LT[E] and GT[E] should return all not-null if selected range fully encompasses valid bsiGroup range.
		if (cond.Op == pql.LT && value > bsig.Max) || (cond.Op == pql.LTE && value >= bsig.Max) ||
			(cond.Op == pql.GT && value < bsig.Min) || (cond.Op == pql.GTE && value <= bsig.Min) {
			return frag.notNull(tx)
		}

		// outOfRange for NEQ should return all not-null.
		if outOfRange && cond.Op == pql.NEQ {
			return frag.notNull(tx)
		}

		return frag.rangeOp(tx, cond.Op, bsig.BitDepth, baseValue)
	}
}

// executeIntersectShard executes a intersect() call for a local shard.
func (e *executor) executeIntersectShard(ctx context.Context, qcx *Qcx, index string, c *pql.Call, shard uint64) (_ *Row, err error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "executor.executeIntersectShard")
	defer span.Finish()

	var other *Row
	if len(c.Children) == 0 {
		return nil, fmt.Errorf("empty Intersect query is currently not supported")
	}
	for i, input := range c.Children {
		row, err := e.executeBitmapCallShard(ctx, qcx, index, input, shard)
		if err != nil {
			return nil, err
		}

		if i == 0 {
			other = row
		} else {
			other = other.Intersect(row)
		}
	}
	other.invalidateCount()
	return other, nil
}

// executeUnionShard executes a union() call for a local shard.
func (e *executor) executeUnionShard(ctx context.Context, qcx *Qcx, index string, c *pql.Call, shard uint64) (out *Row, err error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "executor.executeUnionShard")
	defer span.Finish()

	if len(c.Children) == 0 {
		return NewRow(), nil
	}
	if len(c.Children) == 1 {
		return e.executeBitmapCallShard(ctx, qcx, index, c.Children[0], shard)
	}
	// we have at least two, so...
	rows := make([]*Row, len(c.Children))
	for i, input := range c.Children {
		rows[i], err = e.executeBitmapCallShard(ctx, qcx, index, input, shard)
		if err != nil {
			return nil, err
		}
	}
	return rows[0].Union(rows[1:]...), nil
}

// executeInnerUnionRowsShard executes a special magical call which is actually
// more like Row() than Union(), and takes a call plus a []uint64 of rows, and
// generates the union of the rows in the []uint64.
func (e *executor) executeInnerUnionRowsShard(ctx context.Context, qcx *Qcx, index string, c *pql.Call, shard uint64) (out *Row, err0 error) {
	span, _ := tracing.StartSpanFromContext(ctx, "Executor.executeInnerUnionRowsShard")
	defer span.Finish()

	// Fetch index.
	idx := e.Holder.Index(index)
	if idx == nil {
		return nil, newNotFoundError(ErrIndexNotFound, index)
	}

	fieldName, ok, err := c.StringArg("_field")
	if err != nil {
		return nil, errors.Wrap(err, "finding field")
	}
	if !ok {
		return nil, errors.New("InnerUnionRows requires _field")
	}

	f := idx.Field(fieldName)
	if f == nil {
		return nil, newNotFoundError(ErrFieldNotFound, fieldName)
	}

	// Parse "from" time, if set.
	var fromTime time.Time
	if v, ok := c.Args["from"]; ok {
		if fromTime, err = parseTime(v); err != nil {
			return nil, errors.Wrap(err, "parsing from time")
		}
	}

	// Parse "to" time, if set.
	var toTime time.Time
	if v, ok := c.Args["to"]; ok {
		if toTime, err = parseTime(v); err != nil {
			return nil, errors.Wrap(err, "parsing to time")
		}
	}

	rowIDs, rowOK, err := c.UintSliceArg("rows")
	if err != nil {
		return nil, fmt.Errorf("extracting rows argument: %v", err)
	}
	if !rowOK {
		return nil, fmt.Errorf("InnerUnionRows() must specify rows")
	}

	// Simply return row if times are not set.
	timeNotSet := fromTime.IsZero() && toTime.IsZero()
	if timeNotSet {
		frag := e.Holder.fragment(index, fieldName, viewStandard, shard)
		if frag == nil {
			return NewRow(), nil
		}

		tx, finisher, err := qcx.GetTx(Txo{Write: !writable, Fragment: frag, Index: idx, Shard: shard})
		if err != nil {
			return nil, err
		}
		defer finisher(&err0)
		row, err := frag.unionRows(ctx, tx, rowIDs)
		if qcx.write && err == nil {
			row = row.Clone()
		}
		return row, err
	}

	views, err := f.viewsByTimeRange(fromTime, toTime)
	if err != nil {
		return nil, err
	}

	// Union bitmaps across all time-based views.
	rows := make([]*Row, 0, len(views))
	tx, finisher, err := qcx.GetTx(Txo{Write: !writable, Index: idx, Shard: shard})
	defer finisher(&err0)
	for _, view := range views {
		f := e.Holder.fragment(index, fieldName, view, shard)
		if f == nil {
			continue
		}
		if err != nil {
			return nil, err
		}

		row, err := f.unionRows(ctx, tx, rowIDs)
		if err != nil {
			return nil, err
		}
		rows = append(rows, row)
	}
	if len(rows) == 0 {
		return &Row{}, nil
	} else if len(rows) == 1 {
		if qcx.write {
			return rows[0].Clone(), nil
		}
		return rows[0], nil
	}
	row := rows[0].Union(rows[1:]...)
	if qcx.write {
		row = row.Clone()
	}
	return row, nil
}

// executeXorShard executes a xor() call for a local shard.
func (e *executor) executeXorShard(ctx context.Context, qcx *Qcx, index string, c *pql.Call, shard uint64) (_ *Row, err error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "executor.executeXorShard")
	defer span.Finish()

	other := NewRow()
	for i, input := range c.Children {
		row, err := e.executeBitmapCallShard(ctx, qcx, index, input, shard)
		if err != nil {
			return nil, err
		}

		if i == 0 {
			other = row
		} else {
			other = other.Xor(row)
		}
	}
	other.invalidateCount()
	return other, nil
}

// executePrecomputedCallShard pretends to execute a precomputed call for a local shard.
func (e *executor) executePrecomputedCallShard(ctx context.Context, qcx *Qcx, index string, c *pql.Call, shard uint64) (_ *Row, err error) {
	if c.Precomputed != nil {
		v := c.Precomputed[shard]
		if v == nil {
			return NewRow(), nil
		}
		if r, ok := v.(*Row); ok {
			if r != nil {
				return r, nil
			} else {
				return NewRow(), nil
			}
		}
		return nil, fmt.Errorf("precomputed value is not a row: %T", v)
	}
	return nil, fmt.Errorf("per-shard: missing precomputed values for shard %d", shard)
}

// executeNotShard executes a Not() call for a local shard.
func (e *executor) executeNotShard(ctx context.Context, qcx *Qcx, index string, c *pql.Call, shard uint64) (_ *Row, err0 error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "executor.executeNotShard")
	defer span.Finish()

	if len(c.Children) == 0 {
		return nil, errors.New("Not() requires an input row")
	} else if len(c.Children) > 1 {
		return nil, errors.New("Not() only accepts a single row input")
	}

	// Make sure the index supports existence tracking.
	idx := e.Holder.Index(index)
	if idx == nil {
		return nil, newNotFoundError(ErrIndexNotFound, index)
	} else if idx.existenceField() == nil {
		return nil, errors.Errorf("index does not support existence tracking: %s", index)
	}

	tx, finisher, err := qcx.GetTx(Txo{Write: !writable, Index: idx, Shard: shard})
	if err != nil {
		return nil, err
	}
	defer finisher(nil)

	var existenceRow *Row
	existenceFrag := e.Holder.fragment(index, existenceFieldName, viewStandard, shard)
	if existenceFrag == nil {
		existenceRow = NewRow()
	} else {
		if existenceRow, err = existenceFrag.row(tx, 0); err != nil {
			return nil, err
		}
		if qcx.write {
			existenceRow = existenceRow.Clone()
		}
	}
	// the finishers returned by a write tx, which we might be in if there's
	// a higher-level write in this call OR ANY OTHER CALL, are safe to
	// double-call, but we have to be sure of finishing before starting a
	// bitmap call, or we lock against ourselves.
	finisher(nil)

	row, err := e.executeBitmapCallShard(ctx, qcx, index, c.Children[0], shard)
	if err != nil {
		return nil, err
	}

	return existenceRow.Difference(row), nil
}

func (e *executor) executeConstRow(ctx context.Context, index string, c *pql.Call) (res *Row, err error) {
	// Fetch user-provided columns list.
	ids, ok := c.Args["columns"].([]uint64)
	if !ok {
		return nil, errors.New("missing columns list")
	}

	return NewRow(ids...), nil
}

func (e *executor) executeUnionRows(ctx context.Context, qcx *Qcx, index string, c *pql.Call, shards []uint64, opt *ExecOptions) (*Row, error) {
	// Turn UnionRows(Rows(...)) into Union(Row(...), ...).
	var rows []*pql.Call
	for _, child := range c.Children {
		// Check that we can use the call.
		switch child.Name {
		case "Rows":
		case "TopN":
		default:
			return nil, errors.Errorf("cannot use %v as a rows query", child)
		}

		// Execute the call.
		rowsResult, err := e.executeCall(ctx, qcx, index, child, shards, opt)
		if err != nil {
			return nil, err
		}

		// Turn the results into rows calls.
		var resultRows []*pql.Call
		switch rowsResult := rowsResult.(type) {
		case *PairsField:
			// Translate pairs into rows calls.
			// TODO: This should probably also be adjusted to use InnerUnionRows,
			// but we can't do that for the string key case.
			for _, p := range rowsResult.Pairs {
				var val interface{}
				switch {
				case p.Key != "":
					val = p.Key
				default:
					val = p.ID
				}
				resultRows = append(resultRows, &pql.Call{
					Name: "Row",
					Args: map[string]interface{}{
						rowsResult.Field: val,
					},
				})
			}
		case RowIDs:
			// Make a single InnerUnionRows from this
			resultRows = append(resultRows, &pql.Call{
				Name: "InnerUnionRows",
				Args: map[string]interface{}{
					"_field": child.Args["_field"],
					"rows":   []uint64(rowsResult),
				},
			})
		default:
			return nil, errors.Errorf("unexpected Rows type %T", rowsResult)
		}

		// Propogate any special properties of the call.
		switch child.Name {
		case "Rows":
			// Propogate "from" time, if set.
			if v, ok := child.Args["from"]; ok {
				for _, rowCall := range resultRows {
					rowCall.Args["from"] = v
				}
			}

			// Propogate "to" time, if set.
			if v, ok := child.Args["to"]; ok {
				for _, rowCall := range resultRows {
					rowCall.Args["to"] = v
				}
			}
		}

		rows = append(rows, resultRows...)
	}

	// Generate a Union call over the rows.
	c = &pql.Call{
		Name:     "Union",
		Children: rows,
	}

	// Execute the generated Union() call.
	return e.executeBitmapCall(ctx, qcx, index, c, shards, opt)
}

// executeAllCallShard executes an All() call for a local shard.
func (e *executor) executeAllCallShard(ctx context.Context, qcx *Qcx, index string, c *pql.Call, shard uint64) (res *Row, err0 error) {
	span, _ := tracing.StartSpanFromContext(ctx, "executor.executeAllCallShard")
	defer span.Finish()

	if len(c.Children) > 0 {
		return nil, errors.New("All() does not accept an input row")
	}

	// Make sure the index supports existence tracking.
	idx := e.Holder.Index(index)
	if idx == nil {
		return nil, newNotFoundError(ErrIndexNotFound, index)
	} else if idx.existenceField() == nil {
		return nil, errors.Errorf("index does not support existence tracking: %s", index)
	}

	var existenceRow *Row
	existenceFrag := e.Holder.fragment(index, existenceFieldName, viewStandard, shard)
	if existenceFrag == nil {
		existenceRow = NewRow()
	} else {
		tx, finisher, err := qcx.GetTx(Txo{Write: !writable, Index: idx, Fragment: existenceFrag, Shard: shard})
		if err != nil {
			return nil, err
		}

		defer finisher(&err0)

		if existenceRow, err = existenceFrag.row(tx, 0); err != nil {
			return nil, err
		}
	}

	return existenceRow, nil
}

// executeShiftShard executes a shift() call for a local shard.
func (e *executor) executeShiftShard(ctx context.Context, qcx *Qcx, index string, c *pql.Call, shard uint64) (_ *Row, err error) {
	n, _, err := c.IntArg("n")
	if err != nil {
		return nil, fmt.Errorf("executeShiftShard: %v", err)
	}

	if len(c.Children) == 0 {
		return nil, errors.New("Shift() requires an input row")
	} else if len(c.Children) > 1 {
		return nil, errors.New("Shift() only accepts a single row input")
	}

	row, err := e.executeBitmapCallShard(ctx, qcx, index, c.Children[0], shard)
	if err != nil {
		return nil, err
	}

	return row.Shift(n)
}

// executeCount executes a count() call.
func (e *executor) executeCount(ctx context.Context, qcx *Qcx, index string, c *pql.Call, shards []uint64, opt *ExecOptions) (uint64, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "executor.executeCount")
	defer span.Finish()

	if len(c.Children) == 0 {
		return 0, errors.New("Count() requires an input bitmap")
	} else if len(c.Children) > 1 {
		return 0, errors.New("Count() only accepts a single bitmap input")
	}

	child := c.Children[0]

	// If the child is distinct/similar, execute it directly here and count the result.
	if child.Type == pql.PrecallGlobal {
		result, err := e.executeCall(ctx, qcx, index, child, shards, opt)
		if err != nil {
			return 0, err
		}

		switch row := result.(type) {
		case *Row:
			return row.Count(), nil
		case SignedRow:
			return row.Pos.Count() + row.Neg.Count(), nil
		case DistinctTimestamp:
			return uint64(len(row.Values)), nil
		default:
			return 0, errors.Errorf("cannot count result of type %T from call %q", row, child.String())
		}
	}

	// Execute calls in bulk on each remote node and merge.
	mapFn := func(ctx context.Context, shard uint64, mopt *mapOptions) (_ interface{}, err error) {
		row, err := e.executeBitmapCallShard(ctx, qcx, index, child, shard)
		if err != nil {
			return 0, err
		}
		return row.Count(), nil
	}

	// Merge returned results at coordinating node.
	reduceFn := func(ctx context.Context, prev, v interface{}) interface{} {
		other, _ := prev.(uint64)
		return other + v.(uint64)
	}

	result, err := e.mapReduce(ctx, index, shards, c, opt, mapFn, reduceFn)
	if err != nil {
		return 0, err
	}
	n, _ := result.(uint64)

	return n, nil
}

// executeClearBit executes a Clear() call.
func (e *executor) executeClearBit(ctx context.Context, qcx *Qcx, index string, c *pql.Call, opt *ExecOptions) (bool, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "executor.executeClearBit")
	defer span.Finish()

	// Read colID
	colID, ok, err := c.UintArg("_" + columnLabel)
	if err != nil {
		return false, fmt.Errorf("reading Clear() column: %v", err)
	} else if !ok {
		return false, fmt.Errorf("column argument to Clear(<COLUMN>, <FIELD>=<ROW>) required")
	}
	// Read field name.
	fieldName, err := c.FieldArg()
	if err != nil {
		return false, errors.New("Clear() argument required: field")
	}

	// Retrieve field.
	idx := e.Holder.Index(index)
	if idx == nil {
		return false, newNotFoundError(ErrIndexNotFound, index)
	}

	f := idx.Field(fieldName)
	if f == nil {
		return false, newNotFoundError(ErrFieldNotFound, fieldName)
	}

	// BSI field
	if f.Type() == FieldTypeInt || f.Type() == FieldTypeDecimal || f.Type() == FieldTypeTimestamp {
		return e.executeClearValueField(ctx, qcx, index, c, f, colID, opt)
	}

	rowID, ok, err := c.UintArg(fieldName)
	if err != nil {
		return false, fmt.Errorf("reading Clear() row: %v", err)
	} else if !ok {
		return false, fmt.Errorf("row=<row> argument required to Clear() call")
	}

	return e.executeClearBitField(ctx, qcx, index, c, f, colID, rowID, opt)
}

// executeClearBitField executes a Clear() call for a field.
func (e *executor) executeClearBitField(ctx context.Context, qcx *Qcx, index string, c *pql.Call, f *Field, colID, rowID uint64, opt *ExecOptions) (_ bool, err0 error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "executor.executeClearBitField")
	defer span.Finish()

	shard := colID / ShardWidth

	// Create a snapshot of the cluster to use for node/partition calculations.
	snap := e.Cluster.NewSnapshot()

	ret := false
	for _, node := range snap.ShardNodes(index, shard) {
		// Update locally if host matches.
		if node.ID == e.Node.ID {
			val, err := f.ClearBit(qcx, rowID, colID)
			if err != nil {
				return false, err
			} else if val {
				ret = true
			}
			continue
		}
		// Do not forward call if this is already being forwarded.
		if opt.Remote {
			continue
		}

		// Forward call to remote node otherwise.
		res, err := e.remoteExec(ctx, node, index, &pql.Query{Calls: []*pql.Call{c}}, nil, nil, 0)
		if err != nil {
			return false, err
		}
		ret = res[0].(bool)
	}
	return ret, nil
}

// executeClearRow executes a ClearRow() call.
func (e *executor) executeClearRow(ctx context.Context, qcx *Qcx, index string, c *pql.Call, shards []uint64, opt *ExecOptions) (_ bool, err error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "executor.executeClearRow")
	defer span.Finish()

	// Ensure the field type supports ClearRow().
	var fieldName string
	fieldName, err = c.FieldArg()
	if err != nil {
		return false, errors.New("ClearRow() argument required: field")
	}
	field := e.Holder.Field(index, fieldName)
	if field == nil {
		return false, newNotFoundError(ErrFieldNotFound, fieldName)
	}

	switch field.Type() {
	case FieldTypeSet, FieldTypeTime, FieldTypeMutex, FieldTypeBool:
		// These field types support ClearRow().
	default:
		return false, fmt.Errorf("ClearRow() is not supported on %s field types", field.Type())
	}

	// Execute calls in bulk on each remote node and merge.
	mapFn := func(ctx context.Context, shard uint64, mopt *mapOptions) (_ interface{}, err error) {
		return e.executeClearRowShard(ctx, qcx, index, c, shard)
	}

	// Merge returned results at coordinating node.
	reduceFn := func(ctx context.Context, prev, v interface{}) interface{} {
		val, ok := v.(bool)
		if !ok {
			return errors.Errorf("executeClearRow.reduceFn: val is non-bool (%+v)", v)
		}
		if prev == nil || val {
			return val
		}
		pval, ok := prev.(bool)
		if !ok {
			return errors.Errorf("executeClearRow.reduceFn: prev is non-bool (%+v)", prev)
		}
		return pval
	}

	result, err := e.mapReduce(ctx, index, shards, c, opt, mapFn, reduceFn)
	if err != nil {
		return false, errors.Wrap(err, "mapreducing clearrow")
	}
	return result.(bool), err
}

// executeClearRowShard executes a ClearRow() call for a single shard.
func (e *executor) executeClearRowShard(ctx context.Context, qcx *Qcx, index string, c *pql.Call, shard uint64) (_ bool, err0 error) {
	span, _ := tracing.StartSpanFromContext(ctx, "executor.executeClearRowShard")
	defer span.Finish()

	fieldName, err := c.FieldArg()
	if err != nil {
		return false, errors.New("ClearRow() argument required: field")
	}

	// Read fields using labels.
	var rowID uint64
	var ok bool
	rowID, ok, err = c.UintArg(fieldName)
	if err != nil {
		return false, fmt.Errorf("reading ClearRow() row: %v", err)
	} else if !ok {
		return false, fmt.Errorf("ClearRow() row argument '%v' required", rowLabel)
	}

	field := e.Holder.Field(index, fieldName)
	if field == nil {
		return false, newNotFoundError(ErrFieldNotFound, fieldName)
	}

	idx := e.Holder.Index(index)
	tx, finisher, err := qcx.GetTx(Txo{Write: writable, Index: idx, Shard: shard})
	if err != nil {
		return false, err
	}
	defer finisher(&err0)

	// Remove the row from all views.
	changed := false
	for _, view := range field.views() {
		fragment := e.Holder.fragment(index, fieldName, view.name, shard)
		if fragment == nil {
			continue
		}
		cleared, err := fragment.clearRow(tx, rowID)
		if err != nil {
			return false, errors.Wrapf(err, "clearing row %d on view %s shard %d", rowID, view.name, shard)
		}
		changed = changed || cleared
	}

	return changed, nil
}

// executeSetRow executes a Store() call.

func (e *executor) executeSetRow(ctx context.Context, qcx *Qcx, indexName string, c *pql.Call, shards []uint64, opt *ExecOptions) (bool, error) {
	// Parse arguments.
	fieldName, err := c.FieldArg()
	if err != nil {
		return false, errors.New("field required for Store()")
	}

	field := e.Holder.Field(indexName, fieldName)
	if field == nil {
		return false, newNotFoundError(ErrFieldNotFound, fieldName)
	}
	// Ensure the field type supports Store().
	if field.Type() != FieldTypeSet {
		return false, fmt.Errorf("can't Store() on a %s field", field.Type())
	}

	// Execute calls in bulk on each remote node and merge.
	mapFn := func(ctx context.Context, shard uint64, mopt *mapOptions) (_ interface{}, err error) {
		return e.executeSetRowShard(ctx, qcx, indexName, c, shard)
	}

	// Merge returned results at coordinating node.
	reduceFn := func(ctx context.Context, prev, v interface{}) interface{} {
		val, ok := v.(bool)
		if !ok {
			return errors.Errorf("executeSetRow.reduceFn: val is non-bool (%+v)", v)
		}
		if prev == nil || val {
			return val
		}

		pval, ok := prev.(bool)
		if !ok {
			return errors.Errorf("executeSetRow.reduceFn: prev is non-bool (%+v)", prev)
		}
		return pval
	}

	result, err := e.mapReduce(ctx, indexName, shards, c, opt, mapFn, reduceFn)
	if err != nil {
		return false, err
	}

	b, ok := result.(bool)
	if !ok {
		return false, errors.New("unsupported result type")
	}
	return b, nil
}

// executeSetRowShard executes a SetRow() call for a single shard.
func (e *executor) executeSetRowShard(ctx context.Context, qcx *Qcx, index string, c *pql.Call, shard uint64) (_ bool, err0 error) {
	fieldName, err := c.FieldArg()
	if err != nil {
		return false, errors.New("Store() argument required: field")
	}

	// Read fields using labels.
	var rowID uint64
	var ok bool
	rowID, ok, err = c.UintArg(fieldName)
	if err != nil {
		return false, fmt.Errorf("reading Store() row: %v", err)
	} else if !ok {
		return false, fmt.Errorf("need the <FIELD>=<ROW> argument on Store()")
	}

	field := e.Holder.Field(index, fieldName)
	if field == nil {
		return false, newNotFoundError(ErrFieldNotFound, fieldName)
	}

	// Retrieve source row.
	var src *Row
	if len(c.Children) == 1 {
		row, err := e.executeBitmapCallShard(ctx, qcx, index, c.Children[0], shard)
		if err != nil {
			return false, errors.Wrap(err, "getting source row")
		}
		src = row
	} else {
		return false, errors.New("Store() requires a source row")
	}

	// Set the row on the standard view.
	changed := false
	fragment := e.Holder.fragment(index, fieldName, viewStandard, shard)
	if fragment == nil {
		// Since the destination fragment doesn't exist, create one.
		view, err := field.createViewIfNotExists(viewStandard)
		if err != nil {
			return false, errors.Wrap(err, "creating view")
		}
		fragment, err = view.CreateFragmentIfNotExists(shard)
		if err != nil {
			return false, errors.Wrapf(err, "creating fragment: %d", shard)
		}
	}

	idx := e.Holder.Index(index)
	tx, finisher, err := qcx.GetTx(Txo{Write: writable, Index: idx, Shard: shard})
	if err != nil {
		return false, err
	}

	defer finisher(&err0)

	set, err := fragment.setRow(tx, src, rowID)
	if err != nil {
		return false, errors.Wrapf(err, "storing row %d on view %s shard %d", rowID, viewStandard, shard)
	}
	changed = changed || set

	return changed, nil
}

// executeSet executes a Set() call.
func (e *executor) executeSet(ctx context.Context, qcx *Qcx, index string, c *pql.Call, opt *ExecOptions) (_ bool, err0 error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "executor.executeSet")
	defer span.Finish()

	// Read colID.
	colID, ok, err := c.UintArg("_" + columnLabel)
	if err != nil {
		return false, fmt.Errorf("reading Set() column: %v", err)
	} else if !ok {
		return false, fmt.Errorf("Set() column argument '%v' required", columnLabel)
	}

	idx := e.Holder.Index(index)
	if idx == nil {
		return false, ErrIndexNotFound
	}

	// Read field name.
	fieldName, err := c.FieldArg()
	if err != nil {
		return false, errors.New("Set() argument required: field")
	}

	// Retrieve field.
	f := idx.Field(fieldName)
	if f == nil {
		return false, newNotFoundError(ErrFieldNotFound, fieldName)
	}

	// Set column on existence field.
	if ef := idx.existenceField(); ef != nil {
		if _, err := ef.SetBit(qcx, 0, colID, nil); err != nil {
			return false, errors.Wrap(err, "setting existence column")
		}
	}

	switch f.Type() {
	case FieldTypeInt, FieldTypeDecimal, FieldTypeTimestamp:
		// Fetch field
		v, ok := c.Arg(fieldName)
		if !ok {
			return false, fmt.Errorf("Set() row argument '%v' required", rowLabel)
		}

		// Before we scale a decimal to an integer, we need to make sure the decimal
		// is between min/max for the field. If it's not, converting to an integer
		// can result in an overflow.
		if dec, ok := v.(pql.Decimal); ok && f.Options().Type == FieldTypeDecimal {
			if dec.LessThan(f.Options().Min) || dec.GreaterThan(f.Options().Max) {
				return false, ErrDecimalOutOfRange
			}
		}

		// Read row value.
		rowVal, err := getScaledInt(f, v)
		if err != nil {
			return false, fmt.Errorf("reading Set() row (int/decimal): %v", err)
		}
		return e.executeSetValueField(ctx, qcx, index, c, f, colID, rowVal, opt)

	default:
		// Read row ID.
		rowID, ok, err := c.UintArg(fieldName)
		if err != nil {
			return false, fmt.Errorf("reading Set() row: %v", err)
		} else if !ok {
			return false, fmt.Errorf("Set() row argument '%v' required", rowLabel)
		}

		var timestamp *time.Time
		sTimestamp, ok := c.Args["_timestamp"].(string)
		if ok {
			t, err := time.Parse(TimeFormat, sTimestamp)
			if err != nil {
				return false, fmt.Errorf("invalid date: %s", sTimestamp)
			}
			timestamp = &t
		}

		return e.executeSetBitField(ctx, qcx, index, c, f, colID, rowID, timestamp, opt)
	}
}

// executeSetBitField executes a Set() call for a specific field.
func (e *executor) executeSetBitField(ctx context.Context, qcx *Qcx, index string, c *pql.Call, f *Field, colID, rowID uint64, timestamp *time.Time, opt *ExecOptions) (_ bool, err0 error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "executor.executeSetBitField")
	defer span.Finish()

	shard := colID / ShardWidth
	ret := false

	// Create a snapshot of the cluster to use for node/partition calculations.
	snap := e.Cluster.NewSnapshot()

	for _, node := range snap.ShardNodes(index, shard) {
		// Update locally if host matches.
		if node.ID == e.Node.ID {
			val, err := f.SetBit(qcx, rowID, colID, timestamp)
			if err != nil {
				return false, err
			} else if val {
				ret = true
			}
			continue
		}

		// Do not forward call if this is already being forwarded.
		if opt.Remote {
			continue
		}

		// Forward call to remote node otherwise.
		res, err := e.remoteExec(ctx, node, index, &pql.Query{Calls: []*pql.Call{c}}, nil, nil, 0)
		if err != nil {
			return false, err
		}
		ret = res[0].(bool)
	}
	return ret, nil
}

// executeSetValueField executes a Set() call for a specific int field.
func (e *executor) executeSetValueField(ctx context.Context, qcx *Qcx, index string, c *pql.Call, f *Field, colID uint64, value int64, opt *ExecOptions) (_ bool, err0 error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "executor.executeSetValueField")
	defer span.Finish()

	shard := colID / ShardWidth
	ret := false

	// Create a snapshot of the cluster to use for node/partition calculations.
	snap := e.Cluster.NewSnapshot()

	for _, node := range snap.ShardNodes(index, shard) {
		// Update locally if host matches.
		if node.ID == e.Node.ID {
			val, err := f.SetValue(qcx, colID, value)
			if err != nil {
				return false, err
			} else if val {
				ret = true
			}
			continue
		}

		// Do not forward call if this is already being forwarded.
		if opt.Remote {
			continue
		}

		// Forward call to remote node otherwise.
		res, err := e.remoteExec(ctx, node, index, &pql.Query{Calls: []*pql.Call{c}}, nil, nil, 0)
		if err != nil {
			return false, err
		}
		ret = res[0].(bool)
	}
	return ret, nil
}

// executeClearValueField removes value for colID if present
func (e *executor) executeClearValueField(ctx context.Context, qcx *Qcx, index string, c *pql.Call, f *Field, colID uint64, opt *ExecOptions) (_ bool, err0 error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "executor.executeClearValueField")
	defer span.Finish()

	shard := colID / ShardWidth
	ret := false

	// Create a snapshot of the cluster to use for node/partition calculations.
	snap := e.Cluster.NewSnapshot()

	for _, node := range snap.ShardNodes(index, shard) {
		// Update locally if host matches.
		if node.ID == e.Node.ID {
			val, err := f.ClearValue(qcx, colID)
			if err != nil {
				return false, err
			} else if val {
				ret = true
			}
			continue
		}

		// Do not forward call if this is already being forwarded.
		if opt.Remote {
			continue
		}

		// Forward call to remote node otherwise.
		res, err := e.remoteExec(ctx, node, index, &pql.Query{Calls: []*pql.Call{c}}, nil, nil, 0)
		if err != nil {
			return false, err
		}
		ret = res[0].(bool)
	}
	return ret, nil
}

// remoteExec executes a PQL query remotely for a set of shards on a node.
func (e *executor) remoteExec(ctx context.Context, node *disco.Node, index string, q *pql.Query, shards []uint64, embed []*Row, maxMemory int64) (results []interface{}, err error) { // nolint: interfacer
	span, ctx := tracing.StartSpanFromContext(ctx, "executor.executeExec")
	defer span.Finish()

	// Encode request object.
	pbreq := &QueryRequest{
		Query:        q.String(),
		Shards:       shards,
		Remote:       true,
		EmbeddedData: embed,
		MaxMemory:    maxMemory,
	}

	addr := dax.Address(node.URI.String())
	resp, err := e.client.QueryNode(ctx, addr, index, pbreq)
	if err != nil {
		return nil, err
	}

	return resp.Results, resp.Err
}

// shardsByNode returns a mapping of nodes to shards.
// Returns errShardUnavailable if a shard cannot be allocated to a node.
func (e *executor) shardsByNode(nodes []*disco.Node, index string, shards []uint64) (map[*disco.Node][]uint64, error) {
	m := make(map[*disco.Node][]uint64)

	// Create a snapshot of the cluster to use for node/partition calculations.
	// We use e.Cluster.Nodes() here instead of e.Cluster.noder because we need
	// the node states in order to ensure that we don't include an unavailable
	// node in the map of nodes to which we distribute the query.
	snap := disco.NewClusterSnapshot(disco.NewLocalNoder(e.Cluster.Nodes()), e.Cluster.Hasher, e.Cluster.partitionAssigner, e.Cluster.ReplicaN)

loop:
	for _, shard := range shards {
		for _, node := range snap.ShardNodes(index, shard) {
			// If the node being considered is in any state other than STARTED,
			// then exclude it from the map. This way, one of that node's
			// healthy replicas will be included instead.
			if disco.Nodes(nodes).ContainsID(node.ID) && (node.State == disco.NodeStateStarted || node.State == disco.NodeStateUnknown) {
				m[node] = append(m[node], shard)
				continue loop
			}
		}
		return nil, errors.Wrapf(errShardUnavailable, "%s:%d:%v", index, shard, nodes)
	}
	return m, nil
}

// mapReduce maps and reduces data across the cluster.
//
// If a mapping of shards to a node fails then the shards are resplit across
// secondary nodes and retried. This continues to occur until all nodes are exhausted.
//
// mapReduce has to ensure that it never returns before any work it spawned has
// terminated. It's not enough to cancel the jobs; we have to wait for them to be
// done, or we can unmap resources they're still using.
func (e *executor) mapReduce(ctx context.Context, index string, shards []uint64, c *pql.Call, opt *ExecOptions, mapFn mapFunc, reduceFn reduceFunc) (result interface{}, err error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "executor.mapReduce")
	defer span.Finish()

	ch := make(chan mapResponse)

	// Wrap context with a cancel to kill goroutines on exit.
	ctx, cancel := context.WithCancel(ctx)
	// Create an errgroup so we can wait for all the goroutines to exit
	eg, ctx := errgroup.WithContext(ctx)

	// After we're done processing, we have to wait for any outstanding
	// functions in the ErrGroup to complete. If we didn't have an error
	// already at that point, we'll report any errors from the ErrGroup
	// instead.
	defer func() {
		cancel()
		errWait := eg.Wait()
		if err == nil {
			err = errWait
		}
	}()
	// If this is the coordinating node then start with all nodes in the cluster.
	//
	// However, if this request is being sent from the primary then all
	// processing should be done locally so we start with just the local node.
	var nodes []*disco.Node
	if !opt.Remote {
		nodes = disco.Nodes(e.Cluster.Nodes()).Clone()
	} else {
		nodes = []*disco.Node{e.Cluster.nodeByID(e.Node.ID)}
	}

	// Start mapping across all primary owners.
	if err = e.mapper(ctx, eg, ch, nodes, index, shards, c, opt, e.Cluster.ReplicaN == 1, mapFn, reduceFn); err != nil {
		return nil, errors.Wrap(err, "starting mapper")
	}

	// Iterate over all map responses and reduce.
	expected := len(shards)
	done := ctx.Done()
	for expected > 0 {
		select {
		case <-done:
			return nil, ctx.Err()
		case resp := <-ch:
			// On error retry against remaining nodes. If an error returns then
			// the context will cancel and cause all open goroutines to return.

			// We distinguish here between an error which indicates that the
			// node is not available (and therefore we need to failover to a
			// replica) and a valid error from a healthy node. In the case of
			// the latter, there's no need to retry a replica, we should trust
			// the error from the healthy node and return that immediately.
			if resp.err != nil && strings.Contains(resp.err.Error(), errConnectionRefused) {
				// Filter out unavailable nodes.
				nodes = disco.Nodes(nodes).FilterID(resp.node.ID)

				// Begin mapper against secondary nodes.
				if err := e.mapper(ctx, eg, ch, nodes, index, resp.shards, c, opt, true, mapFn, reduceFn); errors.Cause(err) == errShardUnavailable {
					return nil, resp.err
				} else if err != nil {
					return nil, errors.Wrap(err, "mapping on secondary node")
				}
				continue
			} else if resp.err != nil {
				return nil, errors.Wrap(resp.err, "mapping on primary node")
			}
			// if we got a response that we aren't discarding
			// because it's an error, subtract it from our count...
			expected -= len(resp.shards)

			// Reduce value.
			result = reduceFn(ctx, result, resp.result)
			var ok bool
			// note *not* shadowed.
			if err, ok = result.(error); ok {
				cancel()
				return nil, err
			}
		}
	}
	// note the deferred Wait above which might override this nil.
	return result, nil
}

// makeEmbeddedDataForShards produces new rows containing the rowSegments
// that would correspond to a given set of shards.
func makeEmbeddedDataForShards(allRows []*Row, shards []uint64) []*Row {
	if len(allRows) == 0 || len(shards) == 0 {
		return nil
	}
	newRows := make([]*Row, len(allRows))
	for i, row := range allRows {
		if row == nil || len(row.Segments) == 0 {
			continue
		}
		if row.NoSplit {
			newRows[i] = row
			continue
		}
		segments := row.Segments
		segmentIndex := 0
		newRows[i] = &Row{
			Index: row.Index,
			Field: row.Field,
		}
		for _, shard := range shards {
			for segmentIndex < len(segments) && segments[segmentIndex].shard < shard {
				segmentIndex++
			}
			// no more segments in this row
			if segmentIndex >= len(segments) {
				break
			}
			if segments[segmentIndex].shard == shard {
				newRows[i].Segments = append(newRows[i].Segments, segments[segmentIndex])
				segmentIndex++
				if segmentIndex >= len(segments) {
					// no more segments, we're done
					break
				}
			}
			// if we got here, segments[segmentIndex].shard exists
			// but is greater than the current shard, so we continue.
		}
	}
	return newRows
}

func (e *executor) mapper(ctx context.Context, eg *errgroup.Group, ch chan mapResponse, nodes []*disco.Node, index string, shards []uint64, c *pql.Call, opt *ExecOptions, lastAttempt bool, mapFn mapFunc, reduceFn reduceFunc) (reterr error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "executor.mapper")
	defer span.Finish()

	// Group shards together by nodes.
	m, err := e.shardsByNode(nodes, index, shards)
	if err != nil {
		return errors.Wrapf(err, "shards by node")
	}
	done := ctx.Done()

	// Execute each node in a separate goroutine.
	var memoryUsed int64
	var mu sync.Mutex
	for n, nodeShards := range m {
		n := n
		nodeShards := nodeShards
		eg.Go(func() error {
			// Execute serially max memory is specified.
			if opt.MaxMemory > 0 {
				mu.Lock()
				defer mu.Unlock()
			}

			resp := mapResponse{node: n, shards: nodeShards}

			// Calculate remaining memory. This applies to Extract() only.
			// Default to a high number if we are not tracking memory.
			memoryAvailable := opt.MaxMemory - atomic.LoadInt64(&memoryUsed)
			if opt.MaxMemory <= 0 {
				memoryAvailable = math.MaxInt64
			}

			// Send local shards to mapper, otherwise remote exec.
			if n.ID == e.Node.ID {
				resp.result, resp.err = e.mapperLocal(ctx, nodeShards, mapFn, reduceFn, memoryAvailable)
			} else if !opt.Remote {
				var embeddedRowsForNode []*Row
				if opt.EmbeddedData != nil {
					embeddedRowsForNode = makeEmbeddedDataForShards(opt.EmbeddedData, nodeShards)
				}
				results, err := e.remoteExec(ctx, n, index, &pql.Query{Calls: []*pql.Call{c}}, nodeShards, embeddedRowsForNode, memoryAvailable)
				if len(results) > 0 {
					resp.result = results[0]
				}
				resp.err = err
			}

			// Track total memory used in response.
			if v := atomic.AddInt64(&memoryUsed, calcResultMemory(resp.result)); opt.MaxMemory > 0 && v > opt.MaxMemory {
				return fmt.Errorf("query result exceeded memory threshold")
			}

			// Return response to the channel.
			select {
			case <-done:
				// If someone just canceled the context
				// arbitrarily, we could end up here with this
				// being the first non-nil error handed to
				// the ErrGroup, in which case, it's the best
				// explanation we have for why everything's
				// stopping.
				return ctx.Err()
			case ch <- resp:
				// If we return a non-nil error from this, the
				// entire errGroup gets canceled. So we don't
				// want to return a non-nil error if mapReduce
				// might try to run another mapper against a
				// different set of nodes. Note that this shouldn't
				// matter; we just sent the error to mapReduce
				// anyway, so it probably cancels the ErrGroup
				// too.
				if resp.err != nil && lastAttempt {
					return resp.err
				}
			}
			return nil
		})
		if reterr != nil {
			return reterr // exit early if error occurs when running serially
		}
	}
	return nil
}

// calcResultMemory recursively computes the total memory used by v.
func calcResultMemory(v interface{}) (n int64) {
	switch v := v.(type) {
	case ExtractedIDColumn:
		n += 8 // ColumnID
		for _, row := range v.Rows {
			n += 24 + int64(len(row)*8) // slice header + data
		}
		return n

	case ExtractedIDMatrix:
		n += 24 // slice size
		for _, field := range v.Fields {
			n += 16 + int64(len(field)) // string header + data
		}

		n += 24 // Columns slice
		for _, col := range v.Columns {
			n += calcResultMemory(col)
		}
		return n

	case ExtractedTableColumn:
		n += 8 + 16 + int64(len(v.Column.Key)) + 8 // KeyOrID
		for _, row := range v.Rows {
			n += 8 + calcResultMemory(row) // ptr + value size
		}
		return n

	case string:
		return 16 + int64(len(v))
	case bool, int64, uint64:
		return 8
	case []string:
		n += 24 // slice header
		for i := range v {
			n += 16 + int64(len(v[i]))
		}
		return n
	case []uint64:
		return 24 + int64(8*len(v)) // slice header + data size
	case pql.Decimal:
		return 16
	case time.Time:
		return 24
	default:
		return n
	}
}

type job struct {
	shard           uint64
	mapFn           mapFunc
	ctx             context.Context
	memoryAvailable *int64 // shared, atomic value
	resultChan      chan mapResponse
}

// doOneJob had one job. *disappointed sigh*
func (e *executor) doOneJob() {
	j, ok := <-e.work
	if !ok {
		return
	}
	// Skip out early if the context is done, but still send
	// an ack so mapperLocal can be sure we aren't about to
	// work on something it sent us.
	if err := j.ctx.Err(); err != nil {
		j.resultChan <- mapResponse{result: nil, err: err}
		return
	}
	result, err := j.mapFn(j.ctx, j.shard, &mapOptions{memoryAvailable: j.memoryAvailable})
	j.resultChan <- mapResponse{result: result, err: err}
}

var errShutdown = errors.New("executor has shut down")

// mapperLocal performs map & reduce entirely on the local node.
func (e *executor) mapperLocal(ctx context.Context, shards []uint64, mapFn mapFunc, reduceFn reduceFunc, memoryAvailable int64) (_ interface{}, err error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "executor.mapperLocal")
	defer span.Finish()
	ctx, cancel := context.WithCancel(ctx)
	defer func() {
		cancel()
	}()
	done := ctx.Done()
	// the close process needs to know whether any mappers are currently active.
	// note that we bump this BEFORE checking e.shutdown. if we check shutdown
	// first, then we can check shutdown, after which the close process closes
	// shutdown and checks the count of active mappers, after which we try to
	// bump that count.
	atomic.AddUint64(&e.activeMappers, 1)
	defer atomic.AddUint64(&e.activeMappers, ^uint64(0))
	select {
	case <-e.shutdown:
		return nil, errShutdown
	default:
	}

	ch := make(chan mapResponse, len(shards))

	expected := 0
shardLoop:
	for _, shard := range shards {
		j := job{
			shard:           shard,
			mapFn:           mapFn,
			ctx:             ctx,
			resultChan:      ch,
			memoryAvailable: &memoryAvailable,
		}
		select {
		case <-done: // this request's context terminated
			break shardLoop
		case <-e.shutdown: // whole executor shutting down
			break shardLoop
		case e.work <- j:
			expected++
		}
	}
	// we *absolutely must* get responses for everything we successfully
	// transmitted to the work queue, or there could be ongoing access to
	// the parent Qcx's stuff.
	//
	// Even if our context is done, or the executor is shutting down,
	// we still have to wait for responses, because the responders are
	// going to send them and block waiting for us to receive them.

	// Reduce results
	var result interface{}
	for expected > 0 {
		resp := <-ch
		expected--
		if resp.err != nil && err == nil {
			err = resp.err
		}
		if resp.err == nil && ctx.Err() == nil {
			// Only useful to do a possibly-expensive
			// reduce if we don't already know we don't
			// need it.
			result = reduceFn(ctx, result, resp.result)
			if resultErr, ok := result.(error); ok {
				cancel()
				err = resultErr
			}
		}
	}
	return result, err
}

func (e *executor) preTranslate(ctx context.Context, index string, calls ...*pql.Call) (cols map[string]map[string]uint64, rows map[string]map[string]map[string]uint64, err error) {
	// Collect all of the required keys.
	collector := keyCollector{
		createCols: make(map[string][]string),
		findCols:   make(map[string][]string),
		createRows: make(map[string]map[string][]string),
		findRows:   make(map[string]map[string][]string),
	}
	for _, call := range calls {
		err := e.collectCallKeys(&collector, call, index)
		if err != nil {
			return nil, nil, err
		}
	}

	// Create keys.
	// Both rows and columns need to be created first because of foreign index keys.
	cols = make(map[string]map[string]uint64)
	rows = make(map[string]map[string]map[string]uint64)
	for index, keys := range collector.createCols {
		translations, err := e.Cluster.createIndexKeys(ctx, index, keys...)
		if err != nil {
			return nil, nil, errors.Wrap(err, "creating query column keys")
		}
		cols[index] = translations
	}
	for index, fields := range collector.createRows {
		idxRows := make(map[string]map[string]uint64)
		idx := e.Holder.Index(index)
		if idx == nil {
			return nil, nil, errors.Wrapf(ErrIndexNotFound, "creating rows on index %q", index)
		}
		for field, keys := range fields {
			f := idx.Field(field)
			if f == nil {
				return nil, nil, errors.Wrapf(ErrFieldNotFound, "creating rows on field %q in index %q", field, index)
			}
			translations, err := e.Cluster.createFieldKeys(ctx, f, keys...)
			if err != nil {
				return nil, nil, errors.Wrap(err, "creating query row keys")
			}
			idxRows[field] = translations
		}
		rows[index] = idxRows
	}

	// Find other keys.
	for index, keys := range collector.findCols {
		translations, err := e.Cluster.findIndexKeys(ctx, index, keys...)
		if err != nil {
			return nil, nil, errors.Wrap(err, "finding query column keys")
		}
		if prev := cols[index]; prev != nil {
			for key, id := range translations {
				prev[key] = id
			}
		} else {
			cols[index] = translations
		}
	}
	for index, fields := range collector.findRows {
		idxRows := rows[index]
		if idxRows == nil {
			idxRows = make(map[string]map[string]uint64)
			rows[index] = idxRows
		}
		idx := e.Holder.Index(index)
		if idx == nil {
			return nil, nil, errors.Wrapf(ErrIndexNotFound, "finding rows on index %q", index)
		}
		for field, keys := range fields {
			f := idx.Field(field)
			if f == nil {
				return nil, nil, errors.Wrapf(ErrFieldNotFound, "finding rows on field %q in index %q", field, index)
			}
			translations, err := e.Cluster.findFieldKeys(ctx, f, keys...)
			if err != nil {
				return nil, nil, errors.Wrap(err, "finding query row keys")
			}
			if prev := idxRows[field]; prev != nil {
				for key, id := range translations {
					prev[key] = id
				}
			} else {
				idxRows[field] = translations
			}
		}
	}

	return cols, rows, nil
}

func (e *executor) collectCallKeys(dst *keyCollector, c *pql.Call, index string) error {
	// Check for an overriding 'index' argument.
	// This also applies to all child calls.
	if callIndex := c.CallIndex(); callIndex != "" {
		index = callIndex
	}

	// Handle the field arg.
	switch c.Name {
	case "Set":
		if field, err := c.FieldArg(); err == nil {
			if arg, ok := c.Args[field].(string); ok {
				dst.CreateRows(index, field, arg)
			}
		}

	case "Store":
		if field, err := c.FieldArg(); err == nil {
			idx := e.Holder.Index(index)
			if idx == nil {
				return errors.Wrapf(ErrIndexNotFound, "translating store field argument")
			}
			f := idx.Field(field)
			if f == nil {
				// Create the field.
				// This is messy, because if a query leading up to the store fails, we will have created the field without executing the store.
				var keyed bool
				switch v := c.Args[field].(type) {
				case string:
					keyed = true
				case uint64:
				case int64:
					if v < 0 {
						return errors.Errorf("negative store row ID %d", v)
					}
				default:
					return errors.Errorf("invalid store row identifier: %v of %T", v, v)
				}
				opts := []FieldOption{OptFieldTypeSet(CacheTypeNone, 0)}
				if keyed {
					opts = append(opts, OptFieldKeys())
				}
				if _, err := idx.CreateField(field, "", opts...); err != nil {
					// We wrap these because we want to indicate that it wasn't found,
					// but also the problem we encountered trying to create it.
					return newNotFoundError(errors.Wrap(err, "creating field"), field)
				}
			}
			if arg, ok := c.Args[field].(string); ok {
				dst.CreateRows(index, field, arg)
			}
		}

	case "Clear", "Row", "Range", "ClearRow":
		if field, err := c.FieldArg(); err == nil {
			switch arg := c.Args[field].(type) {
			case string:
				dst.FindRows(index, field, arg)
			case *pql.Condition:
				// This is a workaround to allow `==` and `!=` to work on foreign index fields.
				if key, ok := arg.Value.(string); ok {
					switch arg.Op {
					case pql.EQ, pql.NEQ:
						dst.FindRows(index, field, key)
					default:
						return errors.Errorf("operator %v not defined on strings", arg.Op)
					}
				}
			}
		}
	}

	// Handle _col.
	if col, ok := c.Args["_col"].(string); ok {
		switch c.Name {
		case "Set":
			dst.CreateColumns(index, col)
		default:
			dst.FindColumns(index, col)
		}
	}

	// Handle _row.
	if row, ok := c.Args["_row"].(string); ok {
		// Find the field.
		field, ok, err := c.StringArg("_field")
		if err != nil {
			return errors.Wrap(err, "finding field")
		}
		if !ok {
			return errors.Wrap(ErrFieldNotFound, "finding field for _row argument")
		}

		dst.FindRows(index, field, row)
	}

	// Handle queries that need a "column" argument.
	switch c.Name {
	case "Rows", "GroupBy", "FieldValue", "IncludesColumn":
		if col, ok := c.Args["column"].(string); ok {
			dst.FindColumns(index, col)
		}
	}

	// Handle special per-query arguments.
	switch c.Name {
	case "ConstRow":
		// Translate the columns list.
		if cols, ok := c.Args["columns"].([]interface{}); ok {
			keys := make([]string, 0, len(cols))
			for _, v := range cols {
				switch v := v.(type) {
				case string:
					keys = append(keys, v)
				case uint64:
				case int64:
				default:
					return errors.Errorf("invalid column identifier %v of type %T", c, c)
				}
			}
			dst.FindColumns(index, keys...)
		}

	case "Rows":
		// Find the field.
		var field string
		if f, ok, err := c.StringArg("_field"); err != nil {
			return errors.Wrap(err, "finding field for Rows previous translation")
		} else if ok {
			field = f
		} else if f, ok, err := c.StringArg("field"); err != nil {
			return errors.Wrap(err, "finding field for Rows previous translation")
		} else if ok {
			field = f
		} else {
			return errors.New("missing field in Rows call")
		}
		if prev, ok := c.Args["previous"].(string); ok {
			dst.FindRows(index, field, prev)
		}
		if in, ok := c.Args["in"]; ok {
			inIn, ok := in.([]interface{})
			if !ok {
				return errors.Errorf("unexpected type for argument 'in' %v of %[1]T", inIn)
			}
			inStrs := make([]string, 0)
			for _, v := range inIn {
				if vstr, ok := v.(string); ok {
					inStrs = append(inStrs, vstr)
				}
			}
			dst.FindRows(index, field, inStrs...)
		}
	}

	// Collect keys from child calls.
	for _, child := range c.Children {
		err := e.collectCallKeys(dst, child, index)
		if err != nil {
			return err
		}
	}

	// Collect keys from argument calls.
	for _, arg := range c.Args {
		argCall, ok := arg.(*pql.Call)
		if !ok {
			continue
		}

		err := e.collectCallKeys(dst, argCall, index)
		if err != nil {
			return err
		}
	}

	return nil
}

type keyCollector struct {
	createCols, findCols map[string][]string            // map[index] -> column keys
	createRows, findRows map[string]map[string][]string // map[index]map[field] -> row keys
}

func (c *keyCollector) CreateColumns(index string, columns ...string) {
	if len(columns) == 0 {
		return
	}
	c.createCols[index] = append(c.createCols[index], columns...)
}

func (c *keyCollector) FindColumns(index string, columns ...string) {
	if len(columns) == 0 {
		return
	}
	c.findCols[index] = append(c.findCols[index], columns...)
}

func (c *keyCollector) CreateRows(index string, field string, columns ...string) {
	if len(columns) == 0 {
		return
	}
	idx := c.createRows[index]
	if idx == nil {
		idx = make(map[string][]string)
		c.createRows[index] = idx
	}
	idx[field] = append(idx[field], columns...)
}

func (c *keyCollector) FindRows(index string, field string, columns ...string) {
	if len(columns) == 0 {
		return
	}
	idx := c.findRows[index]
	if idx == nil {
		idx = make(map[string][]string)
		c.findRows[index] = idx
	}
	idx[field] = append(idx[field], columns...)
}

func fieldValidateValue(f *Field, val interface{}) error {
	if val == nil {
		return nil
	}

	// Validate special types.
	switch val := val.(type) {
	case string:
		if !f.Keys() {
			return errors.Errorf("string value on unkeyed field %q", f.Name())
		}
		return nil
	case *pql.Condition:
		switch v := val.Value.(type) {
		case nil:
		case string:
		case uint64:
		case int64:
		case float64:
		case pql.Decimal:
		case time.Time:
		case []interface{}:
			for _, v := range v {
				if err := fieldValidateValue(f, v); err != nil {
					return err
				}
			}
			return nil
		default:
			return errors.Errorf("invalid value %v in condition %q", v, val.String())
		}
		return fieldValidateValue(f, val.Value)
	}

	switch f.Type() {
	case FieldTypeSet, FieldTypeMutex, FieldTypeTime:
		switch v := val.(type) {
		case uint64:
		case int64:
			if v < 0 {
				return errors.Errorf("negative ID %d for set field %q", v, f.Name())
			}
		default:
			return errors.Errorf("invalid value %v for field %q of type %s", v, f.Name(), f.Type())
		}
		if f.Keys() {
			return errors.Errorf("found integer ID %d on keyed field %q", val, f.Name())
		}
	case FieldTypeBool:
		switch v := val.(type) {
		case bool:
		default:
			return errors.Errorf("invalid value %v for bool field %q", v, f.Name())
		}
	case FieldTypeInt:
		switch v := val.(type) {
		case uint64:
			if v > 1<<63 {
				return errors.Errorf("oversized integer %d for int field %q (range: -2^63 to 2^63-1)", v, f.Name())
			}
		case int64:
		default:
			return errors.Errorf("invalid value %v for int field %q", v, f.Name())
		}
	case FieldTypeDecimal:
		switch v := val.(type) {
		case uint64:
		case int64:
		case float64:
		case pql.Decimal:
		default:
			return errors.Errorf("invalid value %v for decimal field %q", v, f.Name())
		}
	case FieldTypeTimestamp:
		switch v := val.(type) {
		case time.Time:
		case int64: // TODO(twg) 2022/09/14 revisit this hack
		default:
			return errors.Errorf("invalid value %v for timestamp field %q", v, f.Name())
		}
	default:
		return errors.Errorf("unsupported type %s of field %q", f.Type(), f.Name())
	}

	return nil
}

func (e *executor) translateCall(c *pql.Call, index string, columnKeys map[string]map[string]uint64, rowKeys map[string]map[string]map[string]uint64) (*pql.Call, error) {
	// Check for an overriding 'index' argument.
	// This also applies to all child calls.
	if callIndex := c.CallIndex(); callIndex != "" {
		index = callIndex
	}
	idx := e.Holder.Index(index)
	if idx == nil {
		return nil, errors.Wrapf(ErrIndexNotFound, "translating query on index %q", index)
	}

	// Fetch the column keys list for this index.
	indexCols, indexRows := columnKeys[index], rowKeys[index]

	// Handle the field arg.
	switch c.Name {
	case "Set", "Store":
		if field, err := c.FieldArg(); err == nil {
			f := e.Holder.Field(index, field)
			if f == nil {
				return nil, errors.Wrapf(ErrFieldNotFound, "validating value for field %q", field)
			}
			arg := c.Args[field]
			if err := fieldValidateValue(f, arg); err != nil {
				return nil, errors.Wrap(err, "validating store value")
			}
			switch arg := arg.(type) {
			case string:
				if translation, ok := indexRows[field][arg]; ok {
					c.Args[field] = translation
				} else {
					return nil, errors.Wrapf(ErrTranslatingKeyNotFound, "destination key not found %q in %q in index %q", arg, field, index)
				}
			case bool:
				if arg {
					c.Args[field] = trueRowID
				} else {
					c.Args[field] = falseRowID
				}
			}
		}

	case "Clear", "Row", "Range", "ClearRow":
		if field, err := c.FieldArg(); err == nil {
			f := e.Holder.Field(index, field)
			if f == nil {
				return nil, errors.Wrapf(ErrFieldNotFound, "validating value for field %q", field)
			}
			arg := c.Args[field]
			if err := fieldValidateValue(f, arg); err != nil {
				return nil, errors.Wrap(err, "validating field parameter value")
			}
			if c.Name == "Row" {
				switch f.Type() {
				case FieldTypeInt, FieldTypeDecimal, FieldTypeTimestamp:
					if _, ok := arg.(*pql.Condition); !ok {
						// This is workaround to support pql.ASSIGN ('=') as condition ('==') for BSI fields.
						arg = &pql.Condition{
							Op:    pql.EQ,
							Value: arg,
						}
						c.Args[field] = arg
					}
				}
			}
			switch arg := arg.(type) {
			case string:
				if translation, ok := indexRows[field][arg]; ok {
					c.Args[field] = translation
				} else {
					// Rewrite the call into a zero value call.
					return e.callZero(c), nil
				}
			case bool:
				if arg {
					c.Args[field] = trueRowID
				} else {
					c.Args[field] = falseRowID
				}
			case *pql.Condition:
				// This is a workaround to allow `==` and `!=` to work on foreign index fields.
				if key, ok := arg.Value.(string); ok {
					switch arg.Op {
					case pql.EQ, pql.NEQ:
						if translation, ok := indexRows[field][key]; ok {
							arg.Value = translation
						} else {
							// Rewrite the call into a zero value call.
							return e.callZero(c), nil
						}
					default:
						return nil, errors.Errorf("operator %v not defined on strings", arg.Op)
					}
				}
			}
		}
	}

	// Handle _col.
	if col, ok := c.Args["_col"].(string); ok {
		if !idx.Keys() {
			return nil, errors.Wrapf(ErrTranslatingKeyNotFound, "translating column on unkeyed index %q", index)
		}
		if id, ok := indexCols[col]; ok {
			c.Args["_col"] = id
		} else {
			switch c.Name {
			case "Set":
				return nil, errors.Wrapf(ErrTranslatingKeyNotFound, "destination key not found %q in index %q", col, index)
			default:
				return e.callZero(c), nil
			}
		}
	}

	// Handle _row.
	if row, ok := c.Args["_row"]; ok {
		// Find the field.
		var field string
		if f, ok, err := c.StringArg("_field"); err != nil {
			return nil, errors.Wrap(err, "finding field")
		} else if ok {
			field = f
		} else if f, ok, err := c.StringArg("field"); err != nil {
			return nil, errors.Wrap(err, "finding field")
		} else if ok {
			field = f
		} else {
			return nil, errors.New("missing field")
		}

		f := e.Holder.Field(index, field)
		if f == nil {
			return nil, errors.Wrapf(ErrFieldNotFound, "validating value for field %q", field)
		}
		if err := fieldValidateValue(f, row); err != nil {
			return nil, errors.Wrap(err, "validating row value")
		}
		switch row := row.(type) {
		case string:
			if translation, ok := indexRows[field][row]; ok {
				c.Args["_row"] = translation
			} else {
				return e.callZero(c), nil
			}
		}
	}

	// Handle queries that need a "column" argument.
	switch c.Name {
	case "Rows", "GroupBy", "FieldValue", "IncludesColumn":
		if col, ok := c.Args["column"].(string); ok {
			if translation, ok := indexCols[col]; ok {
				c.Args["column"] = translation
			} else {
				// Rewrite the call into a zero value call.
				return e.callZero(c), nil
			}
		}
	}

	// Handle special per-query arguments.
	switch c.Name {
	case "ConstRow":
		// Translate the columns list.
		if cols, ok := c.Args["columns"].([]interface{}); ok {
			out := make([]uint64, 0, len(cols))
			for _, v := range cols {
				switch v := v.(type) {
				case string:
					if id, ok := indexCols[v]; ok {
						out = append(out, id)
					}
				case uint64:
					out = append(out, v)
				case int64:
					out = append(out, uint64(v))
				default:
					return nil, errors.Errorf("invalid column identifier %v of type %T", c, c)
				}
			}
			c.Args["columns"] = out
		}

	case "Rows":
		// Find the field.
		var field string
		if f, ok, err := c.StringArg("_field"); err != nil {
			return nil, errors.Wrap(err, "finding field for Rows previous translation")
		} else if ok {
			field = f
		} else if f, ok, err := c.StringArg("field"); err != nil {
			return nil, errors.Wrap(err, "finding field for Rows previous translation")
		} else if ok {
			field = f
		} else {
			return nil, errors.New("missing field in Rows call")
		}
		// Translate the previous row key.
		if prev, ok := c.Args["previous"]; ok {
			// Validate the type.
			f := e.Holder.Field(index, field)
			if f == nil {
				return nil, errors.Wrapf(ErrFieldNotFound, "validating value for field %q", field)
			}
			if err := fieldValidateValue(f, prev); err != nil {
				return nil, errors.Wrap(err, "validating prev value")
			}

			switch prev := prev.(type) {
			case string:
				// Look up a translation for the previous row key.
				if translation, ok := indexRows[field][prev]; ok {
					c.Args["previous"] = translation
				} else {
					return nil, errors.Wrapf(ErrTranslatingKeyNotFound, "translating previous key %q from field %q in index %q in Rows call", prev, field, index)
				}
			case bool:
				if prev {
					c.Args["previous"] = trueRowID
				} else {
					c.Args["previous"] = falseRowID
				}
			}
		}

		// Check if "like" argument is applied to keyed fields.
		if _, found := c.Args["like"].(string); found {
			fieldName, err := c.FirstStringArg("_field", "field")
			if err != nil || fieldName == "" {
				return nil, fmt.Errorf("cannot read field name for Rows call")
			}
			if !idx.Field(fieldName).options.Keys {
				return nil, fmt.Errorf("'%s' is not a set/mutex/time field with a string key", fieldName)
			}
		}

		if in, ok := c.Args["in"]; ok {
			inIn, ok := in.([]interface{})
			if !ok {
				return nil, errors.Errorf("unexpected type for argument 'in' %v of %[1]T", in)
			}
			inIDs := make([]interface{}, 0, len(inIn))
			for _, inVal := range inIn {
				if inStr, ok := inVal.(string); ok {
					id, found := rowKeys[index][field][inStr]
					if found {
						inIDs = append(inIDs, id)
					}
				} else {
					inIDs = append(inIDs, inVal)
				}
			}
			c.Args["in"] = inIDs
		}
	}

	// Translate child calls.
	for i, child := range c.Children {
		translated, err := e.translateCall(child, index, columnKeys, rowKeys)
		if err != nil {
			return nil, err
		}
		c.Children[i] = translated
	}

	// Translate argument calls.
	for k, arg := range c.Args {
		argCall, ok := arg.(*pql.Call)
		if !ok {
			continue
		}

		translated, err := e.translateCall(argCall, index, columnKeys, rowKeys)
		if err != nil {
			return nil, err
		}

		c.Args[k] = translated
	}

	return c, nil
}

func (e *executor) callZero(c *pql.Call) *pql.Call {
	switch c.Name {
	case "Row", "Range":
		if field, err := c.FieldArg(); err == nil {
			if cond, ok := c.Args[field].(*pql.Condition); ok {
				if cond.Op == pql.NEQ {
					// Turn not nothing into everything.
					return &pql.Call{Name: "All"}
				}
			}
		}

		// Use an empty union as a placeholder.
		return &pql.Call{Name: "Union"}

	default:
		return nil
	}
}

func (e *executor) translateResults(ctx context.Context, index string, idx *Index, calls []*pql.Call, results []interface{}, memoryAvailable int64) (err error) {
	span, _ := tracing.StartSpanFromContext(ctx, "executor.translateResults")
	defer span.Finish()

	idMap := make(map[uint64]string)
	if idx.Keys() {
		// Collect all index ids.
		idSet := make(map[uint64]struct{})
		for i := range calls {
			if err := e.collectResultIDs(index, idx, calls[i], results[i], idSet); err != nil {
				return err
			}
		}
		if idMap, err = e.Cluster.translateIndexIDSet(ctx, index, idSet); err != nil {
			return err
		}
	}

	for i := range results {
		results[i], err = e.translateResult(ctx, index, idx, calls[i], results[i], idMap, &memoryAvailable)
		if err != nil {
			return err
		}
	}
	return nil
}

// translationStrategy denotes the several different ways the bits in
// a *Row could be translated to string keys.
type translationStrategy int

const (
	// byCurrentIndex means to interpret the bits as IDs in "top
	// level" index for this query (e.g. the index specified in the
	// path of the HTTP request).
	byCurrentIndex translationStrategy = iota + 1
	// byRowField means that the bits in this *Row are row IDs which
	// should be translated using the field's (*Row.Field) translation store.
	byRowField
	// byRowFieldForeignIndex means that the bits in this *Row should
	// be interpreted as IDs in the foreign index of the *Row.Field.
	byRowFieldForeignIndex
	// byRowIndex means the bits in this *Row should be translated
	// according to the index named by *Row.Index
	byRowIndex
	// noTranslation means the bits should not be translated to string
	// keys.
	noTranslation
)

// howToTranslate determines how a *Row object's bits should be
// translated to keys (if at all). There are several different options
// detailed by the various const values of translationStrategy. In
// order to do this it has to figure out the row's index and field
// which it also returns as the caller may need them to actually
// execute the translation or do whatever else it's doing with the
// translationStrategy information.
func (e *executor) howToTranslate(idx *Index, row *Row) (rowIdx *Index, rowField *Field, strat translationStrategy, err error) {
	// First get the index and field the row specifies (if any).
	rowIdx = idx
	if row.Index != "" && row.Index != idx.Name() {
		rowIdx = e.Holder.Index(row.Index)
		if rowIdx == nil {
			return nil, nil, 0, errors.Errorf("got a row with unknown index: %s", row.Index)
		}
	}
	if row.Field != "" {
		rowField = rowIdx.Field(row.Field)
		if rowField == nil {
			return nil, nil, 0, errors.Errorf("got a row with unknown index/field %s/%s", idx.Name(), row.Field)
		}
	}

	// Handle the case where the Row has specified a field.
	if rowField != nil {
		// Handle the case where field has a foreign index.
		if rowField.ForeignIndex() != "" {
			fidx := e.Holder.Index(rowField.ForeignIndex())
			if fidx == nil {
				return nil, nil, 0, errors.Errorf("foreign index %s not found for field %s in index %s", rowField.ForeignIndex(), rowField.Name(), rowField.Index())
			}
			if fidx.Keys() {
				return rowIdx, rowField, byRowFieldForeignIndex, nil
			}
		} else if rowField.Keys() {
			return rowIdx, rowField, byRowField, nil
		}
		return rowIdx, rowField, noTranslation, nil
	}

	// In this case, the row has specified an index, but not a field,
	// so we translate according to that index.
	if rowIdx != idx && rowIdx.Keys() {
		return rowIdx, rowField, byRowIndex, nil
	}

	// Handle the normal case (row represents a set of records in
	// the top level index, Row has not specifed a different index
	// or field).
	if rowIdx == idx && idx.Keys() && rowField == nil {
		return rowIdx, rowField, byCurrentIndex, nil
	}
	return rowIdx, rowField, noTranslation, nil
}

func (e *executor) collectResultIDs(index string, idx *Index, call *pql.Call, result interface{}, idSet map[uint64]struct{}) error {
	switch result := result.(type) {
	case *Row:
		// Only collect result IDs if they are in the current index.
		_, _, strategy, err := e.howToTranslate(idx, result)
		if err != nil {
			return errors.Wrap(err, "determining how to translate")
		}
		if strategy == byCurrentIndex {
			for _, segment := range result.Segments {
				for _, col := range segment.Columns() {
					idSet[col] = struct{}{}
				}
			}
		}
	case ExtractedIDMatrix:
		for _, col := range result.Columns {
			idSet[col.ColumnID] = struct{}{}
		}
	}

	return nil
}

// preTranslateMatrixSet translates the IDs of a set field in an extracted matrix.
func (e *executor) preTranslateMatrixSet(ctx context.Context, mat ExtractedIDMatrix, fieldIdx uint, field *Field) (map[uint64]string, error) {
	ids := make(map[uint64]struct{}, len(mat.Columns))
	for _, col := range mat.Columns {
		for _, v := range col.Rows[fieldIdx] {
			ids[v] = struct{}{}
		}
	}

	return e.Cluster.translateFieldIDs(ctx, field, ids)
}

func (e *executor) translateResult(ctx context.Context, index string, idx *Index, call *pql.Call, result interface{}, idSet map[uint64]string, memoryAvailable *int64) (_ interface{}, err error) {
	switch result := result.(type) {
	case *Row:
		rowIdx, rowField, strategy, err := e.howToTranslate(idx, result)
		if err != nil {
			return nil, errors.Wrap(err, "determining translation strategy")
		}
		switch strategy {
		case byCurrentIndex:
			other := &Row{}
			for _, segment := range result.Segments {
				for _, col := range segment.Columns() {
					other.Keys = append(other.Keys, idSet[col])
				}
			}
			return other, nil
		case byRowField:
			keys, err := e.Cluster.translateFieldListIDs(ctx, rowField, result.Columns())
			if err != nil {
				return nil, errors.Wrap(err, "translating Row to field keys")
			}
			result.Keys = keys
		case byRowFieldForeignIndex:
			idx = e.Holder.Index(rowField.ForeignIndex())
			if idx == nil {
				return nil, errors.Errorf("foreign index %s not found for field %s in index %s", rowField.ForeignIndex(), rowField.Name(), rowField.Index())
			}
			for _, segment := range result.Segments {
				keys, err := e.Cluster.translateIndexIDs(context.Background(), rowField.ForeignIndex(), segment.Columns())
				if err != nil {
					return nil, errors.Wrap(err, "translating index ids")
				}
				result.Keys = append(result.Keys, keys...)
			}

		case byRowIndex:
			for _, segment := range result.Segments {
				keys, err := e.Cluster.translateIndexIDs(context.Background(), rowIdx.Name(), segment.Columns())
				if err != nil {
					return nil, errors.Wrap(err, "translating index ids")
				}
				result.Keys = append(result.Keys, keys...)
			}
			return result, nil

		case noTranslation:
			return result, nil
		default:
			return nil, errors.Errorf("unknown translation strategy %d", strategy)
		}
	case SignedRow:
		sr, err := func() (*SignedRow, error) {
			fieldName := callArgString(call, "field")
			if fieldName == "" {
				return nil, nil
			}

			field := idx.Field(fieldName)
			if field == nil {
				return nil, nil
			}

			if field.Keys() {
				rslt := result.Pos
				if rslt == nil {
					return &SignedRow{Pos: &Row{}}, nil
				}
				other := &Row{}
				for _, segment := range rslt.Segments {
					keys, err := e.Cluster.translateIndexIDs(context.Background(), field.ForeignIndex(), segment.Columns())
					if err != nil {
						return nil, errors.Wrap(err, "translating index ids")
					}
					other.Keys = append(other.Keys, keys...)
				}
				return &SignedRow{Pos: other}, nil
			}

			return nil, nil
		}()
		if err != nil {
			return nil, err
		} else if sr != nil {
			return *sr, nil
		}

	case PairField:
		if fieldName := callArgString(call, "field"); fieldName != "" {
			field := idx.Field(fieldName)
			if field == nil {
				return nil, fmt.Errorf("field %q not found", fieldName)
			}
			if field.Keys() {
				key, err := field.TranslateStore().TranslateID(result.Pair.ID)
				if err != nil {
					return nil, err
				}
				if call.Name == "MinRow" || call.Name == "MaxRow" {
					result.Pair.Key = key
					return result, nil
				}
				return PairField{
					Pair:  Pair{Key: key, Count: result.Pair.Count},
					Field: fieldName,
				}, nil
			}
		}

	case *PairsField:
		if fieldName := callArgString(call, "_field"); fieldName != "" {
			field := idx.Field(fieldName)
			if field == nil {
				return nil, fmt.Errorf("field %q not found", fieldName)
			}
			if field.Keys() {
				ids := make([]uint64, len(result.Pairs))
				for i := range result.Pairs {
					ids[i] = result.Pairs[i].ID
				}
				keys, err := e.Cluster.translateFieldListIDs(ctx, field, ids)
				if err != nil {
					return nil, err
				}
				other := make([]Pair, len(result.Pairs))
				for i := range result.Pairs {
					other[i] = Pair{Key: keys[i], Count: result.Pairs[i].Count}
				}
				return &PairsField{
					Pairs: other,
					Field: fieldName,
				}, nil
			}
		}

	case *GroupCounts:
		fieldIDs := make(map[*Field]map[uint64]struct{})
		foreignIDs := make(map[*Field]map[uint64]struct{})
		groups := result.Groups()
		for _, gl := range groups {
			for _, g := range gl.Group {
				field := idx.Field(g.Field)
				if field == nil {
					return nil, newNotFoundError(ErrFieldNotFound, g.Field)
				}
				if field.Keys() {
					if g.Value != nil {
						if fi := field.ForeignIndex(); fi != "" {
							m, ok := foreignIDs[field]
							if !ok {
								m = make(map[uint64]struct{}, len(groups))
								foreignIDs[field] = m
							}

							m[uint64(*g.Value)] = struct{}{}
							continue
						}
					}

					m, ok := fieldIDs[field]
					if !ok {
						m = make(map[uint64]struct{}, len(groups))
						fieldIDs[field] = m
					}

					m[g.RowID] = struct{}{}
				}
			}
		}

		fieldTranslations := make(map[string]map[uint64]string)
		for field, ids := range fieldIDs {
			trans, err := e.Cluster.translateFieldIDs(ctx, field, ids)
			if err != nil {
				return nil, errors.Wrapf(err, "translating IDs in field %q", field.Name())
			}
			fieldTranslations[field.Name()] = trans
		}

		foreignTranslations := make(map[string]map[uint64]string)
		for field, ids := range foreignIDs {
			trans, err := e.Cluster.translateIndexIDSet(ctx, field.ForeignIndex(), ids)
			if err != nil {
				return nil, errors.Wrapf(err, "translating foreign IDs from index %q", field.ForeignIndex())
			}
			foreignTranslations[field.Name()] = trans
		}

		// We are reluctant to smash result, and I'm not sure we need
		// to be but I'm not sure we don't need to be.
		newGroups := make([]GroupCount, len(groups))
		copy(newGroups, groups)
		for gi, gl := range groups {

			group := make([]FieldRow, len(gl.Group))
			for i, g := range gl.Group {
				if ft, ok := fieldTranslations[g.Field]; ok {
					g.RowKey = ft[g.RowID]
				} else if ft, ok := foreignTranslations[g.Field]; ok && g.Value != nil {
					g.RowKey = ft[uint64(*g.Value)]
					g.Value = nil
				}

				group[i] = g
			}
			// Replace with translated group.
			newGroups[gi].Group = group
		}
		other := &GroupCounts{}
		if result != nil {
			other.aggregateType = result.aggregateType
		}
		other.groups = newGroups
		return other, nil
	case RowIDs:
		fieldName := callArgString(call, "_field")
		if fieldName == "" {
			return nil, ErrFieldNotFound
		}

		other := RowIdentifiers{
			Field: fieldName,
		}

		if field := idx.Field(fieldName); field == nil {
			return nil, newNotFoundError(ErrFieldNotFound, fieldName)
		} else if field.Keys() {
			keys, err := e.Cluster.translateFieldListIDs(ctx, field, result)
			if err != nil {
				return nil, errors.Wrap(err, "translating row IDs")
			}
			other.Keys = keys
		} else {
			other.Rows = result
		}

		return other, nil

	case ExtractedIDMatrix:
		type fieldMapper = func([]uint64) (_ interface{}, err error)

		fields := make([]ExtractedTableField, len(result.Fields))
		mappers := make([]fieldMapper, len(result.Fields))
		for i, v := range result.Fields {
			field := idx.Field(v)
			if field == nil {
				return nil, newNotFoundError(ErrFieldNotFound, v)
			}

			var mapper fieldMapper
			var datatype string
			switch typ := field.Type(); typ {
			case FieldTypeBool:
				datatype = "bool"
				mapper = func(ids []uint64) (_ interface{}, err error) {
					switch len(ids) {
					case 0:
						return nil, nil
					case 1:
						switch ids[0] {
						case 0:
							return false, nil
						case 1:
							return true, nil
						default:
							return nil, errors.Errorf("invalid ID for boolean %q: %d", field.Name(), ids[0])
						}
					default:
						return nil, errors.Errorf("boolean %q has too many values: %v", field.Name(), ids)
					}
				}
			case FieldTypeSet, FieldTypeTime:
				if field.Keys() {
					datatype = "[]string"
					translations, err := e.preTranslateMatrixSet(ctx, result, uint(i), field)
					if err != nil {
						return nil, errors.Wrapf(err, "translating IDs of field %q", v)
					}
					mapper = func(ids []uint64) (interface{}, error) {
						keys := make([]string, len(ids))
						for i, id := range ids {
							keys[i] = translations[id]
						}
						return keys, nil
					}
				} else {
					datatype = "[]uint64"
					mapper = func(ids []uint64) (interface{}, error) {
						if ids == nil {
							ids = []uint64{}
						}
						return ids, nil
					}
				}
			case FieldTypeMutex:
				if field.Keys() {
					datatype = "string"
					translations, err := e.preTranslateMatrixSet(ctx, result, uint(i), field)
					if err != nil {
						return nil, errors.Wrapf(err, "translating IDs of field %q", v)
					}
					mapper = func(ids []uint64) (interface{}, error) {
						switch len(ids) {
						case 0:
							return nil, nil
						case 1:
							return translations[ids[0]], nil
						default:
							return nil, errors.Errorf("mutex %q has too many values: %v", field.Name(), ids)
						}
					}
				} else {
					datatype = "uint64"
					mapper = func(ids []uint64) (_ interface{}, err error) {
						switch len(ids) {
						case 0:
							return nil, nil
						case 1:
							return ids[0], nil
						default:
							return nil, errors.Errorf("mutex %q has too many values: %v", field.Name(), ids)
						}
					}
				}
			case FieldTypeInt:
				if fi := field.ForeignIndex(); fi != "" {
					if field.Keys() {
						datatype = "string"
						ids := make(map[uint64]struct{}, len(result.Columns))
						for _, col := range result.Columns {
							for _, v := range col.Rows[i] {
								ids[v] = struct{}{}
							}
						}
						trans, err := e.Cluster.translateIndexIDSet(ctx, field.ForeignIndex(), ids)
						if err != nil {
							return nil, errors.Wrapf(err, "translating foreign IDs from index %q", field.ForeignIndex())
						}
						mapper = func(ids []uint64) (interface{}, error) {
							switch len(ids) {
							case 0:
								return nil, nil
							case 1:
								return trans[ids[0]], nil
							default:
								return nil, errors.Errorf("BSI field %q has too many values: %v", field.Name(), ids)
							}
						}
					} else {
						datatype = "uint64"
						mapper = func(ids []uint64) (interface{}, error) {
							switch len(ids) {
							case 0:
								return nil, nil
							case 1:
								return ids[0], nil
							default:
								return nil, errors.Errorf("BSI field %q has too many values: %v", field.Name(), ids)
							}
						}
					}
				} else {
					datatype = "int64"
					mapper = func(ids []uint64) (interface{}, error) {
						switch len(ids) {
						case 0:
							return nil, nil
						case 1:
							return int64(ids[0]), nil
						default:
							return nil, errors.Errorf("BSI field %q has too many values: %v", field.Name(), ids)
						}
					}
				}
			case FieldTypeDecimal:
				datatype = "decimal"
				scale := field.Options().Scale
				mapper = func(ids []uint64) (_ interface{}, err error) {
					switch len(ids) {
					case 0:
						return nil, nil
					case 1:
						return pql.NewDecimal(int64(ids[0]), scale), nil
					default:
						return nil, errors.Errorf("BSI field %q has too many values: %v", field.Name(), ids)
					}
				}
			case FieldTypeTimestamp:
				datatype = "timestamp"
				unit := field.Options().TimeUnit
				mapper = func(ids []uint64) (_ interface{}, err error) {
					switch len(ids) {
					case 0:
						return nil, nil
					case 1:
						ts, err := ValToTimestamp(unit, int64(ids[0]))
						if err != nil {
							return nil, err
						}
						return ts, nil
					default:
						return nil, errors.Errorf("BSI field %q has too many values: %v", field.Name(), ids)
					}
				}
			default:
				return nil, errors.Errorf("field type %q not yet supported", typ)
			}
			mappers[i] = mapper
			fields[i] = ExtractedTableField{
				Name: v,
				Type: datatype,
			}
		}

		var translateCol func(uint64) (KeyOrID, error)
		if idx.keys {
			translateCol = func(id uint64) (KeyOrID, error) {
				return KeyOrID{Keyed: true, Key: idSet[id]}, nil
			}
		} else {
			translateCol = func(id uint64) (KeyOrID, error) {
				return KeyOrID{ID: id}, nil
			}
		}

		cols := make([]ExtractedTableColumn, len(result.Columns))
		colData := make([]interface{}, len(cols)*len(result.Fields))
		for i, col := range result.Columns {
			data := colData[i*len(result.Fields) : (i+1)*len(result.Fields) : (i+1)*len(result.Fields)]
			for j, rows := range col.Rows {
				v, err := mappers[j](rows)
				if err != nil {
					return nil, errors.Wrap(err, "translating extracted table value")
				}
				data[j] = v
			}

			colTrans, err := translateCol(col.ColumnID)
			if err != nil {
				return nil, errors.Wrap(err, "translating column ID in extracted table")
			}

			cols[i] = ExtractedTableColumn{
				Column: colTrans,
				Rows:   data,
			}
			if *memoryAvailable -= calcResultMemory(cols[i]); *memoryAvailable < 0 {
				return nil, fmt.Errorf("table exceeds available memory")
			}
		}

		return ExtractedTable{
			Fields:  fields,
			Columns: cols,
		}, nil
	}

	return result, nil
}

// ValToTimestamp takes a timeunit and an integer value and converts it to time.Time
func ValToTimestamp(unit string, val int64) (time.Time, error) {
	switch unit {
	case TimeUnitSeconds:
		return time.Unix(val, 0).UTC(), nil
	case TimeUnitMilliseconds:
		return time.UnixMilli(val).UTC(), nil
	case TimeUnitMicroseconds, TimeUnitUSeconds:
		return time.UnixMicro(val).UTC(), nil
	case TimeUnitNanoseconds:
		return time.Unix(0, val).UTC(), nil
	default:
		return time.Time{}, errors.Errorf("Unknown time unit: '%v'", unit)
	}
}

// TimestampToVal takes a time unit and a time.Time and converts it to an integer value
func TimestampToVal(unit string, ts time.Time) int64 {
	switch unit {
	case TimeUnitSeconds:
		return ts.Unix()
	case TimeUnitMilliseconds:
		return ts.UnixMilli()
	case TimeUnitMicroseconds, TimeUnitUSeconds:
		return ts.UnixMicro()
	case TimeUnitNanoseconds:
		return ts.UnixNano()
	}
	return 0
}

// detectRangeCall returns true if the call or one of its children contains a Range call
// TODO: Remove at version 2.0
func (e *executor) detectRangeCall(c *pql.Call) bool {
	// detect whether there is a Range call
	if c.Name == "Range" {
		return true
	}
	for _, c := range c.Children {
		if e.detectRangeCall(c) {
			return true
		}
	}
	return false
}

// validateQueryContext returns a query-appropriate error if the context is done.
func validateQueryContext(ctx context.Context) error {
	select {
	case <-ctx.Done():
		switch err := ctx.Err(); err {
		case context.Canceled:
			return ErrQueryCancelled
		case context.DeadlineExceeded:
			return ErrQueryTimeout
		default:
			return err
		}
	default:
		return nil
	}
}

// errShardUnavailable is a marker error if no nodes are available.
var errShardUnavailable = errors.New("shard unavailable")

type mapFunc func(ctx context.Context, shard uint64, mopt *mapOptions) (_ interface{}, err error)

type mapOptions struct {
	memoryAvailable *int64
}

type reduceFunc func(ctx context.Context, prev, v interface{}) interface{}

type mapResponse struct {
	node   *disco.Node
	shards []uint64

	result interface{}
	err    error
}

// ExecOptions represents an execution context for a single Execute() call.
type ExecOptions struct {
	Remote        bool
	Profile       bool
	PreTranslated bool
	EmbeddedData  []*Row
	MaxMemory     int64
}

func needsShards(call *pql.Call) bool {
	if call == nil {
		return false
	}
	switch call.Name {
	case "Clear", "Set":
		return false
	case "Count", "TopN", "Rows":
		return true
	}
	// default catches Bitmap calls
	return true
}

// SignedRow represents a signed *Row with two (neg/pos) *Rows.
type SignedRow struct {
	Neg   *Row   `json:"neg"`
	Pos   *Row   `json:"pos"`
	Field string `json:"-"`
}

func (s *SignedRow) Clone() (r *SignedRow) {
	r = &SignedRow{
		Neg:   s.Neg.Clone(), // Row.Clone() returns nil for nil.
		Pos:   s.Pos.Clone(),
		Field: s.Field,
	}
	return
}

// ToTable implements the ToTabler interface.
func (s SignedRow) ToTable() (*proto.TableResponse, error) {
	var n uint64
	if s.Neg != nil {
		n += s.Neg.Count()
	}
	if s.Pos != nil {
		n += s.Pos.Count()
	}
	return proto.RowsToTable(&s, int(n))
}

// ToRows implements the ToRowser interface.
func (s SignedRow) ToRows(callback func(*proto.RowResponse) error) error {
	ci := []*proto.ColumnInfo{{Name: s.Field, Datatype: "int64"}}
	if s.Neg != nil {
		negs := s.Neg.Columns()
		for i := len(negs) - 1; i >= 0; i-- {
			val, err := toNegInt64(negs[i])
			if err != nil {
				return errors.Wrap(err, "converting uint64 to int64 (negative)")
			}

			if err := callback(&proto.RowResponse{
				Headers: ci,
				Columns: []*proto.ColumnResponse{
					{ColumnVal: &proto.ColumnResponse_Int64Val{Int64Val: val}},
				},
			}); err != nil {
				return errors.Wrap(err, "calling callback")
			}
			ci = nil
		}
	}
	if s.Pos != nil {
		for _, id := range s.Pos.Columns() {
			val, err := toInt64(id)
			if err != nil {
				return errors.Wrap(err, "converting uint64 to int64 (positive)")
			}

			if err := callback(&proto.RowResponse{
				Headers: ci,
				Columns: []*proto.ColumnResponse{
					{ColumnVal: &proto.ColumnResponse_Int64Val{Int64Val: val}},
				},
			}); err != nil {
				return errors.Wrap(err, "calling callback")
			}
			ci = nil
		}
	}
	return nil
}

func toNegInt64(n uint64) (int64, error) {
	const absMinInt64 = uint64(1 << 63)

	if n > absMinInt64 {
		return 0, errors.Errorf("value %d overflows int64", n)
	}

	if n == absMinInt64 {
		return int64(-1 << 63), nil
	}

	// n < 1 << 63
	return -int64(n), nil
}

func toInt64(n uint64) (int64, error) {
	const maxInt64 = uint64(1<<63) - 1

	if n > maxInt64 {
		return 0, errors.Errorf("value %d overflows int64", n)
	}

	return int64(n), nil
}

func (sr *SignedRow) Union(other SignedRow) SignedRow {
	ret := SignedRow{&Row{}, &Row{}, ""}

	// merge in sr
	if sr != nil {
		if sr.Neg != nil {
			ret.Neg = ret.Neg.Union(sr.Neg)
		}
		if sr.Pos != nil {
			ret.Pos = ret.Pos.Union(sr.Pos)
		}
	}

	// merge in other
	if other.Neg != nil {
		ret.Neg = ret.Neg.Union(other.Neg)
	}
	if other.Pos != nil {
		ret.Pos = ret.Pos.Union(other.Pos)
	}

	return ret
}

// ValCount represents a grouping of sum & count for Sum() and Average() calls. Also Min, Max....
type ValCount struct {
	Val          int64        `json:"value"`
	FloatVal     float64      `json:"floatValue"`
	DecimalVal   *pql.Decimal `json:"decimalValue"`
	TimestampVal time.Time    `json:"timestampValue"`
	Count        int64        `json:"count"`
}

func (v *ValCount) Clone() (r *ValCount) {
	r = &ValCount{
		Val:          v.Val,
		FloatVal:     v.FloatVal,
		TimestampVal: v.TimestampVal,
		Count:        v.Count,
	}
	if v.DecimalVal != nil {
		r.DecimalVal = v.DecimalVal.Clone()
	}
	return
}

// ToTable implements the ToTabler interface.
func (v ValCount) ToTable() (*proto.TableResponse, error) {
	return proto.RowsToTable(&v, 1)
}

// ToRows implements the ToRowser interface.
func (v ValCount) ToRows(callback func(*proto.RowResponse) error) error {
	var ci []*proto.ColumnInfo
	// ValCount can have a decimal, float, or integer value, but
	// not more than one (as of this writing).
	if v.DecimalVal != nil {
		ci = []*proto.ColumnInfo{
			{Name: "value", Datatype: "decimal"},
			{Name: "count", Datatype: "int64"},
		}
		vValue := v.DecimalVal.Value()
		vValuePtr := &vValue
		if err := callback(&proto.RowResponse{
			Headers: ci,
			Columns: []*proto.ColumnResponse{
				{ColumnVal: &proto.ColumnResponse_DecimalVal{DecimalVal: &proto.Decimal{Value: vValuePtr.Int64(), Scale: v.DecimalVal.Scale}}},
				{ColumnVal: &proto.ColumnResponse_Int64Val{Int64Val: v.Count}},
			},
		}); err != nil {
			return errors.Wrap(err, "calling callback")
		}
	} else if v.FloatVal != 0 {
		ci = []*proto.ColumnInfo{
			{Name: "value", Datatype: "float64"},
			{Name: "count", Datatype: "int64"},
		}
		if err := callback(&proto.RowResponse{
			Headers: ci,
			Columns: []*proto.ColumnResponse{
				{ColumnVal: &proto.ColumnResponse_Float64Val{Float64Val: v.FloatVal}},
				{ColumnVal: &proto.ColumnResponse_Int64Val{Int64Val: v.Count}},
			},
		}); err != nil {
			return errors.Wrap(err, "calling callback")
		}
	} else if !v.TimestampVal.IsZero() {
		ci = []*proto.ColumnInfo{
			{Name: "value", Datatype: "string"},
			{Name: "count", Datatype: "int64"},
		}
		if err := callback(&proto.RowResponse{
			Headers: ci,
			Columns: []*proto.ColumnResponse{
				{ColumnVal: &proto.ColumnResponse_StringVal{StringVal: v.TimestampVal.Format(time.RFC3339Nano)}},
				{ColumnVal: &proto.ColumnResponse_Int64Val{Int64Val: v.Count}},
			},
		}); err != nil {
			return errors.Wrap(err, "calling callback")
		}
	} else {
		ci = []*proto.ColumnInfo{
			{Name: "value", Datatype: "int64"},
			{Name: "count", Datatype: "int64"},
		}
		if err := callback(&proto.RowResponse{
			Headers: ci,
			Columns: []*proto.ColumnResponse{
				{ColumnVal: &proto.ColumnResponse_Int64Val{Int64Val: v.Val}},
				{ColumnVal: &proto.ColumnResponse_Int64Val{Int64Val: v.Count}},
			},
		}); err != nil {
			return errors.Wrap(err, "calling callback")
		}
	}
	return nil
}

func (vc *ValCount) Add(other ValCount) ValCount {
	return ValCount{
		Val:   vc.Val + other.Val,
		Count: vc.Count + other.Count,
	}
}

// smaller returns the smaller of the two ValCounts.
func (vc *ValCount) Smaller(other ValCount) ValCount {
	if vc.DecimalVal != nil || other.DecimalVal != nil {
		return vc.decimalSmaller(other)
	} else if vc.FloatVal != 0 || other.FloatVal != 0 {
		return vc.floatSmaller(other)
	} else if !vc.TimestampVal.IsZero() || !other.TimestampVal.IsZero() {
		return vc.timestampSmaller(other)
	}
	if vc.Count == 0 || (other.Val < vc.Val && other.Count > 0) {
		return other
	}
	extra := int64(0)
	if vc.Val == other.Val {
		extra += other.Count
	}
	return ValCount{
		Val:          vc.Val,
		Count:        vc.Count + extra,
		DecimalVal:   vc.DecimalVal,
		FloatVal:     vc.FloatVal,
		TimestampVal: vc.TimestampVal,
	}
}

// timestampSmaller returns the smaller of the two (vc or other), while merging the count
// if they are equal.
func (vc *ValCount) timestampSmaller(other ValCount) ValCount {
	if other.TimestampVal.Equal(time.Time{}) {
		return *vc
	}
	if vc.Count == 0 || vc.TimestampVal.Equal(time.Time{}) || (other.TimestampVal.Before(vc.TimestampVal) && other.Count > 0) {
		return other
	}
	extra := int64(0)
	if vc.TimestampVal.Equal(other.TimestampVal) {
		extra += other.Count
	}
	return ValCount{
		Val:          vc.Val,
		TimestampVal: vc.TimestampVal,
		Count:        vc.Count + extra,
	}
}

// decimalSmaller returns the smaller of the two (vc or other), while merging the count
// if they are equal.
func (vc *ValCount) decimalSmaller(other ValCount) ValCount {
	if other.DecimalVal == nil {
		return *vc
	}
	if vc.Count == 0 || vc.DecimalVal == nil || (other.DecimalVal.LessThan(*vc.DecimalVal) && other.Count > 0) {
		return other
	}
	extra := int64(0)
	if vc.DecimalVal.EqualTo(*other.DecimalVal) {
		extra += other.Count
	}
	return ValCount{
		DecimalVal: vc.DecimalVal,
		Count:      vc.Count + extra,
	}
}

// floatSmaller returns the smaller of the two (vc or other), while merging the count
// if they are equal.
func (vc *ValCount) floatSmaller(other ValCount) ValCount {
	if vc.Count == 0 || (other.FloatVal < vc.FloatVal && other.Count > 0) {
		return other
	}
	extra := int64(0)
	if vc.FloatVal == other.FloatVal {
		extra += other.Count
	}
	return ValCount{
		FloatVal: vc.FloatVal,
		Count:    vc.Count + extra,
	}
}

// larger returns the larger of the two ValCounts.
func (vc *ValCount) Larger(other ValCount) ValCount {
	if vc.DecimalVal != nil || other.DecimalVal != nil {
		return vc.decimalLarger(other)
	} else if vc.FloatVal != 0 || other.FloatVal != 0 {
		return vc.floatLarger(other)
	} else if !vc.TimestampVal.Equal(time.Time{}) || !other.TimestampVal.Equal(time.Time{}) {
		return vc.timestampLarger(other)
	}
	if vc.Count == 0 || (other.Val > vc.Val && other.Count > 0) {
		return other
	}
	extra := int64(0)
	if vc.Val == other.Val {
		extra += other.Count
	}
	return ValCount{
		Val:          vc.Val,
		Count:        vc.Count + extra,
		DecimalVal:   vc.DecimalVal,
		FloatVal:     vc.FloatVal,
		TimestampVal: vc.TimestampVal,
	}
}

// timestampLarger returns the larger of the two (vc or other), while merging the count
// if they are equal.
func (vc *ValCount) timestampLarger(other ValCount) ValCount {
	if other.TimestampVal.Equal(time.Time{}) {
		return *vc
	}
	if vc.Count == 0 || vc.TimestampVal.Equal(time.Time{}) || (other.TimestampVal.After(vc.TimestampVal) && other.Count > 0) {
		return other
	}
	extra := int64(0)
	if vc.TimestampVal.Equal(other.TimestampVal) {
		extra += other.Count
	}
	return ValCount{
		Val:          vc.Val,
		TimestampVal: vc.TimestampVal,
		Count:        vc.Count + extra,
	}
}

// decimalLarger returns the larger of the two (vc or other), while merging the count
// if they are equal.
func (vc *ValCount) decimalLarger(other ValCount) ValCount {
	if other.DecimalVal == nil {
		return *vc
	}
	if vc.Count == 0 || vc.DecimalVal == nil || (other.DecimalVal.GreaterThan(*vc.DecimalVal) && other.Count > 0) {
		return other
	}
	extra := int64(0)
	if vc.DecimalVal.EqualTo(*other.DecimalVal) {
		extra += other.Count
	}
	return ValCount{
		DecimalVal: vc.DecimalVal,
		Count:      vc.Count + extra,
	}
}

// floatLarger returns the larger of the two (vc or other), while merging the count
// if they are equal.
func (vc *ValCount) floatLarger(other ValCount) ValCount {
	if vc.Count == 0 || (other.FloatVal > vc.FloatVal && other.Count > 0) {
		return other
	}
	extra := int64(0)
	if vc.FloatVal == other.FloatVal {
		extra += other.Count
	}
	return ValCount{
		FloatVal: vc.FloatVal,
		Count:    vc.Count + extra,
	}
}

func callArgString(call *pql.Call, key string) string {
	value, ok := call.Args[key]
	if !ok {
		return ""
	}
	s, _ := value.(string)
	return s
}

// groupByIterator contains several slices. Each slice contains a number of
// elements equal to the number of fields in the group by (the number of Rows
// calls).
type groupByIterator struct {
	executor *executor
	qcx      *Qcx
	index    string
	shard    uint64

	// rowIters contains a rowIterator for each of the fields in the Group By.
	rowIters []rowIterator
	// rows contains the current row data for each of the fields in the Group
	// By. Each row is the intersection of itself and the rows of the fields
	// with an index lower than its own. This is a performance optimization so
	// that the expected common case of getting the next row in the furthest
	// field to the right require only a single intersect with the row of the
	// previous field to determine the count of the new group.
	rows []struct {
		row   *Row
		id    uint64
		value *int64
	}

	// fields helps with the construction of GroupCount results by holding all
	// the field names that are being grouped by. Each results makes a copy of
	// fields and then sets the row ids.
	fields []FieldRow
	done   bool

	// Optional filter row to intersect against first level of values.
	filter *Row

	// Optional aggregate function to execute for each group.
	aggregate *pql.Call
}

// newGroupByIterator initializes a new groupByIterator.
func newGroupByIterator(executor *executor, qcx *Qcx, rowIDs []RowIDs, children []*pql.Call, aggregate *pql.Call, filter *Row, index string, shard uint64, holder *Holder) (_ *groupByIterator, err0 error) {
	gbi := &groupByIterator{
		executor: executor,
		qcx:      qcx,
		index:    index,
		shard:    shard,
		rowIters: make([]rowIterator, len(children)),
		rows: make([]struct {
			row   *Row
			id    uint64
			value *int64
		}, len(children)),
		filter:    filter,
		aggregate: aggregate,
		fields:    make([]FieldRow, len(children)),
	}
	idx := holder.Index(index)

	var (
		fieldName string
		viewName  string
		ok        bool
		views     []string
	)
	ignorePrev := false
	for i, call := range children {
		var isTimeField bool
		if fieldName, ok = call.Args["_field"].(string); !ok {
			return nil, errors.Errorf("%s call must have field with valid (string) field name. Got %v of type %[2]T", call.Name, call.Args["_field"])
		}
		field := holder.Field(index, fieldName)
		if field == nil {
			return nil, newNotFoundError(ErrFieldNotFound, fieldName)
		}
		gbi.fields[i].Field = fieldName
		options := field.Options()
		gbi.fields[i].FieldOptions = &options

		switch field.Type() {
		case FieldTypeSet, FieldTypeMutex, FieldTypeBool:
			viewName = viewStandard
		case FieldTypeTime:
			var (
				err error
				v   interface{}
			)

			// Parse "from" time, if set.
			var (
				hasFrom  bool
				fromTime time.Time
			)
			if v, hasFrom = call.Args["from"]; hasFrom {
				if fromTime, err = parseTime(v); err != nil {
					return nil, errors.Wrap(err, "parsing from time")
				}
			}

			// Parse "to" time, if set.
			var (
				hasTo  bool
				toTime time.Time
			)
			if v, hasTo = call.Args["to"]; hasTo {
				if toTime, err = parseTime(v); err != nil {
					return nil, errors.Wrap(err, "parsing to time")
				}
			}

			if hasTo || hasFrom {
				// Determine the views based on the specified time range.
				var err error
				views, err = field.viewsByTimeRange(fromTime, toTime)
				if err != nil {
					return nil, err
				}
				isTimeField = true
			} else {
				viewName = viewStandard
			}
		case FieldTypeInt, FieldTypeTimestamp:
			viewName = viewBSIGroupPrefix + fieldName

		default: // FieldTypeDecimal
			return nil, errors.Errorf("%s call must have field of one of types: %s",
				call.Name, strings.Join([]string{FieldTypeSet, FieldTypeTime, FieldTypeMutex, FieldTypeBool, FieldTypeInt, FieldTypeTimestamp}, ","))
		}

		filters := []roaring.BitmapFilter{}
		if len(rowIDs[i]) > 0 {
			filters = append(filters, roaring.NewBitmapRowsFilter(rowIDs[i]))
		}

		tx, finisher, err := qcx.GetTx(Txo{Write: !writable, Index: idx, Shard: shard})
		if err != nil {
			return nil, err
		}
		defer finisher(&err0)

		// Fetch fragment(s), get rowIterator
		if isTimeField {
			var fragments []*fragment
			for _, viewName := range views {
				fragment := holder.fragment(index, fieldName, viewName, shard)
				if fragment != nil {
					fragments = append(fragments, fragment)
				}
			}
			if len(fragments) == 0 {
				// whole shard doesn't have all it needs to continue ?
				return nil, nil
			}

			gbi.rowIters[i], err = timeFragmentsRowIterator(fragments, tx, i != 0, filters...)
			if err != nil {
				return nil, err
			}
		} else {
			frag := holder.fragment(index, fieldName, viewName, shard)
			if frag == nil { // this means this whole shard doesn't have all it needs to continue
				return nil, nil
			}

			gbi.rowIters[i], err = frag.rowIterator(tx, i != 0, filters...)
			if err != nil {
				return nil, err
			}

		}

		prev, hasPrev, err := call.UintArg("previous")
		if err != nil {
			return nil, errors.Wrap(err, "getting previous")
		} else if hasPrev && !ignorePrev {
			if i == len(children)-1 {
				prev++
			}
			gbi.rowIters[i].Seek(prev)
		}
		nextRow, rowID, value, wrapped, err := gbi.rowIters[i].Next()
		if err != nil {
			return nil, err
		} else if nextRow == nil {
			gbi.done = true
			return gbi, nil
		}
		gbi.rows[i].row = nextRow
		gbi.rows[i].id = rowID
		gbi.rows[i].value = value
		if hasPrev && rowID != prev {
			// ignorePrev signals that we didn't find a previous row, so all
			// Rows queries "deeper" than it need to ignore the previous
			// argument and start at the beginning.
			ignorePrev = true
		}
		if wrapped {
			// if a field has wrapped, we need to get the next row for the
			// previous field, and if that one wraps we need to keep going
			// backward.
			for j := i - 1; j >= 0; j-- {
				nextRow, rowID, value, wrapped, err := gbi.rowIters[j].Next()
				if err != nil {
					return nil, err
				} else if nextRow == nil {
					gbi.done = true
					return gbi, nil
				}
				gbi.rows[j].row = nextRow
				gbi.rows[j].id = rowID
				gbi.rows[j].value = value
				if !wrapped {
					break
				}
			}
		}
	}

	// Apply filter to first level, if available.
	if gbi.filter != nil && len(gbi.rows) > 0 {
		gbi.rows[0].row = gbi.rows[0].row.Intersect(gbi.filter)
	}

	for i := 1; i < len(gbi.rows)-1; i++ {
		gbi.rows[i].row = gbi.rows[i].row.Intersect(gbi.rows[i-1].row)
	}

	return gbi, nil
}

// nextAtIdx is a recursive helper method for getting the next row for the field
// at index i, and then updating the rows in the "higher" fields if it wraps.
func (gbi *groupByIterator) nextAtIdx(ctx context.Context, i int) (err error) {
	// loop until we find a non-empty row. This is an optimization - the loop and if/break can be removed.
	for {
		if err = ctx.Err(); err != nil {
			return err
		}
		nr, rowID, value, wrapped, err := gbi.rowIters[i].Next()
		if err != nil {
			return err
		} else if nr == nil {
			gbi.done = true
			return nil
		}
		if wrapped && i != 0 {
			err = gbi.nextAtIdx(ctx, i-1)
			if gbi.done || err != nil {
				return err
			}
		}
		if i == 0 && gbi.filter != nil {
			gbi.rows[i].row = nr.Intersect(gbi.filter)
		} else if i == 0 || i == len(gbi.rows)-1 {
			gbi.rows[i].row = nr
		} else {
			gbi.rows[i].row = nr.Intersect(gbi.rows[i-1].row)
		}
		gbi.rows[i].id = rowID
		gbi.rows[i].value = value

		if !gbi.rows[i].row.IsEmpty() {
			break
		}
	}
	return nil
}

// Next returns a GroupCount representing the next group by record. When there
// are no more records it will return an empty GroupCount and done==true.
func (gbi *groupByIterator) Next(ctx context.Context) (ret GroupCount, done bool, err error) {
	// loop until we find a result with count > 0
	for {
		if err := ctx.Err(); err != nil {
			return ret, false, err
		}
		if gbi.done {
			return ret, true, nil
		}
		if gbi.aggregate == nil || gbi.aggregate.Name == "Count" {
			if len(gbi.rows) == 1 {
				ret.Count = gbi.rows[len(gbi.rows)-1].row.Count()
			} else {
				ret.Count = gbi.rows[len(gbi.rows)-1].row.intersectionCount(gbi.rows[len(gbi.rows)-2].row)
			}
		} else {
			gr := gbi.rows[len(gbi.rows)-1]
			filter := gr.row
			if len(gbi.rows) != 1 {
				filter = filter.Intersect(gbi.rows[len(gbi.rows)-2].row)
			}

			switch gbi.aggregate.Name {
			case "Sum":
				result, err := gbi.executor.executeSumCountShard(ctx, gbi.qcx, gbi.index, gbi.aggregate, filter, gbi.shard)
				if err != nil {
					return ret, false, err
				}
				ret.Count = uint64(result.Count)
				ret.Agg = result.Val
				ret.DecimalAgg = result.DecimalVal
			}
		}
		if ret.Count == 0 {
			err := gbi.nextAtIdx(ctx, len(gbi.rows)-1)
			if err != nil {
				return ret, false, err
			}
			continue
		}
		break
	}

	ret.Group = make([]FieldRow, len(gbi.rows))
	copy(ret.Group, gbi.fields)
	for i, r := range gbi.rows {
		ret.Group[i].RowID = r.id
		ret.Group[i].Value = r.value
	}

	// set up for next call
	err = gbi.nextAtIdx(ctx, len(gbi.rows)-1)

	return ret, false, err
}

// getCondIntSlice looks at the field, the cond op type (which is
// expected to be one of the BETWEEN ops types), and the values in the
// conditional and returns a slice of int64 which is scaled for
// decimal fields and has the values modulated such that the BETWEEN
// op can be treated as being of the form a<=x<=b.
func getCondIntSlice(f *Field, cond *pql.Condition) ([]int64, error) {
	val, ok := cond.Value.([]interface{})
	if !ok {
		return nil, errors.Errorf("expected conditional to have []interface{} Value, but got %v of %[1]T", cond.Value)
	}

	ret := make([]int64, len(val))
	for i, v := range val {
		s, err := getScaledInt(f, v)
		if err != nil {
			return nil, errors.Wrap(err, "getting scaled integer")
		}
		ret[i] = s
	}

	// In the case where one (or both) of the predicates is on the
	// opposite edge, return early to avoid the increment/decrement
	// logic below and prevent an overflow.
	if ret[0] == math.MaxInt64 || ret[1] == math.MinInt64 {
		return ret, nil
	}

	switch cond.Op {
	case pql.BTWN_LT_LTE: // a < x <= b
		ret[0]++
	case pql.BTWN_LTE_LT: // a <= x < b
		ret[1]--
	case pql.BTWN_LT_LT: // a < x < b
		ret[0]++
		ret[1]--
	}

	return ret, nil
}

// getScaledInt gets the scaled integer value for v based on
// the field type. In the `decimalToInt64()` function, the
// returned int64 value will be adjusted to correspond to the
// range of the field. This is only necessary for pql.Decimal
// values. For example, if v is less than f.Options.Min, int64 will
// return int64(f.Options.Min)-1, or math.MinInt64 if f.Options.Min
// is already equal to math.MinInt64.
func getScaledInt(f *Field, v interface{}) (int64, error) {
	var value int64

	opt := f.Options()
	if opt.Type == FieldTypeDecimal {
		switch tv := v.(type) {
		case uint64:
			if tv > math.MaxInt64 {
				return 0, errors.Errorf("uint64 value out of range for pql.Decimal: %d", tv)
			}
			dec := pql.NewDecimal(int64(tv), 0)
			value = decimalToInt64(dec, opt)
		case int64:
			dec := pql.NewDecimal(tv, 0)
			value = decimalToInt64(dec, opt)
		case pql.Decimal:
			value = decimalToInt64(tv, opt)
		case float64:
			value = int64(tv * math.Pow10(int(opt.Scale)))
		default:
			return 0, errors.Errorf("unexpected decimal value type %T, val %v", tv, tv)
		}
	} else if opt.Type == FieldTypeTimestamp {
		switch tv := v.(type) {
		case time.Time:
			v := TimestampToVal(f.options.TimeUnit, tv)
			value = v
		case int64:
			value = tv
		default:
			return 0, errors.Errorf("unexpected timestamp value type %T, val %v", tv, tv)
		}
	} else {
		switch tv := v.(type) {
		case int64:
			value = tv
		case uint64:
			value = int64(tv)
		default:
			return 0, errors.Errorf("unexpected value type %T, val %v", tv, tv)
		}
	}
	return value, nil
}

func decimalToInt64(dec pql.Decimal, opt FieldOptions) int64 {
	scale := opt.Scale
	if dec.GreaterThanOrEqualTo(opt.Min) && dec.LessThanOrEqualTo(opt.Max) {
		return dec.ToInt64(scale)
	} else if dec.LessThan(opt.Min) {
		value := opt.Min.ToInt64(scale)
		if value != math.MinInt64 {
			value--
		}
		return value
	} else if dec.GreaterThan(opt.Max) {
		value := opt.Max.ToInt64(scale)
		if value != math.MaxInt64 {
			value++
		}
		return value
	}

	return 0
}

// executeDeleteRecords executes a delete() call.
func (e *executor) executeDeleteRecords(ctx context.Context, qcx *Qcx, index string, c *pql.Call, shards []uint64, opt *ExecOptions) (bool, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "executor.executeDelete")
	defer span.Finish()

	if len(c.Children) == 0 {
		return false, errors.New("Delete() requires an input bitmap")
	} else if len(c.Children) > 1 {
		return false, errors.New("Delete() only accepts a single bitmap input")
	}
	qcx.Abort()
	qcx.Reset() // release the qcx to allow for rbf checkpoint

	// Execute calls in bulk on each remote node and merge.
	mapFn := func(ctx context.Context, shard uint64, mopt *mapOptions) (_ interface{}, err error) {
		return e.executeDeleteRecordFromShard(ctx, index, c.Children[0], shard)
	}

	// Merge returned results at coordinating node.
	reduceFn := func(ctx context.Context, prev, v interface{}) interface{} {
		other, _ := prev.(bool)
		return other || v.(bool)
	}

	result, err := e.mapReduce(ctx, index, shards, c, opt, mapFn, reduceFn)
	if err != nil {
		return false, err
	}
	n, _ := result.(bool)

	return n, nil
}

func transactExistRow(ctx context.Context, idx *Index, shard uint64, frag *fragment, src *Row) (uint64, error) {
	holder := idx.Holder()
	tx := holder.Txf().NewTx(Txo{Write: writable, Index: idx, Shard: shard})
	rows, err := frag.rows(ctx, tx, 1)
	if err != nil {
		tx.Rollback()
		return 0, err

	}
	// obtain a rowID which is higher than any currently present row ID.
	rowID := uint64(1)
	if len(rows) > 0 {
		rowID = rows[len(rows)-1] + 1
	}
	_, err = frag.setRow(tx, src, rowID)
	if err != nil {
		tx.Rollback()
		return 0, err
	}
	return rowID, tx.Commit()
}

func (e *executor) executeDeleteRecordFromShard(ctx context.Context, index string, bmCall *pql.Call, shard uint64) (changed bool, err error) {
	span, _ := tracing.StartSpanFromContext(ctx, "Executor.executeDeleteRecordFromShard")
	defer span.Finish()
	// Fetch index.
	idx := e.Holder.Index(index)
	if idx == nil {
		err = newNotFoundError(ErrIndexNotFound, index)
		return
	}
	qcx := e.Holder.Txf().NewQcx()
	// bmCall is a bitmap
	row, err := e.executeBitmapCallShard(ctx, qcx, index, bmCall, shard)
	qcx.Abort()
	if err != nil {
		return false, err
	}
	if len(row.Segments) == 0 {
		return
	}
	columns := row.Segments[0].data
	if columns.Count() == 0 {
		return
	}
	src := NewRowFromBitmap(columns)
	return DeleteRowsWithFlow(ctx, src, idx, shard, true)
}

func DeleteRows(ctx context.Context, src *Row, idx *Index, shard uint64) (bool, error) {
	return DeleteRowsWithFlow(ctx, src, idx, shard, false)
}

func DeleteRowsWithFlowWithKeys(ctx context.Context, columns *roaring.Bitmap, idx *Index, shard uint64, normalFlow bool) (bool, error) {
	var existenceFragment *fragment
	var deletedRowID uint64
	var commitor Commitor = &NopCommitor{}
	var err error // store columns in exits field ToBeDelete row commited
	holder := idx.Holder()
	if normalFlow { // normalFlow is the standard path, "not normal" is recoverory
		existenceFragment = holder.fragment(idx.Name(), existenceFieldName, viewStandard, shard)
		if existenceFragment == nil {
			// no exists field
			return false, errors.New("can't bulk delete without existence field")
		}
		src := NewRowFromBitmap(columns)
		deletedRowID, err = transactExistRow(ctx, idx, shard, existenceFragment, src)
		if err != nil {
			return false, err
		}
	}
	commitor, err = deleteKeyTranslation(ctx, idx, shard, columns)
	if err != nil {
		return false, err
	}
	writeTx := holder.Txf().NewTx(Txo{Write: writable, Index: idx, Shard: shard})
	if err != nil {
		return false, err
	}
	defer writeTx.Rollback()
	changed := false
	defer func() {
		// if there is an error on the bit clearing rollback the keys
		if err != nil {
			changed = false
			commitor.Rollback()
			return
		}
		// if there is an error in the key commit, then rollback the delete
		// write records before keys to remove possiblity of unmatch keys=records
		err = writeTx.Commit()
		if err != nil {
			changed = false
			commitor.Rollback()
			return
		}
		if er := commitor.Commit(); er != nil {
			err = er
		}
		if err != nil {
			holder.Logger.Errorf("problems committing delete in rbf %v shard %v", err, shard)
		}
	}()

	for _, field := range idx.Fields() {
		for _, view := range field.views() {
			frag := view.Fragment(shard)
			if frag == nil {
				continue
			}
			c, err := frag.clearRecordsByBitmap(writeTx, columns)
			if err != nil {
				return false, err
			}
			if c {
				changed = true
			}
		}
	}
	if existenceFragment != nil { // a string keys have been deleted and the deleteRow was created
		if normalFlow {
			existenceFragment.clearRow(writeTx, deletedRowID)
		} else {
			// this is if we are recovering from failure and cleaning up
			rows, err := existenceFragment.rows(ctx, writeTx, 1)
			if err != nil {
				return false, err
			}
			for _, rowId := range rows {
				existenceFragment.clearRow(writeTx, rowId)
			}
		}
	}
	return changed, nil
}

func DeleteRowsWithOutKeysFlow(ctx context.Context, columns *roaring.Bitmap, idx *Index, shard uint64, normalFlow bool) (changed bool, err error) {
	var existenceFragment *fragment
	var deletedRowID uint64
	var commitor Commitor = &NopCommitor{}
	holder := idx.Holder()
	writeTx := holder.Txf().NewTx(Txo{Write: writable, Index: idx, Shard: shard})
	defer writeTx.Rollback()
	defer func() {
		// if there is an error in the key commit, then rollback the delete
		// write records before keys to remove possiblity of unmatch keys=records
		err := writeTx.Commit()
		if err != nil {
			changed = false
			commitor.Rollback()
			return
		}
	}()
	for _, field := range idx.Fields() {
		for _, view := range field.views() {

			frag := view.Fragment(shard)
			if frag == nil {
				continue
			}
			c, err := frag.clearRecordsByBitmap(writeTx, columns)
			if err != nil {
				return false, err
			}
			if c {
				changed = true
			}

		}
	}
	if existenceFragment == nil { // a string keys have been deleted and the deleteRow was created
		return changed, nil
	}

	if normalFlow {
		existenceFragment.clearRow(writeTx, deletedRowID)
		return changed, nil
	}

	// this is if we are recovering from failure and cleaning up
	rows, err := existenceFragment.rows(ctx, writeTx, 1)
	if err != nil {
		return false, err
	}
	for _, rowId := range rows {
		existenceFragment.clearRow(writeTx, rowId)
	}
	return changed, nil
}

func DeleteRowsWithFlow(ctx context.Context, src *Row, idx *Index, shard uint64, normalFlow bool) (change bool, err error) {
	if len(src.Segments) == 0 { // nothing to remove
		return false, nil
	}
	columns := src.Segments[0].data // should only be one segment
	if columns.Count() == 0 {
		return false, nil
	}
	bits := src.Segments[0].data.Slice()
	min := func(a, b int) int {
		if a <= b {
			return a
		}
		return b
	}
	limit := idx.holder.cfg.RBFConfig.MaxDelete
	for i := 0; i < len(bits); i += limit {
		batch := roaring.NewBitmap(bits[i:min(i+limit, len(bits))]...)
		if idx.Keys() {
			change, err = DeleteRowsWithFlowWithKeys(ctx, batch, idx, shard, normalFlow)
		} else {
			change, err = DeleteRowsWithOutKeysFlow(ctx, batch, idx, shard, normalFlow)
		}
		if err != nil {
			return change, err
		}
	}
	return change, err
}

type Commitor interface {
	Rollback()
	Commit() error
}
type NopCommitor struct{}

func (c *NopCommitor) Rollback() {
}

func (c *NopCommitor) Commit() error {
	return nil
}

func deleteKeyTranslation(ctx context.Context, idx *Index, shard uint64, records *roaring.Bitmap) (Commitor, error) {
	// ShardToShardParition ...
	paritionID := disco.ShardToShardPartition(idx.name, shard, idx.holder.partitionN)

	return idx.TranslateStore(paritionID).Delete(records)
}

func (e *executor) executeSort(ctx context.Context, qcx *Qcx, index string, c *pql.Call, shards []uint64, opt *ExecOptions) (*SortedRow, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "Executor.executeSort")
	defer span.Finish()

	sort_desc, _, err := c.BoolArg("sort-desc")
	if err != nil {
		return nil, errors.Wrap(err, " getting sort-desc")
	}

	mapFn := func(ctx context.Context, shard uint64, mopt *mapOptions) (_ interface{}, err error) {
		return e.executeSortShard(ctx, qcx, index, c, shard)
	}

	reduceFn := func(ctx context.Context, prev, v interface{}) interface{} {
		if err := ctx.Err(); err != nil {
			return err
		}
		// var result *SortedRow
		switch other := prev.(type) {
		case *SortedRow:
			if err := other.Merge(v.(*SortedRow), sort_desc); err != nil {
				return err
			} else {
				return other
			}
		case *Row:
			other.Merge(v.(*Row))
			return other
		case nil:
			if v == nil {
				return nil
			}
			if out, ok := v.(*SortedRow); ok {
				return out
			}
			return v.(*Row)
		default:
			return errors.Errorf("unexpected return type from executeSortShard: %+v %T", other, other)
		}
	}

	res, err := e.mapReduce(ctx, index, shards, c, opt, mapFn, reduceFn)
	if err != nil {
		return nil, errors.Wrap(err, "mapReduce")
	}

	offset, hasOffset, err := c.UintArg("offset")
	if err != nil {
		return nil, err
	}
	limit, hasLimit, err := c.UintArg("limit")
	if err != nil {
		return nil, err
	}
	result := res.(*SortedRow)
	if hasOffset {
		result.RowKVs = result.RowKVs[offset:]
		result.Row = NewRow(result.Columns()...)
	}
	if hasLimit && limit < result.Row.Count() {
		result.RowKVs = result.RowKVs[:limit]
		result.Row = NewRow(result.Columns()...)
	}
	return result, nil
}

func (e *executor) executeSortShard(ctx context.Context, qcx *Qcx, index string, c *pql.Call, shard uint64) (*SortedRow, error) {
	var filter *Row
	if len(c.Children) == 1 {
		row, err := e.executeBitmapCallShard(ctx, qcx, index, c.Children[0], shard)
		if err != nil {
			return nil, err
		}
		filter = row
	}

	idx := e.Holder.Index(index)
	if idx == nil {
		return nil, newNotFoundError(ErrIndexNotFound, index)
	}

	fieldName, err := c.FirstStringArg("field", "_field")
	if err != nil {
		return nil, errors.Wrap(err, "getting field")
	}
	f := idx.Field(fieldName)
	if f == nil {
		return nil, newNotFoundError(ErrFieldNotFound, fieldName)
	}

	tx, finisher, err := qcx.GetTx(Txo{Write: !writable, Index: idx, Shard: shard})
	if err != nil {
		return nil, ErrQcxDone
	}
	defer finisher(&err)

	sort_desc, _, err := c.BoolArg("sort-desc")
	if err != nil {
		return nil, errors.Wrap(err, " getting sort-desc")
	}

	switch f.Type() {
	case FieldTypeBool:
		fragment := e.Holder.fragment(index, f.name, viewStandard, shard)
		if fragment == nil {
			return nil, errors.New("bool fragment not found")
		}
		falses, err := fragment.row(tx, falseRowID)
		if err != nil {
			return nil, errors.New("error loading false from fragment")
		}
		trues, err := fragment.row(tx, trueRowID)
		if err != nil {
			return nil, errors.New("error loading true from fragment")
		}
		falses = filter.Intersect(falses)
		trues = filter.Intersect(trues)
		rowKVs := make([]RowKV, len(filter.Columns()))
		var i int
		if sort_desc {
			for _, col := range trues.Columns() {
				rowKVs[i] = RowKV{
					RowID: col,
					Value: true,
				}
				i++
			}
			for _, col := range falses.Columns() {
				rowKVs[i] = RowKV{
					RowID: col,
					Value: false,
				}
				i++
			}
		} else {
			for _, col := range falses.Columns() {
				rowKVs[i] = RowKV{
					RowID: col,
					Value: false,
				}
				i++
			}
			for _, col := range trues.Columns() {
				rowKVs[i] = RowKV{
					RowID: col,
					Value: true,
				}
				i++
			}
		}
		return &SortedRow{
			Row:    filter,
			RowKVs: rowKVs,
		}, nil
	case FieldTypeDecimal, FieldTypeInt, FieldTypeTimestamp:
		return f.SortShardRow(tx, shard, filter, sort_desc)
	case FieldTypeMutex:
		fragment := e.Holder.fragment(index, f.name, viewStandard, shard)
		if fragment == nil {
			return nil, errors.Errorf("fragment not found for field %s", f.name)
		}
		rows, err := fragment.rows(ctx, tx, 0)
		if err != nil {
			return nil, errors.Wrap(err, " ggettign rows error")
		}
		rowKVs := make([]RowKV, filter.Count())
		i := 0
		for _, rowID := range rows {
			row, err := fragment.row(tx, rowID)
			if err != nil {
				return nil, errors.Wrap(err, "couldn't load row from fragment")
			}
			row = row.Intersect(filter)
			if row.Count() > 0 {
				if f.Keys() {
					if rowStr, err := f.translateStore.TranslateID(rowID); err != nil {
						return nil, errors.Wrap(err, "error getting translateIDs")
					} else {
						for _, v := range row.Columns() {
							rowKVs[i] = RowKV{
								RowID: v,
								Value: rowStr,
							}
							i++
						}
					}
				} else {
					for _, v := range row.Columns() {
						rowKVs[i] = RowKV{
							RowID: v,
							Value: rowID,
						}
						i++
					}
				}
			}
		}
		// this is to make sure compare function worked.
		ok := true
		sort.SliceStable(rowKVs, func(i, j int) bool {
			if c, k := rowKVs[i].Compare(rowKVs[j], sort_desc); k {
				return c
			} else {
				ok = false
				return !k
			}
		})
		if !ok {
			return nil, errors.New("could not compare values for sort")
		}
		return &SortedRow{
			Row:    filter,
			RowKVs: rowKVs,
		}, nil
	default:
		return nil, errors.Errorf("Sort of field type %s not implemented yet", f.Type())
	}
}

type SortedRow struct {
	Row    *Row
	RowKVs []RowKV
}

func (s *SortedRow) Columns() []uint64 {
	out := make([]uint64, len(s.RowKVs))
	for i, rkv := range s.RowKVs {
		out[i] = rkv.RowID
	}
	return out
}

func (s *SortedRow) ToRows(callback func(*proto.RowResponse) error) error {
	ci := []*proto.ColumnInfo{{Name: "_id", Datatype: "int64"}}

	for _, kvs := range s.RowKVs {
		val, err := toInt64(kvs.RowID)
		if err != nil {
			return errors.Wrap(err, "converting uint64 to int64 (positive)")
		}
		if err := callback(&proto.RowResponse{
			Headers: ci,
			Columns: []*proto.ColumnResponse{
				{ColumnVal: &proto.ColumnResponse_Int64Val{Int64Val: val}},
			},
		}); err != nil {
			return errors.Wrap(err, "calling callback")
		}
		ci = nil
	}
	return nil
}

func (s *SortedRow) Merge(o *SortedRow, sort_desc bool) error {
	rowKVs := make([]RowKV, len(s.RowKVs)+len(o.RowKVs))

	i, j, k := 0, 0, 0
	for i < len(s.RowKVs) && j < len(o.RowKVs) {
		if c, ok := s.RowKVs[i].Compare(o.RowKVs[j], sort_desc); ok {
			if c {
				rowKVs[k] = s.RowKVs[i]
				i++
			} else {
				rowKVs[k] = o.RowKVs[j]
				j++
			}
			k++
		} else {
			return errors.Errorf("Coultn't compare %v and %v", s.RowKVs[i], o.RowKVs[j])
		}
	}

	for i < len(s.RowKVs) {
		rowKVs[k] = s.RowKVs[i]
		i++
		k++
	}

	for j < len(o.RowKVs) {
		rowKVs[k] = o.RowKVs[j]
		j++
		k++
	}

	s.RowKVs = rowKVs
	s.Row.Merge(o.Row)
	return nil
}

type ExtractedIDMatrixSorted struct {
	ExtractedIDMatrix *ExtractedIDMatrix
	RowKVs            []RowKV
}

func MergeExtractedIDMatrixSorted(a, b ExtractedIDMatrixSorted, sort_desc bool) (ExtractedIDMatrixSorted, error) {
	columns := make([]ExtractedIDColumn, len(a.ExtractedIDMatrix.Columns)+len(b.ExtractedIDMatrix.Columns))
	rowKVs := make([]RowKV, len(a.RowKVs)+len(b.RowKVs))
	i, j, k := 0, 0, 0
	for i < len(a.ExtractedIDMatrix.Columns) && j < len(b.ExtractedIDMatrix.Columns) {
		if c, ok := a.RowKVs[i].Compare(b.RowKVs[j], sort_desc); ok {
			if c {
				columns[k] = a.ExtractedIDMatrix.Columns[i]
				rowKVs[k] = a.RowKVs[i]
				i++
			} else {
				columns[k] = b.ExtractedIDMatrix.Columns[j]
				rowKVs[k] = b.RowKVs[j]
				j++
			}
			k++
		} else {
			return ExtractedIDMatrixSorted{}, errors.Errorf("error comparing %v and %v", a.RowKVs[i], b.RowKVs[j])
		}
	}
	for i < len(a.ExtractedIDMatrix.Columns) {
		columns[k] = a.ExtractedIDMatrix.Columns[i]
		rowKVs[k] = a.RowKVs[i]
		i++
		k++
	}
	for j < len(b.ExtractedIDMatrix.Columns) {
		columns[k] = b.ExtractedIDMatrix.Columns[j]
		rowKVs[k] = b.RowKVs[j]
		j++
		k++
	}
	if a.ExtractedIDMatrix.Fields == nil {
		a.ExtractedIDMatrix.Fields = b.ExtractedIDMatrix.Fields
	}
	return ExtractedIDMatrixSorted{
		ExtractedIDMatrix: &ExtractedIDMatrix{
			Columns: columns,
			Fields:  a.ExtractedIDMatrix.Fields,
		},
		RowKVs: rowKVs,
	}, nil
}
