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
	"encoding/json"
	"fmt"
	"math"
	"math/bits"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/pilosa/pilosa/v2/pql"
	pb "github.com/pilosa/pilosa/v2/proto"
	"github.com/pilosa/pilosa/v2/roaring"
	"github.com/pilosa/pilosa/v2/shardwidth"
	"github.com/pilosa/pilosa/v2/tracing"
	"github.com/pkg/errors"
)

// defaultField is the field used if one is not specified.
const (
	defaultField = "general"

	// defaultMinThreshold is the lowest count to use in a Top-N operation when
	// looking for additional id/count pairs.
	defaultMinThreshold = 1

	columnLabel = "col"
	rowLabel    = "row"
)

// executor recursively executes calls in a PQL query across all shards.
type executor struct {
	Holder *Holder

	// Local hostname & cluster configuration.
	Node    *Node
	Cluster *cluster

	// Client used for remote requests.
	client InternalQueryClient

	// Maximum number of Set() or Clear() commands per request.
	MaxWritesPerRequest int

	shutdown       bool
	workMu         sync.RWMutex
	workersWG      sync.WaitGroup
	workerPoolSize int
	work           chan job
}

// executorOption is a functional option type for pilosa.Executor
type executorOption func(e *executor) error

func optExecutorInternalQueryClient(c InternalQueryClient) executorOption {
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

// newExecutor returns a new instance of Executor.
func newExecutor(opts ...executorOption) *executor {
	e := &executor{
		client:         newNopInternalQueryClient(),
		workerPoolSize: 2,
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
	for i := 0; i < e.workerPoolSize; i++ {
		e.workersWG.Add(1)
		go func() {
			defer e.workersWG.Done()
			worker(e.work)
		}()
	}
	return e
}

func (e *executor) Close() error {
	e.workMu.Lock()
	defer e.workMu.Unlock()
	e.shutdown = true
	close(e.work)
	e.workersWG.Wait()
	return nil
}

// Execute executes a PQL query.
func (e *executor) Execute(ctx context.Context, index string, q *pql.Query, shards []uint64, opt *execOptions) (QueryResponse, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "Executor.Execute")
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
		return resp, ErrIndexNotFound
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
		opt = &execOptions{}
	}

	// Translate query keys to ids, if necessary.
	// No need to translate a remote call.
	if !opt.Remote {
		if err := e.translateCalls(ctx, index, q.Calls); err != nil {
			return resp, err
		} else if err := validateQueryContext(ctx); err != nil {
			return resp, err
		}
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

	tx, err := e.Holder.BeginTx(needWriteTxn, idx)
	if err != nil {
		return resp, err
	}
	defer tx.Rollback()

	results, err := e.execute(ctx, tx, index, q, shards, opt)
	if err != nil {
		return resp, err
	} else if err := validateQueryContext(ctx); err != nil {
		return resp, err
	}
	resp.Results = results

	// Fill column attributes if requested.
	if opt.ColumnAttrs {
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
		columnAttrSets, err := e.readColumnAttrSets(e.Holder.Index(index), columnIDs)
		if err != nil {
			return resp, errors.Wrap(err, "reading column attrs")
		}

		// Translate column attributes, if necessary.
		if idx.Keys() {
			idSet := make(map[uint64]struct{})
			for _, col := range columnAttrSets {
				idSet[col.ID] = struct{}{}
			}

			idMap, err := e.Cluster.translateIndexIDSet(ctx, index, idSet)
			if err != nil {
				return resp, errors.Wrap(err, "translating id set")
			}

			for _, col := range columnAttrSets {
				col.Key, col.ID = idMap[col.ID], 0
			}
		}

		resp.ColumnAttrSets = columnAttrSets
	}

	// Translate response objects from ids to keys, if necessary.
	// No need to translate a remote call.
	if !opt.Remote {
		// only translateResults if this local node is the final destination. only string/column keys.
		if err := e.translateResults(ctx, index, idx, q.Calls, results); err != nil {
			return resp, err
		} else if err := validateQueryContext(ctx); err != nil {
			return resp, err
		}
	}
	// Must copy out of Tx data before Commiting, because it will become invalid afterwards.
	respSafeNoTxData := e.safeCopy(resp)

	// Commit transaction if writing; else let the defer Rollback have it.
	if needWriteTxn {
		if err := tx.Commit(); err != nil {
			return respSafeNoTxData, err
		}
	}
	return respSafeNoTxData, nil
}

// safeCopy copies everything in resp that has Bitmap material,
// to avoid anything coming from the mmap-ed Tx storage.
func (e *executor) safeCopy(resp QueryResponse) (out QueryResponse) {
	out = QueryResponse{
		// not transactional, from attribute storage so no need to clone these:
		ColumnAttrSets: resp.ColumnAttrSets, // []*ColumnAttrSet
		Err:            resp.Err,            //  error
		Profile:        resp.Profile,        //  *tracing.Profile
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
		default:
			panic(fmt.Sprintf("handle %T here", v))
		}
	}
	return
}

// readColumnAttrSets returns a list of column attribute objects by id.
func (e *executor) readColumnAttrSets(index *Index, ids []uint64) ([]*ColumnAttrSet, error) {
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

// handlePreCalls traverses the call tree looking for calls that need
// precomputed values. Right now, that's just Distinct.
func (e *executor) handlePreCalls(ctx context.Context, tx Tx, index string, c *pql.Call, shards []uint64, opt *execOptions) error {
	if c.Name == "Precomputed" {
		idx := c.Args["valueidx"].(int64)
		if idx >= 0 && idx < int64(len(opt.EmbeddedData)) {
			row := opt.EmbeddedData[idx]
			c.Precomputed = make(map[uint64]interface{}, len(row.segments))
			for _, segment := range row.segments {
				c.Precomputed[segment.shard] = &Row{segments: []rowSegment{segment}}
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
	if err := e.handlePreCallChildren(ctx, tx, index, c, shards, opt); err != nil {
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
	v, err := e.executeCall(ctx, tx, index, c, shards, opt)
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
	c.Precomputed = make(map[uint64]interface{}, len(row.segments))
	for _, segment := range row.segments {
		c.Precomputed[segment.shard] = &Row{segments: []rowSegment{segment}}
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
func (e *executor) handlePreCallChildren(ctx context.Context, tx Tx, index string, c *pql.Call, shards []uint64, opt *execOptions) error {
	for i := range c.Children {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := e.handlePreCalls(ctx, tx, index, c.Children[i], shards, opt); err != nil {
			return err
		}
	}
	for _, val := range c.Args {
		// Handle Call() operations which exist inside named arguments, too.
		if call, ok := val.(*pql.Call); ok {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := e.handlePreCalls(ctx, tx, index, call, shards, opt); err != nil {
				return err
			}
		}
	}
	return nil
}

func (e *executor) execute(ctx context.Context, tx Tx, index string, q *pql.Query, shards []uint64, opt *execOptions) ([]interface{}, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "Executor.execute")
	defer span.Finish()

	// Don't bother calculating shards for query types that don't require it.
	needsShards := needsShards(q.Calls)

	// If shards are specified, then use that value for shards. If shards aren't
	// specified, then include all of them.
	if len(shards) == 0 && needsShards {
		// Round up the number of shards.
		idx := e.Holder.Index(index)
		if idx == nil {
			return nil, ErrIndexNotFound
		}
		shards = idx.AvailableShards().Slice()
		if len(shards) == 0 {
			shards = []uint64{0}
		}
	}

	// Optimize handling for bulk attribute insertion.
	if hasOnlySetRowAttrs(q.Calls) {
		return e.executeBulkSetRowAttrs(ctx, tx, index, q.Calls, opt)
	}

	// Execute each call serially.
	results := make([]interface{}, 0, len(q.Calls))
	for i, call := range q.Calls {

		if err := validateQueryContext(ctx); err != nil {
			return nil, err
		}

		// If you actually make a top-level Distinct call, you
		// want a SignedRow back. Otherwise, it's something else
		// that will be using it as a row, and we only care
		// about the positive values, because only positive values
		// are valid column IDs. So we don't actually eat top-level
		// pre calls.
		err := e.handlePreCallChildren(ctx, tx, index, call, shards, opt)
		if err != nil {
			return nil, err
		}
		var v interface{}
		// Top-level calls don't need to precompute cross-index things,
		// because we can just pick whatever index we want, but we
		// still need to handle them. Since everything else was
		// already precomputed by handlePreCallChildren, though,
		// we don't need this logic in executeCall.
		newIndex := call.CallIndex()
		if newIndex != "" && newIndex != index {
			v, err = e.executeCall(ctx, tx, newIndex, call, nil, opt)
		} else {
			v, err = e.executeCall(ctx, tx, index, call, shards, opt)
		}
		if err != nil {
			return nil, err
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

// preprocessQuery expands any calls that need preprocessing.
func (e *executor) preprocessQuery(ctx context.Context, tx Tx, index string, c *pql.Call, shards []uint64, opt *execOptions) (*pql.Call, error) {
	switch c.Name {
	case "UnionRows":
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
			rowsResult, err := e.executeCall(ctx, tx, index, child, shards, opt)
			if err != nil {
				return nil, err
			}

			// Turn the results into rows calls.
			var resultRows []*pql.Call
			switch rowsResult := rowsResult.(type) {
			case *PairsField:
				// Translate pairs into rows calls.
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
				// Translate Row IDs into Row calls.
				for _, id := range rowsResult {
					resultRows = append(resultRows, &pql.Call{
						Name: "Row",
						Args: map[string]interface{}{
							child.Args["_field"].(string): id,
						},
					})
				}
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
		return &pql.Call{
			Name:     "Union",
			Children: rows,
		}, nil

	case "ConstRow":
		// Fetch user-provided columns list.
		cols, _ := c.Args["columns"].([]interface{})
		var ids []uint64
		var keys []string
		for _, c := range cols {
			switch c := c.(type) {
			case uint64:
				ids = append(ids, c)
			case int64:
				ids = append(ids, uint64(c))
			case string:
				keys = append(keys, c)
			default:
				return nil, errors.Errorf("invalid column identifier %v of type %T", c, c)
			}
		}

		// Translate keys to IDs.
		if len(keys) > 0 {
			keyIDs, err := e.Cluster.translateIndexKeys(ctx, index, keys)
			if err != nil {
				return nil, errors.Wrap(err, "translating column IDs in ConstRow")
			}
			ids = append(ids, keyIDs...)
		}

		// Split IDs by shard.
		shardSet := make(map[uint64][]uint64)
		for _, id := range ids {
			shardSet[id/ShardWidth] = append(shardSet[id/ShardWidth], id)
		}

		// Convert ID sets to per-shard Row objects.
		precomputed := make(map[uint64]interface{})
		for _, s := range shards {
			precomputed[s] = NewRow(shardSet[s]...)
		}

		// Generate a precomputed call with the data.
		return &pql.Call{
			Name:        "Precomputed",
			Precomputed: precomputed,
		}, nil

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
		fallthrough

	case "Limit":
		if len(c.Children) != 1 {
			return nil, errors.Errorf("expected 1 child of limit call but got %d", len(c.Children))
		}
		res, err := e.preprocessQuery(ctx, tx, index, c.Children[0], shards, opt)
		if err != nil {
			return nil, err
		}
		c.Children[0] = res
		err = e.executeLimitCall(ctx, tx, index, c, shards, opt)
		if err != nil {
			return nil, err
		}
		return c, nil

	default:
		// Recurse through child calls.
		out := make([]*pql.Call, len(c.Children))
		var changed bool
		for i, child := range c.Children {
			res, err := e.preprocessQuery(ctx, tx, index, child, shards, opt)
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
func (e *executor) executeCall(ctx context.Context, tx Tx, index string, c *pql.Call, shards []uint64, opt *execOptions) (interface{}, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "Executor.executeCall")
	defer span.Finish()

	if err := validateQueryContext(ctx); err != nil {
		return nil, err
	} else if err := e.validateCallArgs(c); err != nil {
		return nil, errors.Wrap(err, "validating args")
	}
	indexTag := "index:" + index
	metricName := "query_" + strings.ToLower(c.Name) + "_total"
	statFn := func() {
		if !opt.Remote {
			e.Holder.Stats.CountWithCustomTags(metricName, 1, 1.0, []string{indexTag})
		}
	}

	// Fixes #2009
	// See: https://github.com/pilosa/pilosa/issues/2009
	// TODO: Remove at version 2.0
	if e.detectRangeCall(c) {
		e.Holder.Logger.Printf("DEPRECATED: Range() is deprecated, please use Row() instead.")
	}

	// If shards are specified, then use that value for shards. If shards aren't
	// specified, then include all of them.
	if shards == nil && needsShards([]*pql.Call{c}) {
		// Round up the number of shards.
		idx := e.Holder.Index(index)
		if idx == nil {
			return nil, ErrIndexNotFound
		}
		shards = idx.AvailableShards().Slice()
		if len(shards) == 0 {
			shards = []uint64{0}
		}
	}

	// Preprocess the query.
	c, err := e.preprocessQuery(ctx, tx, index, c, shards, opt)
	if err != nil {
		return nil, err
	}

	switch c.Name {
	case "Sum":
		statFn()
		return e.executeSum(ctx, tx, index, c, shards, opt)
	case "Min":
		statFn()
		return e.executeMin(ctx, tx, index, c, shards, opt)
	case "Max":
		statFn()
		return e.executeMax(ctx, tx, index, c, shards, opt)
	case "MinRow":
		statFn()
		return e.executeMinRow(ctx, tx, index, c, shards, opt)
	case "MaxRow":
		statFn()
		return e.executeMaxRow(ctx, tx, index, c, shards, opt)
	case "Clear":
		statFn()
		return e.executeClearBit(ctx, tx, index, c, opt)
	case "ClearRow":
		statFn()
		return e.executeClearRow(ctx, tx, index, c, shards, opt)
	case "Distinct":
		statFn()
		return e.executeDistinct(ctx, tx, index, c, shards, opt)
	case "Store":
		statFn()
		return e.executeSetRow(ctx, tx, index, c, shards, opt)
	case "Count":
		statFn()
		return e.executeCount(ctx, tx, index, c, shards, opt)
	case "Set":
		statFn()
		return e.executeSet(ctx, tx, index, c, opt)
	case "SetRowAttrs":
		statFn()
		return nil, e.executeSetRowAttrs(ctx, tx, index, c, opt)
	case "SetColumnAttrs":
		statFn()
		return nil, e.executeSetColumnAttrs(ctx, tx, index, c, opt)
	case "TopN":
		statFn()
		return e.executeTopN(ctx, tx, index, c, shards, opt)
	case "Rows":
		statFn()
		return e.executeRows(ctx, tx, index, c, shards, opt)
	case "Extract":
		statFn()
		return e.executeExtract(ctx, tx, index, c, shards, opt)
	case "GroupBy":
		statFn()
		return e.executeGroupBy(ctx, tx, index, c, shards, opt)
	case "Options":
		statFn()
		return e.executeOptionsCall(ctx, tx, index, c, shards, opt)
	case "IncludesColumn":
		return e.executeIncludesColumnCall(ctx, tx, index, c, shards, opt)
	case "FieldValue":
		statFn()
		return e.executeFieldValueCall(ctx, tx, index, c, shards, opt)
	case "Precomputed":
		return e.executePrecomputedCall(ctx, tx, index, c, shards, opt)
	default: // e.g. "Row", "Union", "Intersect" or anything that returns a bitmap.
		statFn()
		return e.executeBitmapCall(ctx, tx, index, c, shards, opt)
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

func (e *executor) executeOptionsCall(ctx context.Context, tx Tx, index string, c *pql.Call, shards []uint64, opt *execOptions) (interface{}, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "Executor.executeOptionsCall")
	defer span.Finish()

	optCopy := &execOptions{}
	*optCopy = *opt
	if arg, ok := c.Args["columnAttrs"]; ok {
		if value, ok := arg.(bool); ok {
			opt.ColumnAttrs = value
		} else {
			return nil, errors.New("Query(): columnAttrs must be a bool")
		}
	}
	if arg, ok := c.Args["excludeRowAttrs"]; ok {
		if value, ok := arg.(bool); ok {
			optCopy.ExcludeRowAttrs = value
		} else {
			return nil, errors.New("Query(): excludeRowAttrs must be a bool")
		}
	}
	if arg, ok := c.Args["excludeColumns"]; ok {
		if value, ok := arg.(bool); ok {
			optCopy.ExcludeColumns = value
		} else {
			return nil, errors.New("Query(): excludeColumns must be a bool")
		}
	}
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
	return e.executeCall(ctx, tx, index, c.Children[0], shards, optCopy)
}

// executeIncludesColumnCall executes an IncludesColumn() call.
func (e *executor) executeIncludesColumnCall(ctx context.Context, tx Tx, index string, c *pql.Call, shards []uint64, opt *execOptions) (bool, error) {
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
	mapFn := func(ctx context.Context, shard uint64) (interface{}, error) {
		return e.executeIncludesColumnCallShard(ctx, tx, index, c, shard, col)
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
func (e *executor) executeFieldValueCall(ctx context.Context, tx Tx, index string, c *pql.Call, shards []uint64, opt *execOptions) (ValCount, error) {
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
		return ValCount{}, ErrIndexNotFound
	}

	// Fetch field.
	field := idx.Field(fieldName)
	if field == nil {
		return ValCount{}, ErrFieldNotFound
	}

	var colID uint64
	if key, ok := colKey.(string); ok && idx.Keys() {
		id, err := e.Cluster.translateIndexKey(ctx, index, key)
		if err != nil {
			return ValCount{}, errors.Wrap(err, "getting column id")
		}
		colID = id
	} else {
		id, ok, err := c.UintArg("column")
		if !ok || err != nil {
			return ValCount{}, errors.Wrap(err, "getting column argument")
		}
		colID = id
	}

	shard := colID / ShardWidth

	// Execute calls in bulk on each remote node and merge.
	mapFn := func(ctx context.Context, shard uint64) (interface{}, error) {
		return e.executeFieldValueCallShard(ctx, tx, field, colID, shard)
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

func (e *executor) executeFieldValueCallShard(ctx context.Context, tx Tx, field *Field, col uint64, shard uint64) (ValCount, error) {
	value, exists, err := field.Value(tx, col)
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
		other.DecimalVal = &pql.Decimal{
			Value: value,
			Scale: field.Options().Scale}
		other.FloatVal = 0
		other.Val = 0
	}

	return other, nil
}

// executeLimitCall executes a Limit() call, **rewriting it to a precomputed call**.
func (e *executor) executeLimitCall(ctx context.Context, tx Tx, index string, c *pql.Call, shards []uint64, opt *execOptions) error {
	bitmapCall := c.Children[0]

	limit, hasLimit, err := c.UintArg("limit")
	if err != nil {
		return errors.Wrap(err, "getting limit")
	}
	offset, _, err := c.UintArg("offset")
	if err != nil {
		return errors.Wrap(err, "getting offset")
	}

	if !hasLimit {
		limit = math.MaxUint64
	}

	// skip tracks the number of records left to be skipped
	// in support of getting to the offset.
	var skip uint64 = offset

	// got tracks the number of records gotten to that point.
	var got uint64

	c.Precomputed = make(map[uint64]interface{})

	for _, shard := range shards {
		// Execute calls in bulk on each remote node and merge.
		mapFn := func(ctx context.Context, shard uint64) (interface{}, error) {
			return e.executeBitmapCallShard(ctx, tx, index, bitmapCall, shard)
		}

		// Merge returned results at coordinating node.
		reduceFn := func(ctx context.Context, prev, v interface{}) interface{} {
			if err := ctx.Err(); err != nil {
				return err
			}
			other, _ := prev.(*Row)
			if other == nil {
				other = NewRow()
			}
			other.Merge(v.(*Row))
			return other
		}

		result, err := e.mapReduce(ctx, index, []uint64{shard}, c, opt, mapFn, reduceFn)
		if err != nil {
			return errors.Wrap(err, "limit map reduce")
		}

		row, _ := result.(*Row)

		segCnt := row.Count()

		// If this segment doesn't reach the offset, skip it.
		if segCnt <= skip {
			skip -= segCnt
			continue
		}

		// This segment doesn't have enough to finish fulfilling the limit
		// (or it has exactly enough).
		if segCnt-skip <= limit-got {
			if skip == 0 {
				c.Precomputed[shard] = row
			} else {
				cols := row.Columns()
				partialRow := NewRow()
				for _, bit := range cols[skip:] {
					partialRow.SetBit(bit)
				}
				c.Precomputed[shard] = partialRow
			}
			got += segCnt - skip
			// In the case where this segment exactly fulfills the limit, break.
			if got == limit {
				break
			}
			skip = 0
			continue
		}

		// This segment has more records than the remaining limit requires.
		cols := row.Columns()
		partialRow := NewRow()
		for _, bit := range cols[skip : skip+limit-got] {
			partialRow.SetBit(bit)
		}
		c.Precomputed[shard] = partialRow
		break
	}

	c.Name = "Precomputed"
	return nil
}

// executeIncludesColumnCallShard
func (e *executor) executeIncludesColumnCallShard(ctx context.Context, tx Tx, index string, c *pql.Call, shard uint64, column uint64) (bool, error) {

	span, ctx := tracing.StartSpanFromContext(ctx, "Executor.executeIncludesColumnCallShard")
	defer span.Finish()

	if len(c.Children) == 1 {
		row, err := e.executeBitmapCallShard(ctx, tx, index, c.Children[0], shard)
		if err != nil {
			return false, errors.Wrap(err, "executing bitmap call")
		}
		return row.Includes(column), nil
	}

	return false, errors.New("IncludesColumn call must specify a row query")
}

// executeSum executes a Sum() call.
func (e *executor) executeSum(ctx context.Context, tx Tx, index string, c *pql.Call, shards []uint64, opt *execOptions) (ValCount, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "Executor.executeSum")
	defer span.Finish()

	fieldName, ok := c.Args["field"].(string)
	if !ok || fieldName == "" {
		return ValCount{}, errors.New("Sum(): field required")
	}

	if len(c.Children) > 1 {
		return ValCount{}, errors.New("Sum() only accepts a single bitmap input")
	}

	// Execute calls in bulk on each remote node and merge.
	mapFn := func(ctx context.Context, shard uint64) (interface{}, error) {
		return e.executeSumCountShard(ctx, tx, index, c, nil, shard)
	}

	// Merge returned results at coordinating node.
	reduceFn := func(ctx context.Context, prev, v interface{}) interface{} {
		other, _ := prev.(ValCount)
		return other.add(v.(ValCount))
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
			return ValCount{}, ErrFieldNotFound
		}
		if field.Type() == FieldTypeDecimal {
			other.DecimalVal = &pql.Decimal{
				Value: other.Val,
				Scale: field.Options().Scale}
			other.FloatVal = 0
			other.Val = 0
		}
	}

	return other, nil
}

// executeDistinct executes a Distinct call on a field.
func (e *executor) executeDistinct(ctx context.Context, tx Tx, index string, c *pql.Call, shards []uint64, opt *execOptions) (SignedRow, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "Executor.executeDistinct")
	defer span.Finish()

	field := c.Args["field"]
	if field == "" {
		return SignedRow{}, fmt.Errorf("plugin operation %s(): field required", c.Name)
	}

	// Execute calls in bulk on each remote node and merge.
	mapFn := func(ctx context.Context, shard uint64) (interface{}, error) {
		return e.executeDistinctShard(ctx, tx, index, c, shard)
	}

	// Merge returned results at coordinating node.
	reduceFn := func(ctx context.Context, prev, v interface{}) interface{} {
		other, _ := prev.(SignedRow)
		if err := ctx.Err(); err != nil {
			return err
		}
		return other.union(v.(SignedRow))
	}

	result, err := e.mapReduce(ctx, index, shards, c, opt, mapFn, reduceFn)
	if err != nil {
		return SignedRow{}, err
	}
	other, _ := result.(SignedRow)
	other.field = field.(string)

	return other, nil
}

// executeMin executes a Min() call.
func (e *executor) executeMin(ctx context.Context, tx Tx, index string, c *pql.Call, shards []uint64, opt *execOptions) (ValCount, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "Executor.executeMin")
	defer span.Finish()
	if field := c.Args["field"]; field == "" {
		return ValCount{}, errors.New("Min(): field required")
	}

	if len(c.Children) > 1 {
		return ValCount{}, errors.New("Min() only accepts a single bitmap input")
	}

	// Execute calls in bulk on each remote node and merge.
	mapFn := func(ctx context.Context, shard uint64) (interface{}, error) {
		return e.executeMinShard(ctx, tx, index, c, shard)
	}

	// Merge returned results at coordinating node.
	reduceFn := func(ctx context.Context, prev, v interface{}) interface{} {
		other, _ := prev.(ValCount)
		return other.smaller(v.(ValCount))
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
func (e *executor) executeMax(ctx context.Context, tx Tx, index string, c *pql.Call, shards []uint64, opt *execOptions) (ValCount, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "Executor.executeMax")
	defer span.Finish()

	if field := c.Args["field"]; field == "" {
		return ValCount{}, errors.New("Max(): field required")
	}

	if len(c.Children) > 1 {
		return ValCount{}, errors.New("Max() only accepts a single bitmap input")
	}

	// Execute calls in bulk on each remote node and merge.
	mapFn := func(ctx context.Context, shard uint64) (interface{}, error) {
		return e.executeMaxShard(ctx, tx, index, c, shard)
	}

	// Merge returned results at coordinating node.
	reduceFn := func(ctx context.Context, prev, v interface{}) interface{} {
		other, _ := prev.(ValCount)
		return other.larger(v.(ValCount))
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

// executeMinRow executes a MinRow() call.
func (e *executor) executeMinRow(ctx context.Context, tx Tx, index string, c *pql.Call, shards []uint64, opt *execOptions) (interface{}, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "Executor.executeMinRow")
	defer span.Finish()

	if field := c.Args["field"]; field == "" {
		return ValCount{}, errors.New("MinRow(): field required")
	}

	// Execute calls in bulk on each remote node and merge.
	mapFn := func(ctx context.Context, shard uint64) (interface{}, error) {
		return e.executeMinRowShard(ctx, tx, index, c, shard)
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
func (e *executor) executeMaxRow(ctx context.Context, tx Tx, index string, c *pql.Call, shards []uint64, opt *execOptions) (interface{}, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "Executor.executeMaxRow")
	defer span.Finish()

	if field := c.Args["field"]; field == "" {
		return ValCount{}, errors.New("MaxRow(): field required")
	}

	// Execute calls in bulk on each remote node and merge.
	mapFn := func(ctx context.Context, shard uint64) (interface{}, error) {
		return e.executeMaxRowShard(ctx, tx, index, c, shard)
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
func (e *executor) executePrecomputedCall(ctx context.Context, tx Tx, index string, c *pql.Call, shards []uint64, opt *execOptions) (*Row, error) {
	span, _ := tracing.StartSpanFromContext(ctx, "Executor.executePrecomputedCall")
	defer span.Finish()
	result := NewRow()

	for _, row := range c.Precomputed {
		result.Merge(row.(*Row))
	}
	return result, nil
}

// executeBitmapCall executes a call that returns a bitmap.
func (e *executor) executeBitmapCall(ctx context.Context, tx Tx, index string, c *pql.Call, shards []uint64, opt *execOptions) (*Row, error) {

	span, ctx := tracing.StartSpanFromContext(ctx, "Executor.executeBitmapCall")
	span.LogKV("pqlCallName", c.Name)
	defer span.Finish()

	indexTag := "index:" + index
	metricName := "query_" + strings.ToLower(c.Name) + "_total"
	if c.Name == "Row" && c.HasConditionArg() {
		metricName = "query_row_bsi_total"
	}
	if !opt.Remote {
		e.Holder.Stats.CountWithCustomTags(metricName, 1, 1.0, []string{indexTag})
	}

	// Execute calls in bulk on each remote node and merge.
	mapFn := func(ctx context.Context, shard uint64) (interface{}, error) {
		return e.executeBitmapCallShard(ctx, tx, index, c, shard)
	}

	// Merge returned results at coordinating node.
	reduceFn := func(ctx context.Context, prev, v interface{}) interface{} {
		other, _ := prev.(*Row)
		if other == nil {

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

	// Attach attributes for non-BSI Row() calls.
	// If the column label is used then return column attributes.
	// If the row label is used then return bitmap attributes.
	row, _ := other.(*Row)
	if c.Name == "Row" && !c.HasConditionArg() {
		if opt.ExcludeRowAttrs {
			row.Attrs = map[string]interface{}{}
		} else {
			idx := e.Holder.Index(index)
			if idx != nil {
				if columnID, ok, err := c.UintArg("_" + columnLabel); ok && err == nil {
					attrs, err := idx.ColumnAttrStore().Attrs(columnID)
					if err != nil {
						return nil, errors.Wrap(err, "getting column attrs")
					}
					row.Attrs = attrs
				} else if err != nil {
					return nil, err
				} else {
					// field, _ := c.Args["field"].(string)
					fieldName, _ := c.FieldArg()
					if fr := idx.Field(fieldName); fr != nil {
						rowID, _, err := c.UintArg(fieldName)
						if err != nil {
							return nil, errors.Wrap(err, "getting row")
						}
						attrs, err := fr.RowAttrStore().Attrs(rowID)
						if err != nil {
							return nil, errors.Wrap(err, "getting row attrs")
						}
						row.Attrs = attrs
					}
				}
			}
		}
	}

	if opt.ExcludeColumns {
		row.segments = []rowSegment{}
	}

	return row, nil
}

// executeBitmapCallShard executes a bitmap call for a single shard.
func (e *executor) executeBitmapCallShard(ctx context.Context, tx Tx, index string, c *pql.Call, shard uint64) (*Row, error) {
	if err := validateQueryContext(ctx); err != nil {
		return nil, err
	}

	span, ctx := tracing.StartSpanFromContext(ctx, "Executor.executeBitmapCallShard")
	defer span.Finish()

	switch c.Name {
	case "Row", "Range":
		return e.executeRowShard(ctx, tx, index, c, shard)
	case "Difference":
		return e.executeDifferenceShard(ctx, tx, index, c, shard)
	case "Intersect":
		return e.executeIntersectShard(ctx, tx, index, c, shard)
	case "Union":
		return e.executeUnionShard(ctx, tx, index, c, shard)
	case "Xor":
		return e.executeXorShard(ctx, tx, index, c, shard)
	case "Not":
		return e.executeNotShard(ctx, tx, index, c, shard)
	case "Shift":
		return e.executeShiftShard(ctx, tx, index, c, shard)
	case "All": // Allow a shard computation to use All()
		return e.executeAllCallShard(ctx, tx, index, c, shard)
	case "Distinct":
		return nil, errors.New("Distinct shouldn't be hit as a bitmap call")
	case "Precomputed":
		return e.executePrecomputedCallShard(ctx, tx, index, c, shard)
	default:
		return nil, fmt.Errorf("unknown call: %s", c.Name)
	}
}

// executeDistinctShard executes a Distinct call on a single shard, yielding
// a SignedRow of the values found.
func (e *executor) executeDistinctShard(ctx context.Context, tx Tx, index string, c *pql.Call, shard uint64) (result SignedRow, err error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "Executor.executeDistinctShard")
	defer span.Finish()

	var filter *Row
	var filterBitmap *roaring.Bitmap
	if len(c.Children) == 1 {
		row, err := e.executeBitmapCallShard(ctx, tx, index, c.Children[0], shard)
		if err != nil {
			return result, errors.Wrap(err, "executing bitmap call")
		}
		filter = row
		if filter != nil && len(filter.segments) > 0 {
			filterBitmap = filter.segments[0].data
		} else {
			filterBitmap = roaring.NewFileBitmap()
		}
	}

	fieldName, _ := c.Args["field"].(string)

	field := e.Holder.Field(index, fieldName)
	if field == nil {
		return result, nil
	}

	bsig := field.bsiGroup(fieldName)
	if bsig == nil {
		return result, nil
	}
	view := viewBSIGroupPrefix + fieldName

	depth := uint64(bsig.BitDepth)
	offset := bsig.Base

	existsBitmap, err := tx.OffsetRange(index, fieldName, view, shard, 0, ShardWidth*0, ShardWidth*1)
	if err != nil {
		return result, err
	}
	if filter != nil {
		existsBitmap = existsBitmap.Intersect(filterBitmap)
	}
	if !existsBitmap.Any() {
		return result, nil
	}

	signBitmap, err := tx.OffsetRange(index, fieldName, view, shard, 0, ShardWidth*1, ShardWidth*2)
	if err != nil {
		return result, nil
	}

	dataBitmaps := make([]*roaring.Bitmap, depth)

	for i := uint64(0); i < depth; i++ {
		dataBitmaps[i], err = tx.OffsetRange(index, fieldName, view, shard, 0, ShardWidth*(i+2), ShardWidth*(i+3))
		if err != nil {
			return result, err
		}
	}

	// we need spaces for sign bit, existence/filter bit, and data
	// row bits, which we'll be grabbing 65k bits at a time
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
	return SignedRow{
		Neg: NewRowFromBitmap(negBitmap),
		Pos: NewRowFromBitmap(posBitmap),
	}, nil
}

// executeSumCountShard calculates the sum and count for bsiGroups on a shard.
func (e *executor) executeSumCountShard(ctx context.Context, tx Tx, index string, c *pql.Call, filter *Row, shard uint64) (ValCount, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "Executor.executeSumCountShard")
	defer span.Finish()

	// Only calculate the filter if it doesn't exist and a child call as been passed in.
	if filter == nil && len(c.Children) == 1 {
		row, err := e.executeBitmapCallShard(ctx, tx, index, c.Children[0], shard)
		if err != nil {
			return ValCount{}, errors.Wrap(err, "executing bitmap call")
		}
		filter = row
	}

	fieldName, _ := c.Args["field"].(string)

	field := e.Holder.Field(index, fieldName)
	if field == nil {
		return ValCount{}, nil
	}

	bsig := field.bsiGroup(fieldName)
	if bsig == nil {
		return ValCount{}, nil
	}

	fragment := e.Holder.fragment(index, fieldName, viewBSIGroupPrefix+fieldName, shard)
	if fragment == nil {
		return ValCount{}, nil
	}

	sumspan, _ := tracing.StartSpanFromContext(ctx, "Executor.executeSumCountShard_fragment.sum")
	defer sumspan.Finish()
	vsum, vcount, err := fragment.sum(tx, filter, bsig.BitDepth)
	if err != nil {
		return ValCount{}, errors.Wrap(err, "computing sum")
	}
	return ValCount{
		Val:   int64(vsum) + (int64(vcount) * bsig.Base),
		Count: int64(vcount),
	}, nil
}

// executeMinShard calculates the min for bsiGroups on a shard.
func (e *executor) executeMinShard(ctx context.Context, tx Tx, index string, c *pql.Call, shard uint64) (ValCount, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "Executor.executeMinShard")
	defer span.Finish()

	var filter *Row
	if len(c.Children) == 1 {
		row, err := e.executeBitmapCallShard(ctx, tx, index, c.Children[0], shard)
		if err != nil {
			return ValCount{}, err
		}
		filter = row
	}

	fieldName, _ := c.Args["field"].(string)

	field := e.Holder.Field(index, fieldName)
	if field == nil {
		return ValCount{}, nil
	}

	return field.MinForShard(tx, shard, filter)
}

// executeMaxShard calculates the max for bsiGroups on a shard.
func (e *executor) executeMaxShard(ctx context.Context, tx Tx, index string, c *pql.Call, shard uint64) (ValCount, error) {
	var filter *Row
	if len(c.Children) == 1 {
		row, err := e.executeBitmapCallShard(ctx, tx, index, c.Children[0], shard)
		if err != nil {
			return ValCount{}, err
		}
		filter = row
	}

	fieldName, _ := c.Args["field"].(string)

	field := e.Holder.Field(index, fieldName)
	if field == nil {
		return ValCount{}, nil
	}

	return field.MaxForShard(tx, shard, filter)
}

// executeMinRowShard returns the minimum row ID for a shard.
func (e *executor) executeMinRowShard(ctx context.Context, tx Tx, index string, c *pql.Call, shard uint64) (PairField, error) {
	var filter *Row
	if len(c.Children) == 1 {
		row, err := e.executeBitmapCallShard(ctx, tx, index, c.Children[0], shard)
		if err != nil {
			return PairField{}, err
		}
		filter = row
	}

	fieldName, _ := c.Args["field"].(string)
	field := e.Holder.Field(index, fieldName)
	if field == nil {
		return PairField{}, nil
	}

	fragment := e.Holder.fragment(index, fieldName, viewStandard, shard)
	if fragment == nil {
		return PairField{}, nil
	}

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
func (e *executor) executeMaxRowShard(ctx context.Context, tx Tx, index string, c *pql.Call, shard uint64) (PairField, error) {
	var filter *Row
	if len(c.Children) == 1 {
		row, err := e.executeBitmapCallShard(ctx, tx, index, c.Children[0], shard)
		if err != nil {
			return PairField{}, err
		}
		filter = row
	}

	fieldName, _ := c.Args["field"].(string)
	field := e.Holder.Field(index, fieldName)
	if field == nil {
		return PairField{}, nil
	}

	fragment := e.Holder.fragment(index, fieldName, viewStandard, shard)
	if fragment == nil {
		return PairField{}, nil
	}

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

// executeTopN executes a TopN() call.
// This first performs the TopN() to determine the top results and then
// requeries to retrieve the full counts for each of the top results.
func (e *executor) executeTopN(ctx context.Context, tx Tx, index string, c *pql.Call, shards []uint64, opt *execOptions) (*PairsField, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "Executor.executeTopN")
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
	pairs, err := e.executeTopNShards(ctx, tx, index, c, shards, opt)
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

	trimmedList, err := e.executeTopNShards(ctx, tx, index, other, shards, opt)
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

func (e *executor) executeTopNShards(ctx context.Context, tx Tx, index string, c *pql.Call, shards []uint64, opt *execOptions) (*PairsField, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "Executor.executeTopNShards")
	defer span.Finish()

	// Execute calls in bulk on each remote node and merge.
	mapFn := func(ctx context.Context, shard uint64) (interface{}, error) {
		return e.executeTopNShard(ctx, tx, index, c, shard)
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
func (e *executor) executeTopNShard(ctx context.Context, tx Tx, index string, c *pql.Call, shard uint64) (*PairsField, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "Executor.executeTopNShard")
	defer span.Finish()

	fieldName, _ := c.Args["_field"].(string)
	n, _, err := c.UintArg("n")
	if err != nil {
		return nil, fmt.Errorf("executeTopNShard: %v", err)
	} else if f := e.Holder.Field(index, fieldName); f != nil && (f.Type() == FieldTypeInt || f.Type() == FieldTypeDecimal) {
		return nil, fmt.Errorf("cannot compute TopN() on integer field: %q", fieldName)
	}

	attrName, _ := c.Args["attrName"].(string)
	rowIDs, _, err := c.UintSliceArg("ids")
	if err != nil {
		return nil, fmt.Errorf("executeTopNShard: %v", err)
	}
	minThreshold, _, err := c.UintArg("threshold")
	if err != nil {
		return nil, fmt.Errorf("executeTopNShard: %v", err)
	}
	attrValues, _ := c.Args["attrValues"].([]interface{})
	tanimotoThreshold, _, err := c.UintArg("tanimotoThreshold")
	if err != nil {
		return nil, fmt.Errorf("executeTopNShard: %v", err)
	}

	// Retrieve bitmap used to intersect.
	var src *Row
	if len(c.Children) == 1 {
		row, err := e.executeBitmapCallShard(ctx, tx, index, c.Children[0], shard)
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
	pairs, err := f.top(tx, topOptions{
		N:                 int(n),
		Src:               src,
		RowIDs:            rowIDs,
		FilterName:        attrName,
		FilterValues:      attrValues,
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
func (e *executor) executeDifferenceShard(ctx context.Context, tx Tx, index string, c *pql.Call, shard uint64) (*Row, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "Executor.executeDifferenceShard")
	defer span.Finish()

	var other *Row
	if len(c.Children) == 0 {
		return nil, fmt.Errorf("empty Difference query is currently not supported")
	}
	for i, input := range c.Children {
		row, err := e.executeBitmapCallShard(ctx, tx, index, input, shard)
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
	field string
}

func (r *RowIdentifiers) Clone() (clone *RowIdentifiers) {
	clone = &RowIdentifiers{
		field: r.field,
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
func (r RowIdentifiers) ToTable() (*pb.TableResponse, error) {
	var n int
	if len(r.Keys) > 0 {
		n = len(r.Keys)
	} else {
		n = len(r.Rows)
	}
	return pb.RowsToTable(&r, n)
}

// ToRows implements the ToRowser interface.
func (r RowIdentifiers) ToRows(callback func(*pb.RowResponse) error) error {
	if len(r.Keys) > 0 {
		ci := []*pb.ColumnInfo{{Name: r.Field(), Datatype: "string"}}
		for _, key := range r.Keys {
			if err := callback(&pb.RowResponse{
				Headers: ci,
				Columns: []*pb.ColumnResponse{
					&pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_StringVal{StringVal: key}},
				}}); err != nil {
				return errors.Wrap(err, "calling callback")
			}
			ci = nil
		}
	} else {
		ci := []*pb.ColumnInfo{{Name: r.Field(), Datatype: "uint64"}}
		for _, id := range r.Rows {
			if err := callback(&pb.RowResponse{
				Headers: ci,
				Columns: []*pb.ColumnResponse{
					&pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_Uint64Val{Uint64Val: uint64(id)}},
				}}); err != nil {
				return errors.Wrap(err, "calling callback")
			}
			ci = nil
		}
	}
	return nil
}

// Field returns the field name associated to the row.
func (r *RowIdentifiers) Field() string {
	return r.field
}

// RowIDs is a query return type for just uint64 row ids.
// It should only be used internally (since RowIdentifiers
// is the external return type), but it is exported because
// the proto package needs access to it.
type RowIDs []uint64

func (r RowIDs) merge(other RowIDs, limit int) RowIDs {
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

func (e *executor) executeGroupBy(ctx context.Context, tx Tx, index string, c *pql.Call, shards []uint64, opt *execOptions) ([]GroupCount, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "Executor.executeGroupBy")
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

	idx := e.Holder.Index(index)
	if idx == nil {
		return nil, ErrIndexNotFound
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
		fieldName, ok := child.Args["_field"].(string)
		if !ok {
			return nil, errors.Errorf("%s call must have field with valid (string) field name. Got %v of type %[2]T", child.Name, child.Args["_field"])
		}
		f := idx.Field(fieldName)
		if f == nil {
			return nil, ErrFieldNotFound
		}
		switch f.Type() {
		case FieldTypeInt:
			bases[i] = f.bsiGroup(f.name).Base
		}

		if hasLimit || hasCol { // we need to perform this query cluster-wide ahead of executeGroupByShard
			childRows[i], err = e.executeRows(ctx, tx, index, child, shards, opt)
			if err != nil {
				return nil, errors.Wrap(err, "getting rows for ")
			}
			if len(childRows[i]) == 0 { // there are no results because this field has no values.
				return []GroupCount{}, nil
			}
		}
	}

	// Execute calls in bulk on each remote node and merge.
	mapFn := func(ctx context.Context, shard uint64) (interface{}, error) {
		return e.executeGroupByShard(ctx, tx, index, c, filter, shard, childRows, bases)
	}
	// Merge returned results at coordinating node.
	reduceFn := func(ctx context.Context, prev, v interface{}) interface{} {
		other, _ := prev.([]GroupCount)
		if err := ctx.Err(); err != nil {
			return err
		}
		return mergeGroupCounts(other, v.([]GroupCount), limit)
	}
	// Get full result set.
	other, err := e.mapReduce(ctx, index, shards, c, opt, mapFn, reduceFn)
	if err != nil {
		return nil, err
	}
	results, _ := other.([]GroupCount)

	// Apply having.
	if having, hasHaving, err := c.CallArg("having"); err != nil {
		return nil, err
	} else if hasHaving {
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
				results = applyConditionToGroupCounts(results, subj, cond.(*pql.Condition))
			default:
				return nil, errors.New("Condition() only supports count or sum")
			}
		}
	}

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
	Field  string `json:"field"`
	RowID  uint64 `json:"rowID"`
	RowKey string `json:"rowKey,omitempty"`
	Value  *int64 `json:"value,omitempty"`
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
	return
}

// MarshalJSON marshals FieldRow to JSON such that
// either a Key or an ID is included.
func (fr FieldRow) MarshalJSON() ([]byte, error) {
	if fr.Value != nil {
		return json.Marshal(struct {
			Field string `json:"field"`
			Value int64  `json:"value"`
		}{
			Field: fr.Field,
			Value: *fr.Value,
		})
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

// GroupCounts is a list of GroupCount.
type GroupCounts []GroupCount

// ToTable implements the ToTabler interface.
func (g GroupCounts) ToTable() (*pb.TableResponse, error) {
	return pb.RowsToTable(&g, len(g))
}

// ToRows implements the ToRowser interface.
func (g GroupCounts) ToRows(callback func(*pb.RowResponse) error) error {
	for i, gc := range g {
		var ci []*pb.ColumnInfo
		if i == 0 {
			for _, fieldRow := range gc.Group {
				if fieldRow.RowKey != "" {
					ci = append(ci, &pb.ColumnInfo{Name: fieldRow.Field, Datatype: "string"})
				} else if fieldRow.Value != nil {
					ci = append(ci, &pb.ColumnInfo{Name: fieldRow.Field, Datatype: "int64"})
				} else {
					ci = append(ci, &pb.ColumnInfo{Name: fieldRow.Field, Datatype: "uint64"})
				}
			}
			ci = append(ci, &pb.ColumnInfo{Name: "count", Datatype: "uint64"})
			ci = append(ci, &pb.ColumnInfo{Name: "sum", Datatype: "int64"})
		}
		rowResp := &pb.RowResponse{
			Headers: ci,
			Columns: []*pb.ColumnResponse{},
		}

		for _, fieldRow := range gc.Group {
			if fieldRow.RowKey != "" {
				rowResp.Columns = append(rowResp.Columns, &pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_StringVal{StringVal: fieldRow.RowKey}})
			} else if fieldRow.Value != nil {
				rowResp.Columns = append(rowResp.Columns, &pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_Int64Val{Int64Val: *fieldRow.Value}})
			} else {
				rowResp.Columns = append(rowResp.Columns, &pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_Uint64Val{Uint64Val: fieldRow.RowID}})
			}
		}
		rowResp.Columns = append(rowResp.Columns,
			&pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_Uint64Val{Uint64Val: gc.Count}},
			&pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_Int64Val{Int64Val: gc.Sum}},
		)
		if err := callback(rowResp); err != nil {
			return errors.Wrap(err, "calling callback")
		}
	}
	return nil
}

// GroupCount represents a result item for a group by query.
type GroupCount struct {
	Group []FieldRow `json:"group"`
	Count uint64     `json:"count"`
	Sum   int64      `json:"sum"`
}

func (g *GroupCount) Clone() (r *GroupCount) {
	r = &GroupCount{
		Group: make([]FieldRow, len(g.Group)),
		Count: g.Count,
		Sum:   g.Sum,
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
			a[i].Sum += b[j].Sum
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
				if g.Sum == val {
					return true
				}
			} else if cond.Op == pql.NEQ {
				if g.Sum != val {
					return true
				}
			} else if cond.Op == pql.LT {
				if g.Sum < val {
					return true
				}
			} else if cond.Op == pql.LTE {
				if g.Sum <= val {
					return true
				}
			} else if cond.Op == pql.GT {
				if g.Sum > val {
					return true
				}
			} else if cond.Op == pql.GTE {
				if g.Sum >= val {
					return true
				}
			}
		case pql.BETWEEN, pql.BTWN_LT_LTE, pql.BTWN_LTE_LT, pql.BTWN_LT_LT:
			val, ok := cond.Int64SliceValue()
			if !ok {
				return false
			}
			if cond.Op == pql.BETWEEN {
				if val[0] <= g.Sum && g.Sum <= val[1] {
					return true
				}
			} else if cond.Op == pql.BTWN_LT_LTE {
				if val[0] < g.Sum && g.Sum <= val[1] {
					return true
				}
			} else if cond.Op == pql.BTWN_LTE_LT {
				if val[0] <= g.Sum && g.Sum < val[1] {
					return true
				}
			} else if cond.Op == pql.BTWN_LT_LT {
				if val[0] < g.Sum && g.Sum < val[1] {
					return true
				}
			}
		}
	}
	return false
}

// applyConditionToGroupCounts filters the contents of gcs according
// to the condition. Currently, `count` and `sum` are the only
// fields supported.
func applyConditionToGroupCounts(gcs []GroupCount, subj string, cond *pql.Condition) []GroupCount {
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

func (e *executor) executeGroupByShard(ctx context.Context, tx Tx, index string, c *pql.Call, filter *pql.Call, shard uint64, childRows []RowIDs, bases map[int]int64) (_ []GroupCount, err error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "Executor.executeGroupByShard")
	defer span.Finish()

	var filterRow *Row
	if filter != nil {
		if filterRow, err = e.executeBitmapCallShard(ctx, tx, index, filter, shard); err != nil {
			return nil, errors.Wrapf(err, "executing group by filter for shard %d", shard)
		}
	}

	aggregate, _, err := c.CallArg("aggregate")
	if err != nil {
		return nil, err
	}

	newspan, ctx := tracing.StartSpanFromContext(ctx, "Executor.executeGroupByShard_newGroupByIterator")
	iter, err := newGroupByIterator(e, tx, childRows, c.Children, aggregate, filterRow, index, shard, e.Holder)
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
	} else if hasLimit {
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
	for i, base := range bases {
		for _, r := range results {
			*r.Group[i].Value += base
		}
	}

	return results, nil
}

func (e *executor) executeRows(ctx context.Context, tx Tx, index string, c *pql.Call, shards []uint64, opt *execOptions) (RowIDs, error) {
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

	// Execute calls in bulk on each remote node and merge.
	mapFn := func(ctx context.Context, shard uint64) (interface{}, error) {
		return e.executeRowsShard(ctx, tx, index, fieldName, c, shard)
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
		return other.merge(v.(RowIDs), limit)
	}
	// Get full result set.
	other, err := e.mapReduce(ctx, index, shards, c, opt, mapFn, reduceFn)
	if err != nil {
		return nil, err
	}
	results, _ := other.(RowIDs)
	return results, nil
}

func (e *executor) executeRowsShard(ctx context.Context, tx Tx, index string, fieldName string, c *pql.Call, shard uint64) (RowIDs, error) {
	// Fetch index.
	idx := e.Holder.Index(index)
	if idx == nil {
		return nil, ErrIndexNotFound
	}
	// Fetch field.
	f := e.Holder.Field(index, fieldName)
	if f == nil {
		return nil, ErrFieldNotFound
	}

	// rowIDs is the result set.
	var rowIDs RowIDs

	// views contains the list of views to inspect (and merge)
	// in order to represent `Rows` for the field.
	var views = []string{viewStandard}

	// Handle `int` and `time` fields.
	switch f.Type() {
	case FieldTypeInt:
		return nil, errors.New("int fields not supported by Rows() query")
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

		// Calculate the views for a range as long as some piece of the range
		// (from/to) are specified, or if there's no standard view to represent
		// all dates.
		if !fromTime.IsZero() || !toTime.IsZero() || f.options.NoStandardView {
			// If no quantum exists then return an empty result set.
			q := f.TimeQuantum()
			if q == "" {
				return rowIDs, nil
			}

			// Get min/max based on existing views.
			var vs []string
			for _, v := range f.views() {
				vs = append(vs, v.name)
			}
			min, max := minMaxViews(vs, q)

			// If min/max are empty, there were no time views.
			if min == "" || max == "" {
				return rowIDs, nil
			}

			// Convert min/max from string to time.Time.
			minTime, err := timeOfView(min, false)
			if err != nil {
				return rowIDs, errors.Wrapf(err, "getting min time from view: %s", min)
			}
			if fromTime.IsZero() || fromTime.Before(minTime) {
				fromTime = minTime
			}

			maxTime, err := timeOfView(max, true)
			if err != nil {
				return rowIDs, errors.Wrapf(err, "getting max time from view: %s", max)
			}
			if toTime.IsZero() || toTime.After(maxTime) {
				toTime = maxTime
			}

			// Determine the views based on the specified time range.
			views = viewsByTimeRange(viewStandard, fromTime, toTime, q)
		}
	}

	start := uint64(0)
	if previous, ok, err := c.UintArg("previous"); err != nil {
		return nil, errors.Wrap(err, "getting previous")
	} else if ok {
		start = previous + 1
	}

	filters := []rowFilter{}
	if columnID, ok, err := c.UintArg("column"); err != nil {
		return nil, err
	} else if ok {
		colShard := columnID >> shardwidth.Exponent
		if colShard != shard {
			return rowIDs, nil
		}
		filters = append(filters, filterColumn(columnID))
	}

	limit := int(^uint(0) >> 1)
	if lim, hasLimit, err := c.UintArg("limit"); err != nil {
		return nil, errors.Wrap(err, "getting limit")
	} else if hasLimit {
		filters = append(filters, filterWithLimit(lim))
		limit = int(lim)
	}

	var likeErr chan error
	if like, hasLike, err := c.StringArg("like"); err != nil {
		return nil, errors.Wrap(err, "getting like pattern")
	} else if hasLike {
		likeErr = make(chan error, 1)
		filters = append(filters, filterLike(like, f.TranslateStore(), likeErr))
	}

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
		select {
		case err = <-likeErr:
			return nil, err
		default:
		}
		rowIDs = rowIDs.merge(viewRows, limit)
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

func (e *executor) executeExtract(ctx context.Context, tx Tx, index string, c *pql.Call, shards []uint64, opt *execOptions) (ExtractedIDMatrix, error) {
	// Extract the column filter call.
	if len(c.Children) < 1 {
		return ExtractedIDMatrix{}, errors.New("missing column filter in Extract")
	}
	filter := c.Children[0]

	// Extract fields from rows calls.
	fields := make([]string, len(c.Children)-1)
	for i, rows := range c.Children[1:] {
		if rows.Name != "Rows" {
			return ExtractedIDMatrix{}, errors.Errorf("child call of Extract is %q but expected Rows", rows.Name)
		}
		var fieldName string
		var ok bool
		for k, v := range rows.Args {
			switch k {
			case "field", "_field":
				fieldName = v.(string)
				ok = true
			default:
				return ExtractedIDMatrix{}, errors.Errorf("unsupported Rows argument for Extract: %q", k)
			}
		}
		if !ok {
			return ExtractedIDMatrix{}, errors.New("missing field specification in Rows")
		}
		fields[i] = fieldName
	}

	// Execute calls in bulk on each remote node and merge.
	mapFn := func(ctx context.Context, shard uint64) (interface{}, error) {
		return e.executeExtractShard(ctx, tx, index, fields, filter, shard)
	}

	// Merge returned results at coordinating node.
	reduceFn := func(ctx context.Context, prev, v interface{}) interface{} {
		other, _ := prev.(ExtractedIDMatrix)
		if err := ctx.Err(); err != nil {
			return err
		}
		other.Append(v.(ExtractedIDMatrix))
		return other
	}

	// Get full result set.
	other, err := e.mapReduce(ctx, index, shards, c, opt, mapFn, reduceFn)
	if err != nil {
		return ExtractedIDMatrix{}, err
	}
	results, _ := other.(ExtractedIDMatrix)
	sort.Slice(results.Columns, func(i, j int) bool {
		return results.Columns[i].ColumnID < results.Columns[j].ColumnID
	})
	return results, nil
}

func mergeBits(bits *Row, mask uint64, out map[uint64]uint64) {
	for _, v := range bits.Columns() {
		out[v] |= mask
	}
}

var trueRowFakeID = []uint64{1}
var falseRowFakeID = []uint64{0}

func (e *executor) executeExtractShard(ctx context.Context, tx Tx, index string, fields []string, filter *pql.Call, shard uint64) (ExtractedIDMatrix, error) {
	// Execute filter.
	colsBitmap, err := e.executeBitmapCallShard(ctx, tx, index, filter, shard)
	if err != nil {
		return ExtractedIDMatrix{}, errors.Wrap(err, "failed to get extraction column filter")
	}

	// Fetch index.
	idx := e.Holder.Index(index)
	if idx == nil {
		return ExtractedIDMatrix{}, ErrIndexNotFound
	}

	// Decompress columns bitmap.
	cols := colsBitmap.Columns()

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
			return ExtractedIDMatrix{}, ErrFieldNotFound
		}

		switch field.Type() {
		case FieldTypeSet, FieldTypeMutex, FieldTypeTime:
			// Handle a set field by listing the rows and then intersecting them with the filter.

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

		case FieldTypeInt, FieldTypeDecimal:
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
			for i := uint(0); i < bsig.BitDepth; i++ {
				bits, err := fragment.row(tx, bsiOffsetBit+uint64(i))
				if err != nil {
					return ExtractedIDMatrix{}, errors.Wrap(err, "loading BSI significand bit from fragment")
				}
				bits = bits.Intersect(exists)
				mergeBits(bits, 1<<i, data)
			}

			// Store the results back into the matrix.
			for columnID, val := range data {
				// Convert to two's complement.
				val = uint64((2*(int64(val)>>63) + 1) * int64(val&^(1<<63)))

				m[mLookup[columnID]].Rows[i] = []uint64{val}
			}
		}
	}

	// Emit the final matrix.
	// Like RowIDs, this is an internal type and will need to be converted.
	return ExtractedIDMatrix{
		Fields:  fields,
		Columns: m,
	}, nil
}

func (e *executor) executeRowShard(ctx context.Context, tx Tx, index string, c *pql.Call, shard uint64) (*Row, error) {

	span, _ := tracing.StartSpanFromContext(ctx, "Executor.executeRowShard")
	defer span.Finish()

	// Handle bsiGroup ranges differently.
	if c.HasConditionArg() {
		// looks the same on badger/roaring. we think.
		return e.executeRowBSIGroupShard(ctx, tx, index, c, shard)
	}

	// Fetch index.
	idx := e.Holder.Index(index)
	if idx == nil {
		return nil, ErrIndexNotFound
	}

	// Fetch field name from argument.
	fieldName, err := c.FieldArg()
	if err != nil {
		return nil, errors.New("Row() argument required: field")
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

	timeNotSet := fromTime.IsZero() && toTime.IsZero()

	// This is workaround to support pql.ASSIGN ('=') as condition ('==') for int and decimal fields
	if c.Name == "Row" && timeNotSet &&
		(f.Type() == FieldTypeInt || f.Type() == FieldTypeDecimal) {
		// re-write args as conditions for fieldName
		for k, v := range c.Args {
			if _, ok := v.(*pql.Condition); k == fieldName && !ok {
				c.Args[k] = &pql.Condition{
					Op:    pql.EQ,
					Value: v,
				}

				return e.executeRowBSIGroupShard(ctx, tx, index, c, shard)
			}
		}
	}

	rowID, rowOK, rowErr := c.UintArg(fieldName)
	if rowErr != nil {
		return nil, fmt.Errorf("Row() error with arg for row: %v", rowErr)
	} else if !rowOK {
		return nil, fmt.Errorf("Row() must specify %v", rowLabel)
	}

	// Simply return row if times are not set.
	if c.Name == "Row" && timeNotSet {

		frag := e.Holder.fragment(index, fieldName, viewStandard, shard)
		if frag == nil {
			return NewRow(), nil
		}
		return frag.row(tx, rowID)
	}

	// If no quantum exists then return an empty bitmap.
	q := f.TimeQuantum()
	if q == "" {
		return &Row{}, nil
	}

	// Set maximum "to" value if only "from" is set. We don't need to worry
	// about setting the minimum "from" since it is the zero value if omitted.
	if toTime.IsZero() {
		// Set the end timestamp to current time + 1 day, in order to account for timezone differences.
		toTime = time.Now().AddDate(0, 0, 1)
	}

	// Union bitmaps across all time-based views.
	views := viewsByTimeRange(viewStandard, fromTime, toTime, q)
	rows := make([]*Row, 0, len(views))
	for _, view := range views {
		f := e.Holder.fragment(index, fieldName, view, shard)
		if f == nil {
			continue
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
		return rows[0], nil
	}
	row := rows[0].Union(rows[1:]...)
	return row, nil

}

// executeRowBSIGroupShard executes a range(bsiGroup) call for a local shard.
func (e *executor) executeRowBSIGroupShard(ctx context.Context, tx Tx, index string, c *pql.Call, shard uint64) (_ *Row, err error) {

	span, _ := tracing.StartSpanFromContext(ctx, "Executor.executeRowBSIGroupShard")
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
		return nil, ErrFieldNotFound
	}

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
			return nil, ErrIndexNotFound
		} else if idx.existenceField() == nil {
			return nil, errors.Errorf("index does not support existence tracking: %s", index)
		}

		var existenceRow *Row
		existenceFrag := e.Holder.fragment(index, existenceFieldName, viewStandard, shard)
		if existenceFrag == nil {
			existenceRow = NewRow()
		} else {
			if existenceRow, err = existenceFrag.row(tx, 0); err != nil {
				return nil, err
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
func (e *executor) executeIntersectShard(ctx context.Context, tx Tx, index string, c *pql.Call, shard uint64) (*Row, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "Executor.executeIntersectShard")
	defer span.Finish()

	var other *Row
	if len(c.Children) == 0 {
		return nil, fmt.Errorf("empty Intersect query is currently not supported")
	}
	for i, input := range c.Children {
		row, err := e.executeBitmapCallShard(ctx, tx, index, input, shard)
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
func (e *executor) executeUnionShard(ctx context.Context, tx Tx, index string, c *pql.Call, shard uint64) (*Row, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "Executor.executeUnionShard")
	defer span.Finish()

	other := NewRow()
	for i, input := range c.Children {
		row, err := e.executeBitmapCallShard(ctx, tx, index, input, shard)
		if err != nil {
			return nil, err
		}

		if i == 0 {
			other = row
		} else {
			other = other.Union(row)
		}
	}
	other.invalidateCount()
	return other, nil
}

// executeXorShard executes a xor() call for a local shard.
func (e *executor) executeXorShard(ctx context.Context, tx Tx, index string, c *pql.Call, shard uint64) (*Row, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "Executor.executeXorShard")
	defer span.Finish()

	other := NewRow()
	for i, input := range c.Children {
		row, err := e.executeBitmapCallShard(ctx, tx, index, input, shard)
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
func (e *executor) executePrecomputedCallShard(ctx context.Context, tx Tx, index string, c *pql.Call, shard uint64) (*Row, error) {
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
func (e *executor) executeNotShard(ctx context.Context, tx Tx, index string, c *pql.Call, shard uint64) (_ *Row, err error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "Executor.executeNotShard")
	defer span.Finish()

	if len(c.Children) == 0 {
		return nil, errors.New("Not() requires an input row")
	} else if len(c.Children) > 1 {
		return nil, errors.New("Not() only accepts a single row input")
	}

	// Make sure the index supports existence tracking.
	idx := e.Holder.Index(index)
	if idx == nil {
		return nil, ErrIndexNotFound
	} else if idx.existenceField() == nil {
		return nil, errors.Errorf("index does not support existence tracking: %s", index)
	}

	var existenceRow *Row
	existenceFrag := e.Holder.fragment(index, existenceFieldName, viewStandard, shard)
	if existenceFrag == nil {
		existenceRow = NewRow()
	} else {
		if existenceRow, err = existenceFrag.row(tx, 0); err != nil {
			return nil, err
		}
	}

	row, err := e.executeBitmapCallShard(ctx, tx, index, c.Children[0], shard)
	if err != nil {
		return nil, err
	}

	return existenceRow.Difference(row), nil
}

// executeAllCallShard executes an All() call for a local shard.
func (e *executor) executeAllCallShard(ctx context.Context, tx Tx, index string, c *pql.Call, shard uint64) (_ *Row, err error) {
	span, _ := tracing.StartSpanFromContext(ctx, "Executor.executeAllCallShard")
	defer span.Finish()

	if len(c.Children) > 0 {
		return nil, errors.New("All() does not accept an input row")
	}

	// Make sure the index supports existence tracking.
	idx := e.Holder.Index(index)
	if idx == nil {
		return nil, ErrIndexNotFound
	} else if idx.existenceField() == nil {
		return nil, errors.Errorf("index does not support existence tracking: %s", index)
	}

	var existenceRow *Row
	existenceFrag := e.Holder.fragment(index, existenceFieldName, viewStandard, shard)
	if existenceFrag == nil {
		existenceRow = NewRow()
	} else {
		if existenceRow, err = existenceFrag.row(tx, 0); err != nil {
			return nil, err
		}
	}

	return existenceRow, nil
}

// executeShiftShard executes a shift() call for a local shard.
func (e *executor) executeShiftShard(ctx context.Context, tx Tx, index string, c *pql.Call, shard uint64) (*Row, error) {
	n, _, err := c.IntArg("n")
	if err != nil {
		return nil, fmt.Errorf("executeShiftShard: %v", err)
	}

	if len(c.Children) == 0 {
		return nil, errors.New("Shift() requires an input row")
	} else if len(c.Children) > 1 {
		return nil, errors.New("Shift() only accepts a single row input")
	}

	row, err := e.executeBitmapCallShard(ctx, tx, index, c.Children[0], shard)
	if err != nil {
		return nil, err
	}

	return row.Shift(n)
}

// executeCount executes a count() call.
func (e *executor) executeCount(ctx context.Context, tx Tx, index string, c *pql.Call, shards []uint64, opt *execOptions) (uint64, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "Executor.executeCount")
	defer span.Finish()

	if len(c.Children) == 0 {
		return 0, errors.New("Count() requires an input bitmap")
	} else if len(c.Children) > 1 {
		return 0, errors.New("Count() only accepts a single bitmap input")
	}

	// Execute calls in bulk on each remote node and merge.
	mapFn := func(ctx context.Context, shard uint64) (interface{}, error) {
		row, err := e.executeBitmapCallShard(ctx, tx, index, c.Children[0], shard)
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
func (e *executor) executeClearBit(ctx context.Context, tx Tx, index string, c *pql.Call, opt *execOptions) (bool, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "Executor.executeClearBit")
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
		return false, ErrIndexNotFound
	}
	f := idx.Field(fieldName)
	if f == nil {
		return false, ErrFieldNotFound
	}

	// Int field.
	if f.Type() == FieldTypeInt || f.Type() == FieldTypeDecimal {
		return e.executeClearValueField(ctx, tx, index, c, f, colID, opt)
	}

	rowID, ok, err := c.UintArg(fieldName)
	if err != nil {
		return false, fmt.Errorf("reading Clear() row: %v", err)
	} else if !ok {
		return false, fmt.Errorf("row=<row> argument required to Clear() call")
	}

	return e.executeClearBitField(ctx, tx, index, c, f, colID, rowID, opt)
}

// executeClearBitField executes a Clear() call for a field.
func (e *executor) executeClearBitField(ctx context.Context, tx Tx, index string, c *pql.Call, f *Field, colID, rowID uint64, opt *execOptions) (bool, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "Executor.executeClearBitField")
	defer span.Finish()

	shard := colID / ShardWidth
	ret := false
	for _, node := range e.Cluster.shardNodes(index, shard) {
		// Update locally if host matches.
		if node.ID == e.Node.ID {
			val, err := f.ClearBit(tx, rowID, colID)
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
		res, err := e.remoteExec(ctx, node, index, &pql.Query{Calls: []*pql.Call{c}}, nil, nil)
		if err != nil {
			return false, err
		}
		ret = res[0].(bool)
	}
	return ret, nil
}

// executeClearRow executes a ClearRow() call.
func (e *executor) executeClearRow(ctx context.Context, tx Tx, index string, c *pql.Call, shards []uint64, opt *execOptions) (bool, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "Executor.executeClearRow")
	defer span.Finish()

	// Ensure the field type supports ClearRow().
	fieldName, err := c.FieldArg()
	if err != nil {
		return false, errors.New("ClearRow() argument required: field")
	}
	field := e.Holder.Field(index, fieldName)
	if field == nil {
		return false, ErrFieldNotFound
	}

	switch field.Type() {
	case FieldTypeSet, FieldTypeTime, FieldTypeMutex, FieldTypeBool:
		// These field types support ClearRow().
	default:
		return false, fmt.Errorf("ClearRow() is not supported on %s field types", field.Type())
	}

	// Execute calls in bulk on each remote node and merge.
	mapFn := func(ctx context.Context, shard uint64) (interface{}, error) {
		return e.executeClearRowShard(ctx, tx, index, c, shard)
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
func (e *executor) executeClearRowShard(ctx context.Context, tx Tx, index string, c *pql.Call, shard uint64) (bool, error) {
	span, _ := tracing.StartSpanFromContext(ctx, "Executor.executeClearRowShard")
	defer span.Finish()

	fieldName, err := c.FieldArg()
	if err != nil {
		return false, errors.New("ClearRow() argument required: field")
	}

	// Read fields using labels.
	rowID, ok, err := c.UintArg(fieldName)
	if err != nil {
		return false, fmt.Errorf("reading ClearRow() row: %v", err)
	} else if !ok {
		return false, fmt.Errorf("ClearRow() row argument '%v' required", rowLabel)
	}

	field := e.Holder.Field(index, fieldName)
	if field == nil {
		return false, ErrFieldNotFound
	}

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
func (e *executor) executeSetRow(ctx context.Context, tx Tx, indexName string, c *pql.Call, shards []uint64, opt *execOptions) (bool, error) {
	// Ensure the field type supports Store().
	fieldName, err := c.FieldArg()
	if err != nil {
		return false, errors.New("field required for Store()")
	}
	field := e.Holder.Field(indexName, fieldName)
	if field == nil {
		// Find index.
		index := e.Holder.Index(indexName)
		if index == nil {
			return false, newNotFoundError(ErrIndexNotFound)
		}

		// Create field.
		field, err = index.CreateField(fieldName, OptFieldTypeSet(CacheTypeNone, 0))
		if err != nil {
			// We wrap these because we want to indicate that it wasn't found,
			// but also the problem we encountered trying to create it.
			return false, newNotFoundError(errors.Wrap(err, "creating field"))
		}
	}
	if field.Type() != FieldTypeSet {
		return false, fmt.Errorf("can't Store() on a %s field", field.Type())
	}

	// Execute calls in bulk on each remote node and merge.
	mapFn := func(ctx context.Context, shard uint64) (interface{}, error) {
		return e.executeSetRowShard(ctx, tx, indexName, c, shard)
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
func (e *executor) executeSetRowShard(ctx context.Context, tx Tx, index string, c *pql.Call, shard uint64) (bool, error) {
	fieldName, err := c.FieldArg()
	if err != nil {
		return false, errors.New("Store() argument required: field")
	}

	// Read fields using labels.
	rowID, ok, err := c.UintArg(fieldName)
	if err != nil {
		return false, fmt.Errorf("reading Store() row: %v", err)
	} else if !ok {
		return false, fmt.Errorf("need the <FIELD>=<ROW> argument on Store()")
	}

	field := e.Holder.Field(index, fieldName)
	if field == nil {
		return false, ErrFieldNotFound
	}

	// Retrieve source row.
	var src *Row
	if len(c.Children) == 1 {
		row, err := e.executeBitmapCallShard(ctx, tx, index, c.Children[0], shard)
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
	set, err := fragment.setRow(tx, src, rowID)
	if err != nil {
		return false, errors.Wrapf(err, "storing row %d on view %s shard %d", rowID, viewStandard, shard)
	}
	changed = changed || set

	return changed, nil
}

// executeSet executes a Set() call.
func (e *executor) executeSet(ctx context.Context, tx Tx, index string, c *pql.Call, opt *execOptions) (bool, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "Executor.executeSet")
	defer span.Finish()

	// Read colID.
	colID, ok, err := c.UintArg("_" + columnLabel)
	if err != nil {
		return false, fmt.Errorf("reading Set() column: %v", err)
	} else if !ok {
		return false, fmt.Errorf("Set() column argument '%v' required", columnLabel)
	}

	// Read field name.
	fieldName, err := c.FieldArg()
	if err != nil {
		return false, errors.New("Set() argument required: field")
	}

	// Retrieve field.
	idx := e.Holder.Index(index)
	if idx == nil {
		return false, ErrIndexNotFound
	}
	f := idx.Field(fieldName)
	if f == nil {
		return false, ErrFieldNotFound
	}

	// Set column on existence field.
	if ef := idx.existenceField(); ef != nil {
		if _, err := ef.SetBit(tx, 0, colID, nil); err != nil {
			return false, errors.Wrap(err, "setting existence column")
		}
	}

	switch f.Type() {
	case FieldTypeInt, FieldTypeDecimal:
		// Int or Decimal field.
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
		return e.executeSetValueField(ctx, tx, index, c, f, colID, rowVal, opt)

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

		return e.executeSetBitField(ctx, tx, index, c, f, colID, rowID, timestamp, opt)
	}
}

// executeSetBitField executes a Set() call for a specific field.
func (e *executor) executeSetBitField(ctx context.Context, tx Tx, index string, c *pql.Call, f *Field, colID, rowID uint64, timestamp *time.Time, opt *execOptions) (bool, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "Executor.executeSetBitField")
	defer span.Finish()

	shard := colID / ShardWidth
	ret := false

	for _, node := range e.Cluster.shardNodes(index, shard) {
		// Update locally if host matches.
		if node.ID == e.Node.ID {
			val, err := f.SetBit(tx, rowID, colID, timestamp)
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
		res, err := e.remoteExec(ctx, node, index, &pql.Query{Calls: []*pql.Call{c}}, nil, nil)
		if err != nil {
			return false, err
		}
		ret = res[0].(bool)
	}
	return ret, nil
}

// executeSetValueField executes a Set() call for a specific int field.
func (e *executor) executeSetValueField(ctx context.Context, tx Tx, index string, c *pql.Call, f *Field, colID uint64, value int64, opt *execOptions) (bool, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "Executor.executeSetValueField")
	defer span.Finish()

	shard := colID / ShardWidth
	ret := false

	for _, node := range e.Cluster.shardNodes(index, shard) {
		// Update locally if host matches.
		if node.ID == e.Node.ID {
			val, err := f.SetValue(tx, colID, value)
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
		res, err := e.remoteExec(ctx, node, index, &pql.Query{Calls: []*pql.Call{c}}, nil, nil)
		if err != nil {
			return false, err
		}
		ret = res[0].(bool)
	}
	return ret, nil
}

// executeClearValueField removes value for colID if present
func (e *executor) executeClearValueField(ctx context.Context, tx Tx, index string, c *pql.Call, f *Field, colID uint64, opt *execOptions) (bool, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "Executor.executeClearValueField")
	defer span.Finish()

	shard := colID / ShardWidth
	ret := false

	for _, node := range e.Cluster.shardNodes(index, shard) {
		// Update locally if host matches.
		if node.ID == e.Node.ID {
			val, err := f.ClearValue(tx, colID)
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
		res, err := e.remoteExec(ctx, node, index, &pql.Query{Calls: []*pql.Call{c}}, nil, nil)
		if err != nil {
			return false, err
		}
		ret = res[0].(bool)
	}
	return ret, nil
}

// executeSetRowAttrs executes a SetRowAttrs() call.
func (e *executor) executeSetRowAttrs(ctx context.Context, tx Tx, index string, c *pql.Call, opt *execOptions) error {
	span, ctx := tracing.StartSpanFromContext(ctx, "Executor.executeSetRowAttrs")
	defer span.Finish()

	fieldName, ok := c.Args["_field"].(string)
	if !ok {
		return errors.New("SetRowAttrs() field required")
	}

	// Retrieve field.
	field := e.Holder.Field(index, fieldName)
	if field == nil {
		return ErrFieldNotFound
	}

	// Parse labels.
	rowID, ok, err := c.UintArg("_" + rowLabel)
	if err != nil {
		return fmt.Errorf("reading SetRowAttrs() row: %v", err)
	} else if !ok {
		return fmt.Errorf("SetRowAttrs() row field '%v' required", rowLabel)
	}

	// Copy args and remove reserved fields.
	attrs := pql.CopyArgs(c.Args)
	delete(attrs, "_field")
	delete(attrs, "_"+rowLabel)

	// Set attributes.
	if err := field.RowAttrStore().SetAttrs(rowID, attrs); err != nil {
		return err
	}

	// Do not forward call if this is already being forwarded.
	if opt.Remote {
		return nil
	}

	// Execute on remote nodes in parallel.
	nodes := Nodes(e.Cluster.nodes).FilterID(e.Node.ID)
	resp := make(chan error, len(nodes))
	for _, node := range nodes {
		go func(node *Node) {
			_, err := e.remoteExec(ctx, node, index, &pql.Query{Calls: []*pql.Call{c}}, nil, nil)
			resp <- err
		}(node)
	}

	// Return first error.
	for range nodes {
		if err := <-resp; err != nil {
			return err
		}
	}

	return nil
}

// executeBulkSetRowAttrs executes a set of SetRowAttrs() calls.
func (e *executor) executeBulkSetRowAttrs(ctx context.Context, tx Tx, index string, calls []*pql.Call, opt *execOptions) ([]interface{}, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "Executor.executeBulkSetRowAttrs")
	defer span.Finish()

	// Collect attributes by field/id.
	m := make(map[string]map[uint64]map[string]interface{})
	for i, c := range calls {
		if i%10 == 0 {
			if err := validateQueryContext(ctx); err != nil {
				return nil, err
			}
		}

		field, ok := c.Args["_field"].(string)
		if !ok {
			return nil, errors.New("SetRowAttrs() field required")
		}

		// Retrieve field.
		f := e.Holder.Field(index, field)
		if f == nil {
			return nil, ErrFieldNotFound
		}

		rowID, ok, err := c.UintArg("_" + rowLabel)
		if err != nil {
			return nil, errors.Wrap(err, "reading SetRowAttrs() row")
		} else if !ok {
			return nil, fmt.Errorf("SetRowAttrs row field '%v' required", rowLabel)
		}

		// Copy args and remove reserved fields.
		attrs := pql.CopyArgs(c.Args)
		delete(attrs, "_field")
		delete(attrs, "_"+rowLabel)

		// Create field group, if not exists.
		fieldMap := m[field]
		if fieldMap == nil {
			fieldMap = make(map[uint64]map[string]interface{})
			m[field] = fieldMap
		}

		// Set or merge attributes.
		attr := fieldMap[rowID]
		if attr == nil {
			fieldMap[rowID] = cloneAttrs(attrs)
		} else {
			for k, v := range attrs {
				attr[k] = v
			}
		}
	}

	// Bulk insert attributes by field.
	for name, fieldMap := range m {
		// Retrieve field.
		field := e.Holder.Field(index, name)
		if field == nil {
			return nil, ErrFieldNotFound
		}

		// Set attributes.
		if err := field.RowAttrStore().SetBulkAttrs(fieldMap); err != nil {
			return nil, err
		}
	}

	if !opt.Remote {
		tags := []string{"index:" + index, "bulk:true"}
		e.Holder.Stats.CountWithCustomTags(MetricSetRowAttrs, int64(len(m)), 1.0, tags)
	}

	// Do not forward call if this is already being forwarded.
	if opt.Remote {
		return make([]interface{}, len(calls)), nil
	}

	// Execute on remote nodes in parallel.
	nodes := Nodes(e.Cluster.nodes).FilterID(e.Node.ID)
	resp := make(chan error, len(nodes))
	for _, node := range nodes {
		go func(node *Node) {
			_, err := e.remoteExec(ctx, node, index, &pql.Query{Calls: calls}, nil, nil)
			resp <- err
		}(node)
	}

	// Return first error.
	for range nodes {
		if err := <-resp; err != nil {
			return nil, err
		}
	}

	// Return a set of nil responses to match the non-optimized return.
	return make([]interface{}, len(calls)), nil
}

// executeSetColumnAttrs executes a SetColumnAttrs() call.
func (e *executor) executeSetColumnAttrs(ctx context.Context, tx Tx, index string, c *pql.Call, opt *execOptions) error {
	span, ctx := tracing.StartSpanFromContext(ctx, "Executor.executeSetColumnAttrs")
	defer span.Finish()

	// Retrieve index.
	idx := e.Holder.Index(index)
	if idx == nil {
		return ErrIndexNotFound
	}

	col, okCol, errCol := c.UintArg("_" + columnLabel)
	if errCol != nil || !okCol {
		return fmt.Errorf("reading SetColumnAttrs() col errs: %v found %v", errCol, okCol)
	}

	// Copy args and remove reserved fields.
	attrs := pql.CopyArgs(c.Args)
	delete(attrs, "_"+columnLabel)
	delete(attrs, "field")

	// Set attributes.

	if err := idx.ColumnAttrStore().SetAttrs(col, attrs); err != nil {
		return err
	}
	// Do not forward call if this is already being forwarded.
	if opt.Remote {
		return nil
	}

	// Execute on remote nodes in parallel.
	nodes := Nodes(e.Cluster.nodes).FilterID(e.Node.ID)
	resp := make(chan error, len(nodes))
	for _, node := range nodes {
		go func(node *Node) {
			_, err := e.remoteExec(ctx, node, index, &pql.Query{Calls: []*pql.Call{c}}, nil, nil)
			resp <- err
		}(node)
	}

	// Return first error.
	for range nodes {
		if err := <-resp; err != nil {
			return err
		}
	}

	return nil
}

// remoteExec executes a PQL query remotely for a set of shards on a node.
func (e *executor) remoteExec(ctx context.Context, node *Node, index string, q *pql.Query, shards []uint64, embed []*Row) (results []interface{}, err error) { // nolint: interfacer
	span, ctx := tracing.StartSpanFromContext(ctx, "Executor.executeExec")
	defer span.Finish()

	// Encode request object.
	pbreq := &QueryRequest{
		Query:        q.String(),
		Shards:       shards,
		Remote:       true,
		EmbeddedData: embed,
	}

	pb, err := e.client.QueryNode(ctx, &node.URI, index, pbreq)
	if err != nil {
		return nil, err
	}

	return pb.Results, pb.Err
}

// shardsByNode returns a mapping of nodes to shards.
// Returns errShardUnavailable if a shard cannot be allocated to a node.
func (e *executor) shardsByNode(nodes []*Node, index string, shards []uint64) (map[*Node][]uint64, error) {
	m := make(map[*Node][]uint64)

loop:
	for _, shard := range shards {
		for _, node := range e.Cluster.ShardNodes(index, shard) {
			if Nodes(nodes).Contains(node) {
				m[node] = append(m[node], shard)
				continue loop
			}
		}
		return nil, errShardUnavailable
	}
	return m, nil
}

// mapReduce maps and reduces data across the cluster.
//
// If a mapping of shards to a node fails then the shards are resplit across
// secondary nodes and retried. This continues to occur until all nodes are exhausted.
func (e *executor) mapReduce(ctx context.Context, index string, shards []uint64, c *pql.Call, opt *execOptions, mapFn mapFunc, reduceFn reduceFunc) (interface{}, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "Executor.mapReduce")
	defer span.Finish()

	ch := make(chan mapResponse)

	// Wrap context with a cancel to kill goroutines on exit.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// If this is the coordinating node then start with all nodes in the cluster.
	//
	// However, if this request is being sent from the coordinator then all
	// processing should be done locally so we start with just the local node.
	var nodes []*Node
	if !opt.Remote {
		nodes = Nodes(e.Cluster.nodes).Clone()
	} else {
		nodes = []*Node{e.Cluster.nodeByID(e.Node.ID)}
	}

	// Start mapping across all primary owners.
	if err := e.mapper(ctx, cancel, ch, nodes, index, shards, c, opt, mapFn, reduceFn); err != nil {
		return nil, errors.Wrap(err, "starting mapper")
	}

	// Iterate over all map responses and reduce.
	var result interface{}
	var shardN int
	for {
		select {
		case <-ctx.Done():
			return nil, errors.Wrap(ctx.Err(), "context done")
		case resp := <-ch:
			// On error retry against remaining nodes. If an error returns then
			// the context will cancel and cause all open goroutines to return.

			if resp.err != nil {
				// Filter out unavailable nodes.
				nodes = Nodes(nodes).Filter(resp.node)

				// Begin mapper against secondary nodes.
				if err := e.mapper(ctx, cancel, ch, nodes, index, resp.shards, c, opt, mapFn, reduceFn); errors.Cause(err) == errShardUnavailable {
					return nil, resp.err
				} else if err != nil {
					return nil, errors.Wrap(err, "calling mapper")
				}
				continue
			}

			// Reduce value.
			result = reduceFn(ctx, result, resp.result)
			if err, ok := result.(error); ok {
				cancel()
				return nil, err
			}

			// If all shards have been processed then return.
			shardN += len(resp.shards)
			if shardN >= len(shards) {
				return result, nil
			}
		}
	}
}

// makeEmbeddedDataForShards produces new rows containing the rowSegments
// that would correspond to a given set of shards.
func makeEmbeddedDataForShards(allRows []*Row, shards []uint64) []*Row {
	if len(allRows) == 0 || len(shards) == 0 {
		return nil
	}
	newRows := make([]*Row, len(allRows))
	for i, row := range allRows {
		if row == nil || len(row.segments) == 0 {
			continue
		}
		segments := row.segments
		segmentIndex := 0
		newRows[i] = &Row{}
		for _, shard := range shards {
			for segmentIndex < len(segments) && segments[segmentIndex].shard < shard {
				segmentIndex++
			}
			// no more segments in this row
			if segmentIndex >= len(segments) {
				break
			}
			if segments[segmentIndex].shard == shard {
				newRows[i].segments = append(newRows[i].segments, segments[segmentIndex])
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

func (e *executor) mapper(ctx context.Context, cancel context.CancelFunc, ch chan mapResponse, nodes []*Node, index string, shards []uint64, c *pql.Call, opt *execOptions, mapFn mapFunc, reduceFn reduceFunc) error {
	span, ctx := tracing.StartSpanFromContext(ctx, "Executor.mapper")
	defer span.Finish()
	done := ctx.Done()

	// Group shards together by nodes.
	m, err := e.shardsByNode(nodes, index, shards)
	if err != nil {
		return errors.Wrap(err, "shards by node")
	}

	// Execute each node in a separate goroutine.
	for n, nodeShards := range m {
		go func(n *Node, nodeShards []uint64) {
			resp := mapResponse{node: n, shards: nodeShards}

			// Send local shards to mapper, otherwise remote exec.
			if n.ID == e.Node.ID {
				resp.result, resp.err = e.mapperLocal(ctx, nodeShards, mapFn, reduceFn)
			} else if !opt.Remote {
				var embeddedRowsForNode []*Row
				if opt.EmbeddedData != nil {
					embeddedRowsForNode = makeEmbeddedDataForShards(opt.EmbeddedData, nodeShards)
				}
				results, err := e.remoteExec(ctx, n, index, &pql.Query{Calls: []*pql.Call{c}}, nodeShards, embeddedRowsForNode)
				if len(results) > 0 {
					resp.result = results[0]
				}
				resp.err = err
			}
			// Return response to the channel.
			select {
			case <-done:
			case ch <- resp:
				// The cancel coming after the above send is intentional.
				// We want to report the actual error that happened
				// before we cause anything to return "context canceled".
				if resp.err != nil {
					cancel()
				}
			}
		}(n, nodeShards)
	}

	return nil
}

type job struct {
	shard      uint64
	mapFn      mapFunc
	ctx        context.Context
	resultChan chan mapResponse
}

func worker(work chan job) {
	for j := range work {
		result, err := j.mapFn(j.ctx, j.shard)

		select {
		case <-j.ctx.Done():
		case j.resultChan <- mapResponse{result: result, err: err}:
		}
	}
}

var errShutdown = errors.New("executor has shut down")

// mapperLocal performs map & reduce entirely on the local node.
func (e *executor) mapperLocal(ctx context.Context, shards []uint64, mapFn mapFunc, reduceFn reduceFunc) (interface{}, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "Executor.mapperLocal")
	defer span.Finish()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := ctx.Done()
	e.workMu.RLock()
	defer e.workMu.RUnlock()

	if e.shutdown {
		return nil, errShutdown
	}

	ch := make(chan mapResponse, len(shards))

	for _, shard := range shards {
		e.work <- job{
			shard:      shard,
			mapFn:      mapFn,
			ctx:        ctx,
			resultChan: ch,
		}
	}

	// Reduce results
	var maxShard int
	var result interface{}
	for {
		select {
		case <-done:
			return nil, ctx.Err()
		case resp := <-ch:
			if resp.err != nil {
				return nil, resp.err
			}
			result = reduceFn(ctx, result, resp.result)
			if err, ok := result.(error); ok {
				cancel()
				return nil, err
			}
			maxShard++
		}

		// Exit once all shards are processed.
		if maxShard == len(shards) {
			return result, nil
		}
	}
}

func (e *executor) translateCalls(ctx context.Context, defaultIndexName string, calls []*pql.Call) (err error) {
	span, _ := tracing.StartSpanFromContext(ctx, "Executor.translateCalls")
	defer span.Finish()

	// Generate a list of all used
	keySets := make(map[string]map[string]struct{})
	keySets[defaultIndexName] = make(map[string]struct{})
	for i := range calls {
		if err := e.collectCallKeySets(ctx, defaultIndexName, calls[i], keySets); err != nil {
			return err
		}
	}

	// Perform a separate batch translation for each separate index used.
	keyMaps := make(map[string]map[string]uint64)
	for indexName, keySet := range keySets {
		idx := e.Holder.indexes[indexName]
		if idx == nil {
			return fmt.Errorf("cannot find index %q", indexName)
		}

		if !idx.Keys() || len(keySets) == 0 {
			continue
		}
		if keyMaps[indexName], err = e.Cluster.translateIndexKeySet(ctx, indexName, keySet); err != nil {
			return err
		}
	}

	// Translate calls.
	for i := range calls {
		if err := e.translateCall(ctx, defaultIndexName, calls[i], keyMaps); err != nil {
			return err
		}
	}

	return nil
}

func (e *executor) collectCallKeySets(ctx context.Context, indexName string, c *pql.Call, m map[string]map[string]struct{}) error {
	// Specifying an 'index' call overrides indexes on subsequent calls.
	if s := c.CallIndex(); s != "" {
		indexName = s
	}

	if m[indexName] == nil {
		m[indexName] = make(map[string]struct{})
	}

	// Collect key for this call.
	colKey, rowKey, fieldName := c.TranslateInfo(columnLabel, rowLabel)
	if c.Args[colKey] != nil && isString(c.Args[colKey]) {
		if value := callArgString(c, colKey); value != "" {
			m[indexName][value] = struct{}{}
		}
	}

	// Collect foreign index keys.
	if fieldName != "" {
		idx, exists := e.Holder.indexes[indexName]
		if !exists {
			return errors.Wrapf(ErrIndexNotFound, "%s", indexName)
		}
		if field := idx.Field(fieldName); field != nil && field.ForeignIndex() != "" {
			foreignIndexName := field.ForeignIndex()
			if m[foreignIndexName] == nil {
				m[foreignIndexName] = make(map[string]struct{})
			}

			if c.Args[rowKey] != nil && isCondition(c.Args[rowKey]) {
				cond := c.Args[rowKey].(*pql.Condition)
				if isString(cond.Value) {
					m[foreignIndexName][cond.Value.(string)] = struct{}{}
				}
			} else if value := callArgString(c, rowKey); value != "" {
				m[foreignIndexName][value] = struct{}{}
			}
		}
	}

	// Recursively collect argument calls.
	for _, arg := range c.Args {
		if arg, ok := arg.(*pql.Call); ok {
			if err := e.collectCallKeySets(ctx, indexName, arg, m); err != nil {
				return errors.Wrap(err, "collecting group by call index name")
			}
		}
	}

	// Recursively collect child calls.
	for _, child := range c.Children {
		if err := e.collectCallKeySets(ctx, indexName, child, m); err != nil {
			return err
		}
	}
	return nil
}

func (e *executor) translateCall(ctx context.Context, indexName string, c *pql.Call, keyMaps map[string]map[string]uint64) (err error) {
	// Specifying an 'index' arg applies to all nested calls.
	if s := c.CallIndex(); s != "" {
		indexName = s
	}
	keyMap := keyMaps[indexName]

	// Translate column key.
	colKey, rowKey, fieldName := c.TranslateInfo(columnLabel, rowLabel)
	idx, exists := e.Holder.indexes[indexName]
	if !exists {
		return errors.Wrapf(ErrIndexNotFound, "%s", indexName)
	}
	if idx.Keys() {
		if c.Args[colKey] != nil && !isString(c.Args[colKey]) {
			if !isValidID(c.Args[colKey]) {
				return errors.Errorf("column value must be a string or non-negative integer, but got: %v of %[1]T", c.Args[colKey])
			}
		} else if value := callArgString(c, colKey); value != "" {
			c.Args[colKey] = keyMap[value]
		}
	} else {
		if isString(c.Args[colKey]) {
			return errors.New("string 'col' value not allowed unless index 'keys' option enabled")
		}
	}

	// Translate row key, if field is specified & key exists.
	if fieldName != "" {
		field := idx.Field(fieldName)
		if field == nil {
			// Instead of returning ErrFieldNotFound here,
			// we just return, and don't attempt the translation.
			// The assumption is that the non-existent field
			// will raise an error downstream when it's used.
			return nil
		}

		// Bool field keys do not use the translator because there
		// are only two possible values. Instead, they are handled
		// directly.
		if field.Type() == FieldTypeBool {
			if c.Name == "Rows" {
				// TranslateInfo for Rows returns "previous" as rowKey,
				// so for bool fields we would get "missing bool argument" error
				return nil
			}
			boolVal, err := callArgBool(c, rowKey)
			if err != nil {
				return errors.Wrapf(err, "getting bool key (%+v)", rowKey)
			}
			rowID := falseRowID
			if boolVal {
				rowID = trueRowID
			}
			c.Args[rowKey] = rowID
		} else if field.Keys() {
			foreignIndexName := field.ForeignIndex()
			if c.Args[rowKey] != nil && isCondition(c.Args[rowKey]) {
				// In the case where a field has a foreign index with keys,
				// allow `== "key"` or `!= "key"` to be used against the BSI
				// field.
				cond := c.Args[rowKey].(*pql.Condition)
				if isString(cond.Value) {
					switch cond.Op {
					case pql.EQ, pql.NEQ:
						var id uint64
						if foreignIndexName != "" {
							id = keyMaps[foreignIndexName][cond.Value.(string)]
						} else {
							if id, err = e.Cluster.translateFieldKey(ctx, field, cond.Value.(string)); err != nil {
								return errors.Wrapf(err, "translating field key: %s", cond.Value)
							}
						}

						c.Args[rowKey] = &pql.Condition{
							Op:    cond.Op,
							Value: id,
						}
					default:
						return errors.Errorf("conditional is not supported with string predicates: %s", cond.Op)
					}
				}
			} else if c.Args[rowKey] != nil && !isString(c.Args[rowKey]) {
				// allow passing row id directly (this can come in handy, but make sure it is a valid row id)
				if !isValidID(c.Args[rowKey]) {
					return errors.Errorf("row value must be a string or non-negative integer, but got: %v of %[1]T", c.Args[rowKey])
				}
			} else if value := callArgString(c, rowKey); value != "" {
				var id uint64
				if foreignIndexName != "" {
					id = keyMaps[foreignIndexName][value]
				} else {
					if id, err = e.Cluster.translateFieldKey(ctx, field, value); err != nil {
						return errors.Wrapf(err, "translating field key: %s", value)
					}
				}
				c.Args[rowKey] = id
			}
		} else {
			if isString(c.Args[rowKey]) {
				return errors.New("string 'row' value not allowed unless field 'keys' option enabled")
			}
		}
	}

	// Translate child calls.
	for _, child := range c.Children {
		if err := e.translateCall(ctx, indexName, child, keyMaps); err != nil {
			return err
		}
	}

	// Translate call args.
	for _, arg := range c.Args {
		if arg, ok := arg.(*pql.Call); ok {
			if err := e.translateCall(ctx, indexName, arg, keyMaps); err != nil {
				return errors.Wrap(err, "translating arg")
			}
		}
	}

	// GroupBy-specific call translation.
	if c.Name == "GroupBy" {
		prev, ok := c.Args["previous"]
		if !ok {
			return nil // nothing else to be translated
		}
		previous, ok := prev.([]interface{})
		if !ok {
			return errors.Errorf("'previous' argument must be list, but got %T", prev)
		}
		if len(c.Children) != len(previous) {
			return errors.Errorf("mismatched lengths for previous: %d and children: %d in %s", len(previous), len(c.Children), c)
		}

		fields := make([]*Field, len(c.Children))
		for i, child := range c.Children {
			var fieldname string
			if fieldname = callArgString(child, "_field"); fieldname == "" {
				// TODO: it's unsettling that we expect "_field" but in some
				// cases get "field". We should figure out why that happens.
				fieldname = callArgString(child, "field")
			}
			field := idx.Field(fieldname)
			if field == nil {
				return errors.Wrapf(ErrFieldNotFound, "getting field '%s' from '%s'", fieldname, child)
			}
			fields[i] = field
		}

		for i, field := range fields {
			prev := previous[i]
			if field.Keys() {
				prevStr, ok := prev.(string)
				if !ok {
					return errors.New("prev value must be a string when field 'keys' option enabled")
				}
				var id uint64
				var err error
				if fi := field.ForeignIndex(); fi != "" {
					ids, err := e.Cluster.translateIndexKeys(ctx, fi, []string{prevStr})
					if err != nil {
						return errors.Wrap(err, "translating foreign index key in groupby previous")
					}
					if len(ids) == 1 {
						id = ids[0]
					}
				} else {
					id, err = e.Cluster.translateFieldKey(ctx, field, prevStr)
					if err != nil {
						return errors.Wrapf(err, "translating field key: %s", prevStr)
					}
				}
				previous[i] = id
			} else {
				if prevStr, ok := prev.(string); ok {
					return errors.Errorf("got string row val '%s' in 'previous' for field %s which doesn't use string keys", prevStr, field.Name())
				}
			}
		}
	}

	return nil
}

func (e *executor) translateResults(ctx context.Context, index string, idx *Index, calls []*pql.Call, results []interface{}) (err error) {
	span, _ := tracing.StartSpanFromContext(ctx, "Executor.translateResults")
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
		results[i], err = e.translateResult(ctx, index, idx, calls[i], results[i], idMap)
		if err != nil {
			return err
		}
	}
	return nil
}

func (e *executor) collectResultIDs(index string, idx *Index, call *pql.Call, result interface{}, idSet map[uint64]struct{}) error {
	switch result := result.(type) {
	case *Row:
		if !idx.Keys() {
			return nil
		}

		for _, segment := range result.Segments() {
			for _, col := range segment.Columns() {
				idSet[col] = struct{}{}
			}
		}

	case ExtractedIDMatrix:
		for _, col := range result.Columns {
			idSet[col.ColumnID] = struct{}{}
		}
	}

	return nil
}

func (e *executor) translateResult(ctx context.Context, index string, idx *Index, call *pql.Call, result interface{}, idSet map[uint64]string) (interface{}, error) {
	switch result := result.(type) {
	case *Row:
		if idx.Keys() {
			other := &Row{Attrs: result.Attrs}
			for _, segment := range result.Segments() {
				for _, col := range segment.Columns() {
					other.Keys = append(other.Keys, idSet[col])
				}
			}
			return other, nil
		}

	// TODO: instead of supporting SignedRow here, we may be able to
	// make the return type for an int field with a ForeignIndex be
	// a *Row instead (because it should always be positive).
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
				other := &Row{Attrs: rslt.Attrs}
				for _, segment := range rslt.Segments() {
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
				other := make([]Pair, len(result.Pairs))
				for i := range result.Pairs {
					key, err := field.TranslateStore().TranslateID(result.Pairs[i].ID)
					if err != nil {
						return nil, err
					}
					other[i] = Pair{Key: key, Count: result.Pairs[i].Count}
				}
				return &PairsField{
					Pairs: other,
					Field: fieldName,
				}, nil
			}
		}

	case []GroupCount:
		other := make([]GroupCount, 0)
		for _, gl := range result {

			group := make([]FieldRow, len(gl.Group))
			for i, g := range gl.Group {
				group[i] = g

				// TODO: It may be useful to cache this field lookup.
				field := idx.Field(g.Field)
				if field == nil {
					return nil, ErrFieldNotFound
				}
				if field.Keys() {
					var key string
					var err error
					if fi := field.ForeignIndex(); fi != "" && g.Value != nil {
						val := uint64(*g.Value) // not worried about overflow here because it's a foreign key
						keys, err := e.Cluster.translateIndexIDs(ctx, fi, []uint64{val})
						if err != nil {
							return nil, errors.Wrap(err, "translating foreign index in Group")
						}
						if len(keys) == 1 {
							key = keys[0]
							group[i].Value = nil // Remove value now that it has been translated.
						}
					} else {
						key, err = field.TranslateStore().TranslateID(g.RowID)
						if err != nil {
							return nil, errors.Wrap(err, "translating row ID in Group")
						}
					}
					group[i].RowKey = key
				}
			}

			other = append(other, GroupCount{
				Group: group,
				Count: gl.Count,
				Sum:   gl.Sum,
			})
		}
		return other, nil

	case RowIDs:
		fieldName := callArgString(call, "_field")
		if fieldName == "" {
			return nil, ErrFieldNotFound
		}

		other := RowIdentifiers{
			field: fieldName,
		}

		if field := idx.Field(fieldName); field == nil {
			return nil, ErrFieldNotFound
		} else if field.Keys() {
			other.Keys = make([]string, len(result))
			for i, id := range result {
				key, err := field.TranslateStore().TranslateID(id)
				if err != nil {
					return nil, errors.Wrap(err, "translating row ID")
				}
				other.Keys[i] = key
			}
		} else {
			other.Rows = result
		}

		return other, nil

	case ExtractedIDMatrix:
		type fieldMapper = func([]uint64) (interface{}, error)

		fields := make([]ExtractedTableField, len(result.Fields))
		mappers := make([]fieldMapper, len(result.Fields))
		for i, v := range result.Fields {
			field := idx.Field(v)
			if field == nil {
				return nil, ErrFieldNotFound
			}

			typ := field.Type()

			fields[i] = ExtractedTableField{
				Name: v,
				Type: typ,
			}

			var mapper fieldMapper
			switch typ {
			case FieldTypeBool:
				mapper = func(ids []uint64) (interface{}, error) {
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
					translator := field.TranslateStore()
					mapper = func(ids []uint64) (interface{}, error) {
						return translator.TranslateIDs(ids)
					}
				} else {
					mapper = func(ids []uint64) (interface{}, error) {
						if ids == nil {
							ids = []uint64{}
						}
						return ids, nil
					}
				}
			case FieldTypeMutex:
				if field.Keys() {
					translator := field.TranslateStore()
					mapper = func(ids []uint64) (interface{}, error) {
						switch len(ids) {
						case 0:
							return nil, nil
						case 1:
							return translator.TranslateID(ids[0])
						default:
							return nil, errors.Errorf("mutex %q has too many values: %v", field.Name(), ids)
						}
					}
				} else {
					mapper = func(ids []uint64) (interface{}, error) {
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
			case FieldTypeDecimal:
				scale := field.Options().Scale
				mapper = func(ids []uint64) (interface{}, error) {
					switch len(ids) {
					case 0:
						return nil, nil
					case 1:
						return pql.NewDecimal(int64(ids[0]), scale), nil
					default:
						return nil, errors.Errorf("BSI field %q has too many values: %v", field.Name(), ids)
					}
				}
			default:
				return nil, errors.Errorf("field type %q not yet supported", typ)
			}
			mappers[i] = mapper
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
		}

		return ExtractedTable{
			Fields:  fields,
			Columns: cols,
		}, nil
	}

	return result, nil
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

type mapFunc func(ctx context.Context, shard uint64) (interface{}, error)

type reduceFunc func(ctx context.Context, prev, v interface{}) interface{}

type mapResponse struct {
	node   *Node
	shards []uint64

	result interface{}
	err    error
}

// execOptions represents an execution context for a single Execute() call.
type execOptions struct {
	Remote          bool
	Profile         bool
	ExcludeRowAttrs bool
	ExcludeColumns  bool
	ColumnAttrs     bool
	EmbeddedData    []*Row
}

// hasOnlySetRowAttrs returns true if calls only contains SetRowAttrs() calls.
func hasOnlySetRowAttrs(calls []*pql.Call) bool {
	if len(calls) == 0 {
		return false
	}

	for _, call := range calls {
		if call.Name != "SetRowAttrs" {
			return false
		}
	}
	return true
}

func needsShards(calls []*pql.Call) bool {
	if len(calls) == 0 {
		return false
	}
	for _, call := range calls {
		switch call.Name {
		case "Clear", "Set", "SetRowAttrs", "SetColumnAttrs":
			continue
		case "Count", "TopN", "Rows":
			return true
		// default catches Bitmap calls
		default:
			return true
		}
	}
	return false
}

// SignedRow represents a signed *Row with two (neg/pos) *Rows.
type SignedRow struct {
	Neg   *Row `json:"neg"`
	Pos   *Row `json:"pos"`
	field string
}

func (s *SignedRow) Clone() (r *SignedRow) {
	r = &SignedRow{
		Neg:   s.Neg.Clone(), // Row.Clone() returns nil for nil.
		Pos:   s.Pos.Clone(),
		field: s.field,
	}
	return
}

// Field returns the field name associated to the signed row.
func (s *SignedRow) Field() string {
	return s.field
}

// ToTable implements the ToTabler interface.
func (s SignedRow) ToTable() (*pb.TableResponse, error) {
	var n uint64
	if s.Neg != nil {
		n += s.Neg.Count()
	}
	if s.Pos != nil {
		n += s.Pos.Count()
	}
	return pb.RowsToTable(&s, int(n))
}

// ToRows implements the ToRowser interface.
func (s SignedRow) ToRows(callback func(*pb.RowResponse) error) error {

	ci := []*pb.ColumnInfo{{Name: s.Field(), Datatype: "int64"}}
	negs := s.Neg.Columns()
	for i := len(negs) - 1; i >= 0; i-- {
		val, err := toNegInt64(negs[i])
		if err != nil {
			return errors.Wrap(err, "converting uint64 to int64 (negative)")
		}

		if err := callback(&pb.RowResponse{
			Headers: ci,
			Columns: []*pb.ColumnResponse{
				&pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_Int64Val{Int64Val: val}},
			},
		}); err != nil {
			return errors.Wrap(err, "calling callback")
		}
		ci = nil
	}
	for _, id := range s.Pos.Columns() {
		val, err := toInt64(id)
		if err != nil {
			return errors.Wrap(err, "converting uint64 to int64 (positive)")
		}

		if err := callback(&pb.RowResponse{
			Headers: ci,
			Columns: []*pb.ColumnResponse{
				&pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_Int64Val{Int64Val: val}},
			},
		}); err != nil {
			return errors.Wrap(err, "calling callback")
		}
		ci = nil
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

func (sr *SignedRow) union(other SignedRow) SignedRow {
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
	Val        int64        `json:"value"`
	FloatVal   float64      `json:"floatValue"`
	DecimalVal *pql.Decimal `json:"decimalValue"`
	Count      int64        `json:"count"`
}

func (v *ValCount) Clone() (r *ValCount) {
	r = &ValCount{
		Val:      v.Val,
		FloatVal: v.FloatVal,
		Count:    v.Count,
	}
	if v.DecimalVal != nil {
		r.DecimalVal = v.DecimalVal.Clone()
	}
	return
}

// ToTable implements the ToTabler interface.
func (v ValCount) ToTable() (*pb.TableResponse, error) {
	return pb.RowsToTable(&v, 1)
}

// ToRows implements the ToRowser interface.
func (v ValCount) ToRows(callback func(*pb.RowResponse) error) error {
	var ci []*pb.ColumnInfo
	// ValCount can have a decimal, float, or integer value, but
	// not more than one (as of this writing).
	if v.DecimalVal != nil {
		ci = []*pb.ColumnInfo{
			{Name: "value", Datatype: "decimal"},
			{Name: "count", Datatype: "int64"},
		}
		if err := callback(&pb.RowResponse{
			Headers: ci,
			Columns: []*pb.ColumnResponse{
				&pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_DecimalVal{DecimalVal: &pb.Decimal{Value: v.DecimalVal.Value, Scale: v.DecimalVal.Scale}}},
				&pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_Int64Val{Int64Val: v.Count}},
			}}); err != nil {
			return errors.Wrap(err, "calling callback")
		}
	} else if v.FloatVal != 0 {
		ci = []*pb.ColumnInfo{
			{Name: "value", Datatype: "float64"},
			{Name: "count", Datatype: "int64"},
		}
		if err := callback(&pb.RowResponse{
			Headers: ci,
			Columns: []*pb.ColumnResponse{
				&pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_Float64Val{Float64Val: v.FloatVal}},
				&pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_Int64Val{Int64Val: v.Count}},
			}}); err != nil {
			return errors.Wrap(err, "calling callback")
		}
	} else {
		ci = []*pb.ColumnInfo{
			{Name: "value", Datatype: "int64"},
			{Name: "count", Datatype: "int64"},
		}
		if err := callback(&pb.RowResponse{
			Headers: ci,
			Columns: []*pb.ColumnResponse{
				&pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_Int64Val{Int64Val: v.Val}},
				&pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_Int64Val{Int64Val: v.Count}},
			}}); err != nil {
			return errors.Wrap(err, "calling callback")
		}
	}
	return nil
}

func (vc *ValCount) add(other ValCount) ValCount {
	return ValCount{
		Val:   vc.Val + other.Val,
		Count: vc.Count + other.Count,
	}
}

// smaller returns the smaller of the two ValCounts.
func (vc *ValCount) smaller(other ValCount) ValCount {
	if vc.DecimalVal != nil || other.DecimalVal != nil {
		return vc.decimalSmaller(other)
	} else if vc.FloatVal != 0 || other.FloatVal != 0 {
		return vc.floatSmaller(other)
	}
	if vc.Count == 0 || (other.Val < vc.Val && other.Count > 0) {
		return other
	}
	extra := int64(0)
	if vc.Val == other.Val {
		extra += other.Count
	}
	return ValCount{
		Val:   vc.Val,
		Count: vc.Count + extra,
	}
}

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
func (vc *ValCount) larger(other ValCount) ValCount {
	if vc.DecimalVal != nil || other.DecimalVal != nil {
		return vc.decimalLarger(other)
	} else if vc.FloatVal != 0 || other.FloatVal != 0 {
		return vc.floatLarger(other)
	}
	if vc.Count == 0 || (other.Val > vc.Val && other.Count > 0) {
		return other
	}
	extra := int64(0)
	if vc.Val == other.Val {
		extra += other.Count
	}
	return ValCount{
		Val:   vc.Val,
		Count: vc.Count + extra,
	}
}

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

func callArgBool(call *pql.Call, key string) (bool, error) {
	value, ok := call.Args[key]
	if !ok {
		return false, errors.New("missing bool argument")
	}
	b, ok := value.(bool)
	if !ok {
		return false, fmt.Errorf("invalid bool argument type: %T", value)
	}
	return b, nil
}

func callArgString(call *pql.Call, key string) string {
	value, ok := call.Args[key]
	if !ok {
		return ""
	}
	s, _ := value.(string)
	return s
}

func isString(v interface{}) bool {
	_, ok := v.(string)
	return ok
}

func isCondition(v interface{}) bool {
	_, ok := v.(*pql.Condition)
	return ok
}

// isValidID returns whether v can be interpreted as a valid row or
// column ID. In short, is v a non-negative integer? I think the int64
// and default cases are the only ones actually used since the PQL
// parser doesn't return any other integer types.
func isValidID(v interface{}) bool {
	switch vt := v.(type) {
	case uint, uint64, uint32, uint16, uint8:
		return true
	case int64:
		return vt >= 0
	case int:
		return vt >= 0
	case int32:
		return vt >= 0
	case int16:
		return vt >= 0
	case int8:
		return vt >= 0
	default:
		return false
	}
}

// groupByIterator contains several slices. Each slice contains a number of
// elements equal to the number of fields in the group by (the number of Rows
// calls).
type groupByIterator struct {
	executor *executor
	tx       Tx
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
func newGroupByIterator(executor *executor, tx Tx, rowIDs []RowIDs, children []*pql.Call, aggregate *pql.Call, filter *Row, index string, shard uint64, holder *Holder) (_ *groupByIterator, err error) {
	gbi := &groupByIterator{
		executor: executor,
		tx:       tx,
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

	var (
		fieldName string
		viewName  string
		ok        bool
	)
	ignorePrev := false
	for i, call := range children {
		if fieldName, ok = call.Args["_field"].(string); !ok {
			return nil, errors.Errorf("%s call must have field with valid (string) field name. Got %v of type %[2]T", call.Name, call.Args["_field"])
		}
		field := holder.Field(index, fieldName)
		if field == nil {
			return nil, ErrFieldNotFound
		}
		gbi.fields[i].Field = fieldName

		switch field.Type() {
		case FieldTypeSet, FieldTypeTime, FieldTypeMutex, FieldTypeBool:
			viewName = viewStandard

		case FieldTypeInt:
			viewName = viewBSIGroupPrefix + fieldName

		default: // FieldTypeDecimal
			return nil, errors.Errorf("%s call must have field of one of types: %s",
				call.Name, strings.Join([]string{FieldTypeSet, FieldTypeTime, FieldTypeMutex, FieldTypeBool, FieldTypeInt}, ","))
		}

		// Fetch fragment.
		frag := holder.fragment(index, fieldName, viewName, shard)
		if frag == nil { // this means this whole shard doesn't have all it needs to continue
			return nil, nil
		}
		filters := []rowFilter{}
		if len(rowIDs[i]) > 0 {
			filters = append(filters, filterWithRows(rowIDs[i]))
		}
		gbi.rowIters[i], err = frag.rowIterator(tx, i != 0, filters...)
		if err != nil {
			return nil, err
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
		if gbi.aggregate == nil {
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
				result, err := gbi.executor.executeSumCountShard(ctx, gbi.tx, gbi.index, gbi.aggregate, filter, gbi.shard)
				if err != nil {
					return ret, false, err
				}
				ret.Count = uint64(result.Count)
				ret.Sum = result.Val
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
