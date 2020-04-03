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
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/molecula/ext"
	"github.com/pilosa/pilosa/v2/pql"
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

	workersWG      sync.WaitGroup
	workerPoolSize int
	work           chan job
	// global registry to check for name clashes
	additionalOps map[string]*ext.BitmapOp
	// typed registries we can use in lookups
	additionalBitmapOps map[string]ext.BitmapOpBitmap
	additionalCountOps  map[string]ext.BitmapOpUnaryCount
	additionalFieldOps  map[string]ext.BitmapOpBSIBitmap
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
	close(e.work)
	e.workersWG.Wait()
	return nil
}

func (e *executor) registerOps(ops []ext.BitmapOp) error {
	if e.additionalOps == nil {
		e.additionalOps = make(map[string]*ext.BitmapOp)
		e.additionalBitmapOps = make(map[string]ext.BitmapOpBitmap)
		e.additionalCountOps = make(map[string]ext.BitmapOpUnaryCount)
		e.additionalFieldOps = make(map[string]ext.BitmapOpBSIBitmap)
	}
	for i, op := range ops {
		name := op.Name
		if _, exists := e.additionalOps[name]; exists {
			return fmt.Errorf("op name '%s' already defined", name)
		}
		e.additionalOps[name] = &ops[i]
		typ := ops[i].Func.BitmapOpType()
		switch {
		case typ.Input == ext.OpInputBitmap && typ.Output == ext.OpOutputCount:
			e.additionalCountOps[name] = ops[i].Func.(ext.BitmapOpUnaryCount)
		case typ.Input == ext.OpInputBitmap && typ.Output == ext.OpOutputBitmap:
			e.additionalBitmapOps[name] = ops[i].Func.(ext.BitmapOpBitmap)
		case typ.Input == ext.OpInputNaryBSI && typ.Output == ext.OpOutputSignedBitmap:
			if fn, ok := ops[i].Func.(ext.BitmapOpBSIBitmapPrecall); ok {
				e.additionalFieldOps[name] = ext.BitmapOpBSIBitmap(fn)
			} else {
				e.additionalFieldOps[name] = ops[i].Func.(ext.BitmapOpBSIBitmap)
			}
		default:
			return fmt.Errorf("unsupported types for '%s': input type %d, output type %d", name, typ.Input, typ.Output)
		}
	}
	return nil
}

// Execute executes a PQL query.
func (e *executor) Execute(ctx context.Context, index string, q *pql.Query, shards []uint64, opt *execOptions) (QueryResponse, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "Executor.Execute")
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

	// Verify that the number of writes do not exceed the maximum.
	if e.MaxWritesPerRequest > 0 && q.WriteCallN() > e.MaxWritesPerRequest {
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
	results, err := e.execute(ctx, index, q, shards, opt)
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
		if err := e.translateResults(ctx, index, idx, q.Calls, results); err != nil {
			return resp, err
		} else if err := validateQueryContext(ctx); err != nil {
			return resp, err
		}
	}

	return resp, nil
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
func (e *executor) handlePreCalls(ctx context.Context, index string, c *pql.Call, shards []uint64, opt *execOptions) error {
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
		if err := e.handlePreCallChildren(ctx, index, c, shards, opt); err != nil {
			return err
		}

		c.Type = pql.PrecallGlobal
		index = newIndex
		// we need to recompute shards, then
		shards = nil
	}
	if c.Type == pql.PrecallNone {
		// otherwise, handle the children
		return e.handlePreCallChildren(ctx, index, c, shards, opt)
	}
	// We don't try to handle sub-calls from here. I'm not 100%
	// sure that's right, but I think the fact that they're happening
	// inside a precomputed call may mean they need different
	// handling. In any event, the sub-calls will get handled by
	// the executeCall when it gets to them...

	// We set c to look like a normal call, and actually execute it:
	c.Type = pql.PrecallNone
	// possibly override call index.
	v, err := e.executeCall(ctx, index, c, shards, opt)
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

// handlePreCallChildren handles any pre-calls in the children of a given call.
func (e *executor) handlePreCallChildren(ctx context.Context, index string, c *pql.Call, shards []uint64, opt *execOptions) error {
	for i := range c.Children {
		if err := e.handlePreCalls(ctx, index, c.Children[i], shards, opt); err != nil {
			return err
		}
	}
	for _, val := range c.Args {
		// Handle Call() operations which exist inside named arguments, too.
		if call, ok := val.(*pql.Call); ok {
			if err := e.handlePreCalls(ctx, index, call, shards, opt); err != nil {
				return err
			}
		}
	}
	return nil
}

func (e *executor) execute(ctx context.Context, index string, q *pql.Query, shards []uint64, opt *execOptions) ([]interface{}, error) {
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
		return e.executeBulkSetRowAttrs(ctx, index, q.Calls, opt)
	}

	// Execute each call serially.
	results := make([]interface{}, 0, len(q.Calls))
	for _, call := range q.Calls {
		if err := validateQueryContext(ctx); err != nil {
			return nil, err
		}

		// If you actually make a top-level Distinct call, you
		// want a SignedRow back. Otherwise, it's something else
		// that will be using it as a row, and we only care
		// about the positive values, because only positive values
		// are valid column IDs. So we don't actually eat top-level
		// pre calls.
		err := e.handlePreCallChildren(ctx, index, call, shards, opt)
		if err != nil {
			return nil, err
		}
		var v interface{}
		// Top-level calls don't need to precompute cross-index things,
		// because we can just pick whatever index we want, but we
		// still need to handle them. Since everything else was
		// already precomputed by handlePreCallChildren, though,
		// we don't need this logic in executeCall.
		if newIndex := call.CallIndex(); newIndex != "" && newIndex != index {
			v, err = e.executeCall(ctx, newIndex, call, nil, opt)
		} else {
			v, err = e.executeCall(ctx, index, call, shards, opt)
		}
		if err != nil {
			return nil, err
		}
		results = append(results, v)
	}
	return results, nil
}

// executeCall executes a call.
func (e *executor) executeCall(ctx context.Context, index string, c *pql.Call, shards []uint64, opt *execOptions) (interface{}, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "Executor.executeCall")
	defer span.Finish()

	if err := validateQueryContext(ctx); err != nil {
		return nil, err
	} else if err := e.validateCallArgs(c); err != nil {
		return nil, errors.Wrap(err, "validating args")
	}
	indexTag := "index:" + index
	metricName := "query_" + c.Name

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

	// Special handling for mutation and top-n calls.
	if op, ok := e.additionalCountOps[c.Name]; ok {
		e.Holder.Stats.CountWithCustomTags(metricName, 1, 1.0, []string{indexTag})
		return e.executeGenericCount(ctx, index, c, op, shards, opt)
	}
	if op, ok := e.additionalFieldOps[c.Name]; ok {
		e.Holder.Stats.CountWithCustomTags(metricName, 1, 1.0, []string{indexTag})
		return e.executeGenericField(ctx, index, c, op, shards, opt)
	}
	switch c.Name {
	case "Sum":
		e.Holder.Stats.CountWithCustomTags(metricName, 1, 1.0, []string{indexTag})
		return e.executeSum(ctx, index, c, shards, opt)
	case "Min":
		e.Holder.Stats.CountWithCustomTags(metricName, 1, 1.0, []string{indexTag})
		return e.executeMin(ctx, index, c, shards, opt)
	case "Max":
		e.Holder.Stats.CountWithCustomTags(metricName, 1, 1.0, []string{indexTag})
		return e.executeMax(ctx, index, c, shards, opt)
	case "MinRow":
		e.Holder.Stats.CountWithCustomTags(metricName, 1, 1.0, []string{indexTag})
		return e.executeMinRow(ctx, index, c, shards, opt)
	case "MaxRow":
		e.Holder.Stats.CountWithCustomTags(metricName, 1, 1.0, []string{indexTag})
		return e.executeMaxRow(ctx, index, c, shards, opt)
	case "Clear":
		return e.executeClearBit(ctx, index, c, opt)
	case "ClearRow":
		return e.executeClearRow(ctx, index, c, shards, opt)
	case "Store":
		return e.executeSetRow(ctx, index, c, shards, opt)
	case "Count":
		e.Holder.Stats.CountWithCustomTags(metricName, 1, 1.0, []string{indexTag})
		return e.executeCount(ctx, index, c, shards, opt)
	case "Set":
		return e.executeSet(ctx, index, c, opt)
	case "SetRowAttrs":
		return nil, e.executeSetRowAttrs(ctx, index, c, opt)
	case "SetColumnAttrs":
		return nil, e.executeSetColumnAttrs(ctx, index, c, opt)
	case "TopN":
		e.Holder.Stats.CountWithCustomTags(metricName, 1, 1.0, []string{indexTag})
		return e.executeTopN(ctx, index, c, shards, opt)
	case "Rows":
		e.Holder.Stats.CountWithCustomTags(metricName, 1, 1.0, []string{indexTag})
		return e.executeRows(ctx, index, c, shards, opt)
	case "GroupBy":
		e.Holder.Stats.CountWithCustomTags(metricName, 1, 1.0, []string{indexTag})
		return e.executeGroupBy(ctx, index, c, shards, opt)
	case "Options":
		return e.executeOptionsCall(ctx, index, c, shards, opt)
	case "IncludesColumn":
		return e.executeIncludesColumnCall(ctx, index, c, shards, opt)
	case "All":
		return e.executeAllCall(ctx, index, c, shards, opt)
	case "Precomputed":
		return e.executePrecomputedCall(ctx, index, c, shards, opt)
	default:
		e.Holder.Stats.CountWithCustomTags(metricName, 1, 1.0, []string{indexTag})
		return e.executeBitmapCall(ctx, index, c, shards, opt)
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

func (e *executor) executeOptionsCall(ctx context.Context, index string, c *pql.Call, shards []uint64, opt *execOptions) (interface{}, error) {
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
	return e.executeCall(ctx, index, c.Children[0], shards, optCopy)
}

// executeIncludesColumnCall executes an IncludesColumn() call.
func (e *executor) executeIncludesColumnCall(ctx context.Context, index string, c *pql.Call, shards []uint64, opt *execOptions) (bool, error) {
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
	mapFn := func(shard uint64) (interface{}, error) {
		return e.executeIncludesColumnCallShard(ctx, index, c, shard, col)
	}

	// Merge returned results at coordinating node.
	reduceFn := func(prev, v interface{}) interface{} {
		other, _ := prev.(bool)
		return other || v.(bool)
	}

	result, err := e.mapReduce(ctx, index, []uint64{shard}, c, opt, mapFn, reduceFn)
	if err != nil {
		return false, err
	}
	return result.(bool), nil
}

// executeAllCall executes an All() call.
func (e *executor) executeAllCall(ctx context.Context, index string, c *pql.Call, shards []uint64, opt *execOptions) (*Row, error) {
	rslt := NewRow()

	var limit uint64
	var offset uint64

	if lim, hasLimit, err := c.UintArg("limit"); err != nil {
		return nil, errors.Wrap(err, "getting limit")
	} else if hasLimit && lim > 0 {
		limit = uint64(lim)
	}
	if off, hasOffset, err := c.UintArg("offset"); err != nil {
		return nil, errors.Wrap(err, "getting offset")
	} else if hasOffset && off > 0 {
		offset = uint64(off)
	}

	if limit == 0 {
		limit = math.MaxUint64
	}

	// skip tracks the number of records left to be skipped
	// in support of getting to the offset.
	var skip uint64 = offset

	// got tracks the number of records gotten to that point.
	var got uint64

	for _, shard := range shards {
		row, err := e.executeAllCallMapReduce(ctx, index, c, shard, opt)
		if err != nil {
			return nil, errors.Wrap(err, "executing map reduce on shard")
		}

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
				rslt.Merge(row)
			} else {
				cols := row.Columns()
				partialRow := NewRow()
				for _, bit := range cols[skip:] {
					partialRow.SetBit(bit)
				}
				rslt.Merge(partialRow)
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
		rslt.Merge(partialRow)
		break
	}

	return rslt, nil
}

// executeAllCallMapReduce executes a single shard of the All() call
// using the executor.mapReduce() method.
func (e *executor) executeAllCallMapReduce(ctx context.Context, index string, c *pql.Call, shard uint64, opt *execOptions) (*Row, error) {
	// Execute calls in bulk on each remote node and merge.
	mapFn := func(shard uint64) (interface{}, error) {
		return e.executeAllCallShard(ctx, index, c, shard)
	}

	// Merge returned results at coordinating node.
	reduceFn := func(prev, v interface{}) interface{} {
		other, _ := prev.(*Row)
		if other == nil {
			other = NewRow()
		}
		other.Merge(v.(*Row))
		return other
	}

	result, err := e.mapReduce(ctx, index, []uint64{shard}, c, opt, mapFn, reduceFn)
	if err != nil {
		return nil, errors.Wrap(err, "map reduce")
	}

	row, _ := result.(*Row)

	return row, nil
}

// executeIncludesColumnCallShard
func (e *executor) executeIncludesColumnCallShard(ctx context.Context, index string, c *pql.Call, shard uint64, column uint64) (bool, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "Executor.executeIncludesColumnCallShard")
	defer span.Finish()

	if len(c.Children) == 1 {
		row, err := e.executeBitmapCallShard(ctx, index, c.Children[0], shard)
		if err != nil {
			return false, errors.Wrap(err, "executing bitmap call")
		}
		return row.Includes(column), nil
	}

	return false, errors.New("IncludesColumn call must specify a row query")
}

// executeSum executes a Sum() call.
func (e *executor) executeSum(ctx context.Context, index string, c *pql.Call, shards []uint64, opt *execOptions) (ValCount, error) {
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
	mapFn := func(shard uint64) (interface{}, error) {
		return e.executeSumCountShard(ctx, index, c, nil, shard)
	}

	// Merge returned results at coordinating node.
	reduceFn := func(prev, v interface{}) interface{} {
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

// executeGenericField executes a generic call on a field. Note that in this
// implementation, the operation is always a BSI op.
func (e *executor) executeGenericField(ctx context.Context, index string, c *pql.Call, op ext.BitmapOpBSIBitmap, shards []uint64, opt *execOptions) (SignedRow, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "Executor.executeGenericField")
	span.LogKV("name", c.Name)
	defer span.Finish()

	field := c.Args["field"]
	if field == "" {
		return SignedRow{}, fmt.Errorf("plugin operation %s(): field required", c.Name)
	}

	// Execute calls in bulk on each remote node and merge.
	mapFn := func(shard uint64) (interface{}, error) {
		return e.executeGenericFieldShard(ctx, index, c, op, shard)
	}

	// Merge returned results at coordinating node.
	reduceFn := func(prev, v interface{}) interface{} {
		other, _ := prev.(SignedRow)
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
func (e *executor) executeMin(ctx context.Context, index string, c *pql.Call, shards []uint64, opt *execOptions) (ValCount, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "Executor.executeMin")
	defer span.Finish()
	if field := c.Args["field"]; field == "" {
		return ValCount{}, errors.New("Min(): field required")
	}

	if len(c.Children) > 1 {
		return ValCount{}, errors.New("Min() only accepts a single bitmap input")
	}

	// Execute calls in bulk on each remote node and merge.
	mapFn := func(shard uint64) (interface{}, error) {
		return e.executeMinShard(ctx, index, c, shard)
	}

	// Merge returned results at coordinating node.
	reduceFn := func(prev, v interface{}) interface{} {
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
func (e *executor) executeMax(ctx context.Context, index string, c *pql.Call, shards []uint64, opt *execOptions) (ValCount, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "Executor.executeMax")
	defer span.Finish()

	if field := c.Args["field"]; field == "" {
		return ValCount{}, errors.New("Max(): field required")
	}

	if len(c.Children) > 1 {
		return ValCount{}, errors.New("Max() only accepts a single bitmap input")
	}

	// Execute calls in bulk on each remote node and merge.
	mapFn := func(shard uint64) (interface{}, error) {
		return e.executeMaxShard(ctx, index, c, shard)
	}

	// Merge returned results at coordinating node.
	reduceFn := func(prev, v interface{}) interface{} {
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
func (e *executor) executeMinRow(ctx context.Context, index string, c *pql.Call, shards []uint64, opt *execOptions) (interface{}, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "Executor.executeMinRow")
	defer span.Finish()

	if field := c.Args["field"]; field == "" {
		return ValCount{}, errors.New("MinRow(): field required")
	}

	// Execute calls in bulk on each remote node and merge.
	mapFn := func(shard uint64) (interface{}, error) {
		return e.executeMinRowShard(ctx, index, c, shard)
	}

	// Merge returned results at coordinating node.
	reduceFn := func(prev, v interface{}) interface{} {
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
func (e *executor) executeMaxRow(ctx context.Context, index string, c *pql.Call, shards []uint64, opt *execOptions) (interface{}, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "Executor.executeMaxRow")
	defer span.Finish()

	if field := c.Args["field"]; field == "" {
		return ValCount{}, errors.New("MaxRow(): field required")
	}

	// Execute calls in bulk on each remote node and merge.
	mapFn := func(shard uint64) (interface{}, error) {
		return e.executeMaxRowShard(ctx, index, c, shard)
	}

	// Merge returned results at coordinating node.
	reduceFn := func(prev, v interface{}) interface{} {
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
func (e *executor) executePrecomputedCall(ctx context.Context, index string, c *pql.Call, shards []uint64, opt *execOptions) (*Row, error) {
	span, _ := tracing.StartSpanFromContext(ctx, "Executor.executePrecomputedCall")
	defer span.Finish()
	result := NewRow()

	for _, row := range c.Precomputed {
		result.Merge(row.(*Row))
	}
	return result, nil
}

// executeBitmapCall executes a call that returns a bitmap.
func (e *executor) executeBitmapCall(ctx context.Context, index string, c *pql.Call, shards []uint64, opt *execOptions) (*Row, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "Executor.executeBitmapCall")
	defer span.Finish()

	// Execute calls in bulk on each remote node and merge.
	mapFn := func(shard uint64) (interface{}, error) {
		return e.executeBitmapCallShard(ctx, index, c, shard)
	}

	// Merge returned results at coordinating node.
	reduceFn := func(prev, v interface{}) interface{} {
		other, _ := prev.(*Row)
		if other == nil {
			other = NewRow()
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
func (e *executor) executeBitmapCallShard(ctx context.Context, index string, c *pql.Call, shard uint64) (*Row, error) {
	if err := validateQueryContext(ctx); err != nil {
		return nil, err
	}

	span, ctx := tracing.StartSpanFromContext(ctx, "Executor.executeBitmapCallShard")
	defer span.Finish()

	if _, ok := e.additionalCountOps[c.Name]; ok {
		return nil, fmt.Errorf("count op %s used as bitmap call", c.Name)
	}
	if op, ok := e.additionalBitmapOps[c.Name]; ok {
		return e.executeGenericBitmapShard(ctx, index, c, op, shard)
	}

	switch c.Name {
	case "Row", "Range":
		return e.executeRowShard(ctx, index, c, shard)
	case "Difference":
		return e.executeDifferenceShard(ctx, index, c, shard)
	case "Intersect":
		return e.executeIntersectShard(ctx, index, c, shard)
	case "Union":
		return e.executeUnionShard(ctx, index, c, shard)
	case "Xor":
		return e.executeXorShard(ctx, index, c, shard)
	case "Not":
		return e.executeNotShard(ctx, index, c, shard)
	case "Shift":
		return e.executeShiftShard(ctx, index, c, shard)
	case "All": // Allow a shard computation to use All() (note, limit/offset not applied)
		return e.executeAllCallShard(ctx, index, c, shard)
	case "Precomputed":
		return e.executePrecomputedCallShard(ctx, index, c, shard)
	default:
		return nil, fmt.Errorf("unknown call: %s", c.Name)
	}
}

// executeGenericFieldShard executes a generic/extension command on a
// single shard. Note that in this implementation, the op is always
// a BSI op.
func (e *executor) executeGenericFieldShard(ctx context.Context, index string, c *pql.Call, op ext.BitmapOpBSIBitmap, shard uint64) (SignedRow, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "Executor.executeGenericShard")
	defer span.Finish()

	var filter *Row
	var filterBitmap *roaring.Bitmap
	if len(c.Children) == 1 {
		row, err := e.executeBitmapCallShard(ctx, index, c.Children[0], shard)
		if err != nil {
			return SignedRow{}, errors.Wrap(err, "executing bitmap call")
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
		return SignedRow{}, nil
	}

	bsig := field.bsiGroup(fieldName)
	if bsig == nil {
		return SignedRow{}, nil
	}

	fragment := e.Holder.fragment(index, fieldName, viewBSIGroupPrefix+fieldName, shard)
	if fragment == nil {
		return SignedRow{}, nil
	}

	var out ext.SignedBitmap
	if filterBitmap != nil {
		out = op(ext.BitmapBSI{FieldData: WrapBitmap(fragment.storage), ShardWidth: ShardWidth, Offset: bsig.Base, Depth: bsig.BitDepth}, []ext.Bitmap{WrapBitmap(filterBitmap)}, c.Args)
	} else {
		out = op(ext.BitmapBSI{FieldData: WrapBitmap(fragment.storage), ShardWidth: ShardWidth, Offset: bsig.Base, Depth: bsig.BitDepth}, []ext.Bitmap{}, c.Args)
	}

	return SignedRow{
		Neg: NewRowFromBitmap(UnwrapBitmap(out.Neg)),
		Pos: NewRowFromBitmap(UnwrapBitmap(out.Pos)),
	}, nil
}

// executeSumCountShard calculates the sum and count for bsiGroups on a shard.
func (e *executor) executeSumCountShard(ctx context.Context, index string, c *pql.Call, filter *Row, shard uint64) (ValCount, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "Executor.executeSumCountShard")
	defer span.Finish()

	// Only calculate the filter if it doesn't exist and a child call as been passed in.
	if filter == nil && len(c.Children) == 1 {
		row, err := e.executeBitmapCallShard(ctx, index, c.Children[0], shard)
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
	vsum, vcount, err := fragment.sum(filter, bsig.BitDepth)
	if err != nil {
		return ValCount{}, errors.Wrap(err, "computing sum")
	}
	return ValCount{
		Val:   int64(vsum) + (int64(vcount) * bsig.Base),
		Count: int64(vcount),
	}, nil
}

// executeMinShard calculates the min for bsiGroups on a shard.
func (e *executor) executeMinShard(ctx context.Context, index string, c *pql.Call, shard uint64) (ValCount, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "Executor.executeMinShard")
	defer span.Finish()

	var filter *Row
	if len(c.Children) == 1 {
		row, err := e.executeBitmapCallShard(ctx, index, c.Children[0], shard)
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

	return field.MinForShard(shard, filter)
}

// executeMaxShard calculates the max for bsiGroups on a shard.
func (e *executor) executeMaxShard(ctx context.Context, index string, c *pql.Call, shard uint64) (ValCount, error) {
	var filter *Row
	if len(c.Children) == 1 {
		row, err := e.executeBitmapCallShard(ctx, index, c.Children[0], shard)
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

	return field.MaxForShard(shard, filter)
}

// executeMinRowShard returns the minimum row ID for a shard.
func (e *executor) executeMinRowShard(ctx context.Context, index string, c *pql.Call, shard uint64) (PairField, error) {
	var filter *Row
	if len(c.Children) == 1 {
		row, err := e.executeBitmapCallShard(ctx, index, c.Children[0], shard)
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

	minRowID, count := fragment.minRow(filter)
	return PairField{
		Pair: Pair{
			ID:    minRowID,
			Count: count,
		},
		Field: fieldName,
	}, nil
}

// executeMaxRowShard returns the maximum row ID for a shard.
func (e *executor) executeMaxRowShard(ctx context.Context, index string, c *pql.Call, shard uint64) (PairField, error) {
	var filter *Row
	if len(c.Children) == 1 {
		row, err := e.executeBitmapCallShard(ctx, index, c.Children[0], shard)
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

	maxRowID, count := fragment.maxRow(filter)
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
func (e *executor) executeTopN(ctx context.Context, index string, c *pql.Call, shards []uint64, opt *execOptions) (*PairsField, error) {
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
	pairs, err := e.executeTopNShards(ctx, index, c, shards, opt)
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

	trimmedList, err := e.executeTopNShards(ctx, index, other, shards, opt)
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

func (e *executor) executeTopNShards(ctx context.Context, index string, c *pql.Call, shards []uint64, opt *execOptions) (*PairsField, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "Executor.executeTopNShards")
	defer span.Finish()

	// Execute calls in bulk on each remote node and merge.
	mapFn := func(shard uint64) (interface{}, error) {
		return e.executeTopNShard(ctx, index, c, shard)
	}

	// Merge returned results at coordinating node.
	reduceFn := func(prev, v interface{}) interface{} {
		other, _ := prev.(*PairsField)
		vpf, _ := v.(*PairsField)
		if other == nil {
			return vpf
		} else if vpf == nil {
			return other
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
func (e *executor) executeTopNShard(ctx context.Context, index string, c *pql.Call, shard uint64) (*PairsField, error) {
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
		row, err := e.executeBitmapCallShard(ctx, index, c.Children[0], shard)
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
	pairs, err := f.top(topOptions{
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
func (e *executor) executeDifferenceShard(ctx context.Context, index string, c *pql.Call, shard uint64) (*Row, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "Executor.executeDifferenceShard")
	defer span.Finish()

	var other *Row
	if len(c.Children) == 0 {
		return nil, fmt.Errorf("empty Difference query is currently not supported")
	}
	for i, input := range c.Children {
		row, err := e.executeBitmapCallShard(ctx, index, input, shard)
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

func (e *executor) executeGroupBy(ctx context.Context, index string, c *pql.Call, shards []uint64, opt *execOptions) ([]GroupCount, error) {
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

	// perform necessary Rows queries (any that have limit or columns args) -
	// TODO, call async? would only help if multiple Rows queries had a column
	// or limit arg.
	// TODO support TopN in here would be really cool - and pretty easy I think.
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
		if hasLimit || hasCol { // we need to perform this query cluster-wide ahead of executeGroupByShard
			childRows[i], err = e.executeRows(ctx, index, child, shards, opt)
			if err != nil {
				return nil, errors.Wrap(err, "getting rows for ")
			}
			if len(childRows[i]) == 0 { // there are no results because this field has no values.
				return []GroupCount{}, nil
			}
		}
	}

	// Execute calls in bulk on each remote node and merge.
	mapFn := func(shard uint64) (interface{}, error) {
		return e.executeGroupByShard(ctx, index, c, filter, shard, childRows)
	}
	// Merge returned results at coordinating node.
	reduceFn := func(prev, v interface{}) interface{} {
		other, _ := prev.([]GroupCount)
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

// MarshalJSON marshals FieldRow to JSON such that
// either a Key or an ID is included.
func (fr FieldRow) MarshalJSON() ([]byte, error) {
	if fr.RowKey != "" {
		return json.Marshal(struct {
			Field  string `json:"field"`
			RowKey string `json:"rowKey"`
		}{
			Field:  fr.Field,
			RowKey: fr.RowKey,
		})
	}

	if fr.Value != nil {
		return json.Marshal(struct {
			Field string `json:"field"`
			Value int64  `json:"value"`
		}{
			Field: fr.Field,
			Value: *fr.Value,
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

// GroupCount represents a result item for a group by query.
type GroupCount struct {
	Group []FieldRow `json:"group"`
	Count uint64     `json:"count"`
	Sum   int64      `json:"sum"`
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

func (e *executor) executeGroupByShard(ctx context.Context, index string, c *pql.Call, filter *pql.Call, shard uint64, childRows []RowIDs) (_ []GroupCount, err error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "Executor.executeGroupByShard")
	defer span.Finish()

	var filterRow *Row
	if filter != nil {
		if filterRow, err = e.executeBitmapCallShard(ctx, index, filter, shard); err != nil {
			return nil, errors.Wrapf(err, "executing group by filter for shard %d", shard)
		}
	}

	aggregate, _, err := c.CallArg("aggregate")
	if err != nil {
		return nil, err
	}

	newspan, ctx := tracing.StartSpanFromContext(ctx, "Executor.executeGroupByShard_newGroupByIterator")
	iter, err := newGroupByIterator(e, childRows, c.Children, aggregate, filterRow, index, shard, e.Holder)
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

	return results, nil
}

func (e *executor) executeRows(ctx context.Context, index string, c *pql.Call, shards []uint64, opt *execOptions) (RowIDs, error) {
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
	mapFn := func(shard uint64) (interface{}, error) {
		return e.executeRowsShard(ctx, index, fieldName, c, shard)
	}

	// Determine limit so we can use it when reducing.
	limit := int(^uint(0) >> 1)
	if lim, hasLimit, err := c.UintArg("limit"); err != nil {
		return nil, err
	} else if hasLimit {
		limit = int(lim)
	}

	// Merge returned results at coordinating node.
	reduceFn := func(prev, v interface{}) interface{} {
		other, _ := prev.(RowIDs)
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

func (e *executor) executeRowsShard(_ context.Context, index string, fieldName string, c *pql.Call, shard uint64) (RowIDs, error) {
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

	// Handle `time` fields.
	if f.Type() == FieldTypeTime {
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

	for _, view := range views {
		frag := e.Holder.fragment(index, fieldName, view, shard)
		if frag == nil {
			continue
		}

		viewRows := frag.rows(start, filters...)
		rowIDs = rowIDs.merge(viewRows, limit)
	}

	return rowIDs, nil
}

func (e *executor) executeRowShard(ctx context.Context, index string, c *pql.Call, shard uint64) (*Row, error) {
	span, _ := tracing.StartSpanFromContext(ctx, "Executor.executeRowShard")
	defer span.Finish()

	// Handle bsiGroup ranges differently.
	if c.HasConditionArg() {
		return e.executeRowBSIGroupShard(ctx, index, c, shard)
	}

	// Fetch column label from index.
	idx := e.Holder.Index(index)
	if idx == nil {
		return nil, ErrIndexNotFound
	}

	// Fetch field name from argument.
	fieldName, err := c.FieldArg()
	if err != nil {
		return nil, errors.New("Row() argument required: field")
	}
	f := e.Holder.Field(index, fieldName)
	if f == nil {
		return nil, ErrFieldNotFound
	}

	rowID, rowOK, rowErr := c.UintArg(fieldName)
	if rowErr != nil {
		return nil, fmt.Errorf("Row() error with arg for row: %v", rowErr)
	} else if !rowOK {
		return nil, fmt.Errorf("Row() must specify %v", rowLabel)
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

	// Simply return row if times are not set.
	if c.Name == "Row" && fromTime.IsZero() && toTime.IsZero() {
		frag := e.Holder.fragment(index, fieldName, viewStandard, shard)
		if frag == nil {
			return NewRow(), nil
		}
		return frag.row(rowID), nil
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
		rows = append(rows, f.row(rowID))
	}
	if len(rows) == 0 {
		return &Row{}, nil
	} else if len(rows) == 1 {
		return rows[0], nil
	}
	row := rows[0].Union(rows[1:]...)
	f.Stats.Count(MetricRow, 1, 1.0)
	return row, nil

}

// executeRowBSIGroupShard executes a range(bsiGroup) call for a local shard.
func (e *executor) executeRowBSIGroupShard(ctx context.Context, index string, c *pql.Call, shard uint64) (*Row, error) {
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

	// EQ null           (not implemented: flip frag.NotNull with max ColumnID)
	// NEQ null          frag.NotNull()
	// BETWEEN a,b(in)   BETWEEN/frag.RowBetween()
	// BETWEEN a,b(out)  BETWEEN/frag.NotNull()
	// EQ <int>          frag.RangeOp
	// NEQ <int>         frag.RangeOp

	// Handle `!= null`.
	if cond.Op == pql.NEQ && cond.Value == nil {
		// Find bsiGroup.
		bsig := f.bsiGroup(fieldName)
		if bsig == nil {
			return nil, ErrBSIGroupNotFound
		}

		// Retrieve fragment.
		frag := e.Holder.fragment(index, fieldName, viewBSIGroupPrefix+fieldName, shard)
		if frag == nil {
			return NewRow(), nil
		}

		f.Stats.Count(MetricRowBSI, 1, 1.0)
		return frag.notNull()

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
			return frag.notNull()
		}

		f.Stats.Count(MetricRowBSI, 1, 1.0)
		return frag.rangeBetween(bsig.BitDepth, baseValueMin, baseValueMax)

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
			return frag.notNull()
		}

		// outOfRange for NEQ should return all not-null.
		if outOfRange && cond.Op == pql.NEQ {
			return frag.notNull()
		}

		f.Stats.Count(MetricRowBSI, 1, 1.0)
		return frag.rangeOp(cond.Op, bsig.BitDepth, baseValue)
	}
}

// executeIntersectShard executes a intersect() call for a local shard.
func (e *executor) executeIntersectShard(ctx context.Context, index string, c *pql.Call, shard uint64) (*Row, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "Executor.executeIntersectShard")
	defer span.Finish()

	var other *Row
	if len(c.Children) == 0 {
		return nil, fmt.Errorf("empty Intersect query is currently not supported")
	}
	for i, input := range c.Children {
		row, err := e.executeBitmapCallShard(ctx, index, input, shard)
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

// executeGenericBitmapShard executes a generic bitmap call for a local shard.
func (e *executor) executeGenericBitmapShard(ctx context.Context, index string, c *pql.Call, op ext.BitmapOpBitmap, shard uint64) (*Row, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "Executor.executeGenericBitmapShard")
	defer span.Finish()

	if op.BitmapOpArity() == ext.OpArityUnary {
		if len(c.Children) != 1 {
			return nil, fmt.Errorf("%s needs exactly one row parameter", c.Name)
		}
		row, err := e.executeBitmapCallShard(ctx, index, c.Children[0], shard)
		if err != nil {
			return nil, err
		}
		return row.GenericUnaryOp(op.BitmapOpFunc(), c.Args), nil
	}

	var err error
	rows := make([]*Row, len(c.Children))
	for i, input := range c.Children {
		rows[i], err = e.executeBitmapCallShard(ctx, index, input, shard)
		if err != nil {
			return nil, err
		}
	}
	var other *Row
	switch op.BitmapOpArity() {
	case ext.OpArityBinary:
		other = rows[0]
		for _, row := range rows[1:] {
			other = other.GenericBinaryOp(op.BitmapOpFunc(), row, c.Args)
		}
	case ext.OpArityNary:
		other = rows[0].GenericNaryOp(op.BitmapOpFunc(), rows[1:], c.Args)
	}
	other.invalidateCount()
	return other, nil
}

// executeUnionShard executes a union() call for a local shard.
func (e *executor) executeUnionShard(ctx context.Context, index string, c *pql.Call, shard uint64) (*Row, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "Executor.executeUnionShard")
	defer span.Finish()

	other := NewRow()
	for i, input := range c.Children {
		row, err := e.executeBitmapCallShard(ctx, index, input, shard)
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
func (e *executor) executeXorShard(ctx context.Context, index string, c *pql.Call, shard uint64) (*Row, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "Executor.executeXorShard")
	defer span.Finish()

	other := NewRow()
	for i, input := range c.Children {
		row, err := e.executeBitmapCallShard(ctx, index, input, shard)
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
func (e *executor) executePrecomputedCallShard(ctx context.Context, index string, c *pql.Call, shard uint64) (*Row, error) {
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
func (e *executor) executeNotShard(ctx context.Context, index string, c *pql.Call, shard uint64) (*Row, error) {
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
		existenceRow = existenceFrag.row(0)
	}

	row, err := e.executeBitmapCallShard(ctx, index, c.Children[0], shard)
	if err != nil {
		return nil, err
	}

	return existenceRow.Difference(row), nil
}

// executeAllCallShard executes an All() call for a local shard.
func (e *executor) executeAllCallShard(ctx context.Context, index string, c *pql.Call, shard uint64) (*Row, error) {
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
		existenceRow = existenceFrag.row(0)
	}

	return existenceRow, nil
}

// executeShiftShard executes a shift() call for a local shard.
func (e *executor) executeShiftShard(ctx context.Context, index string, c *pql.Call, shard uint64) (*Row, error) {
	n, _, err := c.IntArg("n")
	if err != nil {
		return nil, fmt.Errorf("executeShiftShard: %v", err)
	}

	if len(c.Children) == 0 {
		return nil, errors.New("Shift() requires an input row")
	} else if len(c.Children) > 1 {
		return nil, errors.New("Shift() only accepts a single row input")
	}

	row, err := e.executeBitmapCallShard(ctx, index, c.Children[0], shard)
	if err != nil {
		return nil, err
	}

	return row.Shift(n)
}

// executeGeneric executes a provided count-like call.
func (e *executor) executeGenericCount(ctx context.Context, index string, c *pql.Call, op ext.BitmapOpUnaryCount, shards []uint64, opt *execOptions) (uint64, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "Executor.executeGenericCount")
	defer span.Finish()

	if len(c.Children) == 0 {
		return 0, fmt.Errorf("%s() requires an input bitmap", c.Name)
	} else if len(c.Children) > 1 {
		return 0, fmt.Errorf("%s() only accepts a single bitmap input", c.Name)
	}

	// Execute calls in bulk on each remote node and merge.
	mapFn := func(shard uint64) (interface{}, error) {
		row, err := e.executeBitmapCallShard(ctx, index, c.Children[0], shard)
		if err != nil {
			return 0, err
		}
		return row.GenericCount(op, c.Args), nil
	}

	// Merge returned results at coordinating node.
	reduceFn := func(prev, v interface{}) interface{} {
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

// executeCount executes a count() call.
func (e *executor) executeCount(ctx context.Context, index string, c *pql.Call, shards []uint64, opt *execOptions) (uint64, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "Executor.executeCount")
	defer span.Finish()

	if len(c.Children) == 0 {
		return 0, errors.New("Count() requires an input bitmap")
	} else if len(c.Children) > 1 {
		return 0, errors.New("Count() only accepts a single bitmap input")
	}

	// Execute calls in bulk on each remote node and merge.
	mapFn := func(shard uint64) (interface{}, error) {
		row, err := e.executeBitmapCallShard(ctx, index, c.Children[0], shard)
		if err != nil {
			return 0, err
		}
		return row.Count(), nil
	}

	// Merge returned results at coordinating node.
	reduceFn := func(prev, v interface{}) interface{} {
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
func (e *executor) executeClearBit(ctx context.Context, index string, c *pql.Call, opt *execOptions) (bool, error) {
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
		return e.executeClearValueField(ctx, index, c, f, colID, opt)
	}

	rowID, ok, err := c.UintArg(fieldName)
	if err != nil {
		return false, fmt.Errorf("reading Clear() row: %v", err)
	} else if !ok {
		return false, fmt.Errorf("row=<row> argument required to Clear() call")
	}

	return e.executeClearBitField(ctx, index, c, f, colID, rowID, opt)
}

// executeClearBitField executes a Clear() call for a field.
func (e *executor) executeClearBitField(ctx context.Context, index string, c *pql.Call, f *Field, colID, rowID uint64, opt *execOptions) (bool, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "Executor.executeClearBitField")
	defer span.Finish()

	shard := colID / ShardWidth
	ret := false
	for _, node := range e.Cluster.shardNodes(index, shard) {
		// Update locally if host matches.
		if node.ID == e.Node.ID {
			val, err := f.ClearBit(rowID, colID)
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
func (e *executor) executeClearRow(ctx context.Context, index string, c *pql.Call, shards []uint64, opt *execOptions) (bool, error) {
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
	mapFn := func(shard uint64) (interface{}, error) {
		return e.executeClearRowShard(ctx, index, c, shard)
	}

	// Merge returned results at coordinating node.
	reduceFn := func(prev, v interface{}) interface{} {
		val := v.(bool)
		if prev == nil {
			return val
		}
		return val || prev.(bool)
	}

	result, err := e.mapReduce(ctx, index, shards, c, opt, mapFn, reduceFn)
	if err != nil {
		return false, errors.Wrap(err, "mapreducing clearrow")
	}
	return result.(bool), err
}

// executeClearRowShard executes a ClearRow() call for a single shard.
func (e *executor) executeClearRowShard(ctx context.Context, index string, c *pql.Call, shard uint64) (bool, error) {
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
		cleared, err := fragment.clearRow(rowID)
		if err != nil {
			return false, errors.Wrapf(err, "clearing row %d on view %s shard %d", rowID, view.name, shard)
		}
		changed = changed || cleared
	}

	return changed, nil
}

// executeSetRow executes a Store() call.
func (e *executor) executeSetRow(ctx context.Context, index string, c *pql.Call, shards []uint64, opt *execOptions) (bool, error) {
	// Ensure the field type supports Store().
	fieldName, err := c.FieldArg()
	if err != nil {
		return false, errors.New("field required for Store()")
	}
	field := e.Holder.Field(index, fieldName)
	if field == nil {
		return false, ErrFieldNotFound
	}
	if field.Type() != FieldTypeSet {
		return false, fmt.Errorf("can't Store() on a %s field", field.Type())
	}

	// Execute calls in bulk on each remote node and merge.
	mapFn := func(shard uint64) (interface{}, error) {
		return e.executeSetRowShard(ctx, index, c, shard)
	}

	// Merge returned results at coordinating node.
	reduceFn := func(prev, v interface{}) interface{} {
		val := v.(bool)
		if prev == nil {
			return val
		}
		return val || prev.(bool)
	}

	result, err := e.mapReduce(ctx, index, shards, c, opt, mapFn, reduceFn)
	return result.(bool), err
}

// executeSetRowShard executes a SetRow() call for a single shard.
func (e *executor) executeSetRowShard(ctx context.Context, index string, c *pql.Call, shard uint64) (bool, error) {
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
		row, err := e.executeBitmapCallShard(ctx, index, c.Children[0], shard)
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
	set, err := fragment.setRow(src, rowID)
	if err != nil {
		return false, errors.Wrapf(err, "storing row %d on view %s shard %d", rowID, viewStandard, shard)
	}
	changed = changed || set

	return changed, nil
}

// executeSet executes a Set() call.
func (e *executor) executeSet(ctx context.Context, index string, c *pql.Call, opt *execOptions) (bool, error) {
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
		if _, err := ef.SetBit(0, colID, nil); err != nil {
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
		// Read row value.
		rowVal, err := getScaledInt(f, v)
		if err != nil {
			return false, fmt.Errorf("reading Set() row (int/decimal): %v", err)
		}
		return e.executeSetValueField(ctx, index, c, f, colID, rowVal, opt)

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

		return e.executeSetBitField(ctx, index, c, f, colID, rowID, timestamp, opt)
	}
}

// executeSetBitField executes a Set() call for a specific field.
func (e *executor) executeSetBitField(ctx context.Context, index string, c *pql.Call, f *Field, colID, rowID uint64, timestamp *time.Time, opt *execOptions) (bool, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "Executor.executeSetBitField")
	defer span.Finish()

	shard := colID / ShardWidth
	ret := false

	for _, node := range e.Cluster.shardNodes(index, shard) {
		// Update locally if host matches.
		if node.ID == e.Node.ID {
			val, err := f.SetBit(rowID, colID, timestamp)
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
func (e *executor) executeSetValueField(ctx context.Context, index string, c *pql.Call, f *Field, colID uint64, value int64, opt *execOptions) (bool, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "Executor.executeSetValueField")
	defer span.Finish()

	shard := colID / ShardWidth
	ret := false

	for _, node := range e.Cluster.shardNodes(index, shard) {
		// Update locally if host matches.
		if node.ID == e.Node.ID {
			val, err := f.SetValue(colID, value)
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
func (e *executor) executeClearValueField(ctx context.Context, index string, c *pql.Call, f *Field, colID uint64, opt *execOptions) (bool, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "Executor.executeClearValueField")
	defer span.Finish()

	shard := colID / ShardWidth
	ret := false

	for _, node := range e.Cluster.shardNodes(index, shard) {
		// Update locally if host matches.
		if node.ID == e.Node.ID {
			val, err := f.ClearValue(colID)
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
func (e *executor) executeSetRowAttrs(ctx context.Context, index string, c *pql.Call, opt *execOptions) error {
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
	field.Stats.Count(MetricSetRowAttrs, 1, 1.0)

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
func (e *executor) executeBulkSetRowAttrs(ctx context.Context, index string, calls []*pql.Call, opt *execOptions) ([]interface{}, error) {
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
		field.Stats.Count(MetricSetRowAttrs, 1, 1.0)
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
func (e *executor) executeSetColumnAttrs(ctx context.Context, index string, c *pql.Call, opt *execOptions) error {
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
	idx.Stats.Count(MetricSetProfileAttrs, 1, 1.0)
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
	if err := e.mapper(ctx, ch, nodes, index, shards, c, opt, mapFn, reduceFn); err != nil {
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
				if err := e.mapper(ctx, ch, nodes, index, resp.shards, c, opt, mapFn, reduceFn); errors.Cause(err) == errShardUnavailable {
					return nil, resp.err
				} else if err != nil {
					return nil, errors.Wrap(err, "calling mapper")
				}
				continue
			}

			// Reduce value.
			result = reduceFn(result, resp.result)

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

func (e *executor) mapper(ctx context.Context, ch chan mapResponse, nodes []*Node, index string, shards []uint64, c *pql.Call, opt *execOptions, mapFn mapFunc, reduceFn reduceFunc) error {
	span, ctx := tracing.StartSpanFromContext(ctx, "Executor.mapper")
	defer span.Finish()

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
			case <-ctx.Done():
			case ch <- resp:
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
		result, err := j.mapFn(j.shard)

		select {
		case <-j.ctx.Done():
		case j.resultChan <- mapResponse{result: result, err: err}:
		}
	}
}

// mapperLocal performs map & reduce entirely on the local node.
func (e *executor) mapperLocal(ctx context.Context, shards []uint64, mapFn mapFunc, reduceFn reduceFunc) (interface{}, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "Executor.mapperLocal")
	defer span.Finish()

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
		case <-ctx.Done():
			return nil, ctx.Err()
		case resp := <-ch:
			if resp.err != nil {
				return nil, resp.err
			}
			result = reduceFn(result, resp.result)
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
			// TODO: This code block doesn't make sense for a `Rows()`
			// queries on a `bool` field. Need to review this better,
			// include it in tests, and probably back-port it to Pilosa.
			if c.Name != "Rows" {
				boolVal, err := callArgBool(c, rowKey)
				if err != nil {
					return errors.Wrap(err, "getting bool key")
				}
				rowID := falseRowID
				if boolVal {
					rowID = trueRowID
				}
				c.Args[rowKey] = rowID
			}
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
			fieldname := callArgString(child, "_field")
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
				// TODO: does this need to take field.ForeignIndex() into consideration?
				id, err := e.Cluster.translateFieldKey(ctx, field, prevStr)
				if err != nil {
					return errors.Wrapf(err, "translating field key: %s", prevStr)
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
		results[i], err = e.translateResult(index, idx, calls[i], results[i], idMap)
		if err != nil {
			return err
		}
	}
	return nil
}

func (e *executor) collectResultIDs(index string, idx *Index, call *pql.Call, result interface{}, idSet map[uint64]struct{}) error {
	row, ok := result.(*Row)
	if !ok {
		return nil
	} else if !idx.Keys() {
		return nil
	}

	for _, segment := range row.Segments() {
		for _, col := range segment.Columns() {
			idSet[col] = struct{}{}
		}
	}
	return nil
}

func (e *executor) translateResult(index string, idx *Index, call *pql.Call, result interface{}, idSet map[uint64]string) (interface{}, error) {
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
					// TODO: does this need to take field.ForeignIndex() into consideration?
					key, err := field.TranslateStore().TranslateID(g.RowID)
					if err != nil {
						return nil, errors.Wrap(err, "translating row ID in Group")
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

type mapFunc func(shard uint64) (interface{}, error)

type reduceFunc func(prev, v interface{}) interface{}

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

// Field returns the field name associated to the signed row.
func (s *SignedRow) Field() string {
	return s.field
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
func newGroupByIterator(executor *executor, rowIDs []RowIDs, children []*pql.Call, aggregate *pql.Call, filter *Row, index string, shard uint64, holder *Holder) (*groupByIterator, error) {
	gbi := &groupByIterator{
		executor: executor,
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
		gbi.rowIters[i] = frag.rowIterator(i != 0, filters...)

		prev, hasPrev, err := call.UintArg("previous")
		if err != nil {
			return nil, errors.Wrap(err, "getting previous")
		} else if hasPrev && !ignorePrev {
			if i == len(children)-1 {
				prev++
			}
			gbi.rowIters[i].Seek(prev)
		}
		nextRow, rowID, value, wrapped := gbi.rowIters[i].Next()
		if nextRow == nil {
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
				nextRow, rowID, value, wrapped := gbi.rowIters[j].Next()
				if nextRow == nil {
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
func (gbi *groupByIterator) nextAtIdx(i int) {
	// loop until we find a non-empty row. This is an optimization - the loop and if/break can be removed.
	for {
		nr, rowID, value, wrapped := gbi.rowIters[i].Next()
		if nr == nil {
			gbi.done = true
			return
		}
		if wrapped && i != 0 {
			gbi.nextAtIdx(i - 1)
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
}

// Next returns a GroupCount representing the next group by record. When there
// are no more records it will return an empty GroupCount and done==true.
func (gbi *groupByIterator) Next(ctx context.Context) (ret GroupCount, done bool, err error) {
	// loop until we find a result with count > 0
	for {
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
				result, err := gbi.executor.executeSumCountShard(ctx, gbi.index, gbi.aggregate, filter, gbi.shard)
				if err != nil {
					return ret, false, err
				}
				ret.Count = uint64(result.Count)
				ret.Sum = result.Val
			}
		}
		if ret.Count == 0 {
			gbi.nextAtIdx(len(gbi.rows) - 1)
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
	gbi.nextAtIdx(len(gbi.rows) - 1)

	return ret, false, nil
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
// the field type.
func getScaledInt(f *Field, v interface{}) (int64, error) {
	var value int64

	opt := f.Options()
	if opt.Type == FieldTypeDecimal {
		scale := opt.Scale
		switch tv := v.(type) {
		case int64:
			value = int64(float64(tv) * math.Pow10(int(scale)))
		case uint64:
			value = int64(float64(tv) * math.Pow10(int(scale)))
		case pql.Decimal:
			value = tv.ToInt64(scale)
		case float64:
			value = int64(tv * math.Pow10(int(scale)))
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
