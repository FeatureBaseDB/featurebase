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
	"sort"
	"time"

	"github.com/pilosa/pilosa/pql"
	"github.com/pilosa/pilosa/shardwidth"
	"github.com/pilosa/pilosa/tracing"
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

	// Stores key/id translation data.
	TranslateStore TranslateStore
}

// executorOption is a functional option type for pilosa.Executor
type executorOption func(e *executor) error

func optExecutorInternalQueryClient(c InternalQueryClient) executorOption {
	return func(e *executor) error {
		e.client = c
		return nil
	}
}

// newExecutor returns a new instance of Executor.
func newExecutor(opts ...executorOption) *executor {
	e := &executor{
		client: newNopInternalQueryClient(),
	}
	for _, opt := range opts {
		err := opt(e)
		if err != nil {
			panic(err)
		}
	}
	return e
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
		if err := e.translateCalls(ctx, index, idx, q.Calls); err != nil {
			return resp, err
		} else if err := validateQueryContext(ctx); err != nil {
			return resp, err
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
			for _, col := range columnAttrSets {
				v, err := e.Holder.translateFile.TranslateColumnToString(index, col.ID)
				if err != nil {
					return resp, err
				}
				col.Key, col.ID = v, 0
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

		v, err := e.executeCall(ctx, index, call, shards, opt)
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
	indexTag := fmt.Sprintf("index:%s", index)
	// Special handling for mutation and top-n calls.
	switch c.Name {
	case "Sum":
		e.Holder.Stats.CountWithCustomTags(c.Name, 1, 1.0, []string{indexTag})
		return e.executeSum(ctx, index, c, shards, opt)
	case "Min":
		e.Holder.Stats.CountWithCustomTags(c.Name, 1, 1.0, []string{indexTag})
		return e.executeMin(ctx, index, c, shards, opt)
	case "Max":
		e.Holder.Stats.CountWithCustomTags(c.Name, 1, 1.0, []string{indexTag})
		return e.executeMax(ctx, index, c, shards, opt)
	case "Clear":
		return e.executeClearBit(ctx, index, c, opt)
	case "ClearRow":
		return e.executeClearRow(ctx, index, c, shards, opt)
	case "Store":
		return e.executeSetRow(ctx, index, c, shards, opt)
	case "Count":
		e.Holder.Stats.CountWithCustomTags(c.Name, 1, 1.0, []string{indexTag})
		return e.executeCount(ctx, index, c, shards, opt)
	case "Set":
		return e.executeSet(ctx, index, c, opt)
	case "SetRowAttrs":
		return nil, e.executeSetRowAttrs(ctx, index, c, opt)
	case "SetColumnAttrs":
		return nil, e.executeSetColumnAttrs(ctx, index, c, opt)
	case "TopN":
		e.Holder.Stats.CountWithCustomTags(c.Name, 1, 1.0, []string{indexTag})
		return e.executeTopN(ctx, index, c, shards, opt)
	case "Rows":
		e.Holder.Stats.CountWithCustomTags(c.Name, 1, 1.0, []string{indexTag})
		return e.executeRows(ctx, index, c, shards, opt)
	case "GroupBy":
		e.Holder.Stats.CountWithCustomTags(c.Name, 1, 1.0, []string{indexTag})
		return e.executeGroupBy(ctx, index, c, shards, opt)
	case "Options":
		return e.executeOptionsCall(ctx, index, c, shards, opt)
	default:
		e.Holder.Stats.CountWithCustomTags(c.Name, 1, 1.0, []string{indexTag})
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

// executeSum executes a Sum() call.
func (e *executor) executeSum(ctx context.Context, index string, c *pql.Call, shards []uint64, opt *execOptions) (ValCount, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "Executor.executeSum")
	defer span.Finish()

	if field := c.Args["field"]; field == "" {
		return ValCount{}, errors.New("Sum(): field required")
	}

	if len(c.Children) > 1 {
		return ValCount{}, errors.New("Sum() only accepts a single bitmap input")
	}

	// Execute calls in bulk on each remote node and merge.
	mapFn := func(shard uint64) (interface{}, error) {
		return e.executeSumCountShard(ctx, index, c, shard)
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
	default:
		return nil, fmt.Errorf("unknown call: %s", c.Name)
	}
}

// executeSumCountShard calculates the sum and count for bsiGroups on a shard.
func (e *executor) executeSumCountShard(ctx context.Context, index string, c *pql.Call, shard uint64) (ValCount, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "Executor.executeSumCountShard")
	defer span.Finish()

	var filter *Row
	if len(c.Children) == 1 {
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

	vsum, vcount, err := fragment.sum(filter, bsig.BitDepth())
	if err != nil {
		return ValCount{}, errors.Wrap(err, "computing sum")
	}
	return ValCount{
		Val:   int64(vsum) + (int64(vcount) * bsig.Min),
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

	bsig := field.bsiGroup(fieldName)
	if bsig == nil {
		return ValCount{}, nil
	}

	fragment := e.Holder.fragment(index, fieldName, viewBSIGroupPrefix+fieldName, shard)
	if fragment == nil {
		return ValCount{}, nil
	}

	fmin, fcount, err := fragment.min(filter, bsig.BitDepth())
	if err != nil {
		return ValCount{}, err
	}
	return ValCount{
		Val:   int64(fmin) + bsig.Min,
		Count: int64(fcount),
	}, nil
}

// executeMaxShard calculates the max for bsiGroups on a shard.
func (e *executor) executeMaxShard(ctx context.Context, index string, c *pql.Call, shard uint64) (ValCount, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "Executor.executeMaxShard")
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

	bsig := field.bsiGroup(fieldName)
	if bsig == nil {
		return ValCount{}, nil
	}

	fragment := e.Holder.fragment(index, fieldName, viewBSIGroupPrefix+fieldName, shard)
	if fragment == nil {
		return ValCount{}, nil
	}

	fmax, fcount, err := fragment.max(filter, bsig.BitDepth())
	if err != nil {
		return ValCount{}, err
	}
	return ValCount{
		Val:   int64(fmax) + bsig.Min,
		Count: int64(fcount),
	}, nil
}

// executeTopN executes a TopN() call.
// This first performs the TopN() to determine the top results and then
// requeries to retrieve the full counts for each of the top results.
func (e *executor) executeTopN(ctx context.Context, index string, c *pql.Call, shards []uint64, opt *execOptions) ([]Pair, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "Executor.executeTopN")
	defer span.Finish()

	idsArg, _, err := c.UintSliceArg("ids")
	if err != nil {
		return nil, fmt.Errorf("executeTopN: %v", err)
	}
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
	if len(pairs) == 0 || len(idsArg) > 0 || opt.Remote {
		return pairs, nil
	}
	// Only the original caller should refetch the full counts.
	other := c.Clone()

	ids := Pairs(pairs).Keys()
	sort.Sort(uint64Slice(ids))
	other.Args["ids"] = ids

	trimmedList, err := e.executeTopNShards(ctx, index, other, shards, opt)
	if err != nil {
		return nil, errors.Wrap(err, "retrieving full counts")
	}

	if n != 0 && int(n) < len(trimmedList) {
		trimmedList = trimmedList[0:n]
	}
	return trimmedList, nil
}

func (e *executor) executeTopNShards(ctx context.Context, index string, c *pql.Call, shards []uint64, opt *execOptions) ([]Pair, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "Executor.executeTopNShards")
	defer span.Finish()

	// Execute calls in bulk on each remote node and merge.
	mapFn := func(shard uint64) (interface{}, error) {
		return e.executeTopNShard(ctx, index, c, shard)
	}

	// Merge returned results at coordinating node.
	reduceFn := func(prev, v interface{}) interface{} {
		other, _ := prev.([]Pair)
		return Pairs(other).Add(v.([]Pair))
	}

	other, err := e.mapReduce(ctx, index, shards, c, opt, mapFn, reduceFn)
	if err != nil {
		return nil, err
	}
	results, _ := other.([]Pair)

	// Sort final merged results.
	sort.Sort(Pairs(results))

	return results, nil
}

// executeTopNShard executes a TopN call for a single shard.
func (e *executor) executeTopNShard(ctx context.Context, index string, c *pql.Call, shard uint64) ([]Pair, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "Executor.executeTopNShard")
	defer span.Finish()

	field, _ := c.Args["_field"].(string)
	n, _, err := c.UintArg("n")
	if err != nil {
		return nil, fmt.Errorf("executeTopNShard: %v", err)
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
	if field == "" {
		field = defaultField
	}

	f := e.Holder.fragment(index, field, viewStandard, shard)
	if f == nil {
		return nil, nil
	}

	if minThreshold == 0 {
		minThreshold = defaultMinThreshold
	}

	if tanimotoThreshold > 100 {
		return nil, errors.New("Tanimoto Threshold is from 1 to 100 only")
	}
	return f.top(topOptions{
		N:                 int(n),
		Src:               src,
		RowIDs:            rowIDs,
		FilterName:        attrName,
		FilterValues:      attrValues,
		MinThreshold:      minThreshold,
		TanimotoThreshold: tanimotoThreshold,
	})
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
	Rows []uint64 `json:"rows"`
	Keys []string `json:"keys,omitempty"`
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
	return fmt.Sprintf("%s.%d.%s", fr.Field, fr.RowID, fr.RowKey)
}

// GroupCount represents a result item for a group by query.
type GroupCount struct {
	Group []FieldRow `json:"group"`
	Count uint64     `json:"count"`
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
	for i := range g.Group {
		if g.Group[i].RowID < o.Group[i].RowID {
			return -1
		}
		if g.Group[i].RowID > o.Group[i].RowID {
			return 1
		}
	}
	return 0
}

func (e *executor) executeGroupByShard(ctx context.Context, index string, c *pql.Call, filter *pql.Call, shard uint64, childRows []RowIDs) (_ []GroupCount, err error) {
	var filterRow *Row
	if filter != nil {
		if filterRow, err = e.executeBitmapCallShard(ctx, index, filter, shard); err != nil {
			return nil, errors.Wrapf(err, "executing group by filter for shard %d", shard)
		}
	}

	iter, err := newGroupByIterator(childRows, c.Children, filterRow, index, shard, e.Holder)
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
	for gc, done := iter.Next(); !done && num < limit; gc, done = iter.Next() {
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

	if c.Name == "Range" {
		e.Holder.Logger.Printf("DEPRECATED: Range() is deprecated, please use Row() instead.")
	}

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
	row := &Row{}
	for _, view := range viewsByTimeRange(viewStandard, fromTime, toTime, q) {
		f := e.Holder.fragment(index, fieldName, view, shard)
		if f == nil {
			continue
		}
		row = row.Union(f.row(rowID))
	}
	f.Stats.Count("range", 1, 1.0)
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

		return frag.notNull(bsig.BitDepth())

	} else if cond.Op == pql.BETWEEN {

		predicates, err := cond.IntSliceValue()
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
			return frag.notNull(bsig.BitDepth())
		}

		return frag.rangeBetween(bsig.BitDepth(), baseValueMin, baseValueMax)

	} else {

		// Only support integers for now.
		value, ok := cond.Value.(int64)
		if !ok {
			return nil, errors.New("Row(): conditions only support integer values")
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
			return frag.notNull(bsig.BitDepth())
		}

		// outOfRange for NEQ should return all not-null.
		if outOfRange && cond.Op == pql.NEQ {
			return frag.notNull(bsig.BitDepth())
		}

		f.Stats.Count("range:bsigroup", 1, 1.0)
		return frag.rangeOp(cond.Op, bsig.BitDepth(), baseValue)
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

// executeNotShard executes a not() call for a local shard.
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

	// Read fields using labels.
	rowID, ok, err := c.UintArg(fieldName)
	if err != nil {
		return false, fmt.Errorf("reading Clear() row: %v", err)
	} else if !ok {
		return false, fmt.Errorf("row=<row> argument required to Clear() call")
	}

	colID, ok, err := c.UintArg("_" + columnLabel)
	if err != nil {
		return false, fmt.Errorf("reading Clear() column: %v", err)
	} else if !ok {
		return false, fmt.Errorf("column argument to Clear(<COLUMN>, <FIELD>=<ROW>) required")
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
		res, err := e.remoteExec(ctx, node, index, &pql.Query{Calls: []*pql.Call{c}}, nil)
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

	// Int field.
	if f.Type() == FieldTypeInt {
		// Read row value.
		rowVal, ok, err := c.IntArg(fieldName)
		if err != nil {
			return false, fmt.Errorf("reading Set() row: %v", err)
		} else if !ok {
			return false, fmt.Errorf("Set() row argument '%v' required", rowLabel)
		}

		return e.executeSetValueField(ctx, index, c, f, colID, rowVal, opt)
	}

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
		res, err := e.remoteExec(ctx, node, index, &pql.Query{Calls: []*pql.Call{c}}, nil)
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
		res, err := e.remoteExec(ctx, node, index, &pql.Query{Calls: []*pql.Call{c}}, nil)
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
	field.Stats.Count("SetRowAttrs", 1, 1.0)

	// Do not forward call if this is already being forwarded.
	if opt.Remote {
		return nil
	}

	// Execute on remote nodes in parallel.
	nodes := Nodes(e.Cluster.nodes).FilterID(e.Node.ID)
	resp := make(chan error, len(nodes))
	for _, node := range nodes {
		go func(node *Node) {
			_, err := e.remoteExec(ctx, node, index, &pql.Query{Calls: []*pql.Call{c}}, nil)
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
		field.Stats.Count("SetRowAttrs", 1, 1.0)
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
			_, err := e.remoteExec(ctx, node, index, &pql.Query{Calls: calls}, nil)
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
	idx.Stats.Count("SetProfileAttrs", 1, 1.0)
	// Do not forward call if this is already being forwarded.
	if opt.Remote {
		return nil
	}

	// Execute on remote nodes in parallel.
	nodes := Nodes(e.Cluster.nodes).FilterID(e.Node.ID)
	resp := make(chan error, len(nodes))
	for _, node := range nodes {
		go func(node *Node) {
			_, err := e.remoteExec(ctx, node, index, &pql.Query{Calls: []*pql.Call{c}}, nil)
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
func (e *executor) remoteExec(ctx context.Context, node *Node, index string, q *pql.Query, shards []uint64) (results []interface{}, err error) { // nolint: interfacer
	span, ctx := tracing.StartSpanFromContext(ctx, "Executor.executeExec")
	defer span.Finish()

	// Encode request object.
	pbreq := &QueryRequest{
		Query:  q.String(),
		Shards: shards,
		Remote: true,
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
				results, err := e.remoteExec(ctx, n, index, &pql.Query{Calls: []*pql.Call{c}}, nodeShards)
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

// mapperLocal performs map & reduce entirely on the local node.
func (e *executor) mapperLocal(ctx context.Context, shards []uint64, mapFn mapFunc, reduceFn reduceFunc) (interface{}, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "Executor.mapperLocal")
	defer span.Finish()

	ch := make(chan mapResponse, len(shards))

	for _, shard := range shards {
		go func(shard uint64) {
			result, err := mapFn(shard)

			// Return response to the channel.
			select {
			case <-ctx.Done():
			case ch <- mapResponse{result: result, err: err}:
			}
		}(shard)
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

func (e *executor) translateCalls(ctx context.Context, index string, idx *Index, calls []*pql.Call) error {
	span, _ := tracing.StartSpanFromContext(ctx, "Executor.translateCalls")
	defer span.Finish()

	for i := range calls {
		if err := e.translateCall(index, idx, calls[i]); err != nil {
			return err
		}
	}
	return nil
}

func (e *executor) translateCall(index string, idx *Index, c *pql.Call) error {
	var colKey, rowKey, fieldName string
	switch c.Name {
	case "Set", "Clear", "Row", "Range", "SetColumnAttrs", "ClearRow":
		// Positional args in new PQL syntax require special handling here.
		colKey = "_" + columnLabel
		fieldName, _ = c.FieldArg()
		rowKey = fieldName
	case "SetRowAttrs":
		// Positional args in new PQL syntax require special handling here.
		rowKey = "_" + rowLabel
		fieldName = callArgString(c, "_field")
	case "Rows":
		fieldName = callArgString(c, "_field")
		rowKey = "previous"
		colKey = "column"
	case "GroupBy":
		return errors.Wrap(e.translateGroupByCall(index, idx, c), "translating GroupBy")
	default:
		colKey = "col"
		fieldName = callArgString(c, "field")
		rowKey = "row"
	}
	// Translate column key.
	if idx.Keys() {
		if c.Args[colKey] != nil && !isString(c.Args[colKey]) {
			return errors.New("column value must be a string when index 'keys' option enabled")
		}
		if value := callArgString(c, colKey); value != "" {
			ids, err := e.TranslateStore.TranslateColumnsToUint64(index, []string{value})
			if err != nil {
				return err
			}
			c.Args[colKey] = ids[0]
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
			boolVal, err := callArgBool(c, rowKey)
			if err != nil {
				return errors.Wrap(err, "getting bool key")
			}
			rowID := falseRowID
			if boolVal {
				rowID = trueRowID
			}
			c.Args[rowKey] = rowID
		} else if field.keys() {
			if c.Args[rowKey] != nil && !isString(c.Args[rowKey]) {
				return errors.New("row value must be a string when field 'keys' option enabled")
			}
			if value := callArgString(c, rowKey); value != "" {
				ids, err := e.TranslateStore.TranslateRowsToUint64(index, fieldName, []string{value})
				if err != nil {
					return err
				}
				c.Args[rowKey] = ids[0]
			}
		} else {
			if isString(c.Args[rowKey]) {
				return errors.New("string 'row' value not allowed unless field 'keys' option enabled")
			}
		}
	}

	// Translate child calls.
	for _, child := range c.Children {
		if err := e.translateCall(index, idx, child); err != nil {
			return err
		}
	}

	return nil
}

func (e *executor) translateGroupByCall(index string, idx *Index, c *pql.Call) error {
	if c.Name != "GroupBy" {
		panic("translateGroupByCall called with '" + c.Name + "'")
	}

	for _, child := range c.Children {
		if err := e.translateCall(index, idx, child); err != nil {
			return errors.Wrapf(err, "translating %s", child)
		}
	}

	if filter, ok, err := c.CallArg("filter"); ok {
		if err != nil {
			return errors.Wrap(err, "getting filter call")
		}
		err = e.translateCall(index, idx, filter)
		if err != nil {
			return errors.Wrap(err, "translating filter call")
		}
	}

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
		if field.keys() {
			prevStr, ok := prev.(string)
			if !ok {
				return errors.New("prev value must be a string when field 'keys' option enabled")
			}
			ids, err := e.TranslateStore.TranslateRowsToUint64(index, field.Name(), []string{prevStr})
			if err != nil {
				return errors.Wrapf(err, "translating row key '%s'", prevStr)
			}
			previous[i] = ids[0]
		} else {
			if prevStr, ok := prev.(string); ok {
				return errors.Errorf("got string row val '%s' in 'previous' for field %s which doesn't use string keys", prevStr, field.Name())
			}
		}

	}
	return nil
}

func (e *executor) translateResults(ctx context.Context, index string, idx *Index, calls []*pql.Call, results []interface{}) (err error) {
	span, _ := tracing.StartSpanFromContext(ctx, "Executor.translateResults")
	defer span.Finish()

	for i := range results {
		results[i], err = e.translateResult(index, idx, calls[i], results[i])
		if err != nil {
			return err
		}
	}
	return nil
}

func (e *executor) translateResult(index string, idx *Index, call *pql.Call, result interface{}) (interface{}, error) {
	switch result := result.(type) {
	case *Row:
		if idx.Keys() {
			other := &Row{Attrs: result.Attrs}
			for _, segment := range result.Segments() {
				for _, col := range segment.Columns() {
					key, err := e.TranslateStore.TranslateColumnToString(index, col)
					if err != nil {
						return nil, err
					}
					other.Keys = append(other.Keys, key)
				}
			}
			return other, nil
		}

	case []Pair:
		if fieldName := callArgString(call, "_field"); fieldName != "" {
			field := idx.Field(fieldName)
			if field == nil {
				return nil, ErrFieldNotFound
			}
			if field.keys() {
				other := make([]Pair, len(result))
				for i := range result {
					key, err := e.TranslateStore.TranslateRowToString(index, fieldName, result[i].ID)
					if err != nil {
						return nil, err
					}
					other[i] = Pair{Key: key, Count: result[i].Count}
				}
				return other, nil
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
				if field.keys() {
					key, err := e.TranslateStore.TranslateRowToString(index, g.Field, g.RowID)
					if err != nil {
						return nil, errors.Wrap(err, "translating row ID in Group")
					}
					group[i].RowKey = key
				}
			}

			other = append(other, GroupCount{
				Group: group,
				Count: gl.Count,
			})
		}
		return other, nil

	case RowIDs:
		other := RowIdentifiers{}

		fieldName := callArgString(call, "_field")
		if fieldName == "" {
			return nil, ErrFieldNotFound
		}

		if field := idx.Field(fieldName); field == nil {
			return nil, ErrFieldNotFound
		} else if field.keys() {
			other.Keys = make([]string, len(result))
			for i, id := range result {
				key, err := e.TranslateStore.TranslateRowToString(index, fieldName, id)
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
	ExcludeRowAttrs bool
	ExcludeColumns  bool
	ColumnAttrs     bool
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

// ValCount represents a grouping of sum & count for Sum() and Average() calls.
type ValCount struct {
	Val   int64 `json:"value"`
	Count int64 `json:"count"`
}

func (vc *ValCount) add(other ValCount) ValCount {
	return ValCount{
		Val:   vc.Val + other.Val,
		Count: vc.Count + other.Count,
	}
}

// smaller returns the smaller of the two ValCounts.
func (vc *ValCount) smaller(other ValCount) ValCount {
	if vc.Count == 0 || (other.Val < vc.Val && other.Count > 0) {
		return other
	}
	return ValCount{
		Val:   vc.Val,
		Count: vc.Count,
	}
}

// larger returns the larger of the two ValCounts.
func (vc *ValCount) larger(other ValCount) ValCount {
	if vc.Count == 0 || (other.Val > vc.Val && other.Count > 0) {
		return other
	}
	return ValCount{
		Val:   vc.Val,
		Count: vc.Count,
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

// groupByIterator contains several slices. Each slice contains a number of
// elements equal to the number of fields in the group by (the number of Rows
// calls).
type groupByIterator struct {
	// rowIters contains a rowIterator for each of the fields in the Group By.
	rowIters []*rowIterator
	// rows contains the current row data for each of the fields in the Group
	// By. Each row is the intersection of itself and the rows of the fields
	// with an index lower than its own. This is a performance optimization so
	// that the expected common case of getting the next row in the furthest
	// field to the right require only a single intersect with the row of the
	// previous field to determine the count of the new group.
	rows []struct {
		row *Row
		id  uint64
	}

	// fields helps with the construction of GroupCount results by holding all
	// the field names that are being grouped by. Each results makes a copy of
	// fields and then sets the row ids.
	fields []FieldRow
	done   bool

	// Optional filter row to intersect against first level of values.
	filter *Row
}

// newGroupByIterator initializes a new groupByIterator.
func newGroupByIterator(rowIDs []RowIDs, children []*pql.Call, filter *Row, index string, shard uint64, holder *Holder) (*groupByIterator, error) {
	gbi := &groupByIterator{
		rowIters: make([]*rowIterator, len(children)),
		rows: make([]struct {
			row *Row
			id  uint64
		}, len(children)),
		filter: filter,
		fields: make([]FieldRow, len(children)),
	}

	var fieldName string
	var ok bool
	ignorePrev := false
	for i, call := range children {
		if fieldName, ok = call.Args["_field"].(string); !ok {
			return nil, errors.Errorf("%s call must have field with valid (string) field name. Got %v of type %[2]T", call.Name, call.Args["_field"])
		}
		if holder.Field(index, fieldName) == nil {
			return nil, ErrFieldNotFound
		}
		gbi.fields[i].Field = fieldName
		// Fetch fragment.
		frag := holder.fragment(index, fieldName, viewStandard, shard)
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
		nextRow, rowID, wrapped := gbi.rowIters[i].Next()
		if nextRow == nil {
			gbi.done = true
			return gbi, nil
		}
		gbi.rows[i].row = nextRow
		gbi.rows[i].id = rowID
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
				nextRow, rowID, wrapped := gbi.rowIters[j].Next()
				if nextRow == nil {
					gbi.done = true
					return gbi, nil
				}
				gbi.rows[j].row = nextRow
				gbi.rows[j].id = rowID
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
		nr, rowID, wrapped := gbi.rowIters[i].Next()
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

		if !gbi.rows[i].row.IsEmpty() {
			break
		}
	}
}

// Next returns a GroupCount representing the next group by record. When there
// are no more records it will return an empty GroupCount and done==true.
func (gbi *groupByIterator) Next() (ret GroupCount, done bool) {
	// loop until we find a result with count > 0
	for {
		if gbi.done {
			return ret, true
		}
		if len(gbi.rows) == 1 {
			ret.Count = gbi.rows[len(gbi.rows)-1].row.Count()
		} else {
			ret.Count = gbi.rows[len(gbi.rows)-1].row.intersectionCount(gbi.rows[len(gbi.rows)-2].row)
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
	}

	// set up for next call

	gbi.nextAtIdx(len(gbi.rows) - 1)

	return ret, false
}
