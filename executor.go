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
	"fmt"
	"sort"
	"time"

	"github.com/pilosa/pilosa/pql"
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
func (e *executor) Execute(ctx context.Context, index string, q *pql.Query, shards []uint64, opt *execOptions) ([]interface{}, error) {
	// Verify that an index is set.
	if index == "" {
		return nil, ErrIndexRequired
	}

	idx := e.Holder.Index(index)
	if idx == nil {
		return nil, ErrIndexNotFound
	}

	// Verify that the number of writes do not exceed the maximum.
	if e.MaxWritesPerRequest > 0 && q.WriteCallN() > e.MaxWritesPerRequest {
		return nil, ErrTooManyWrites
	}

	// Default options.
	if opt == nil {
		opt = &execOptions{}
	}

	// Translate query keys to ids, if necessary.
	// No need to translate a remote call.
	if !opt.Remote {
		for i := range q.Calls {
			if err := e.translateCall(index, idx, q.Calls[i]); err != nil {
				return nil, err
			}
		}
	}

	results, err := e.execute(ctx, index, q, shards, opt)
	if err != nil {
		return nil, err
	}

	// Translate response objects from ids to keys, if necessary.
	// No need to translate a remote call.
	if !opt.Remote {
		for i := range results {
			results[i], err = e.translateResult(index, idx, q.Calls[i], results[i])
			if err != nil {
				return nil, err
			}
		}
	}
	return results, nil
}

func (e *executor) execute(ctx context.Context, index string, q *pql.Query, shards []uint64, opt *execOptions) ([]interface{}, error) {
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
	if err := e.validateCallArgs(c); err != nil {
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

	// Attach attributes for Row() calls.
	// If the column label is used then return column attributes.
	// If the row label is used then return bitmap attributes.
	row, _ := other.(*Row)
	if c.Name == "Row" {
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
	switch c.Name {
	case "Row":
		return e.executeBitmapShard(ctx, index, c, shard)
	case "Difference":
		return e.executeDifferenceShard(ctx, index, c, shard)
	case "Intersect":
		return e.executeIntersectShard(ctx, index, c, shard)
	case "Range":
		return e.executeRangeShard(ctx, index, c, shard)
	case "Union":
		return e.executeUnionShard(ctx, index, c, shard)
	case "Xor":
		return e.executeXorShard(ctx, index, c, shard)
	case "Not":
		return e.executeNotShard(ctx, index, c, shard)
	default:
		return nil, fmt.Errorf("unknown call: %s", c.Name)
	}
}

// executeSumCountShard calculates the sum and count for bsiGroups on a shard.
func (e *executor) executeSumCountShard(ctx context.Context, index string, c *pql.Call, shard uint64) (ValCount, error) {
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

	if minThreshold <= 0 {
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

func (e *executor) executeBitmapShard(_ context.Context, index string, c *pql.Call, shard uint64) (*Row, error) {
	// Fetch column label from index.
	idx := e.Holder.Index(index)
	if idx == nil {
		return nil, ErrIndexNotFound
	}

	// Fetch field & row label based on argument.
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

	frag := e.Holder.fragment(index, fieldName, viewStandard, shard)
	if frag == nil {
		return NewRow(), nil
	}
	return frag.row(rowID), nil
}

// executeIntersectShard executes a intersect() call for a local shard.
func (e *executor) executeIntersectShard(ctx context.Context, index string, c *pql.Call, shard uint64) (*Row, error) {
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

// executeRangeShard executes a range() call for a local shard.
func (e *executor) executeRangeShard(ctx context.Context, index string, c *pql.Call, shard uint64) (*Row, error) {
	// Handle bsiGroup ranges differently.
	if c.HasConditionArg() {
		return e.executeBSIGroupRangeShard(ctx, index, c, shard)
	}

	// Parse field.
	fieldName, err := c.FieldArg()
	if err != nil {
		return nil, errors.New("Range() argument required: field")
	}

	// Retrieve column label.
	idx := e.Holder.Index(index)
	if idx == nil {
		return nil, ErrIndexNotFound
	}

	// Retrieve base field.
	f := idx.Field(fieldName)
	if f == nil {
		return nil, ErrFieldNotFound
	}

	// Read row & column id.
	rowID, rowOK, err := c.UintArg(fieldName)
	if err != nil {
		return nil, fmt.Errorf("executeRangeShard - reading row: %v", err)
	}
	if !rowOK {
		return nil, fmt.Errorf("Range() must specify %q", rowLabel)
	}

	// Parse start time.
	startTimeStr, ok := c.Args["_start"].(string)
	if !ok {
		return nil, errors.New("Range() start time required")
	}
	startTime, err := time.Parse(TimeFormat, startTimeStr)
	if err != nil {
		return nil, errors.New("cannot parse Range() start time")
	}

	// Parse end time.
	endTimeStr, ok := c.Args["_end"].(string)
	if !ok {
		return nil, errors.New("Range() end time required")
	}
	endTime, err := time.Parse(TimeFormat, endTimeStr)
	if err != nil {
		return nil, errors.New("cannot parse Range() end time")
	}

	// If no quantum exists then return an empty bitmap.
	q := f.TimeQuantum()
	if q == "" {
		return &Row{}, nil
	}

	// Union bitmaps across all time-based views.
	row := &Row{}
	for _, view := range viewsByTimeRange(viewStandard, startTime, endTime, q) {
		f := e.Holder.fragment(index, fieldName, view, shard)
		if f == nil {
			continue
		}
		row = row.Union(f.row(rowID))
	}
	f.Stats.Count("range", 1, 1.0)
	return row, nil
}

// executeBSIGroupRangeShard executes a range(bsiGroup) call for a local shard.
func (e *executor) executeBSIGroupRangeShard(_ context.Context, index string, c *pql.Call, shard uint64) (*Row, error) {
	// Only one conditional should be present.
	if len(c.Args) == 0 {
		return nil, errors.New("Range(): condition required")
	} else if len(c.Args) > 1 {
		return nil, errors.New("Range(): too many arguments")
	}

	// Extract conditional.
	var fieldName string
	var cond *pql.Condition
	for k, v := range c.Args {
		vv, ok := v.(*pql.Condition)
		if !ok {
			return nil, fmt.Errorf("Range(): %q: expected condition argument, got %v", k, v)
		}
		fieldName, cond = k, vv
	}

	f := e.Holder.Field(index, fieldName)
	if f == nil {
		return nil, ErrFieldNotFound
	}

	// EQ null           (not implemented: flip frag.NotNull with max ColumnID)
	// NEQ null          frag.NotNull()
	// BETWEEN a,b(in)   BETWEEN/frag.RangeBetween()
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
			return nil, errors.New("Range(): BETWEEN condition requires exactly two integer values")
		}

		// The reason we don't just call:
		//     return f.RangeBetween(fieldName, predicates[0], predicates[1])
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
			return nil, errors.New("Range(): conditions only support integer values")
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

// executeUnionShard executes a union() call for a local shard.
func (e *executor) executeUnionShard(ctx context.Context, index string, c *pql.Call, shard uint64) (*Row, error) {
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

// executeCount executes a count() call.
func (e *executor) executeCount(ctx context.Context, index string, c *pql.Call, shards []uint64, opt *execOptions) (uint64, error) {
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
		return false, fmt.Errorf("Clear() row argument '%v' required", rowLabel)
	}

	colID, ok, err := c.UintArg("_" + columnLabel)
	if err != nil {
		return false, fmt.Errorf("reading Clear() column: %v", err)
	} else if !ok {
		return false, fmt.Errorf("Clear() col argument '%v' required", columnLabel)
	}

	return e.executeClearBitField(ctx, index, c, f, colID, rowID, opt)
}

// executeClearBitField executes a Clear() call for a field.
func (e *executor) executeClearBitField(ctx context.Context, index string, c *pql.Call, f *Field, colID, rowID uint64, opt *execOptions) (bool, error) {
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
		if res, err := e.remoteExec(ctx, node, index, &pql.Query{Calls: []*pql.Call{c}}, nil); err != nil {
			return false, err
		} else {
			ret = res[0].(bool)
		}
	}
	return ret, nil
}

// executeClearRow executes a ClearRow() call.
func (e *executor) executeClearRow(ctx context.Context, index string, c *pql.Call, shards []uint64, opt *execOptions) (bool, error) {
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
	return result.(bool), err
}

// executeClearRowShard executes a ClearRow() call for a single shard.
func (e *executor) executeClearRowShard(_ context.Context, index string, c *pql.Call, shard uint64) (bool, error) {
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

// executeSet executes a Set() call.
func (e *executor) executeSet(ctx context.Context, index string, c *pql.Call, opt *execOptions) (bool, error) {
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
		if res, err := e.remoteExec(ctx, node, index, &pql.Query{Calls: []*pql.Call{c}}, nil); err != nil {
			return false, err
		} else {
			ret = res[0].(bool)
		}
	}
	return ret, nil
}

// executeSetValueField executes a Set() call for a specific int field.
func (e *executor) executeSetValueField(ctx context.Context, index string, c *pql.Call, f *Field, colID uint64, value int64, opt *execOptions) (bool, error) {
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
		if res, err := e.remoteExec(ctx, node, index, &pql.Query{Calls: []*pql.Call{c}}, nil); err != nil {
			return false, err
		} else {
			ret = res[0].(bool)
		}
	}
	return ret, nil
}

// executeSetRowAttrs executes a SetRowAttrs() call.
func (e *executor) executeSetRowAttrs(ctx context.Context, index string, c *pql.Call, opt *execOptions) error {
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
	// Collect attributes by field/id.
	m := make(map[string]map[uint64]map[string]interface{})
	for _, c := range calls {
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
		for _, node := range e.Cluster.shardNodes(index, shard) {
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
		nodes = []*Node{e.Cluster.unprotectedNodeByID(e.Node.ID)}
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

func (e *executor) translateCall(index string, idx *Index, c *pql.Call) error {
	var colKey, rowKey, fieldName string
	if c.Name == "Set" || c.Name == "Clear" || c.Name == "Row" {
		// Positional args in new PQL syntax require special handling here.
		colKey = "_" + columnLabel
		fieldName, _ = c.FieldArg()
		rowKey = fieldName
	} else if c.Name == "SetRowAttrs" {
		// Positional args in new PQL syntax require special handling here.
		rowKey = "_" + rowLabel
		fieldName = callArgString(c, "_field")
	} else {
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
	}
	return result, nil
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
		case "Count", "TopN":
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
