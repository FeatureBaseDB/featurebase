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

	"github.com/pilosa/pilosa/internal"
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

// Executor recursively executes calls in a PQL query across all slices.
type Executor struct {
	Holder *Holder

	// Local hostname & cluster configuration.
	Node    *Node
	Cluster *Cluster

	// Client used for remote requests.
	client InternalQueryClient

	// Maximum number of Set() or Clear() commands per request.
	MaxWritesPerRequest int

	// Stores key/id translation data.
	TranslateStore TranslateStore
}

// ExecutorOption is a functional option type for pilosa.Executor
type ExecutorOption func(e *Executor) error

func OptExecutorInternalQueryClient(c InternalQueryClient) ExecutorOption {
	return func(e *Executor) error {
		e.client = c
		return nil
	}
}

// NewExecutor returns a new instance of Executor.
func NewExecutor(opts ...ExecutorOption) *Executor {
	e := &Executor{
		client: NewNopInternalQueryClient(),
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
func (e *Executor) Execute(ctx context.Context, index string, q *pql.Query, slices []uint64, opt *ExecOptions) ([]interface{}, error) {
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
		opt = &ExecOptions{}
	}

	// Translate query keys to ids, if necessary.
	for i := range q.Calls {
		if err := e.translateCall(index, idx, q.Calls[i]); err != nil {
			return nil, err
		}
	}

	results, err := e.execute(ctx, index, q, slices, opt)
	if err != nil {
		return nil, err
	}

	// Translate response objects from ids to keys, if necessary.
	for i := range results {
		results[i], err = e.translateResult(index, idx, q.Calls[i], results[i])
		if err != nil {
			return nil, err
		}
	}
	return results, nil
}

func (e *Executor) execute(ctx context.Context, index string, q *pql.Query, slices []uint64, opt *ExecOptions) ([]interface{}, error) {
	// Don't bother calculating slices for query types that don't require it.
	needsSlices := needsSlices(q.Calls)

	// If slices are specified, then use that value for slices. If slices aren't
	// specified, then include all of them.
	if len(slices) == 0 && needsSlices {
		// Round up the number of slices.
		idx := e.Holder.Index(index)
		if idx == nil {
			return nil, ErrIndexNotFound
		}
		maxSlice := idx.MaxSlice()

		// Generate a slices of all slices.
		slices = make([]uint64, maxSlice+1)
		for i := range slices {
			slices[i] = uint64(i)
		}
	}

	// Optimize handling for bulk attribute insertion.
	if hasOnlySetRowAttrs(q.Calls) {
		return e.executeBulkSetRowAttrs(ctx, index, q.Calls, opt)
	}

	// Execute each call serially.
	results := make([]interface{}, 0, len(q.Calls))
	for _, call := range q.Calls {
		v, err := e.executeCall(ctx, index, call, slices, opt)
		if err != nil {
			return nil, err
		}
		results = append(results, v)
	}
	return results, nil
}

// executeCall executes a call.
func (e *Executor) executeCall(ctx context.Context, index string, c *pql.Call, slices []uint64, opt *ExecOptions) (interface{}, error) {
	if err := e.validateCallArgs(c); err != nil {
		return nil, errors.Wrap(err, "validating args")
	}
	indexTag := fmt.Sprintf("index:%s", index)
	// Special handling for mutation and top-n calls.
	switch c.Name {
	case "Sum":
		e.Holder.Stats.CountWithCustomTags(c.Name, 1, 1.0, []string{indexTag})
		return e.executeSum(ctx, index, c, slices, opt)
	case "Min":
		e.Holder.Stats.CountWithCustomTags(c.Name, 1, 1.0, []string{indexTag})
		return e.executeMin(ctx, index, c, slices, opt)
	case "Max":
		e.Holder.Stats.CountWithCustomTags(c.Name, 1, 1.0, []string{indexTag})
		return e.executeMax(ctx, index, c, slices, opt)
	case "Clear":
		return e.executeClearBit(ctx, index, c, opt)
	case "Count":
		e.Holder.Stats.CountWithCustomTags(c.Name, 1, 1.0, []string{indexTag})
		return e.executeCount(ctx, index, c, slices, opt)
	case "Set":
		return e.executeSetBit(ctx, index, c, opt)
	case "SetValue":
		return nil, e.executeSetValue(ctx, index, c, opt)
	case "SetRowAttrs":
		return nil, e.executeSetRowAttrs(ctx, index, c, opt)
	case "SetColumnAttrs":
		return nil, e.executeSetColumnAttrs(ctx, index, c, opt)
	case "TopN":
		e.Holder.Stats.CountWithCustomTags(c.Name, 1, 1.0, []string{indexTag})
		return e.executeTopN(ctx, index, c, slices, opt)
	default:
		e.Holder.Stats.CountWithCustomTags(c.Name, 1, 1.0, []string{indexTag})
		return e.executeBitmapCall(ctx, index, c, slices, opt)
	}
}

// validateCallArgs ensures that the value types in call.Args are expected.
func (e *Executor) validateCallArgs(c *pql.Call) error {
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

// executeSum executes a Sum() call.
func (e *Executor) executeSum(ctx context.Context, index string, c *pql.Call, slices []uint64, opt *ExecOptions) (ValCount, error) {
	if field := c.Args["field"]; field == "" {
		return ValCount{}, errors.New("Sum(): field required")
	}

	if len(c.Children) > 1 {
		return ValCount{}, errors.New("Sum() only accepts a single bitmap input")
	}

	// Execute calls in bulk on each remote node and merge.
	mapFn := func(slice uint64) (interface{}, error) {
		return e.executeSumCountSlice(ctx, index, c, slice)
	}

	// Merge returned results at coordinating node.
	reduceFn := func(prev, v interface{}) interface{} {
		other, _ := prev.(ValCount)
		return other.Add(v.(ValCount))
	}

	result, err := e.mapReduce(ctx, index, slices, c, opt, mapFn, reduceFn)
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
func (e *Executor) executeMin(ctx context.Context, index string, c *pql.Call, slices []uint64, opt *ExecOptions) (ValCount, error) {
	if field := c.Args["field"]; field == "" {
		return ValCount{}, errors.New("Min(): field required")
	}

	if len(c.Children) > 1 {
		return ValCount{}, errors.New("Min() only accepts a single bitmap input")
	}

	// Execute calls in bulk on each remote node and merge.
	mapFn := func(slice uint64) (interface{}, error) {
		return e.executeMinSlice(ctx, index, c, slice)
	}

	// Merge returned results at coordinating node.
	reduceFn := func(prev, v interface{}) interface{} {
		other, _ := prev.(ValCount)
		return other.Smaller(v.(ValCount))
	}

	result, err := e.mapReduce(ctx, index, slices, c, opt, mapFn, reduceFn)
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
func (e *Executor) executeMax(ctx context.Context, index string, c *pql.Call, slices []uint64, opt *ExecOptions) (ValCount, error) {
	if field := c.Args["field"]; field == "" {
		return ValCount{}, errors.New("Max(): field required")
	}

	if len(c.Children) > 1 {
		return ValCount{}, errors.New("Max() only accepts a single bitmap input")
	}

	// Execute calls in bulk on each remote node and merge.
	mapFn := func(slice uint64) (interface{}, error) {
		return e.executeMaxSlice(ctx, index, c, slice)
	}

	// Merge returned results at coordinating node.
	reduceFn := func(prev, v interface{}) interface{} {
		other, _ := prev.(ValCount)
		return other.Larger(v.(ValCount))
	}

	result, err := e.mapReduce(ctx, index, slices, c, opt, mapFn, reduceFn)
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
func (e *Executor) executeBitmapCall(ctx context.Context, index string, c *pql.Call, slices []uint64, opt *ExecOptions) (*Row, error) {
	// Execute calls in bulk on each remote node and merge.
	mapFn := func(slice uint64) (interface{}, error) {
		return e.executeBitmapCallSlice(ctx, index, c, slice)
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

	other, err := e.mapReduce(ctx, index, slices, c, opt, mapFn, reduceFn)
	if err != nil {
		return nil, err
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
		row.segments = []RowSegment{}
	}

	return row, nil
}

// executeBitmapCallSlice executes a bitmap call for a single slice.
func (e *Executor) executeBitmapCallSlice(ctx context.Context, index string, c *pql.Call, slice uint64) (*Row, error) {
	switch c.Name {
	case "Row":
		return e.executeBitmapSlice(ctx, index, c, slice)
	case "Difference":
		return e.executeDifferenceSlice(ctx, index, c, slice)
	case "Intersect":
		return e.executeIntersectSlice(ctx, index, c, slice)
	case "Range":
		return e.executeRangeSlice(ctx, index, c, slice)
	case "Union":
		return e.executeUnionSlice(ctx, index, c, slice)
	case "Xor":
		return e.executeXorSlice(ctx, index, c, slice)
	default:
		return nil, fmt.Errorf("unknown call: %s", c.Name)
	}
}

// executeSumCountSlice calculates the sum and count for bsiGroups on a slice.
func (e *Executor) executeSumCountSlice(ctx context.Context, index string, c *pql.Call, slice uint64) (ValCount, error) {
	var filter *Row
	if len(c.Children) == 1 {
		row, err := e.executeBitmapCallSlice(ctx, index, c.Children[0], slice)
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

	fragment := e.Holder.Fragment(index, fieldName, viewBSIGroupPrefix+fieldName, slice)
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

// executeMinSlice calculates the min for bsiGroups on a slice.
func (e *Executor) executeMinSlice(ctx context.Context, index string, c *pql.Call, slice uint64) (ValCount, error) {
	var filter *Row
	if len(c.Children) == 1 {
		row, err := e.executeBitmapCallSlice(ctx, index, c.Children[0], slice)
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

	fragment := e.Holder.Fragment(index, fieldName, viewBSIGroupPrefix+fieldName, slice)
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

// executeMaxSlice calculates the max for bsiGroups on a slice.
func (e *Executor) executeMaxSlice(ctx context.Context, index string, c *pql.Call, slice uint64) (ValCount, error) {
	var filter *Row
	if len(c.Children) == 1 {
		row, err := e.executeBitmapCallSlice(ctx, index, c.Children[0], slice)
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

	fragment := e.Holder.Fragment(index, fieldName, viewBSIGroupPrefix+fieldName, slice)
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
func (e *Executor) executeTopN(ctx context.Context, index string, c *pql.Call, slices []uint64, opt *ExecOptions) ([]Pair, error) {
	idsArg, _, err := c.UintSliceArg("ids")
	if err != nil {
		return nil, fmt.Errorf("executeTopN: %v", err)
	}
	n, _, err := c.UintArg("n")
	if err != nil {
		return nil, fmt.Errorf("executeTopN: %v", err)
	}

	// Execute original query.
	pairs, err := e.executeTopNSlices(ctx, index, c, slices, opt)
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

	trimmedList, err := e.executeTopNSlices(ctx, index, other, slices, opt)
	if err != nil {
		return nil, errors.Wrap(err, "retrieving full counts")
	}

	if n != 0 && int(n) < len(trimmedList) {
		trimmedList = trimmedList[0:n]
	}
	return trimmedList, nil
}

func (e *Executor) executeTopNSlices(ctx context.Context, index string, c *pql.Call, slices []uint64, opt *ExecOptions) ([]Pair, error) {
	// Execute calls in bulk on each remote node and merge.
	mapFn := func(slice uint64) (interface{}, error) {
		return e.executeTopNSlice(ctx, index, c, slice)
	}

	// Merge returned results at coordinating node.
	reduceFn := func(prev, v interface{}) interface{} {
		other, _ := prev.([]Pair)
		return Pairs(other).Add(v.([]Pair))
	}

	other, err := e.mapReduce(ctx, index, slices, c, opt, mapFn, reduceFn)
	if err != nil {
		return nil, err
	}
	results, _ := other.([]Pair)

	// Sort final merged results.
	sort.Sort(Pairs(results))

	return results, nil
}

// executeTopNSlice executes a TopN call for a single slice.
func (e *Executor) executeTopNSlice(ctx context.Context, index string, c *pql.Call, slice uint64) ([]Pair, error) {
	field, _ := c.Args["_field"].(string)
	n, _, err := c.UintArg("n")
	if err != nil {
		return nil, fmt.Errorf("executeTopNSlice: %v", err)
	}
	attrName, _ := c.Args["attrName"].(string)
	rowIDs, _, err := c.UintSliceArg("ids")
	if err != nil {
		return nil, fmt.Errorf("executeTopNSlice: %v", err)
	}
	minThreshold, _, err := c.UintArg("threshold")
	if err != nil {
		return nil, fmt.Errorf("executeTopNSlice: %v", err)
	}
	attrValues, _ := c.Args["attrValues"].([]interface{})
	tanimotoThreshold, _, err := c.UintArg("tanimotoThreshold")
	if err != nil {
		return nil, fmt.Errorf("executeTopNSlice: %v", err)
	}

	// Retrieve bitmap used to intersect.
	var src *Row
	if len(c.Children) == 1 {
		row, err := e.executeBitmapCallSlice(ctx, index, c.Children[0], slice)
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

	f := e.Holder.Fragment(index, field, ViewStandard, slice)
	if f == nil {
		return nil, nil
	}

	if minThreshold <= 0 {
		minThreshold = defaultMinThreshold
	}

	if tanimotoThreshold > 100 {
		return nil, errors.New("Tanimoto Threshold is from 1 to 100 only")
	}
	return f.top(TopOptions{
		N:                 int(n),
		Src:               src,
		RowIDs:            rowIDs,
		FilterName:        attrName,
		FilterValues:      attrValues,
		MinThreshold:      minThreshold,
		TanimotoThreshold: tanimotoThreshold,
	})
}

// executeDifferenceSlice executes a difference() call for a local slice.
func (e *Executor) executeDifferenceSlice(ctx context.Context, index string, c *pql.Call, slice uint64) (*Row, error) {
	var other *Row
	if len(c.Children) == 0 {
		return nil, fmt.Errorf("empty Difference query is currently not supported")
	}
	for i, input := range c.Children {
		row, err := e.executeBitmapCallSlice(ctx, index, input, slice)
		if err != nil {
			return nil, err
		}

		if i == 0 {
			other = row
		} else {
			other = other.Difference(row)
		}
	}
	other.InvalidateCount()
	return other, nil
}

func (e *Executor) executeBitmapSlice(ctx context.Context, index string, c *pql.Call, slice uint64) (*Row, error) {
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
	}
	if !rowOK {
		return nil, fmt.Errorf("Row() must specify %v", rowLabel)
	}

	frag := e.Holder.Fragment(index, fieldName, ViewStandard, slice)
	if frag == nil {
		return NewRow(), nil
	}
	return frag.row(rowID), nil
}

// executeIntersectSlice executes a intersect() call for a local slice.
func (e *Executor) executeIntersectSlice(ctx context.Context, index string, c *pql.Call, slice uint64) (*Row, error) {
	var other *Row
	if len(c.Children) == 0 {
		return nil, fmt.Errorf("empty Intersect query is currently not supported")
	}
	for i, input := range c.Children {
		row, err := e.executeBitmapCallSlice(ctx, index, input, slice)
		if err != nil {
			return nil, err
		}

		if i == 0 {
			other = row
		} else {
			other = other.Intersect(row)
		}
	}
	other.InvalidateCount()
	return other, nil
}

// executeRangeSlice executes a range() call for a local slice.
func (e *Executor) executeRangeSlice(ctx context.Context, index string, c *pql.Call, slice uint64) (*Row, error) {
	// Handle bsiGroup ranges differently.
	if c.HasConditionArg() {
		return e.executeBSIGroupRangeSlice(ctx, index, c, slice)
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
		return nil, fmt.Errorf("executeRangeSlice - reading row: %v", err)
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
	for _, view := range viewsByTimeRange(ViewStandard, startTime, endTime, q) {
		f := e.Holder.Fragment(index, fieldName, view.name, slice)
		if f == nil {
			continue
		}
		row = row.Union(f.row(rowID))
	}
	f.Stats.Count("range", 1, 1.0)
	return row, nil
}

// executeBSIGroupRangeSlice executes a range(bsiGroup) call for a local slice.
func (e *Executor) executeBSIGroupRangeSlice(ctx context.Context, index string, c *pql.Call, slice uint64) (*Row, error) {
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
		frag := e.Holder.Fragment(index, fieldName, viewBSIGroupPrefix+fieldName, slice)
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
		// here is because we need the call to be slice-specific.

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
		frag := e.Holder.Fragment(index, fieldName, viewBSIGroupPrefix+fieldName, slice)
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
		frag := e.Holder.Fragment(index, fieldName, viewBSIGroupPrefix+fieldName, slice)
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

// executeUnionSlice executes a union() call for a local slice.
func (e *Executor) executeUnionSlice(ctx context.Context, index string, c *pql.Call, slice uint64) (*Row, error) {
	other := NewRow()
	for i, input := range c.Children {
		row, err := e.executeBitmapCallSlice(ctx, index, input, slice)
		if err != nil {
			return nil, err
		}

		if i == 0 {
			other = row
		} else {
			other = other.Union(row)
		}
	}
	other.InvalidateCount()
	return other, nil
}

// executeXorSlice executes a xor() call for a local slice.
func (e *Executor) executeXorSlice(ctx context.Context, index string, c *pql.Call, slice uint64) (*Row, error) {
	other := NewRow()
	for i, input := range c.Children {
		row, err := e.executeBitmapCallSlice(ctx, index, input, slice)
		if err != nil {
			return nil, err
		}

		if i == 0 {
			other = row
		} else {
			other = other.Xor(row)
		}
	}
	other.InvalidateCount()
	return other, nil
}

// executeCount executes a count() call.
func (e *Executor) executeCount(ctx context.Context, index string, c *pql.Call, slices []uint64, opt *ExecOptions) (uint64, error) {
	if len(c.Children) == 0 {
		return 0, errors.New("Count() requires an input bitmap")
	} else if len(c.Children) > 1 {
		return 0, errors.New("Count() only accepts a single bitmap input")
	}

	// Execute calls in bulk on each remote node and merge.
	mapFn := func(slice uint64) (interface{}, error) {
		row, err := e.executeBitmapCallSlice(ctx, index, c.Children[0], slice)
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

	result, err := e.mapReduce(ctx, index, slices, c, opt, mapFn, reduceFn)
	if err != nil {
		return 0, err
	}
	n, _ := result.(uint64)

	return n, nil
}

// executeClearBit executes a Clear() call.
func (e *Executor) executeClearBit(ctx context.Context, index string, c *pql.Call, opt *ExecOptions) (bool, error) {
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

// executeClearBitField executes a Clear() call for a single view.
func (e *Executor) executeClearBitField(ctx context.Context, index string, c *pql.Call, f *Field, colID, rowID uint64, opt *ExecOptions) (bool, error) {
	slice := colID / SliceWidth
	ret := false
	for _, node := range e.Cluster.sliceNodes(index, slice) {
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
		if res, err := e.remoteExec(ctx, node, index, &pql.Query{Calls: []*pql.Call{c}}, nil, opt); err != nil {
			return false, err
		} else {
			ret = res[0].(bool)
		}
	}
	return ret, nil
}

// executeSetBit executes a Set() call.
func (e *Executor) executeSetBit(ctx context.Context, index string, c *pql.Call, opt *ExecOptions) (bool, error) {
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

	// Read fields using labels.
	rowID, ok, err := c.UintArg(fieldName)
	if err != nil {
		return false, fmt.Errorf("reading Set() row: %v", err)
	} else if !ok {
		return false, fmt.Errorf("Set() row argument '%v' required", rowLabel)
	}

	colID, ok, err := c.UintArg("_" + columnLabel)
	if err != nil {
		return false, fmt.Errorf("reading Set() column: %v", err)
	} else if !ok {
		return false, fmt.Errorf("Set() column argument '%v' required", columnLabel)
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

// executeSetBitField executes a Set() call for a specific view.
func (e *Executor) executeSetBitField(ctx context.Context, index string, c *pql.Call, f *Field, colID, rowID uint64, timestamp *time.Time, opt *ExecOptions) (bool, error) {
	slice := colID / SliceWidth
	ret := false

	for _, node := range e.Cluster.sliceNodes(index, slice) {
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
		if res, err := e.remoteExec(ctx, node, index, &pql.Query{Calls: []*pql.Call{c}}, nil, opt); err != nil {
			return false, err
		} else {
			ret = res[0].(bool)
		}
	}
	return ret, nil
}

// executeSetValue executes a SetValue() call.
func (e *Executor) executeSetValue(ctx context.Context, index string, c *pql.Call, opt *ExecOptions) error {
	// Parse labels.
	columnID, ok, err := c.UintArg(columnLabel)
	if err != nil {
		return fmt.Errorf("reading SetValue() column: %v", err)
	} else if !ok {
		return fmt.Errorf("SetValue() column field '%v' required", columnLabel)
	}

	// Copy args and remove reserved fields.
	args := pql.CopyArgs(c.Args)
	// While field could technically work as a ColumnAttr argument, we are treating it as a reserved word primarily to avoid confusion.
	// Also, if we ever need to make ColumnAttrs field-specific, then having this reserved word prevents backward incompatibility.
	delete(args, columnLabel)

	// Set values.
	for name, value := range args {
		// Retrieve field.
		field := e.Holder.Field(index, name)
		if field == nil {
			return ErrFieldNotFound
		}

		switch value := value.(type) {
		case int64:
			if _, err := field.SetValue(columnID, value); err != nil {
				return err
			}
		default:
			return ErrInvalidBSIGroupValueType
		}
		field.Stats.Count("SetValue", 1, 1.0)
	}

	// Do not forward call if this is already being forwarded.
	if opt.Remote {
		return nil
	}

	// Execute on remote nodes in parallel.
	nodes := Nodes(e.Cluster.Nodes).FilterID(e.Node.ID)
	resp := make(chan error, len(nodes))
	for _, node := range nodes {
		go func(node *Node) {
			_, err := e.remoteExec(ctx, node, index, &pql.Query{Calls: []*pql.Call{c}}, nil, opt)
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

// executeSetRowAttrs executes a SetRowAttrs() call.
func (e *Executor) executeSetRowAttrs(ctx context.Context, index string, c *pql.Call, opt *ExecOptions) error {
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
	nodes := Nodes(e.Cluster.Nodes).FilterID(e.Node.ID)
	resp := make(chan error, len(nodes))
	for _, node := range nodes {
		go func(node *Node) {
			_, err := e.remoteExec(ctx, node, index, &pql.Query{Calls: []*pql.Call{c}}, nil, opt)
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
func (e *Executor) executeBulkSetRowAttrs(ctx context.Context, index string, calls []*pql.Call, opt *ExecOptions) ([]interface{}, error) {
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
			return nil, fmt.Errorf("reading SetRowAttrs() row: %v", rowLabel)
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
	nodes := Nodes(e.Cluster.Nodes).FilterID(e.Node.ID)
	resp := make(chan error, len(nodes))
	for _, node := range nodes {
		go func(node *Node) {
			_, err := e.remoteExec(ctx, node, index, &pql.Query{Calls: calls}, nil, opt)
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
func (e *Executor) executeSetColumnAttrs(ctx context.Context, index string, c *pql.Call, opt *ExecOptions) error {
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
	nodes := Nodes(e.Cluster.Nodes).FilterID(e.Node.ID)
	resp := make(chan error, len(nodes))
	for _, node := range nodes {
		go func(node *Node) {
			_, err := e.remoteExec(ctx, node, index, &pql.Query{Calls: []*pql.Call{c}}, nil, opt)
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

// exec executes a PQL query remotely for a set of slices on a node.
func (e *Executor) remoteExec(ctx context.Context, node *Node, index string, q *pql.Query, slices []uint64, opt *ExecOptions) (results []interface{}, err error) {
	// Encode request object.
	pbreq := &internal.QueryRequest{
		Query:  q.String(),
		Slices: slices,
		Remote: true,
	}

	pb, err := e.client.QueryNode(ctx, &node.URI, index, pbreq)
	if err != nil {
		return nil, err
	}

	// Return an error, if specified on response.
	if err := decodeError(pb.Err); err != nil {
		return nil, err
	}

	// Return appropriate data for the query.
	results = make([]interface{}, len(q.Calls))
	for i, call := range q.Calls {
		var v interface{}
		var err error

		switch call.Name {
		case "Average", "Sum":
			v, err = decodeValCount(pb.Results[i].GetValCount()), nil
		case "TopN":
			v, err = decodePairs(pb.Results[i].GetPairs()), nil
		case "Count":
			v, err = pb.Results[i].N, nil
		case "Set":
			v, err = pb.Results[i].Changed, nil
		case "Clear":
			v, err = pb.Results[i].Changed, nil
		case "SetRowAttrs":
		case "SetColumnAttrs":
		default:
			v, err = DecodeRow(pb.Results[i].GetRow()), nil
		}
		if err != nil {
			return nil, err
		}

		results[i] = v
	}
	return results, nil
}

// slicesByNode returns a mapping of nodes to slices.
// Returns errSliceUnavailable if a slice cannot be allocated to a node.
func (e *Executor) slicesByNode(nodes []*Node, index string, slices []uint64) (map[*Node][]uint64, error) {
	m := make(map[*Node][]uint64)

loop:
	for _, slice := range slices {
		for _, node := range e.Cluster.sliceNodes(index, slice) {
			if Nodes(nodes).Contains(node) {
				m[node] = append(m[node], slice)
				continue loop
			}
		}
		return nil, errSliceUnavailable
	}
	return m, nil
}

// mapReduce maps and reduces data across the cluster.
//
// If a mapping of slices to a node fails then the slices are resplit across
// secondary nodes and retried. This continues to occur until all nodes are exhausted.
func (e *Executor) mapReduce(ctx context.Context, index string, slices []uint64, c *pql.Call, opt *ExecOptions, mapFn mapFunc, reduceFn reduceFunc) (interface{}, error) {
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
		nodes = Nodes(e.Cluster.Nodes).Clone()
	} else {
		nodes = []*Node{e.Cluster.unprotectedNodeByID(e.Node.ID)}
	}

	// Start mapping across all primary owners.
	if err := e.mapper(ctx, ch, nodes, index, slices, c, opt, mapFn, reduceFn); err != nil {
		return nil, errors.Wrap(err, "starting mapper")
	}

	// Iterate over all map responses and reduce.
	var result interface{}
	var maxSlice int
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case resp := <-ch:
			// On error retry against remaining nodes. If an error returns then
			// the context will cancel and cause all open goroutines to return.

			if resp.err != nil {
				// Filter out unavailable nodes.
				nodes = Nodes(nodes).Filter(resp.node)

				// Begin mapper against secondary nodes.
				if err := e.mapper(ctx, ch, nodes, index, resp.slices, c, opt, mapFn, reduceFn); err == errSliceUnavailable {
					return nil, resp.err
				} else if err != nil {
					return nil, err
				}
				continue
			}

			// Reduce value.
			result = reduceFn(result, resp.result)

			// If all slices have been processed then return.
			maxSlice += len(resp.slices)
			if maxSlice >= len(slices) {
				return result, nil
			}
		}
	}
}

func (e *Executor) mapper(ctx context.Context, ch chan mapResponse, nodes []*Node, index string, slices []uint64, c *pql.Call, opt *ExecOptions, mapFn mapFunc, reduceFn reduceFunc) error {
	// Group slices together by nodes.
	m, err := e.slicesByNode(nodes, index, slices)
	if err != nil {
		return err
	}

	// Execute each node in a separate goroutine.
	for n, nodeSlices := range m {
		go func(n *Node, nodeSlices []uint64) {
			resp := mapResponse{node: n, slices: nodeSlices}

			// Send local slices to mapper, otherwise remote exec.
			if n.ID == e.Node.ID {
				resp.result, resp.err = e.mapperLocal(ctx, nodeSlices, mapFn, reduceFn)
			} else if !opt.Remote {
				results, err := e.remoteExec(ctx, n, index, &pql.Query{Calls: []*pql.Call{c}}, nodeSlices, opt)
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
		}(n, nodeSlices)
	}

	return nil
}

// mapperLocal performs map & reduce entirely on the local node.
func (e *Executor) mapperLocal(ctx context.Context, slices []uint64, mapFn mapFunc, reduceFn reduceFunc) (interface{}, error) {
	ch := make(chan mapResponse, len(slices))

	for _, slice := range slices {
		go func(slice uint64) {
			result, err := mapFn(slice)

			// Return response to the channel.
			select {
			case <-ctx.Done():
			case ch <- mapResponse{result: result, err: err}:
			}
		}(slice)
	}

	// Reduce results
	var maxSlice int
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
			maxSlice++
		}

		// Exit once all slices are processed.
		if maxSlice == len(slices) {
			return result, nil
		}
	}
}

func (e *Executor) translateCall(index string, idx *Index, c *pql.Call) error {
	var colKey, rowKey, fieldName string
	if c.Name == "Set" || c.Name == "Clear" || c.Name == "Row" {
		// Positional args in new PQL syntax require special handling here.
		colKey = "_" + columnLabel
		fieldName, _ = c.FieldArg()
		rowKey = fieldName
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
			return ErrFieldNotFound
		}
		if field.Keys() {
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

func (e *Executor) translateResult(index string, idx *Index, call *pql.Call, result interface{}) (interface{}, error) {
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
			if field.Keys() {
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

// errSliceUnavailable is a marker error if no nodes are available.
var errSliceUnavailable = errors.New("slice unavailable")

type mapFunc func(slice uint64) (interface{}, error)

type reduceFunc func(prev, v interface{}) interface{}

type mapResponse struct {
	node   *Node
	slices []uint64

	result interface{}
	err    error
}

// ExecOptions represents an execution context for a single Execute() call.
type ExecOptions struct {
	Remote          bool
	ExcludeRowAttrs bool
	ExcludeColumns  bool
}

// decodeError returns an error representation of s if s is non-blank.
// Returns nil if s is blank.
func decodeError(s string) error {
	if s == "" {
		return nil
	}
	return errors.New(s)
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

func needsSlices(calls []*pql.Call) bool {
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

func (vc *ValCount) Add(other ValCount) ValCount {
	return ValCount{
		Val:   vc.Val + other.Val,
		Count: vc.Count + other.Count,
	}
}

func EncodeValCount(vc ValCount) *internal.ValCount {
	return &internal.ValCount{
		Val:   vc.Val,
		Count: vc.Count,
	}
}

func decodeValCount(pb *internal.ValCount) ValCount {
	return ValCount{
		Val:   pb.Val,
		Count: pb.Count,
	}
}

// Smaller returns the smaller of the two ValCounts.
func (vc *ValCount) Smaller(other ValCount) ValCount {
	if vc.Count == 0 || (other.Val < vc.Val && other.Count > 0) {
		return other
	}
	return ValCount{
		Val:   vc.Val,
		Count: vc.Count,
	}
}

// Larger returns the larger of the two ValCounts.
func (vc *ValCount) Larger(other ValCount) ValCount {
	if vc.Count == 0 || (other.Val > vc.Val && other.Count > 0) {
		return other
	}
	return ValCount{
		Val:   vc.Val,
		Count: vc.Count,
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

func isString(v interface{}) bool {
	_, ok := v.(string)
	return ok
}
