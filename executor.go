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
	"errors"
	"fmt"
	"net/http"
	"sort"
	"time"

	"github.com/pilosa/pilosa/internal"
	"github.com/pilosa/pilosa/pql"
)

// DefaultFrame is the frame used if one is not specified.
const (
	DefaultFrame = "general"

	// MinThreshold is the lowest count to use in a Top-N operation when
	// looking for additional id/count pairs.
	MinThreshold = 1
)

// Executor recursively executes calls in a PQL query across all slices.
type Executor struct {
	Holder *Holder

	// Local hostname & cluster configuration.
	Scheme  string
	Host    string
	Cluster *Cluster

	// Client used for remote requests.
	client InternalClient

	// Maximum number of SetBit() or ClearBit() commands per request.
	MaxWritesPerRequest int
}

// NewExecutor returns a new instance of Executor.
func NewExecutor(remoteClient *http.Client) *Executor {
	return &Executor{
		client: NewInternalHTTPClientFromURI(nil, remoteClient),
	}
}

// Execute executes a PQL query.
func (e *Executor) Execute(ctx context.Context, index string, q *pql.Query, slices []uint64, opt *ExecOptions) ([]interface{}, error) {
	// Verify that an index is set.
	if index == "" {
		return nil, ErrIndexRequired
	}

	// Verify that the number of writes do not exceed the maximum.
	if e.MaxWritesPerRequest > 0 && q.WriteCallN() > e.MaxWritesPerRequest {
		return nil, ErrTooManyWrites
	}

	// Default options.
	if opt == nil {
		opt = &ExecOptions{}
	}

	// Don't bother calculating slices for query types that don't require it.
	needsSlices := needsSlices(q.Calls)

	// MaxSlice can differ between inverse and standard views, so we need
	// to send queries to different slices based on orientation.
	var inverseSlices []uint64
	rowLabel := DefaultRowLabel
	columnLabel := DefaultColumnLabel

	// If slices aren't specified, then include all of them.
	if len(slices) == 0 {
		// Determine slices and inverseSlices for use in e.executeCall().
		if needsSlices {
			// Round up the number of slices.
			idx := e.Holder.Index(index)
			if idx == nil {
				return nil, ErrIndexNotFound
			}
			maxSlice := idx.MaxSlice()
			maxInverseSlice := idx.MaxInverseSlice()

			// Generate a slices of all slices.
			slices = make([]uint64, maxSlice+1)
			for i := range slices {
				slices[i] = uint64(i)
			}

			// Generate a slices of all inverse slices.
			inverseSlices = make([]uint64, maxInverseSlice+1)
			for i := range inverseSlices {
				inverseSlices[i] = uint64(i)
			}

			// Fetch column label from index.
			columnLabel = idx.ColumnLabel()
		}
	}

	// Optimize handling for bulk attribute insertion.
	if hasOnlySetRowAttrs(q.Calls) {
		return e.executeBulkSetRowAttrs(ctx, index, q.Calls, opt)
	}

	// Execute each call serially.
	results := make([]interface{}, 0, len(q.Calls))
	for _, call := range q.Calls {

		if call.SupportsInverse() && needsSlices {
			// Fetch frame & row label based on argument.
			frame, _ := call.Args["frame"].(string)
			if frame == "" {
				frame = DefaultFrame
			}
			f := e.Holder.Frame(index, frame)
			if f == nil {
				return nil, ErrFrameNotFound
			}
			rowLabel = f.RowLabel()

			// If this call is to an inverse frame send to a different list of slices.
			if call.IsInverse(rowLabel, columnLabel) {
				slices = inverseSlices
			}
		}

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
		return nil, err
	}
	indexTag := fmt.Sprintf("index:%s", index)
	// Special handling for mutation and top-n calls.
	switch c.Name {
	case "Sum":
		e.Holder.Stats.CountWithCustomTags(c.Name, 1, 1.0, []string{indexTag})
		return e.executeSum(ctx, index, c, slices, opt)
	case "ClearBit":
		return e.executeClearBit(ctx, index, c, opt)
	case "Count":
		e.Holder.Stats.CountWithCustomTags(c.Name, 1, 1.0, []string{indexTag})
		return e.executeCount(ctx, index, c, slices, opt)
	case "SetBit":
		return e.executeSetBit(ctx, index, c, opt)
	case "SetFieldValue":
		return nil, e.executeSetFieldValue(ctx, index, c, opt)
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
			b := make([]int64, len(v), len(v))
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
func (e *Executor) executeSum(ctx context.Context, index string, c *pql.Call, slices []uint64, opt *ExecOptions) (SumCount, error) {
	if frame, _ := c.Args["frame"]; frame == "" {
		return SumCount{}, errors.New("Sum(): frame required")
	} else if field, _ := c.Args["field"]; field == "" {
		return SumCount{}, errors.New("Sum(): field required")
	}

	if len(c.Children) > 1 {
		return SumCount{}, errors.New("Sum() only accepts a single bitmap input")
	}

	// Execute calls in bulk on each remote node and merge.
	mapFn := func(slice uint64) (interface{}, error) {
		return e.executeSumCountSlice(ctx, index, c, slice)
	}

	// Merge returned results at coordinating node.
	reduceFn := func(prev, v interface{}) interface{} {
		other, _ := prev.(SumCount)
		return other.Add(v.(SumCount))
	}

	result, err := e.mapReduce(ctx, index, slices, c, opt, mapFn, reduceFn)
	if err != nil {
		return SumCount{}, err
	}
	other, _ := result.(SumCount)

	if other.Count == 0 {
		return SumCount{}, nil
	}
	return other, nil
}

// executeBitmapCall executes a call that returns a bitmap.
func (e *Executor) executeBitmapCall(ctx context.Context, index string, c *pql.Call, slices []uint64, opt *ExecOptions) (*Bitmap, error) {
	// Execute calls in bulk on each remote node and merge.
	mapFn := func(slice uint64) (interface{}, error) {
		return e.executeBitmapCallSlice(ctx, index, c, slice)
	}

	// Merge returned results at coordinating node.
	reduceFn := func(prev, v interface{}) interface{} {
		other, _ := prev.(*Bitmap)
		if other == nil {
			other = NewBitmap()
		}
		other.Merge(v.(*Bitmap))
		return other
	}

	other, err := e.mapReduce(ctx, index, slices, c, opt, mapFn, reduceFn)
	if err != nil {
		return nil, err
	}

	// Attach attributes for Bitmap() calls.
	// If the column label is used then return column attributes.
	// If the row label is used then return bitmap attributes.
	bm, _ := other.(*Bitmap)
	if c.Name == "Bitmap" {
		if opt.ExcludeAttrs {
			bm.Attrs = map[string]interface{}{}
		} else {
			idx := e.Holder.Index(index)
			if idx != nil {
				columnLabel := idx.ColumnLabel()
				if columnID, ok, err := c.UintArg(columnLabel); ok && err == nil {
					attrs, err := idx.ColumnAttrStore().Attrs(columnID)
					if err != nil {
						return nil, err
					}
					bm.Attrs = attrs
				} else if err != nil {
					return nil, err
				} else {
					frame, _ := c.Args["frame"].(string)
					if fr := idx.Frame(frame); fr != nil {
						rowLabel := fr.RowLabel()
						rowID, _, err := c.UintArg(rowLabel)
						if err != nil {
							return nil, err
						}
						attrs, err := fr.RowAttrStore().Attrs(rowID)
						if err != nil {
							return nil, err
						}
						bm.Attrs = attrs
					}
				}
			}
		}
	}

	if opt.ExcludeBits {
		bm.segments = []BitmapSegment{}
	}

	return bm, nil
}

// executeBitmapCallSlice executes a bitmap call for a single slice.
func (e *Executor) executeBitmapCallSlice(ctx context.Context, index string, c *pql.Call, slice uint64) (*Bitmap, error) {
	switch c.Name {
	case "Bitmap":
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

// executeSumCountSlice executes calculates the sum & count for fields on a slice.
func (e *Executor) executeSumCountSlice(ctx context.Context, index string, c *pql.Call, slice uint64) (SumCount, error) {
	var filter *Bitmap
	if len(c.Children) == 1 {
		bm, err := e.executeBitmapCallSlice(ctx, index, c.Children[0], slice)
		if err != nil {
			return SumCount{}, err
		}
		filter = bm
	}

	frameName, _ := c.Args["frame"].(string)
	fieldName, _ := c.Args["field"].(string)

	frame := e.Holder.Frame(index, frameName)
	if frame == nil {
		return SumCount{}, nil
	}

	field := frame.Field(fieldName)
	if field == nil {
		return SumCount{}, nil
	}

	view := e.Holder.Fragment(index, frameName, ViewFieldPrefix+fieldName, slice)
	if view == nil {
		return SumCount{}, nil
	}

	vsum, vcount, err := view.FieldSum(filter, field.BitDepth())
	if err != nil {
		return SumCount{}, err
	}
	return SumCount{
		Sum:   int64(vsum) + (int64(vcount) * field.Min),
		Count: int64(vcount),
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
		return nil, err
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
		return nil, err
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
	frame, _ := c.Args["frame"].(string)
	inverse, _ := c.Args["inverse"].(bool)
	n, _, err := c.UintArg("n")
	if err != nil {
		return nil, fmt.Errorf("executeTopNSlice: %v", err)
	}
	field, _ := c.Args["field"].(string)
	rowIDs, _, err := c.UintSliceArg("ids")
	if err != nil {
		return nil, fmt.Errorf("executeTopNSlice: %v", err)
	}
	minThreshold, _, err := c.UintArg("threshold")
	if err != nil {
		return nil, fmt.Errorf("executeTopNSlice: %v", err)
	}
	filters, _ := c.Args["filters"].([]interface{})
	tanimotoThreshold, _, err := c.UintArg("tanimotoThreshold")
	if err != nil {
		return nil, fmt.Errorf("executeTopNSlice: %v", err)
	}

	// Retrieve bitmap used to intersect.
	var src *Bitmap
	if len(c.Children) == 1 {
		bm, err := e.executeBitmapCallSlice(ctx, index, c.Children[0], slice)
		if err != nil {
			return nil, err
		}
		src = bm
	} else if len(c.Children) > 1 {
		return nil, errors.New("TopN() can only have one input bitmap")
	}

	// Set default frame.
	if frame == "" {
		frame = DefaultFrame
	}

	// Determine view.
	view := ViewStandard
	if inverse {
		view = ViewInverse
	}

	f := e.Holder.Fragment(index, frame, view, slice)
	if f == nil {
		return nil, nil
	}

	if minThreshold <= 0 {
		minThreshold = MinThreshold
	}

	if tanimotoThreshold > 100 {
		return nil, errors.New("Tanimoto Threshold is from 1 to 100 only")
	}
	return f.Top(TopOptions{
		N:                 int(n),
		Src:               src,
		RowIDs:            rowIDs,
		FilterField:       field,
		FilterValues:      filters,
		MinThreshold:      minThreshold,
		TanimotoThreshold: tanimotoThreshold,
	})
}

// executeDifferenceSlice executes a difference() call for a local slice.
func (e *Executor) executeDifferenceSlice(ctx context.Context, index string, c *pql.Call, slice uint64) (*Bitmap, error) {
	var other *Bitmap
	if len(c.Children) == 0 {
		return nil, fmt.Errorf("empty Difference query is currently not supported")
	}
	for i, input := range c.Children {
		bm, err := e.executeBitmapCallSlice(ctx, index, input, slice)
		if err != nil {
			return nil, err
		}

		if i == 0 {
			other = bm
		} else {
			other = other.Difference(bm)
		}
	}
	other.InvalidateCount()
	return other, nil
}

func (e *Executor) executeBitmapSlice(ctx context.Context, index string, c *pql.Call, slice uint64) (*Bitmap, error) {
	// Fetch column label from index.
	idx := e.Holder.Index(index)
	if idx == nil {
		return nil, ErrIndexNotFound
	}
	columnLabel := idx.ColumnLabel()

	// Fetch frame & row label based on argument.
	frame, _ := c.Args["frame"].(string)
	if frame == "" {
		frame = DefaultFrame
	}
	f := e.Holder.Frame(index, frame)
	if f == nil {
		return nil, ErrFrameNotFound
	}
	rowLabel := f.RowLabel()

	// Return an error if both the row and column label are specified.
	rowID, rowOK, rowErr := c.UintArg(rowLabel)
	columnID, columnOK, columnErr := c.UintArg(columnLabel)
	if rowErr != nil || columnErr != nil {
		return nil, fmt.Errorf("Bitmap() error with arg for col: %v or row: %v", columnErr, rowErr)
	}
	if rowOK && columnOK {
		return nil, fmt.Errorf("Bitmap() cannot specify both %s and %s values", rowLabel, columnLabel)
	} else if !rowOK && !columnOK {
		return nil, fmt.Errorf("Bitmap() must specify either %s or %s values", rowLabel, columnLabel)
	}

	// Determine row or column orientation.
	view, id := ViewStandard, rowID
	if columnOK {
		view, id = ViewInverse, columnID
		if !f.InverseEnabled() {
			return nil, fmt.Errorf("Bitmap() cannot retrieve columns unless inverse storage enabled")
		}
	}

	frag := e.Holder.Fragment(index, frame, view, slice)
	if frag == nil {
		return NewBitmap(), nil
	}
	return frag.Row(id), nil
}

// executeIntersectSlice executes a intersect() call for a local slice.
func (e *Executor) executeIntersectSlice(ctx context.Context, index string, c *pql.Call, slice uint64) (*Bitmap, error) {
	var other *Bitmap
	if len(c.Children) == 0 {
		return nil, fmt.Errorf("empty Intersect query is currently not supported")
	}
	for i, input := range c.Children {
		bm, err := e.executeBitmapCallSlice(ctx, index, input, slice)
		if err != nil {
			return nil, err
		}

		if i == 0 {
			other = bm
		} else {
			other = other.Intersect(bm)
		}
	}
	other.InvalidateCount()
	return other, nil
}

// executeRangeSlice executes a range() call for a local slice.
func (e *Executor) executeRangeSlice(ctx context.Context, index string, c *pql.Call, slice uint64) (*Bitmap, error) {
	// Handle field ranges differently.
	if c.HasConditionArg() {
		return e.executeFieldRangeSlice(ctx, index, c, slice)
	}

	// Parse frame, use default if unset.
	frame, _ := c.Args["frame"].(string)
	if frame == "" {
		frame = DefaultFrame
	}

	// Retrieve column label.
	idx := e.Holder.Index(index)
	if idx == nil {
		return nil, ErrIndexNotFound
	}
	columnLabel := idx.ColumnLabel()

	// Retrieve base frame.
	f := idx.Frame(frame)
	if f == nil {
		return nil, ErrFrameNotFound
	}
	rowLabel := f.RowLabel()

	// Read row & column id.
	columnID, columnOK, err := c.UintArg(columnLabel)
	if err != nil {
		return nil, fmt.Errorf("executeRangeSlice - reading column: %v", err)
	}
	rowID, rowOK, err := c.UintArg(rowLabel)
	if err != nil {
		return nil, fmt.Errorf("executeRangeSlice - reading row: %v", err)
	}

	// Determine view.
	var id uint64
	var viewName string
	if columnOK && rowOK {
		return nil, fmt.Errorf("Range() cannot contain both %q and %q", columnLabel, rowLabel)
	} else if !columnOK && !rowOK {
		return nil, fmt.Errorf("Range() must specify either %q or %q", columnLabel, rowLabel)
	} else if columnOK {
		viewName, id = ViewInverse, columnID
	} else {
		viewName, id = ViewStandard, rowID
	}

	// Parse start time.
	startTimeStr, ok := c.Args["start"].(string)
	if !ok {
		return nil, errors.New("Range() start time required")
	}
	startTime, err := time.Parse(TimeFormat, startTimeStr)
	if err != nil {
		return nil, errors.New("cannot parse Range() start time")
	}

	// Parse end time.
	endTimeStr, ok := c.Args["end"].(string)
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
		return &Bitmap{}, nil
	}

	// Union bitmaps across all time-based subframes.
	bm := &Bitmap{}
	for _, view := range ViewsByTimeRange(viewName, startTime, endTime, q) {
		f := e.Holder.Fragment(index, frame, view, slice)
		if f == nil {
			continue
		}
		bm = bm.Union(f.Row(id))
	}
	f.Stats.Count("range", 1, 1.0)
	return bm, nil
}

// executeFieldRangeSlice executes a range(field) call for a local slice.
func (e *Executor) executeFieldRangeSlice(ctx context.Context, index string, c *pql.Call, slice uint64) (*Bitmap, error) {
	// Parse frame, use default if unset.
	frame, _ := c.Args["frame"].(string)
	if frame == "" {
		frame = DefaultFrame
	}
	f := e.Holder.Frame(index, frame)
	if f == nil {
		return nil, ErrFrameNotFound
	}

	// Remove frame field.
	args := pql.CopyArgs(c.Args)
	delete(args, "frame")

	// Only one conditional field should remain.
	if len(args) == 0 {
		return nil, errors.New("Range(): condition required")
	} else if len(args) > 1 {
		return nil, errors.New("Range(): too many arguments")
	}

	// Extract condition field.
	var fieldName string
	var cond *pql.Condition
	for k, v := range args {
		vv, ok := v.(*pql.Condition)
		if !ok {
			return nil, fmt.Errorf("Range(): %q: expected condition argument, got %v", k, v)
		}
		fieldName, cond = k, vv
	}

	// EQ null           (not implemented: flip frag.FieldNotNull with max ColumnID)
	// NEQ null          frag.FieldNotNull()
	// BETWEEN a,b(in)   BETWEEN/frag.FieldRangeBetween()
	// BETWEEN a,b(out)  BETWEEN/frag.FieldNotNull()
	// EQ <int>          frag.FieldRange
	// NEQ <int>         frag.FieldRange

	// Handle `!= null`.
	if cond.Op == pql.NEQ && cond.Value == nil {
		// Find field.
		field := f.Field(fieldName)
		if field == nil {
			return nil, ErrFieldNotFound
		}

		// Retrieve fragment.
		frag := e.Holder.Fragment(index, frame, ViewFieldPrefix+fieldName, slice)
		if frag == nil {
			return NewBitmap(), nil
		}

		return frag.FieldNotNull(field.BitDepth())

	} else if cond.Op == pql.BETWEEN {

		predicates, err := cond.IntSliceValue()
		if err != nil {
			return nil, err
		}

		// Only support two integers for the between operation.
		if len(predicates) != 2 {
			return nil, errors.New("Range(): BETWEEN condition requires exactly two integer values")
		}

		// The reason we don't just call:
		//     return f.FieldRangeBetween(fieldName, predicates[0], predicates[1])
		// here is because we need the call to be slice-specific.

		// Find field.
		field := f.Field(fieldName)
		if field == nil {
			return nil, ErrFieldNotFound
		}

		baseValueMin, baseValueMax, outOfRange := field.BaseValueBetween(predicates[0], predicates[1])
		if outOfRange {
			return NewBitmap(), nil
		}

		// Retrieve fragment.
		frag := e.Holder.Fragment(index, frame, ViewFieldPrefix+fieldName, slice)
		if frag == nil {
			return NewBitmap(), nil
		}

		// If the query is asking for the entire valid range, just return
		// the not-null bitmap for the field.
		if predicates[0] <= field.Min && predicates[1] >= field.Max {
			return frag.FieldNotNull(field.BitDepth())
		}

		return frag.FieldRangeBetween(field.BitDepth(), baseValueMin, baseValueMax)

	} else {

		// Only support integers for now.
		value, ok := cond.Value.(int64)
		if !ok {
			return nil, errors.New("Range(): conditions only support integer values")
		}

		// Find field.
		field := f.Field(fieldName)
		if field == nil {
			return nil, ErrFieldNotFound
		}

		baseValue, outOfRange := field.BaseValue(cond.Op, value)
		if outOfRange && cond.Op != pql.NEQ {
			return NewBitmap(), nil
		}

		// Retrieve fragment.
		frag := e.Holder.Fragment(index, frame, ViewFieldPrefix+fieldName, slice)
		if frag == nil {
			return NewBitmap(), nil
		}

		// LT[E] and GT[E] should return all not-null if selected range fully encompases valid field range.
		if (cond.Op == pql.LT && value > field.Max) || (cond.Op == pql.LTE && value >= field.Max) ||
			(cond.Op == pql.GT && value < field.Min) || (cond.Op == pql.GTE && value <= field.Min) {
			return frag.FieldNotNull(field.BitDepth())
		}

		// outOfRange for NEQ should return all not-null.
		if outOfRange && cond.Op == pql.NEQ {
			return frag.FieldNotNull(field.BitDepth())
		}

		f.Stats.Count("range:field", 1, 1.0)
		return frag.FieldRange(cond.Op, field.BitDepth(), baseValue)
	}
}

// executeUnionSlice executes a union() call for a local slice.
func (e *Executor) executeUnionSlice(ctx context.Context, index string, c *pql.Call, slice uint64) (*Bitmap, error) {
	other := NewBitmap()
	for i, input := range c.Children {
		bm, err := e.executeBitmapCallSlice(ctx, index, input, slice)
		if err != nil {
			return nil, err
		}

		if i == 0 {
			other = bm
		} else {
			other = other.Union(bm)
		}
	}
	other.InvalidateCount()
	return other, nil
}

// executeXorSlice executes a xor() call for a local slice.
func (e *Executor) executeXorSlice(ctx context.Context, index string, c *pql.Call, slice uint64) (*Bitmap, error) {
	other := NewBitmap()
	for i, input := range c.Children {
		bm, err := e.executeBitmapCallSlice(ctx, index, input, slice)
		if err != nil {
			return nil, err
		}

		if i == 0 {
			other = bm
		} else {
			other = other.Xor(bm)
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
		bm, err := e.executeBitmapCallSlice(ctx, index, c.Children[0], slice)
		if err != nil {
			return 0, err
		}
		return bm.Count(), nil
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

// executeClearBit executes a ClearBit() call.
func (e *Executor) executeClearBit(ctx context.Context, index string, c *pql.Call, opt *ExecOptions) (bool, error) {
	view, _ := c.Args["view"].(string)
	frame, ok := c.Args["frame"].(string)
	if !ok {
		return false, errors.New("ClearBit() frame required")
	}

	// Retrieve frame.
	idx := e.Holder.Index(index)
	if idx == nil {
		return false, ErrIndexNotFound
	}
	f := idx.Frame(frame)
	if f == nil {
		return false, ErrFrameNotFound
	}

	// Retrieve labels.
	columnLabel := idx.ColumnLabel()
	rowLabel := f.RowLabel()

	// Read fields using labels.
	rowID, ok, err := c.UintArg(rowLabel)
	if err != nil {
		return false, fmt.Errorf("reading ClearBit() row: %v", err)
	} else if !ok {
		return false, fmt.Errorf("ClearBit() row field '%v' required", rowLabel)
	}

	colID, ok, err := c.UintArg(columnLabel)
	if err != nil {
		return false, fmt.Errorf("reading ClearBit() column: %v", err)
	} else if !ok {
		return false, fmt.Errorf("ClearBit col field '%v' required", columnLabel)
	}

	// Clear bits for each view.
	switch view {
	case ViewStandard:
		return e.executeClearBitView(ctx, index, c, f, view, colID, rowID, opt)
	case ViewInverse:
		return e.executeClearBitView(ctx, index, c, f, view, rowID, colID, opt)
	case "":
		var ret bool
		if changed, err := e.executeClearBitView(ctx, index, c, f, ViewStandard, colID, rowID, opt); err != nil {
			return ret, err
		} else if changed {
			ret = true
		}

		if f.InverseEnabled() {
			if changed, err := e.executeClearBitView(ctx, index, c, f, ViewInverse, rowID, colID, opt); err != nil {
				return ret, err
			} else if changed {
				ret = true
			}
		}
		return ret, nil
	default:
		return false, fmt.Errorf("invalid view: %s", view)
	}
}

// executeClearBitView executes a ClearBit() call for a single view.
func (e *Executor) executeClearBitView(ctx context.Context, index string, c *pql.Call, f *Frame, view string, colID, rowID uint64, opt *ExecOptions) (bool, error) {
	slice := colID / SliceWidth
	ret := false
	for _, node := range e.Cluster.FragmentNodes(index, slice) {
		// Update locally if host matches.
		if node.Host == e.Host {
			val, err := f.ClearBit(view, rowID, colID, nil)
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

// executeSetBit executes a SetBit() call.
func (e *Executor) executeSetBit(ctx context.Context, index string, c *pql.Call, opt *ExecOptions) (bool, error) {
	view, _ := c.Args["view"].(string)
	frame, ok := c.Args["frame"].(string)
	if !ok {
		return false, errors.New("SetBit() field required: frame")
	}

	// Retrieve frame.
	idx := e.Holder.Index(index)
	if idx == nil {
		return false, ErrIndexNotFound
	}
	f := idx.Frame(frame)
	if f == nil {
		return false, ErrFrameNotFound
	}

	// Retrieve labels.
	columnLabel := idx.ColumnLabel()
	rowLabel := f.RowLabel()

	// Read fields using labels.
	rowID, ok, err := c.UintArg(rowLabel)
	if err != nil {
		return false, fmt.Errorf("reading SetBit() row: %v", err)
	} else if !ok {
		return false, fmt.Errorf("SetBit() row field '%v' required", rowLabel)
	}

	colID, ok, err := c.UintArg(columnLabel)
	if err != nil {
		return false, fmt.Errorf("reading SetBit() column: %v", err)
	} else if !ok {
		return false, fmt.Errorf("SetBit() column field '%v' required", columnLabel)
	}

	var timestamp *time.Time
	sTimestamp, ok := c.Args["timestamp"].(string)
	if ok {
		t, err := time.Parse(TimeFormat, sTimestamp)
		if err != nil {
			return false, fmt.Errorf("invalid date: %s", sTimestamp)
		}
		timestamp = &t
	}

	// Set bits for each view.
	switch view {
	case ViewStandard:
		return e.executeSetBitView(ctx, index, c, f, view, colID, rowID, timestamp, opt)
	case ViewInverse:
		return e.executeSetBitView(ctx, index, c, f, view, rowID, colID, timestamp, opt)
	case "":
		var ret bool
		if changed, err := e.executeSetBitView(ctx, index, c, f, ViewStandard, colID, rowID, timestamp, opt); err != nil {
			return ret, err
		} else if changed {
			ret = true
		}

		if f.InverseEnabled() {
			if changed, err := e.executeSetBitView(ctx, index, c, f, ViewInverse, rowID, colID, timestamp, opt); err != nil {
				return ret, err
			} else if changed {
				ret = true
			}
		}
		return ret, nil
	default:
		return false, fmt.Errorf("invalid view: %s", view)
	}
}

// executeSetBitView executes a SetBit() call for a specific view.
func (e *Executor) executeSetBitView(ctx context.Context, index string, c *pql.Call, f *Frame, view string, colID, rowID uint64, timestamp *time.Time, opt *ExecOptions) (bool, error) {
	slice := colID / SliceWidth
	ret := false

	for _, node := range e.Cluster.FragmentNodes(index, slice) {
		// Update locally if host matches.
		if node.Host == e.Host {
			val, err := f.SetBit(view, rowID, colID, timestamp)
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

// executeSetFieldValue executes a SetFieldValue() call.
func (e *Executor) executeSetFieldValue(ctx context.Context, index string, c *pql.Call, opt *ExecOptions) error {
	frameName, ok := c.Args["frame"].(string)
	if !ok {
		return errors.New("SetFieldValue() frame required")
	}

	// Retrieve column label.
	idx := e.Holder.Index(index)
	if idx == nil {
		return ErrIndexNotFound
	}
	columnLabel := idx.ColumnLabel()

	// Retrieve frame.
	frame := e.Holder.Frame(index, frameName)
	if frame == nil {
		return ErrFrameNotFound
	}

	// Parse labels.
	columnID, ok, err := c.UintArg(columnLabel)
	if err != nil {
		return fmt.Errorf("reading SetFieldValue() column: %v", err)
	} else if !ok {
		return fmt.Errorf("SetFieldValue() column field '%v' required", columnLabel)
	}

	// Copy args and remove reserved fields.
	args := pql.CopyArgs(c.Args)
	delete(args, "frame")
	// While frame could technically work as a ColumnAttr argument, we are treating it as a reserved word primarily to avoid confusion.
	// Also, if we ever need to make ColumnAttrs frame-specific, then having this reserved word prevents backward incompatibility.
	delete(args, columnLabel)

	// Set values.
	for name, value := range args {
		switch value := value.(type) {
		case int64:
			if _, err := frame.SetFieldValue(columnID, name, value); err != nil {
				return err
			}
		default:
			return ErrInvalidFieldValueType
		}
	}
	frame.Stats.Count("SetFieldValue", 1, 1.0)

	// Do not forward call if this is already being forwarded.
	if opt.Remote {
		return nil
	}

	// Execute on remote nodes in parallel.
	nodes := Nodes(e.Cluster.Nodes).FilterHost(e.Host)
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
	frameName, ok := c.Args["frame"].(string)
	if !ok {
		return errors.New("SetRowAttrs() frame required")
	}

	// Retrieve frame.
	frame := e.Holder.Frame(index, frameName)
	if frame == nil {
		return ErrFrameNotFound
	}
	rowLabel := frame.RowLabel()

	// Parse labels.
	rowID, ok, err := c.UintArg(rowLabel)
	if err != nil {
		return fmt.Errorf("reading SetRowAttrs() row: %v", err)
	} else if !ok {
		return fmt.Errorf("SetRowAttrs() row field '%v' required", rowLabel)
	}

	// Copy args and remove reserved fields.
	attrs := pql.CopyArgs(c.Args)
	delete(attrs, "frame")
	delete(attrs, rowLabel)

	// Set attributes.
	if err := frame.RowAttrStore().SetAttrs(rowID, attrs); err != nil {
		return err
	}
	frame.Stats.Count("SetBitmapAttrs", 1, 1.0)

	// Do not forward call if this is already being forwarded.
	if opt.Remote {
		return nil
	}

	// Execute on remote nodes in parallel.
	nodes := Nodes(e.Cluster.Nodes).FilterHost(e.Host)
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
	// Collect attributes by frame/id.
	m := make(map[string]map[uint64]map[string]interface{})
	for _, c := range calls {
		frame, ok := c.Args["frame"].(string)
		if !ok {
			return nil, errors.New("SetRowAttrs() frame required")
		}

		// Retrieve frame.
		f := e.Holder.Frame(index, frame)
		if f == nil {
			return nil, ErrFrameNotFound
		}
		rowLabel := f.RowLabel()

		rowID, ok, err := c.UintArg(rowLabel)
		if err != nil {
			return nil, fmt.Errorf("reading SetRowAttrs() row: %v", rowLabel)
		} else if !ok {
			return nil, fmt.Errorf("SetRowAttrs row field '%v' required", rowLabel)
		}

		// Copy args and remove reserved fields.
		attrs := pql.CopyArgs(c.Args)
		delete(attrs, "frame")
		delete(attrs, rowLabel)

		// Create frame group, if not exists.
		frameMap := m[frame]
		if frameMap == nil {
			frameMap = make(map[uint64]map[string]interface{})
			m[frame] = frameMap
		}

		// Set or merge attributes.
		attr := frameMap[rowID]
		if attr == nil {
			frameMap[rowID] = cloneAttrs(attrs)
		} else {
			for k, v := range attrs {
				attr[k] = v
			}
		}
	}

	// Bulk insert attributes by frame.
	for name, frameMap := range m {
		// Retrieve frame.
		frame := e.Holder.Frame(index, name)
		if frame == nil {
			return nil, ErrFrameNotFound
		}

		// Set attributes.
		if err := frame.RowAttrStore().SetBulkAttrs(frameMap); err != nil {
			return nil, err
		}
		frame.Stats.Count("SetBitmapAttrs", 1, 1.0)
	}

	// Do not forward call if this is already being forwarded.
	if opt.Remote {
		return make([]interface{}, len(calls)), nil
	}

	// Execute on remote nodes in parallel.
	nodes := Nodes(e.Cluster.Nodes).FilterHost(e.Host)
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

	var colName string
	id, okID, errID := c.UintArg("id")
	if errID != nil || !okID {
		// Retrieve columnLabel
		columnLabel := idx.columnLabel
		col, okCol, errCol := c.UintArg(columnLabel)
		if errCol != nil || !okCol {
			return fmt.Errorf("reading SetColumnAttrs() id/columnLabel errs: %v/%v found %v/%v", errID, errCol, okID, okCol)
		}
		id = col
		colName = columnLabel
	} else {
		colName = "id"
	}

	// Copy args and remove reserved fields.
	attrs := pql.CopyArgs(c.Args)
	delete(attrs, colName)
	delete(attrs, "frame")

	// Set attributes.
	if err := idx.ColumnAttrStore().SetAttrs(id, attrs); err != nil {
		return err
	}
	idx.Stats.Count("SetProfileAttrs", 1, 1.0)
	// Do not forward call if this is already being forwarded.
	if opt.Remote {
		return nil
	}

	// Execute on remote nodes in parallel.
	nodes := Nodes(e.Cluster.Nodes).FilterHost(e.Host)
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
	uri, err := NewURIFromAddress(node.Host)
	if err != nil {
		return nil, err
	}
	uri.SetScheme(node.Scheme)
	ctx = context.WithValue(ctx, "uri", uri)
	pb, err := e.client.ExecuteQuery(ctx, index, pbreq)
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
			v, err = decodeSumCount(pb.Results[i].GetSumCount()), nil
		case "TopN":
			v, err = decodePairs(pb.Results[i].GetPairs()), nil
		case "Count":
			v, err = pb.Results[i].N, nil
		case "SetBit":
			v, err = pb.Results[i].Changed, nil
		case "ClearBit":
			v, err = pb.Results[i].Changed, nil
		case "SetRowAttrs":
		case "SetColumnAttrs":
		default:
			v, err = decodeBitmap(pb.Results[i].GetBitmap()), nil
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
		for _, node := range e.Cluster.FragmentNodes(index, slice) {
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
	ch := make(chan mapResponse, 0)

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
		nodes = []*Node{e.Cluster.NodeByHost(e.Host)}
	}

	// Start mapping across all primary owners.
	if err := e.mapper(ctx, ch, nodes, index, slices, c, opt, mapFn, reduceFn); err != nil {
		return nil, err
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
			if n.Host == e.Host {
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
	Remote       bool
	ExcludeAttrs bool
	ExcludeBits  bool
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
		case "ClearBit", "SetBit", "SetRowAttrs", "SetColumnAttrs":
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

// SumCount represents a grouping of sum & count for Sum() and Average() calls.
type SumCount struct {
	Sum   int64 `json:"sum"`
	Count int64 `json:"count"`
}

func (sc *SumCount) Add(other SumCount) SumCount {
	return SumCount{
		Sum:   sc.Sum + other.Sum,
		Count: sc.Count + other.Count,
	}
}

func encodeSumCount(sc SumCount) *internal.SumCount {
	return &internal.SumCount{
		Sum:   sc.Sum,
		Count: sc.Count,
	}
}

func decodeSumCount(pb *internal.SumCount) SumCount {
	return SumCount{
		Sum:   pb.Sum,
		Count: pb.Count,
	}
}
