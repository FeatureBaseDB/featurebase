// Copyright 2020 Pilosa Corp.
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

package sql

import (
	"io"
	"sort"

	"github.com/pilosa/pilosa/v2/pql"
	pproto "github.com/pilosa/pilosa/v2/proto"
	"github.com/pkg/errors"
	"google.golang.org/grpc/codes"
)

// DataType contants describe the possible values
// for the Datatype value in the RowResponse header.
const (
	DataTypeDecimal     = "decimal"
	DataTypeFloat64     = "float64"
	DataTypeInt64       = "int64"
	DataTypeString      = "string"
	DataTypeUint64Array = "[]uint64"
)

type Reducer interface {
	Reduce(pproto.StreamClient, pproto.StreamServer) error
}

// LimitReducer limits the number of messages passed through.
type LimitReducer struct {
	limit  uint
	offset uint
}

// NewLimitReducer returns a new instance of LimitReducer.
func NewLimitReducer(limit, offset uint) *LimitReducer {
	return &LimitReducer{
		limit:  limit,
		offset: offset,
	}
}

// Reduce applies the limit reducer to the client stream and sends the results
// to the server stream.
func (l *LimitReducer) Reduce(c pproto.StreamClient, s pproto.StreamServer) error {
	offsetCountdown := l.offset

	// in the case of an offset, since we'll be skipping the first record
	// which contains the headers, we need to pull the headers, save them,
	// and apply them to the first record that we actually send through.
	var headers []*pproto.ColumnInfo

	for i := uint(0); i < l.limit+l.offset || l.limit == 0; i++ {
		r, err := c.Recv()
		if err == io.EOF {
			break
		} else if err != nil {
			return s.Send(pproto.ErrorWrap(err, "receiving on client stream"))
		}
		if offsetCountdown > 0 {
			if headers == nil {
				headers = r.Headers
			}
			offsetCountdown--
			continue
		}
		if headers != nil {
			r.Headers = headers
			headers = nil
		}
		if err := s.Send(r); err != nil {
			return s.Send(pproto.ErrorWrap(err, "sending on server stream"))
		}
	}
	return s.Send(pproto.EOF)
}

// OrderByReducer orders the results based on the provide conditions.
// It also takes limit and offset to reduce the amount of items
// needing to be held in memory for sorting.
type OrderByReducer struct {
	fields       []string
	isDescending []bool // direction[asc: false, desc: true]
	limit        uint
	offset       uint
}

// NewOrderByReducer returns a new instance of OrderByReducer.
func NewOrderByReducer(fields, dirs []string, limit, offset uint) *OrderByReducer {
	descendings := make([]bool, len(fields))
	for i := range dirs {
		if dirs[i] == "desc" {
			descendings[i] = true
		}
	}
	return &OrderByReducer{
		fields:       fields,
		isDescending: descendings,
		limit:        limit,
		offset:       offset,
	}
}

// Reduce applies the order by reducer to the client stream and sends the results
// to the server stream.
func (o *OrderByReducer) Reduce(c pproto.StreamClient, s pproto.StreamServer) error {
	// hold is a slice of row responses, to be sent to the output
	// stream sorted by the sort conditions.
	var hold []*pproto.RowResponse

	// sortColNames contains the names of the columns to
	// sort on.
	sortColNames := o.fields

	// sortColIdxs contains the positions of the sort columns
	// in the result set.
	sortColIdxs := make([]int, len(sortColNames))

	// sortColTypes contains the data types of the columns
	// to be sorted. (ex: "uint64", "string", etc.). This
	// is used to determine how to convert it to a typed
	// field for sorting.
	sortColTypes := make([]string, len(sortColNames))

	// holdHeaders is used to stash the headers (from
	// the first row) so they can be applied later
	// to what will eventually be the first row after
	// sorting has occurred.
	var holdHeaders []*pproto.ColumnInfo

	ii := 0
	for {
		rr, err := c.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			return s.Send(pproto.ErrorWrap(err, "receiving row response"))
		}

		// On the first row, get the sort column information
		// from the headers. Also, stash the headers for
		// later in the `holdHeaders` var.
		if ii == 0 {
			holdHeaders = rr.Headers
			for i, rrHdr := range rr.Headers {
				hdrName := rrHdr.GetName()
				hdrType := rrHdr.GetDatatype()
				for j := range sortColNames {
					if sortColNames[j] == hdrName {
						sortColIdxs[j] = i
						sortColTypes[j] = hdrType
					}
				}
			}
			// Clear the headers in case this record is
			// no longer first (we re-apply the headers
			// to the first outgoing record later).
			rr.Headers = nil
		}

		// Put each row in the hold.
		hold = append(hold, rr)

		ii++

		// TODO: in the case where limit is provided and the number of possible
		// rows is large, it might be more efficient to periodically sort/trim
		// the hold so it doesn't become too large. For example, it could
		// be constrained to size (limit + offset + buffer), where buffer is
		// an amount that the hold can grow before being trimmed.
	}

	// Sort the hold.
	sorter, err := pproto.NewRowResponseSorter(
		sortColIdxs,
		o.isDescending,
		sortColTypes,
		hold,
	)
	if err != nil {
		return s.Send(pproto.ErrorWrap(err, "creating row response sorter"))
	}
	sort.Sort(sorter)

	var rowsToConsider uint = uint(len(hold))
	var offsetCountdown uint
	if o.limit > 0 {
		offsetCountdown = o.offset
		if o.limit+o.offset < rowsToConsider {
			rowsToConsider = o.limit + o.offset
		}
	}

	// Loop over hold and send each row response.
	// Apply the header to the first row that is sent.
	var headerApplied bool
	for i := uint(0); i < rowsToConsider; i++ {
		if offsetCountdown > 0 {
			offsetCountdown--
			continue
		}
		// Re-apply the headers to the first record.
		if !headerApplied {
			hold[i].Headers = holdHeaders
			headerApplied = true
		}
		err := s.Send(hold[i])
		if err != nil {
			return s.Send(pproto.ErrorWrap(err, "sending hold row"))
		}
	}
	return s.Send(pproto.EOF)
}

// ValCountFuncReducer converts a ValCount result to the proper
// result for Func.
type ValCountFuncReducer struct {
	fn FuncName
}

// NewValCountFuncReducer returns a new instance of ValCountFuncReducer.
func NewValCountFuncReducer(fn FuncName) *ValCountFuncReducer {
	return &ValCountFuncReducer{
		fn: fn,
	}
}

// Reduce modifies the stream according to the function.
func (v *ValCountFuncReducer) Reduce(c pproto.StreamClient, s pproto.StreamServer) error {
	r, err := c.Recv()
	if err != nil {
		if err == io.EOF {
			return s.Send(pproto.EOF)
		}
		return s.Send(pproto.Error(err))
	}

	// Get the index of the column with header of "value".
	var idxVal int = -1
	var idxCnt int = -1
	headers := r.GetHeaders()
	for i, hdr := range headers {
		switch hdr.GetName() {
		case "value":
			idxVal = i
		case "count":
			idxCnt = i
		}
	}

	var sourceDataType string
	var returnDataType string

	sourceDataType = headers[idxVal].GetDatatype()
	returnDataType = sourceDataType
	switch v.fn {
	case FuncAvg:
		returnDataType = DataTypeFloat64
	}

	rr := pproto.RowResponse{
		Headers: []*pproto.ColumnInfo{
			{Name: string(v.fn), Datatype: returnDataType},
		},
		Columns: make([]*pproto.ColumnResponse, 1),
	}

	cols := r.GetColumns()
	if len(cols) == 0 {
		return s.Send(pproto.ErrorCode(
			errors.New("empty column set"),
			codes.Unknown,
		))
	}

	if idxVal == -1 {
		return s.Send(pproto.ErrorCode(
			errors.New("result set has no column: value"),
			codes.Unknown,
		))
	}
	if idxCnt == -1 {
		return s.Send(pproto.ErrorCode(
			errors.New("result set has no column: count"),
			codes.Unknown,
		))
	}

	switch v.fn {
	case FuncAvg:
		var avg float64
		if sourceDataType == DataTypeDecimal {
			val := cols[idxVal].GetDecimalVal()
			dec := pql.NewDecimal(val.Value, val.Scale)
			cnt := cols[idxCnt].GetInt64Val()
			avg = dec.Float64() / float64(cnt)
		} else {
			val := cols[idxVal].GetInt64Val()
			cnt := cols[idxCnt].GetInt64Val()
			avg = float64(val) / float64(cnt)
		}
		rr.Columns[0] = &pproto.ColumnResponse{ColumnVal: &pproto.ColumnResponse_Float64Val{Float64Val: avg}}
	default:
		if sourceDataType == DataTypeDecimal {
			val := cols[idxVal].GetDecimalVal()
			rr.Columns[0] = &pproto.ColumnResponse{ColumnVal: &pproto.ColumnResponse_DecimalVal{DecimalVal: &pproto.Decimal{Value: val.Value, Scale: val.Scale}}}
		} else {
			val := cols[idxVal].GetInt64Val()
			rr.Columns[0] = &pproto.ColumnResponse{ColumnVal: &pproto.ColumnResponse_Int64Val{Int64Val: val}}
		}
	}

	if err := s.Send(&rr); err != nil {
		return errors.Wrap(err, "sending row response")
	}
	return s.Send(pproto.EOF)
}

// CountIDReducer returns a stream of _id's as a count.
type CountIDReducer struct{}

// Reduce counts the stream of IDs and returns a single record.
func (r *CountIDReducer) Reduce(c pproto.StreamClient, s pproto.StreamServer) error {
	var cnt uint64

	for {
		_, err := c.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			return s.Send(pproto.ErrorWrap(err, "receiving on client stream"))
		}
		cnt++
	}

	rr := pproto.RowResponse{
		Headers: []*pproto.ColumnInfo{
			{Name: string(FuncCount), Datatype: "uint64"},
		},
		Columns: []*pproto.ColumnResponse{
			&pproto.ColumnResponse{ColumnVal: &pproto.ColumnResponse_Uint64Val{Uint64Val: cnt}},
		},
	}

	if err := s.Send(&rr); err != nil {
		return errors.Wrap(err, "sending row response")
	}
	return s.Send(pproto.EOF)
}

// AssignHeadersReducer overwrites the headers on the first record
// according to field names and aliases from sql. It also reorders
// the columns in the result stream to match the sql select clause.
type AssignHeadersReducer struct {
	cols []Column
}

// NewAssignHeadersReducer returns a new instance of AssignHeadersReducer.
func NewAssignHeadersReducer(cols []Column) *AssignHeadersReducer {
	return &AssignHeadersReducer{
		cols: cols,
	}
}

// Reduce modifies the stream.
func (r *AssignHeadersReducer) Reduce(c pproto.StreamClient, s pproto.StreamServer) error {
	var placement []uint
	var labels []string

	var cnt int
	for {
		rr, err := c.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			return s.Send(pproto.ErrorWrap(err, "receiving on client stream"))
		}

		// If the placement slice is [0-n] where n == len(Headers)
		// then we don't need to alter rr on records after cnt == 0.
		// If we don't apply aliases, we don't have to alter Headers
		// either, but that may not be worth messing with.

		if cnt == 0 {
			placement, labels, err = headerAssignment(r.cols, rr.Headers)
			if err != nil {
				return s.Send(pproto.ErrorWrap(err, "getting header assignment"))
			}

			// mod is the modified RowResponse object that gets populated
			// according to placement and labels, then sent.
			mod := &pproto.RowResponse{
				Headers: make([]*pproto.ColumnInfo, len(placement)),
				Columns: make([]*pproto.ColumnResponse, len(placement)),
			}

			// For now, we assume that the column count in each RowResponse
			// is consistent (i.e. we can validate one time, here, on the
			// first row, and not every time, in the `else` statement below).
			if len(placement) > len(rr.Columns) {
				return s.Send(pproto.ErrorCode(
					errors.New("mismatched header placement and column count"),
					codes.Unknown,
				))
			}

			for i := 0; i < len(placement); i++ {
				mod.Headers[i] = rr.Headers[placement[i]]
				mod.Headers[i].Name = labels[i]
				mod.Columns[i] = rr.Columns[placement[i]]
			}
			if err := s.Send(mod); err != nil {
				return errors.Wrap(err, "sending mod")
			}
		} else {
			// mod is the modified RowResponse object that gets populated
			// according to placement and labels, then sent.
			mod := &pproto.RowResponse{
				Columns: make([]*pproto.ColumnResponse, len(placement)),
			}
			for i := 0; i < len(placement); i++ {
				mod.Columns[i] = rr.Columns[placement[i]]
			}
			if err := s.Send(mod); err != nil {
				return errors.Wrap(err, "sending mod")
			}
		}
		cnt++
	}

	return s.Send(pproto.EOF)
}

var (
	ErrIncompleteHeaders = errors.New("incomplete header assignment")
	ErrFieldNotInHeaders = errors.New("field not found in source header")
)

func headerAssignment(cols []Column, hdrs []*pproto.ColumnInfo) ([]uint, []string, error) {
	// If any of the columns are "*" (i.e. type StarColumn),
	// then ignore everything else and just use all result
	// headers.
	var hasStar bool
	for _, col := range cols {
		if _, ok := col.(*StarColumn); ok {
			hasStar = true
			break
		}
	}
	if hasStar {
		placement := make([]uint, len(hdrs))
		labels := make([]string, len(hdrs))
		for i, hdr := range hdrs {
			placement[i] = uint(i)
			labels[i] = hdr.Name
		}
		return placement, labels, nil
	}

	if len(cols) > len(hdrs) {
		return nil, nil, ErrIncompleteHeaders
	}
	placement := make([]uint, len(cols))
	labels := make([]string, len(cols))

	// Make a map of the RowResponse headers.
	hdrMap := make(map[string]uint)
	for i, hdr := range hdrs {
		hdrMap[hdr.Name] = uint(i)
	}

	// Lookup each column in the hdrMap and determine the desired placement.
	for i, col := range cols {
		if srcHdrIdx, ok := hdrMap[col.Source()]; ok {
			placement[i] = srcHdrIdx
			labels[i] = col.Alias()
		} else if nameHdrIdx, ok := hdrMap[col.Name()]; ok {
			placement[i] = nameHdrIdx
			labels[i] = col.Alias()
		} else {
			return nil, nil, errors.Wrapf(ErrFieldNotInHeaders, "field: %s", col.Name())
		}
	}
	return placement, labels, nil
}
