package sql

import (
	"sort"

	"github.com/molecula/featurebase/v2"
	"github.com/molecula/featurebase/v2/pql"
	pproto "github.com/molecula/featurebase/v2/proto"
	"github.com/pkg/errors"
)

type limitRowser struct {
	rowser pproto.ToRowser
	limit  uint
}

func (l *limitRowser) ToRows(fn func(*pproto.RowResponse) error) error {
	limit := l.limit
	return l.rowser.ToRows(func(row *pproto.RowResponse) error {
		if limit == 0 {
			return nil
		}
		limit--

		return fn(row)
	})
}

// LimitRows applies a limit to a ToRowser.
func LimitRows(rowser pproto.ToRowser, limit uint) pproto.ToRowser {
	switch rowser := rowser.(type) {
	case pilosa.ExtractedTable:
		if uint(len(rowser.Columns)) > limit {
			rowser.Columns = rowser.Columns[:limit]
		}
		return rowser
	default:
		return &limitRowser{rowser, limit}
	}
}

type offsetRowser struct {
	rowser pproto.ToRowser
	offset uint
}

func (o *offsetRowser) ToRows(fn func(*pproto.RowResponse) error) error {
	offset := o.offset
	var headers []*pproto.ColumnInfo
	return o.rowser.ToRows(func(row *pproto.RowResponse) error {
		if headers == nil {
			headers = row.Headers
		}
		if offset > 0 {
			offset--
			return nil
		}
		row.Headers = headers

		return fn(row)
	})
}

// OffsetRows applies an offset to a ToRowser.
func OffsetRows(rowser pproto.ToRowser, offset uint) pproto.ToRowser {
	if offset == 0 {
		return rowser
	}

	switch rowser := rowser.(type) {
	case pilosa.ExtractedTable:
		if uint(len(rowser.Columns)) > offset {
			rowser.Columns = rowser.Columns[:0]
		} else {
			rowser.Columns = rowser.Columns[offset:]
		}
		return rowser
	default:
		return &offsetRowser{rowser, offset}
	}
}

type orderByRowser struct {
	rowser       pproto.ToRowser
	fields       []string
	isDescending []bool // direction[asc: false, desc: true]
}

func (o *orderByRowser) ToRows(fn func(*pproto.RowResponse) error) error {
	// hold is a slice of row responses, to be sent to the output
	// stream sorted by the sort conditions.
	var hold []*pproto.RowResponse
	err := o.rowser.ToRows(func(row *pproto.RowResponse) error {
		hold = append(hold, row)
		return nil
	})
	if err != nil {
		return err
	}
	if len(hold) == 0 {
		return nil
	}

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
	holdHeaders := hold[0].Headers
	for i, hdr := range holdHeaders {
		hdrName := hdr.GetName()
		hdrType := hdr.GetDatatype()
		for j := range sortColNames {
			if sortColNames[j] == hdrName {
				sortColIdxs[j] = i
				sortColTypes[j] = hdrType
			}
		}
	}

	// Sort the hold.
	sorter, err := pproto.NewRowResponseSorter(
		sortColIdxs,
		o.isDescending,
		sortColTypes,
		hold,
	)
	if err != nil {
		return errors.Wrap(err, "creating row response sorter")
	}
	sort.Sort(sorter)

	// Loop over hold and send each row response.
	// Apply the header to the first row that is sent.
	var headerApplied bool
	for i := range hold {
		// Re-apply the headers to the first record.
		if !headerApplied {
			hold[i].Headers = holdHeaders
			headerApplied = true
		}
		err := fn(hold[i])
		if err != nil {
			return errors.Wrap(err, "sending hold row")
		}
	}

	return nil
}

// OrderBy sorts a rowser.
func OrderBy(rowser pproto.ToRowser, fields, dirs []string) pproto.ToRowser {
	descendings := make([]bool, len(fields))
	for i := range dirs {
		if dirs[i] == "desc" {
			descendings[i] = true
		}
	}
	return &orderByRowser{
		rowser:       rowser,
		fields:       fields,
		isDescending: descendings,
	}
}

type valCountRowser struct {
	rowser pproto.ToRowser
	fn     FuncName
}

func (v *valCountRowser) ToRows(fn func(row *pproto.RowResponse) error) error {
	var r *pproto.RowResponse
	err := v.rowser.ToRows(func(row *pproto.RowResponse) error {
		if r != nil {
			return errors.New("extra row in valcount")
		}
		r = row
		return nil
	})
	if err != nil {
		return err
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
		returnDataType = "float64"
	}

	rr := pproto.RowResponse{
		Headers: []*pproto.ColumnInfo{
			{Name: string(v.fn), Datatype: returnDataType},
		},
		Columns: make([]*pproto.ColumnResponse, 1),
	}

	cols := r.GetColumns()
	if len(cols) == 0 {
		return errors.New("empty column set")
	}

	if idxVal == -1 {
		return errors.New("result set has no column: value")
	}
	if idxCnt == -1 {
		return errors.New("result set has no column: count")
	}

	switch v.fn {
	case FuncAvg:
		var avg float64
		if sourceDataType == "decimal" {
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
		if sourceDataType == "decimal" {
			val := cols[idxVal].GetDecimalVal()
			rr.Columns[0] = &pproto.ColumnResponse{ColumnVal: &pproto.ColumnResponse_DecimalVal{DecimalVal: &pproto.Decimal{Value: val.Value, Scale: val.Scale}}}
		} else {
			val := cols[idxVal].GetInt64Val()
			rr.Columns[0] = &pproto.ColumnResponse{ColumnVal: &pproto.ColumnResponse_Int64Val{Int64Val: val}}
		}
	}

	return fn(&rr)
}

// ApplyValCountFunc converts a ValCount result to the proper
// result for Func
func ApplyValCountFunc(rowser pproto.ToRowser, fn FuncName) pproto.ToRowser {
	return &valCountRowser{
		rowser: rowser,
		fn:     fn,
	}
}

type countIDRowser struct {
	rowser pproto.ToRowser
}

func (c *countIDRowser) ToRows(fn func(*pproto.RowResponse) error) error {
	var count uint64
	err := c.rowser.ToRows(func(row *pproto.RowResponse) error {
		count++
		return nil
	})
	if err != nil {
		return err
	}

	return fn(&pproto.RowResponse{
		Headers: []*pproto.ColumnInfo{
			{Name: string(FuncCount), Datatype: "uint64"},
		},
		Columns: []*pproto.ColumnResponse{
			{
				ColumnVal: &pproto.ColumnResponse_Uint64Val{Uint64Val: count},
			},
		},
	})
}

// CountRows counts the rows from the input rowser.
func CountRows(rowser pproto.ToRowser) pproto.ToRowser {
	return &countIDRowser{rowser}
}

type assignHeadersRowser struct {
	rowser pproto.ToRowser
	cols   []Column
}

func (a *assignHeadersRowser) ToRows(fn func(*pproto.RowResponse) error) error {
	var placement []uint

	return a.rowser.ToRows(func(row *pproto.RowResponse) error {
		var out pproto.RowResponse
		if placement == nil {
			// Assign headers and generate placement.
			var err error
			var labels []string
			placement, labels, err = headerAssignment(a.cols, row.Headers)
			if err != nil {
				return errors.Wrap(err, "getting header assignment")
			}
			headers := make([]*pproto.ColumnInfo, len(placement))
			for i, v := range placement {
				header := row.Headers[v]
				header.Name = labels[i]
				headers[i] = header
			}
			out.Headers = headers
		}

		// Re-order the columns.
		cols := make([]*pproto.ColumnResponse, len(placement))
		for i, v := range placement {
			cols[i] = row.Columns[v]
		}
		out.Columns = cols

		return fn(&out)
	})
}

// AssignHeaders assigns headers to a ToRowser.
func AssignHeaders(rowser pproto.ToRowser, headers ...Column) pproto.ToRowser {
	return &assignHeadersRowser{rowser, headers}
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
