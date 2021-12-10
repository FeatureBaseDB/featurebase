package pgtest

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/molecula/featurebase/v2/pg"
)

// HandlerFunc implements a postgres query handler with a function.
type HandlerFunc func(context.Context, pg.QueryResultWriter, pg.Query) error

// HandleQuery calls the user's query handler function.
func (h HandlerFunc) HandleQuery(ctx context.Context, w pg.QueryResultWriter, q pg.Query) error {
	return h(ctx, w, q)
}
func (h HandlerFunc) HandleSchema(ctx context.Context, portal *pg.Portal) error {
	return nil
}
func (h HandlerFunc) Version() string {
	return "testv1"
}

var _ pg.QueryHandler = HandlerFunc(nil)

// ResultSet is a QueryResultWriter that accumulates results in a slice.
type ResultSet struct {
	Columns   []pg.ColumnInfo
	Data      [][]string
	ResultTag string
}

func (s ResultSet) String() string {
	if len(s.Columns) == 0 || len(s.Data) == 0 {
		return "EMPTY"
	}
	colHdr := make([]string, len(s.Columns))
	for i, c := range s.Columns {
		colHdr[i] = fmt.Sprintf("%s:%v", c.Name, c.Type)
	}
	dataBody := make([][]string, len(s.Data))
	for i, v := range s.Data {
		dataBody[i] = append([]string(nil), v...)
	}
	colWidth := make([]int, len(s.Columns))
	for i, c := range colHdr {
		colWidth[i] = len(c)
	}
	for _, row := range dataBody {
		for i, c := range row {
			if len(c) > colWidth[i] {
				colWidth[i] = len(c)
			}
		}
	}
	for i, c := range colHdr {
		c += strings.Repeat(" ", colWidth[i]-len(c))
		colHdr[i] = c
	}
	for _, row := range dataBody {
		for i, c := range row {
			c += strings.Repeat(" ", colWidth[i]-len(c))
			row[i] = c
		}
	}
	var totalWidth int
	for _, width := range colWidth {
		totalWidth += width
	}
	data := make([]string, len(dataBody))
	for i, row := range dataBody {
		data[i] = strings.Join(row, "|")
	}
	return strings.Join(colHdr, "|") + "\n" + strings.Repeat("-", totalWidth+(2*len(colHdr)-1)) + "\n" + strings.Join(data, "\n")
}

// WriteHeader writes headers to the result set.
func (rs *ResultSet) WriteHeader(cols ...pg.ColumnInfo) error {
	if rs.Columns != nil {
		return errors.New("double-write of headers")
	}

	colsCopy := make([]pg.ColumnInfo, len(cols))
	copy(colsCopy, cols)
	rs.Columns = colsCopy

	return nil
}

// WriteRowText writes a row to the result set.
func (rs *ResultSet) WriteRowText(vals ...string) error {
	if rs.Columns == nil {
		return errors.New("wrote a row without headers")
	}

	row := make([]string, len(vals))
	copy(row, vals)

	rs.Data = append(rs.Data, row)

	return nil
}

// Tag applies a tag to the result set.
func (rs *ResultSet) Tag(tag string) {
	rs.ResultTag = tag
}

var _ pg.QueryResultWriter = (*ResultSet)(nil)
