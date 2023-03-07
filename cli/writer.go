package cli

import (
	"fmt"
	"io"
	"time"

	featurebase "github.com/featurebasedb/featurebase/v3"
	"github.com/jedib0t/go-pretty/table"
	"github.com/jedib0t/go-pretty/text"
	"github.com/pkg/errors"
)

// writeOptions contains user configuration options which describe how to write
// the query output.
type writeOptions struct {
	border     int
	expanded   bool
	timing     bool
	tuplesOnly bool
}

func defaultWriteOptions() *writeOptions {
	return &writeOptions{
		border:     1,
		expanded:   false,
		timing:     false,
		tuplesOnly: false,
	}
}

// writeTable writes the query response, taking the format into consideration.
// It sends query output to qOut, non-error informational output (such as query
// timing) to wOut, and errors to wErr.
func writeTable(r *featurebase.WireQueryResponse, format *writeOptions, qOut io.Writer, wOut io.Writer, wErr io.Writer) error {
	if r == nil {
		return errors.New("attempt to write out nil response")
	}
	if r.Error != "" {
		if _, err := wErr.Write([]byte("Error: " + r.Error + "\n")); err != nil {
			return errors.Wrapf(err, "writing error: %s", r.Error)
		}
		return writeWarnings(r, wErr)
	}

	t := table.NewWriter()
	t.SetOutputMirror(qOut)
	switch format.border {
	case 0:
		t.SetStyle(styleBorder0)
	case 1:
		t.SetStyle(styleBorder1)
	default:
		t.SetStyle(styleBorder2)
		// In expanded mode with a border, we need borders between each record.
		if format.expanded {
			t.Style().Options.SeparateRows = true
		}
	}

	// Don't uppercase the header values.
	t.Style().Format.Header = text.FormatDefault

	if format.expanded {
		// Expanded table
		for _, row := range r.Data {
			colRow := make([]interface{}, 2)
			scolRow := make([]string, 2)
			div := "\n"
			for i, col := range r.Schema.Fields {
				if i == len(r.Schema.Fields)-1 {
					div = ""
				}
				scolRow[0] += fmt.Sprintf("%s%s", col.Name, div)
				if row[i] == nil {
					scolRow[1] += fmt.Sprintf("%s%s", nullValue, div)
				} else {
					scolRow[1] += fmt.Sprintf("%v%s", row[i], div)
				}
			}
			colRow[0] = scolRow[0]
			colRow[1] = scolRow[1]
			t.AppendRow(table.Row(colRow[:]))
		}
	} else {
		// Normal table (i.e. NOT expanded)
		if !format.tuplesOnly {
			t.AppendHeader(schemaToRow(r.Schema))
		}
		for _, row := range r.Data {
			// Loop through all the colums of each row and modify any based on
			// type.
			//
			// If the value is nil, replace it with a null string; go-pretty
			// doesn't expect nil pointers in the data values.
			//
			// If the value is a time.Time, we want to print it using
			// RFC3339Nano to be consistent with everything else.
			for i := range row {
				switch v := row[i].(type) {
				case nil:
					row[i] = nullValue
				case time.Time:
					row[i] = v.Format(time.RFC3339Nano)
				}
			}
			t.AppendRow(table.Row(row))
		}
	}
	t.Render()

	if err := writeWarnings(r, wErr); err != nil {
		return err
	}

	// Add some white space after query results.
	qOut.Write([]byte("\n"))

	// Timing.
	if format.timing {
		if _, err := wOut.Write([]byte(fmt.Sprintf("Execution time: %dμs\n", r.ExecutionTime))); err != nil {
			return errors.Wrapf(err, "writing execution time: %s", r.Error)
		}
	}

	return nil
}

func schemaToRow(schema featurebase.WireQuerySchema) []interface{} {
	ret := make([]interface{}, len(schema.Fields))
	for i, field := range schema.Fields {
		ret[i] = field.Name
	}
	return ret
}

func writeWarnings(r *featurebase.WireQueryResponse, w io.Writer) error {
	if len(r.Warnings) == 0 {
		return nil
	}

	if _, err := w.Write([]byte("\n")); err != nil {
		return errors.Wrapf(err, "writing line feed")
	}
	for _, warning := range r.Warnings {
		if _, err := w.Write([]byte("Warning: " + warning + "\n")); err != nil {
			return errors.Wrapf(err, "writing warning: %s", warning)
		}
	}
	return nil
}

var styleBorder2 table.Style = table.StyleDefault

var styleBorder1 table.Style = table.Style{
	Name:   "StyleBorder1",
	Box:    table.StyleBoxDefault,
	Color:  table.ColorOptionsDefault,
	Format: table.FormatOptionsDefault,
	Options: table.Options{
		DrawBorder:      false,
		SeparateColumns: true,
		SeparateFooter:  true,
		SeparateHeader:  true,
		SeparateRows:    false,
	},
	Title: table.TitleOptionsDefault,
}

var styleBorder0 table.Style = table.Style{
	Name: "StyleBorder0",
	Box: table.BoxStyle{
		BottomLeft:       "+",
		BottomRight:      "+",
		BottomSeparator:  "+",
		Left:             "|",
		LeftSeparator:    "+",
		MiddleHorizontal: "-",
		MiddleSeparator:  " ",
		MiddleVertical:   " ",
		PaddingLeft:      "",
		PaddingRight:     "",
		PageSeparator:    "\n",
		Right:            "|",
		RightSeparator:   "+",
		TopLeft:          "+",
		TopRight:         "+",
		TopSeparator:     "+",
		UnfinishedRow:    " ~",
	},
	Color:  table.ColorOptionsDefault,
	Format: table.FormatOptionsDefault,
	Options: table.Options{
		DrawBorder:      false,
		SeparateColumns: true,
		SeparateFooter:  true,
		SeparateHeader:  true,
		SeparateRows:    false,
	},
	Title: table.TitleOptionsDefault,
}
