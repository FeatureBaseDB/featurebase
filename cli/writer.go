package cli

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
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
	format     string
	location   *time.Location
	timing     bool
	tuplesOnly bool
}

const (
	formatAligned = "aligned"
	formatCSV     = "csv"
)

func defaultWriteOptions() *writeOptions {
	return &writeOptions{
		border:     1,
		expanded:   false,
		format:     formatAligned,
		location:   time.Local,
		timing:     false,
		tuplesOnly: false,
	}
}

// writeOutput writes the query response, taking the format into consideration.
// It sends query output to qOut, non-error informational output (such as query
// timing) to wOut, and errors to wErr.
func writeOutput(r *featurebase.WireQueryResponse, opts *writeOptions, qOut io.Writer, wOut io.Writer, wErr io.Writer) error {
	if r == nil {
		return errors.New("attempt to write out nil response")
	}
	if r.Error != "" {
		if _, err := wErr.Write([]byte("Error: " + r.Error + "\n")); err != nil {
			return errors.Wrapf(err, "writing error: %s", r.Error)
		}
		return writeWarnings(r, wErr)
	}

	switch opts.format {
	case formatAligned:
		if err := writeTable(r, opts, qOut); err != nil {
			return errors.Wrap(err, "writing table")
		}

		// Add some white space after query results.
		qOut.Write([]byte("\n"))

	case formatCSV:
		if err := writeCSV(r, opts, qOut); err != nil {
			return errors.Wrap(err, "writing csv")
		}

	default:
		return errors.Errorf("invalid format: %s", opts.format)
	}

	if err := writeWarnings(r, wErr); err != nil {
		return err
	}

	// Timing.
	if opts.timing {
		if _, err := wOut.Write([]byte(fmt.Sprintf("Execution time: %dμs\n", r.ExecutionTime))); err != nil {
			return errors.Wrapf(err, "writing execution time: %s", r.Error)
		}
	}

	return nil
}

// writeCSV writes the WireQueryResponse to qOut as csv.
func writeCSV(r *featurebase.WireQueryResponse, opts *writeOptions, qOut io.Writer) error {
	w := csv.NewWriter(qOut)

	if opts.expanded {
		// Expanded csv

		// rec is used to write the row as a slice of strings. It is reused to
		// avoid unnecessary memory allocation.
		rec := make([]string, 2)

		for _, row := range r.Data {
			cleanRow(row, opts)
			for i, col := range r.Schema.Fields {
				rec[0] = string(col.Name)
				rec[1] = fmt.Sprintf("%v", row[i])

				// Write the record.
				if err := w.Write(rec); err != nil {
					log.Fatalln("error writing expanded record to csv:", err)
				}
			}
		}

	} else {
		// Normal csv (i.e. NOT expanded)

		// Write the schema.
		if !opts.tuplesOnly {
			header := make([]string, 0, len(r.Schema.Fields))
			for i := range r.Schema.Fields {
				header = append(header, string(r.Schema.Fields[i].Name))
			}
			if err := w.Write(header); err != nil {
				return errors.Wrapf(err, "error writing header to csv")
			}
		}

		// Write the records.

		// rec is used to write the row as a slice of strings. It is reused to
		// avoid unnecessary memory allocation.
		rec := make([]string, len(r.Schema.Fields))

		for _, row := range r.Data {
			cleanRow(row, opts)
			for i := range row {
				rec[i] = fmt.Sprintf("%v", row[i])
			}
			if err := w.Write(rec); err != nil {
				log.Fatalln("error writing record to csv:", err)
			}
		}
	}

	// Write any buffered data to the underlying writer (standard output).
	w.Flush()

	return w.Error()
}

// writeTable writes the WireQueryResponse to qOut in a tabular format.
func writeTable(r *featurebase.WireQueryResponse, opts *writeOptions, qOut io.Writer) error {
	t := table.NewWriter()
	t.SetOutputMirror(qOut)
	switch opts.border {
	case 0:
		t.SetStyle(styleBorder0)
	case 1:
		t.SetStyle(styleBorder1)
	default:
		t.SetStyle(styleBorder2)
		// In expanded mode with a border, we need borders between each record.
		if opts.expanded {
			t.Style().Options.SeparateRows = true
		}
	}

	// Don't uppercase the header values.
	t.Style().Format.Header = text.FormatDefault

	if opts.expanded {
		// Expanded table
		for _, row := range r.Data {
			cleanRow(row, opts)
			colRow := make([]interface{}, 2)
			scolRow := make([]string, 2)
			div := "\n"
			for i, col := range r.Schema.Fields {
				if i == len(r.Schema.Fields)-1 {
					div = ""
				}
				scolRow[0] += fmt.Sprintf("%s%s", col.Name, div)
				scolRow[1] += fmt.Sprintf("%v%s", row[i], div)
			}
			colRow[0] = scolRow[0]
			colRow[1] = scolRow[1]
			t.AppendRow(table.Row(colRow[:]))
		}
	} else {
		// Normal table (i.e. NOT expanded)
		if !opts.tuplesOnly {
			t.AppendHeader(schemaToRow(r.Schema))
		}
		for _, row := range r.Data {
			cleanRow(row, opts)
			t.AppendRow(table.Row(row))
		}
	}
	t.Render()

	return nil
}

// cleanRow loops through all the columns of row and modifies its value based on
// type.
//
// If the value is nil, replace it with a null string; go-pretty doesn't expect
// nil pointers in the data values.
//
// If the value is a time.Time, we want to print it using RFC3339Nano to be
// consistent with everything else.
func cleanRow(row []interface{}, opts *writeOptions) {
	for i := range row {
		switch v := row[i].(type) {
		case nil:
			row[i] = nullValue
		case time.Time:
			row[i] = v.In(opts.location).Format(time.RFC3339Nano)
		}
	}
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
