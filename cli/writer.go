package cli

import (
	"fmt"
	"io"

	featurebase "github.com/featurebasedb/featurebase/v3"
	"github.com/jedib0t/go-pretty/table"
	"github.com/jedib0t/go-pretty/text"
	"github.com/pkg/errors"
)

// writeOptions contains user configuration options which describe how to write
// the query output.
type writeOptions struct {
	timing bool
}

func defaultWriteOptions() *writeOptions {
	return &writeOptions{
		timing: true,
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

	// Don't uppercase the header values.
	t.Style().Format.Header = text.FormatDefault

	t.AppendHeader(schemaToRow(r.Schema))
	for _, row := range r.Data {
		// If the value is nil, replace it with a null string; go-pretty doesn't
		// expect nil pointers in the data values.
		for i := range row {
			if row[i] == nil {
				row[i] = nullValue
			}
		}
		t.AppendRow(table.Row(row))
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
