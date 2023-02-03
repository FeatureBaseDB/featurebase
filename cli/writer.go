package cli

import (
	"fmt"
	"io"
	"os"

	featurebase "github.com/featurebasedb/featurebase/v3"
	"github.com/jedib0t/go-pretty/table"
	"github.com/jedib0t/go-pretty/text"
	"github.com/pkg/errors"
)

type Writer interface {
	Write(r *featurebase.WireQueryResponse, format *writeFormat) error
}

// writeFormat contains user configuration options which describe how to write
// the query output.
type writeFormat struct {
	timing bool
}

func defaultWriteFormat() *writeFormat {
	return &writeFormat{
		timing: true,
	}
}

// Ensure type implements interface.
var _ Writer = (*standardWriter)(nil)

// standardWriter is used to write queries to the terminal.
type standardWriter struct {
	Stdout io.Writer
	Stderr io.Writer
}

func newStandardWriter(stdout io.Writer, stderr io.Writer) *standardWriter {
	return &standardWriter{
		Stdout: stdout,
		Stderr: stderr,
	}
}

func (w *standardWriter) Write(r *featurebase.WireQueryResponse, format *writeFormat) error {
	return writeTable(r, format, w.Stdout, w.Stdout, w.Stderr)
}

// Ensure type implements interface.
var _ Writer = (*fileWriter)(nil)

// fileWriter is used to write queries to a file.
type fileWriter struct {
	filePath string
	Stdout   io.Writer
	Stderr   io.Writer
}

func newFileWriter(fpath string, stdout io.Writer, stderr io.Writer) *fileWriter {
	return &fileWriter{
		filePath: fpath,
		Stdout:   stdout,
		Stderr:   stderr,
	}
}

func (w *fileWriter) Write(r *featurebase.WireQueryResponse, format *writeFormat) error {
	file, err := os.OpenFile(w.filePath, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o600)
	if err != nil {
		return errors.Wrapf(err, "opening file: %s", w.filePath)
	}
	defer file.Close()

	return writeTable(r, format, file, w.Stdout, w.Stderr)
}

// writeTable writes the query response, taking the format into consideration.
// It sends query output to qOut, non-error informational output (such as query
// timing) to wOut, and errors to wErr.
func writeTable(r *featurebase.WireQueryResponse, format *writeFormat, qOut io.Writer, wOut io.Writer, wErr io.Writer) error {
	if r == nil {
		return errors.New("attempt to write out nil response")
	}
	if r.Error != "" {
		if _, err := wErr.Write([]byte("Error: " + r.Error + "\n")); err != nil {
			return errors.Wrapf(err, "writing error: %s", r.Error)
		}
		return writeWarnings(r, wOut)
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

	if err := writeWarnings(r, wOut); err != nil {
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
