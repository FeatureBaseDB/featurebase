package ctl

import (
	"context"
	"io"
	"log"
	"os"

	"github.com/pilosa/pilosa"
)

// ExportCommand represents a command for bulk exporting data from a server.
type ExportCommand struct {
	// Remote host and port.
	Host string

	// Name of the index & frame to export from.
	Index string
	Frame string

	// Filename to export to.
	Path string

	// Standard input/output
	*pilosa.CmdIO
}

// NewExportCommand returns a new instance of ExportCommand.
func NewExportCommand(stdin io.Reader, stdout, stderr io.Writer) *ExportCommand {
	return &ExportCommand{
		CmdIO: pilosa.NewCmdIO(stdin, stdout, stderr),
	}
}

// Run executes the export.
func (cmd *ExportCommand) Run(ctx context.Context) error {
	logger := log.New(cmd.Stderr, "", log.LstdFlags)

	// Validate arguments.
	if cmd.Index == "" {
		return pilosa.ErrIndexRequired
	} else if cmd.Frame == "" {
		return pilosa.ErrFrameRequired
	}

	// Use output file, if specified.
	// Otherwise use STDOUT.
	var w io.Writer = cmd.Stdout
	if cmd.Path != "" {
		f, err := os.Create(cmd.Path)
		if err != nil {
			return err
		}
		defer f.Close()

		w = f
	}

	// Create a client to the server.
	client, err := pilosa.NewClient(cmd.Host)
	if err != nil {
		return err
	}

	// Determine slice count.
	maxSlices, err := client.MaxSliceByIndex(ctx)
	if err != nil {
		return err
	}

	// Export each slice.
	for slice := uint64(0); slice <= maxSlices[cmd.Index]; slice++ {
		logger.Printf("exporting slice: %d", slice)
		if err := client.ExportCSV(ctx, cmd.Index, cmd.Frame, slice, w); err != nil {
			return err
		}
	}

	// Close writer, if applicable.
	if w, ok := w.(io.Closer); ok {
		if err := w.Close(); err != nil {
			return err
		}
	}

	return nil
}
