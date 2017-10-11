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
	View  string
	// Filename to export to.
	Path string

	// Standard input/output
	*pilosa.CmdIO

	TLS pilosa.TLSConfig
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
	} else if !(cmd.View == pilosa.ViewStandard || cmd.View == pilosa.ViewInverse) {
		return pilosa.ErrInvalidView
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
	client, err := CommandClient(cmd)
	if err != nil {
		return err
	}

	// Determine slice count.
	var maxSlices map[string]uint64
	if cmd.View == pilosa.ViewStandard {
		maxSlices, err = client.MaxSliceByIndex(ctx)
	} else if cmd.View == pilosa.ViewInverse {
		maxSlices, err = client.MaxInverseSliceByIndex(ctx)
	}

	if err != nil {
		return err
	}

	// Export each slice.
	for slice := uint64(0); slice <= maxSlices[cmd.Index]; slice++ {
		logger.Printf("exporting slice: %d", slice)
		if err := client.ExportCSV(ctx, cmd.Index, cmd.Frame, cmd.View, slice, w); err != nil {
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

func (cmd *ExportCommand) TLSHost() string {
	return cmd.Host
}

func (cmd *ExportCommand) TLSConfiguration() pilosa.TLSConfig {
	return cmd.TLS
}
