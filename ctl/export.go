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
	"github.com/pilosa/pilosa/server"
	"github.com/pkg/errors"
)

// ExportCommand represents a command for bulk exporting data from a server.
type ExportCommand struct {
	// Remote host and port.
	Host string

	// Name of the index & field to export from.
	Index string
	Field string

	// Filename to export to.
	Path string

	// Enabled/disable timestamps
	Timestamps bool

	// Standard input/output
	*pilosa.CmdIO

	TLS server.TLSConfig
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
	} else if cmd.Field == "" {
		return pilosa.ErrFieldRequired
	}

	// Use output file, if specified.
	// Otherwise use STDOUT.
	var w io.Writer = cmd.Stdout
	if cmd.Path != "" {
		f, err := os.Create(cmd.Path)
		if err != nil {
			return errors.Wrap(err, "creating file")
		}
		defer f.Close()

		w = f
	}

	// Create a client to the server.
	client, err := commandClient(cmd)
	if err != nil {
		return errors.Wrap(err, "creating client")
	}

	// Determine shard count.
	maxShards, err := client.MaxShardByIndex(ctx)
	if err != nil {
		return errors.Wrap(err, "getting shard count")
	}

	// Export each shard.
	for shard := uint64(0); shard <= maxShards[cmd.Index]; shard++ {
		logger.Printf("exporting shard: %d", shard)
		if err := client.ExportCSVTimestamp(ctx, cmd.Index, cmd.Field, shard, cmd.Timestamps, w); err != nil {
			return errors.Wrap(err, "exporting")
		}
	}

	// Close writer, if applicable.
	if w, ok := w.(io.Closer); ok {
		if err := w.Close(); err != nil {
			return errors.Wrap(err, "closing")
		}
	}

	return nil
}

func (cmd *ExportCommand) TLSHost() string {
	return cmd.Host
}

func (cmd *ExportCommand) TLSConfiguration() server.TLSConfig {
	return cmd.TLS
}
