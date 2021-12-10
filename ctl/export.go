// Copyright 2021 Molecula Corp. All rights reserved.
package ctl

import (
	"context"
	"io"
	"os"

	"github.com/molecula/featurebase/v2"
	"github.com/molecula/featurebase/v2/server"
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
	logger := cmd.Logger()

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
		if err := client.ExportCSV(ctx, cmd.Index, cmd.Field, shard, w); err != nil {
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
