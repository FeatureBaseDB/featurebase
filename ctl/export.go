// Copyright 2021 Molecula Corp. All rights reserved.
package ctl

import (
	"context"
	"fmt"
	"io"
	"os"

	pilosa "github.com/molecula/featurebase/v3"
	"github.com/molecula/featurebase/v3/logger"
	"github.com/molecula/featurebase/v3/server"
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
	logDest logger.Logger

	TLS server.TLSConfig
}

// Logger returns the command's associated Logger to maintain CommandWithTLSSupport interface compatibility
func (cmd *ExportCommand) Logger() logger.Logger {
	return cmd.logDest
}

// NewExportCommand returns a new instance of ExportCommand.
func NewExportCommand(logdest logger.Logger) *ExportCommand {
	return &ExportCommand{
		logDest: logdest,
	}
}

// Run executes the export.
func (cmd *ExportCommand) Run(ctx context.Context) error {
	logger := cmd.Logger()

	// Validate arguments.
	if cmd.Index == "" {
		return fmt.Errorf("%w: %v", UsageError, pilosa.ErrIndexRequired)
	} else if cmd.Field == "" {
		return fmt.Errorf("%w: %v", UsageError, pilosa.ErrFieldRequired)
	}

	// Use output file, if specified.
	// Otherwise use STDOUT.
	var w io.Writer = os.Stdout
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

	return nil
}

func (cmd *ExportCommand) TLSHost() string {
	return cmd.Host
}

func (cmd *ExportCommand) TLSConfiguration() server.TLSConfig {
	return cmd.TLS
}
