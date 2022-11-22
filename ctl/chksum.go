// Copyright 2021 Molecula Corp. All rights reserved.
package ctl

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"os"

	"github.com/cespare/xxhash"
	pilosa "github.com/molecula/featurebase/v3"
	"github.com/molecula/featurebase/v3/logger"
	"github.com/molecula/featurebase/v3/server"
)

// ChkSumCommand represents a command for backing up a Pilosa node.
type ChkSumCommand struct { // nolint: maligned
	tlsConfig *tls.Config

	// Destination host and port.
	Host string `json:"host"`

	// Reusable client.
	client *pilosa.InternalClient

	// Standard input/output
	stdout  io.Writer
	logDest logger.Logger

	TLS server.TLSConfig
}

// Logger returns the command's associated Logger to maintain CommandWithTLSSupport interface compatibility
func (cmd *ChkSumCommand) Logger() logger.Logger {
	return cmd.logDest
}

// NewChkSumCommand returns a new instance of BackupCommand.
func NewChkSumCommand(logdest logger.Logger) *ChkSumCommand {
	return &ChkSumCommand{
		stdout:  os.Stdout,
		logDest: logdest,
	}
}

// Run executes the main program execution.
func (cmd *ChkSumCommand) Run(ctx context.Context) (err error) {
	// Parse TLS configuration for node-specific clients.
	tls := cmd.TLSConfiguration()
	if cmd.tlsConfig, err = server.GetTLSConfig(&tls, cmd.Logger()); err != nil {
		return fmt.Errorf("parsing tls config: %w", err)
	}

	// Create a client to the server.
	client, err := commandClient(cmd)
	if err != nil {
		return fmt.Errorf("creating client: %w", err)
	}
	cmd.client = client

	// Determine the field type in order to correctly handle the input data.
	indexes, err := cmd.client.Schema(ctx)
	if err != nil {
		return fmt.Errorf("getting schema: %w", err)
	}
	schema := &pilosa.Schema{Indexes: indexes}

	// Create a hash of all the Counts for every row in the index
	h := xxhash.New()

	for _, ii := range schema.Indexes {
		qa := &pilosa.QueryRequest{Index: ii.Name, Query: "All()"}
		rs, err := client.Query(ctx, ii.Name, qa)
		if err != nil {
			return err
		}

		all := rs.Results[0].(*pilosa.Row)
		if len(all.Keys) > 0 {
			allString := fmt.Sprintf("%v", all.Keys)
			_, _ = h.Write([]byte(allString))
		} else {
			_, _ = h.Write(all.Roaring())
		}

		for _, field := range ii.Fields {
			switch field.Options.Type {
			case pilosa.FieldTypeInt, pilosa.FieldTypeDecimal, pilosa.FieldTypeTimestamp:
				sumPql := fmt.Sprintf("Sum(field=%v)", field.Name)
				qr := &pilosa.QueryRequest{Index: ii.Name, Query: sumPql}
				res, err := client.Query(ctx, ii.Name, qr)
				if err != nil {
					return err
				}
				sum := res.Results[0].(pilosa.ValCount)
				s := fmt.Sprintf("%v=%v", field.Name, sum.Count)
				_, _ = h.Write([]byte(s))

			default:
				rowsPql := fmt.Sprintf("Rows(%v)", field.Name)
				qr := &pilosa.QueryRequest{Index: ii.Name, Query: rowsPql}
				res, err := client.Query(ctx, ii.Name, qr)
				if err != nil {
					return err
				}
				for _, item := range res.Results {
					rowids := item.(*pilosa.RowIdentifiers)
					// either rowids or keys
					for _, row := range rowids.Keys {
						countPql := fmt.Sprintf(`Count(Row(%v="%v"))`, field.Name, row)
						qr := &pilosa.QueryRequest{Index: ii.Name, Query: countPql}
						res, err := client.Query(ctx, ii.Name, qr)
						if err != nil {
							return err
						}
						count := res.Results[0].(uint64)
						s := fmt.Sprintf("%v.%v=%v", field.Name, row, count)
						_, _ = h.Write([]byte(s))
					}

					for _, row := range rowids.Rows {
						countPql := fmt.Sprintf("Count(Row(%v=%v))", field.Name, row)
						qr := &pilosa.QueryRequest{Index: ii.Name, Query: countPql}
						res, err := client.Query(ctx, ii.Name, qr)
						if err != nil {
							return err
						}
						count := res.Results[0].(uint64)
						s := fmt.Sprintf("%v.%v=%v", field.Name, row, count)
						_, _ = h.Write([]byte(s))

					}
				}
			}

		}
		fmt.Fprintf(cmd.stdout, "hash:%x\n", h.Sum(nil))
	}

	return nil
}

func (cmd *ChkSumCommand) TLSHost() string { return cmd.Host }

func (cmd *ChkSumCommand) TLSConfiguration() server.TLSConfig { return cmd.TLS }
