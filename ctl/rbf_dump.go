// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package ctl

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/molecula/featurebase/v3/logger"
	"github.com/molecula/featurebase/v3/rbf"
)

// RBFDumpCommand represents a command for dumping raw data for an RBF page.
type RBFDumpCommand struct {
	// Filepath to the RBF database.
	Path string

	// Page numbers to print.
	Pgnos []uint32

	// Standard input/output
	stdout  io.Writer
	logDest logger.Logger
}

// NewRBFDumpCommand returns a new instance of RBFDumpCommand.
func NewRBFDumpCommand(logdest logger.Logger) *RBFDumpCommand {
	return &RBFDumpCommand{
		stdout:  os.Stdout,
		logDest: logdest,
	}
}

// Run executes the export.
func (cmd *RBFDumpCommand) Run(ctx context.Context) error {
	// Open database.
	db := rbf.NewDB(cmd.Path, nil)
	if err := db.Open(); err != nil {
		return err
	}
	defer db.Close()

	// Execute with a transaction.
	tx, err := db.Begin(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Fetch each page & dump.
	for _, pgno := range cmd.Pgnos {
		buf, err := tx.PageData(pgno)
		if err != nil {
			return err
		}

		fmt.Fprintf(cmd.stdout, "## PAGE %d\n", pgno)
		fmt.Fprintln(cmd.stdout, compressedHexDump(buf))
		fmt.Fprintln(cmd.stdout, "")
	}

	return nil
}

func compressedHexDump(b []byte) string {
	const prefixN = len("00000000")

	var output []string
	var prev string
	var ellipsis bool

	lines := strings.Split(strings.TrimSpace(hex.Dump(b)), "\n")
	for i, line := range lines {
		// Add line to output if it is not repeating or the last line.
		if i == 0 || i == len(lines)-1 || trimPrefixN(line, prefixN) != trimPrefixN(prev, prefixN) {
			output = append(output, line)
			prev, ellipsis = line, false
			continue
		}

		// Add an ellipsis for the first duplicate line.
		if !ellipsis {
			output = append(output, "...")
			ellipsis = true
			continue
		}
	}

	return strings.Join(output, "\n")
}

// trimPrefixN trims n bytes from the beginning of a string.
func trimPrefixN(s string, n int) string {
	if len(s) < n {
		return ""
	}
	return s[n:]
}
