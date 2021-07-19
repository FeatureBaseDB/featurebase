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
	"encoding/hex"
	"fmt"
	"io"
	"strings"

	"github.com/molecula/featurebase/v2"
	"github.com/molecula/featurebase/v2/rbf"
)

// RBFDumpCommand represents a command for dumping raw data for an RBF page.
type RBFDumpCommand struct {
	// Filepath to the RBF database.
	Path string

	// Page numbers to print.
	Pgnos []uint32

	// Standard input/output
	*pilosa.CmdIO
}

// NewRBFDumpCommand returns a new instance of RBFDumpCommand.
func NewRBFDumpCommand(stdin io.Reader, stdout, stderr io.Writer) *RBFDumpCommand {
	return &RBFDumpCommand{
		CmdIO: pilosa.NewCmdIO(stdin, stdout, stderr),
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

		fmt.Fprintf(cmd.Stdout, "## PAGE %d\n", pgno)
		fmt.Fprintln(cmd.Stdout, compressedHexDump(buf))
		fmt.Fprintln(cmd.Stdout, "")
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
