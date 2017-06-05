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
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/pilosa/pilosa"
)

// ImportCommand represents a command for bulk importing data.
type ImportCommand struct {
	// Destination host and port.
	Host string `json:"host"`

	// Name of the index & frame to import into.
	Index string `json:"index"`
	Frame string `json:"frame"`

	// Filenames to import from.
	Paths []string `json:"paths"`

	// Size of buffer used to chunk import.
	BufferSize int `json:"bufferSize"`

	// Reusable client.
	Client *pilosa.Client `json:"-"`

	// Standard input/output
	*pilosa.CmdIO
}

// NewImportCommand returns a new instance of ImportCommand.
func NewImportCommand(stdin io.Reader, stdout, stderr io.Writer) *ImportCommand {
	return &ImportCommand{
		CmdIO: pilosa.NewCmdIO(stdin, stdout, stderr),

		BufferSize: 10000000,
	}
}

// Run executes the main program execution.
func (cmd *ImportCommand) Run(ctx context.Context) error {
	logger := log.New(cmd.Stderr, "", log.LstdFlags)

	// Validate arguments.
	// Index and frame are validated early before the files are parsed.
	if cmd.Index == "" {
		return pilosa.ErrIndexRequired
	} else if cmd.Frame == "" {
		return pilosa.ErrFrameRequired
	} else if len(cmd.Paths) == 0 {
		return errors.New("path required")
	}
	// Create a client to the server.
	client, err := pilosa.NewClient(cmd.Host)
	if err != nil {
		return err
	}
	cmd.Client = client

	// Import each path and import by slice.
	for _, path := range cmd.Paths {
		// Parse path into bits.
		logger.Printf("parsing: %s", path)
		if err := cmd.importPath(ctx, path); err != nil {
			return err
		}
	}

	return nil
}

// importPath parses a path into bits and imports it to the server.
func (cmd *ImportCommand) importPath(ctx context.Context, path string) error {
	a := make([]pilosa.Bit, 0, cmd.BufferSize)

	var r *csv.Reader

	if path != "-" {
		// Open file for reading.
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		defer f.Close()

		// Read rows as bits.
		r = csv.NewReader(f)
	} else {
		r = csv.NewReader(cmd.Stdin)
	}

	r.FieldsPerRecord = -1
	rnum := 0
	for {
		rnum++

		// Read CSV row.
		record, err := r.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}

		// Ignore blank rows.
		if record[0] == "" {
			continue
		} else if len(record) < 2 {
			return fmt.Errorf("bad column count on row %d: col=%d", rnum, len(record))
		}

		var bit pilosa.Bit

		// Parse row id.
		rowID, err := strconv.ParseUint(record[0], 10, 64)
		if err != nil {
			return fmt.Errorf("invalid row id on row %d: %q", rnum, record[0])
		}
		bit.RowID = rowID

		// Parse column id.
		columnID, err := strconv.ParseUint(record[1], 10, 64)
		if err != nil {
			return fmt.Errorf("invalid column id on row %d: %q", rnum, record[1])
		}
		bit.ColumnID = columnID

		// Parse time, if exists.
		if len(record) > 2 && record[2] != "" {
			t, err := time.Parse(pilosa.TimeFormat, record[2])
			if err != nil {
				return fmt.Errorf("invalid timestamp on row %d: %q", rnum, record[2])
			}
			bit.Timestamp = t.UnixNano()
		}

		a = append(a, bit)

		// If we've reached the buffer size then import bits.
		if len(a) == cmd.BufferSize {
			if err := cmd.importBits(ctx, a); err != nil {
				return err
			}
			a = a[:0]
		}
	}

	// If there are still bits in the buffer then flush them.
	if err := cmd.importBits(ctx, a); err != nil {
		return err
	}

	return nil
}

// importPath parses a path into bits and imports it to the server.
func (cmd *ImportCommand) importBits(ctx context.Context, bits []pilosa.Bit) error {
	logger := log.New(cmd.Stderr, "", log.LstdFlags)

	// Group bits by slice.
	logger.Printf("grouping %d bits", len(bits))
	bitsBySlice := pilosa.Bits(bits).GroupBySlice()

	// Parse path into bits.
	for slice, bits := range bitsBySlice {
		logger.Printf("importing slice: %d, n=%d", slice, len(bits))
		if err := cmd.Client.Import(ctx, cmd.Index, cmd.Frame, slice, bits); err != nil {
			return err
		}
	}

	return nil
}
