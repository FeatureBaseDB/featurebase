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
	"sort"
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

	// Options for index & frame to be created if they don't exist
	IndexOptions pilosa.IndexOptions
	FrameOptions pilosa.FrameOptions

	// CreateSchema ensures the schema exists before import
	CreateSchema bool

	// For Range-Encoded fields, name of the Field to import into.
	Field string `json:"field"`

	// Filenames to import from.
	Paths []string `json:"paths"`

	// Size of buffer used to chunk import.
	BufferSize int `json:"bufferSize"`

	// Enables sorting of data file before import.
	Sort bool `json:"sort"`

	// Reusable client.
	Client pilosa.InternalClient `json:"-"`

	// Standard input/output
	*pilosa.CmdIO

	TLS pilosa.TLSConfig
}

// NewImportCommand returns a new instance of ImportCommand.
func NewImportCommand(stdin io.Reader, stdout, stderr io.Writer) *ImportCommand {
	return &ImportCommand{
		CmdIO:      pilosa.NewCmdIO(stdin, stdout, stderr),
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
	client, err := CommandClient(cmd)
	if err != nil {
		return err
	}
	cmd.Client = client

	if cmd.CreateSchema {
		err := cmd.ensureSchema(ctx)
		if err != nil {
			return err
		}
	}

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

func (cmd *ImportCommand) ensureSchema(ctx context.Context) error {
	err := cmd.Client.EnsureIndex(ctx, cmd.Index, cmd.IndexOptions)
	if err != nil {
		return fmt.Errorf("Error Creating Index: %s", err)
	}
	err = cmd.Client.EnsureFrame(ctx, cmd.Index, cmd.Frame, cmd.FrameOptions)
	if err != nil {
		return fmt.Errorf("Error Creating Frame: %s", err)
	}
	return nil
}

// importPath parses a path into bits and imports it to the server.
func (cmd *ImportCommand) importPath(ctx context.Context, path string) error {
	// If a field is provided, treat the import data as values to be range-encoded.
	if cmd.Field != "" {
		return cmd.bufferFieldValues(ctx, path)
	} else {
		return cmd.bufferBits(ctx, path)
	}
}

// bufferBits buffers slices of bits to be imported as a batch.
func (cmd *ImportCommand) bufferBits(ctx context.Context, path string) error {
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

// importBits sends batches of bits to the server.
func (cmd *ImportCommand) importBits(ctx context.Context, bits []pilosa.Bit) error {
	logger := log.New(cmd.Stderr, "", log.LstdFlags)

	// Group bits by slice.
	logger.Printf("grouping %d bits", len(bits))
	bitsBySlice := pilosa.Bits(bits).GroupBySlice()

	// Parse path into bits.
	for slice, bits := range bitsBySlice {
		if cmd.Sort {
			sort.Sort(pilosa.BitsByPos(bits))
		}

		logger.Printf("importing slice: %d, n=%d", slice, len(bits))
		if err := cmd.Client.Import(ctx, cmd.Index, cmd.Frame, slice, bits); err != nil {
			return err
		}
	}

	return nil

}

// bufferFieldValues buffers slices of fieldValues to be imported as a batch.
func (cmd *ImportCommand) bufferFieldValues(ctx context.Context, path string) error {
	a := make([]pilosa.FieldValue, 0, cmd.BufferSize)

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

		var val pilosa.FieldValue

		// Parse column id.
		columnID, err := strconv.ParseUint(record[0], 10, 64)
		if err != nil {
			return fmt.Errorf("invalid column id on row %d: %q", rnum, record[0])
		}
		val.ColumnID = columnID

		// Parse field value.
		value, err := strconv.ParseInt(record[1], 10, 64)
		if err != nil {
			return fmt.Errorf("invalid value on row %d: %q", rnum, record[1])
		}
		val.Value = value

		a = append(a, val)

		// If we've reached the buffer size then import field values.
		if len(a) == cmd.BufferSize {
			if err := cmd.importFieldValues(ctx, a); err != nil {
				return err
			}
			a = a[:0]
		}
	}

	// If there are still values in the buffer then flush them.
	if err := cmd.importFieldValues(ctx, a); err != nil {
		return err
	}

	return nil
}

// importFieldValues sends batches of fieldValues to the server.
func (cmd *ImportCommand) importFieldValues(ctx context.Context, vals []pilosa.FieldValue) error {
	logger := log.New(cmd.Stderr, "", log.LstdFlags)

	// Group vals by slice.
	logger.Printf("grouping %d vals", len(vals))
	valsBySlice := pilosa.FieldValues(vals).GroupBySlice()

	// Parse path into field values.
	for slice, vals := range valsBySlice {
		if cmd.Sort {
			sort.Sort(pilosa.FieldValues(vals))
		}

		logger.Printf("importing slice: %d, n=%d", slice, len(vals))
		if err := cmd.Client.ImportValue(ctx, cmd.Index, cmd.Frame, cmd.Field, slice, vals); err != nil {
			return err
		}
	}

	return nil
}

func (cmd *ImportCommand) TLSHost() string {
	return cmd.Host
}

func (cmd *ImportCommand) TLSConfiguration() pilosa.TLSConfig {
	return cmd.TLS
}
