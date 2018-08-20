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
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strconv"
	"time"

	"github.com/pilosa/pilosa"
	"github.com/pilosa/pilosa/http"
	"github.com/pilosa/pilosa/server"
	"github.com/pkg/errors"
)

// ImportCommand represents a command for bulk importing data.
type ImportCommand struct { // nolint: maligned
	// Destination host and port.
	Host string `json:"host"`

	// Name of the index & field to import into.
	Index string `json:"index"`
	Field string `json:"field"`

	// Options for index & field to be created if they don't exist
	indexOptions pilosa.IndexOptions

	// CreateSchema ensures the schema exists before import
	CreateSchema bool

	// REMOVED: Indicates that the payload should be treated as string keys.
	// TODO: remove this in a future release
	StringKeys bool `json:"StringKeys"`

	// Filenames to import from.
	Paths []string `json:"paths"`

	// Size of buffer used to chunk import.
	BufferSize int `json:"bufferSize"`

	// Enables sorting of data file before import.
	Sort bool `json:"sort"`

	// Reusable client.
	client pilosa.InternalClient

	// Standard input/output
	*pilosa.CmdIO

	TLS server.TLSConfig
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

	// REMOVED: warning that --string-keys flag has been deprecated.
	if cmd.StringKeys {
		logger.Printf("REMOVED: The string-keys flag is no longer used.")
	}

	// Validate arguments.
	// Index and field are validated early before the files are parsed.
	if cmd.Index == "" {
		return pilosa.ErrIndexRequired
	} else if cmd.Field == "" {
		return pilosa.ErrFieldRequired
	} else if len(cmd.Paths) == 0 {
		return errors.New("path required")
	}
	// Create a client to the server.
	client, err := commandClient(cmd)
	if err != nil {
		return errors.Wrap(err, "creating client")
	}
	cmd.client = client

	if cmd.CreateSchema {
		err := cmd.ensureSchema(ctx)
		if err != nil {
			return errors.Wrap(err, "ensuring schema")
		}
	}

	// Determine the field type in order to correctly handle the input data.
	fieldType := pilosa.DefaultFieldType
	schema, err := cmd.client.Schema(ctx)
	if err != nil {
		return errors.Wrap(err, "getting schema")
	}

	var useColumnKeys, useRowKeys bool
	for _, index := range schema {
		if index.Name == cmd.Index {
			useColumnKeys = index.Options.Keys
			for _, field := range index.Fields {
				if field.Name == cmd.Field {
					useRowKeys = field.Options.Keys
					fieldType = field.Options.Type
					break
				}
			}
			break
		}
	}

	// Import each path and import by shard.
	for _, path := range cmd.Paths {
		logger.Printf("parsing: %s", path)
		if err := cmd.importPath(ctx, fieldType, useColumnKeys, useRowKeys, path); err != nil {
			return err
		}
	}

	return nil
}

func (cmd *ImportCommand) ensureSchema(ctx context.Context) error {
	err := cmd.client.EnsureIndex(ctx, cmd.Index, cmd.indexOptions)
	if err != nil {
		return fmt.Errorf("Error Creating Index: %s", err)
	}
	err = cmd.client.EnsureField(ctx, cmd.Index, cmd.Field)
	if err != nil {
		return fmt.Errorf("Error Creating Field: %s", err)
	}
	return nil
}

// importPath parses a path into bits and imports it to the server.
func (cmd *ImportCommand) importPath(ctx context.Context, fieldType string, useColumnKeys, useRowKeys bool, path string) error {
	// If fieldType is `int`, treat the import data as values to be range-encoded.
	if fieldType == pilosa.FieldTypeInt {
		return cmd.bufferValues(ctx, useColumnKeys, path)
	}
	return cmd.bufferBits(ctx, useColumnKeys, useRowKeys, path)
}

// bufferBits buffers slices of bits to be imported as a batch.
func (cmd *ImportCommand) bufferBits(ctx context.Context, useColumnKeys, useRowKeys bool, path string) error {
	a := make([]pilosa.Bit, 0, cmd.BufferSize)

	var r *csv.Reader

	if path != "-" {
		// Open file for reading.
		f, err := os.Open(path)
		if err != nil {
			return errors.Wrap(err, "opening file")
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
			return errors.Wrap(err, "reading")
		}

		// Ignore blank rows.
		if record[0] == "" {
			continue
		} else if len(record) < 2 {
			return fmt.Errorf("bad column count on row %d: col=%d", rnum, len(record))
		}

		var bit pilosa.Bit

		// Parse row id.
		if useRowKeys {
			bit.RowKey = record[0]
		} else {
			if bit.RowID, err = strconv.ParseUint(record[0], 10, 64); err != nil {
				return fmt.Errorf("invalid row id on row %d: %q", rnum, record[0])
			}
		}

		// Parse column id.
		if useColumnKeys {
			bit.ColumnKey = record[1]
		} else {
			if bit.ColumnID, err = strconv.ParseUint(record[1], 10, 64); err != nil {
				return fmt.Errorf("invalid column id on row %d: %q", rnum, record[1])
			}
		}

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
			if err := cmd.importBits(ctx, useColumnKeys, useRowKeys, a); err != nil {
				return err
			}
			a = a[:0]
		}
	}

	// If there are still bits in the buffer then flush them.
	return cmd.importBits(ctx, useColumnKeys, useRowKeys, a)
}

// importBits sends batches of bits to the server.
func (cmd *ImportCommand) importBits(ctx context.Context, useColumnKeys, useRowKeys bool, bits []pilosa.Bit) error {
	logger := log.New(cmd.Stderr, "", log.LstdFlags)

	// If keys are used, all bits are sent to the primary translate store (i.e. coordinator).
	if useColumnKeys || useRowKeys {
		logger.Printf("importing keys: n=%d", len(bits))
		if err := cmd.client.ImportK(ctx, cmd.Index, cmd.Field, bits); err != nil {
			return errors.Wrap(err, "importing keys")
		}
		return nil
	}

	// Group bits by shard.
	logger.Printf("grouping %d bits", len(bits))
	bitsByShard := http.Bits(bits).GroupByShard()

	// Parse path into bits.
	for shard, chunk := range bitsByShard {
		if cmd.Sort {
			sort.Sort(http.BitsByPos(chunk))
		}

		logger.Printf("importing shard: %d, n=%d", shard, len(chunk))
		if err := cmd.client.Import(ctx, cmd.Index, cmd.Field, shard, chunk); err != nil {
			return errors.Wrap(err, "importing")
		}
	}

	return nil
}

// bufferValues buffers slices of FieldValues to be imported as a batch.
func (cmd *ImportCommand) bufferValues(ctx context.Context, useColumnKeys bool, path string) error {
	a := make([]pilosa.FieldValue, 0, cmd.BufferSize)

	var r *csv.Reader

	if path != "-" {
		// Open file for reading.
		f, err := os.Open(path)
		if err != nil {
			return errors.Wrap(err, "opening file")
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
			return errors.Wrap(err, "reading")
		}

		// Ignore blank rows.
		if record[0] == "" {
			continue
		} else if len(record) < 2 {
			return fmt.Errorf("bad column count on row %d: col=%d", rnum, len(record))
		}

		var val pilosa.FieldValue

		// Parse column id.
		if useColumnKeys {
			val.ColumnKey = record[0]
		} else {
			if val.ColumnID, err = strconv.ParseUint(record[0], 10, 64); err != nil {
				return fmt.Errorf("invalid column id on row %d: %q", rnum, record[0])
			}
		}

		// Parse FieldValue.
		value, err := strconv.ParseInt(record[1], 10, 64)
		if err != nil {
			return fmt.Errorf("invalid value on row %d: %q", rnum, record[1])
		}
		val.Value = value

		a = append(a, val)

		// If we've reached the buffer size then import FieldValues.
		if len(a) == cmd.BufferSize {
			if err := cmd.importValues(ctx, useColumnKeys, a); err != nil {
				return err
			}
			a = a[:0]
		}
	}

	// If there are still values in the buffer then flush them.
	return cmd.importValues(ctx, useColumnKeys, a)
}

// importValues sends batches of FieldValues to the server.
func (cmd *ImportCommand) importValues(ctx context.Context, useColumnKeys bool, vals []pilosa.FieldValue) error {
	logger := log.New(cmd.Stderr, "", log.LstdFlags)

	// If keys are used, all values are sent to the primary translate store (i.e. coordinator).
	if useColumnKeys {
		logger.Printf("importing keyed values: n=%d", len(vals))
		if err := cmd.client.ImportValueK(ctx, cmd.Index, cmd.Field, vals); err != nil {
			return errors.Wrap(err, "importing keys")
		}
		return nil
	}

	// Group vals by shard.
	logger.Printf("grouping %d vals", len(vals))
	valsByShard := http.FieldValues(vals).GroupByShard()

	// Parse path into FieldValues.
	for shard, vals := range valsByShard {
		if cmd.Sort {
			sort.Sort(http.FieldValues(vals))
		}

		logger.Printf("importing shard: %d, n=%d", shard, len(vals))
		if err := cmd.client.ImportValue(ctx, cmd.Index, cmd.Field, shard, vals); err != nil {
			return errors.Wrap(err, "importing values")
		}
	}

	return nil
}

func (cmd *ImportCommand) TLSHost() string {
	return cmd.Host
}

func (cmd *ImportCommand) TLSConfiguration() server.TLSConfig {
	return cmd.TLS
}
