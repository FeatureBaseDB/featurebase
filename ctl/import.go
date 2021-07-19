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
	"math"
	"os"
	"sort"
	"strconv"
	"time"

	"github.com/molecula/featurebase/v2"
	"github.com/molecula/featurebase/v2/http"
	"github.com/molecula/featurebase/v2/pql"
	"github.com/molecula/featurebase/v2/server"
	"github.com/pkg/errors"
)

// ImportCommand represents a command for bulk importing data.
type ImportCommand struct { // nolint: maligned
	// Destination host and port.
	Host string `json:"host"`

	// Name of the index & field to import into.
	Index string `json:"index"`
	Field string `json:"field"`

	// Options for the index to be created if it doesn't exist
	IndexOptions pilosa.IndexOptions

	// Options for the field to be created if it doesn't exist
	FieldOptions pilosa.FieldOptions

	// CreateSchema ensures the schema exists before import
	CreateSchema bool

	// Clear clears the import data as opposed to setting it.
	Clear bool

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
	logger := cmd.Logger()

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
		if cmd.FieldOptions.Type == "" {
			// set the correct type for the field
			if cmd.FieldOptions.TimeQuantum != "" {
				cmd.FieldOptions.Type = pilosa.FieldTypeTime
			} else if cmd.FieldOptions.Min != pql.NewDecimal(0, 0) || cmd.FieldOptions.Max != pql.NewDecimal(0, 0) {
				cmd.FieldOptions.Type = pilosa.FieldTypeInt
			} else {
				cmd.FieldOptions.Type = pilosa.FieldTypeSet
				cmd.FieldOptions.CacheType = pilosa.CacheTypeRanked
				cmd.FieldOptions.CacheSize = pilosa.DefaultCacheSize
			}
		}
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
	err := cmd.client.EnsureIndex(ctx, cmd.Index, cmd.IndexOptions)
	if err != nil {
		return errors.Wrap(err, "creating index")
	}
	err = cmd.client.EnsureFieldWithOptions(ctx, cmd.Index, cmd.Field, cmd.FieldOptions)
	if err != nil {
		return errors.Wrap(err, "creating field")
	}
	return nil
}

// importPath parses a path into bits and imports it to the server.
func (cmd *ImportCommand) importPath(ctx context.Context, fieldType string, useColumnKeys, useRowKeys bool, path string) error {
	// If fieldType is `int`, treat the import data as values to be range-encoded.
	if fieldType == pilosa.FieldTypeInt || fieldType == pilosa.FieldTypeDecimal {
		return cmd.bufferValues(ctx, useColumnKeys, fieldType == pilosa.FieldTypeDecimal, path)
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

	// If keys are used, all bits are sent to the primary translate store.
	if useColumnKeys || useRowKeys {
		logger.Printf("importing keys: n=%d", len(bits))
		if err := cmd.client.ImportK(ctx, cmd.Index, cmd.Field, bits, pilosa.OptImportOptionsClear(cmd.Clear)); err != nil {
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
		if err := cmd.client.Import(ctx, cmd.Index, cmd.Field, shard, chunk, pilosa.OptImportOptionsClear(cmd.Clear)); err != nil {
			return errors.Wrap(err, "importing")
		}
	}

	return nil
}

// bufferValues buffers slices of record identifiers and values to be imported as a batch.
func (cmd *ImportCommand) bufferValues(ctx context.Context, useColumnKeys, parseAsFloat bool, path string) error {
	req := &pilosa.ImportValueRequest{
		Index: cmd.Index,
		Field: cmd.Field,
		Shard: math.MaxUint64,
	}

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

		// Parse column id.
		if useColumnKeys {
			req.ColumnKeys = append(req.ColumnKeys, record[0])
		} else if columnID, err := strconv.ParseUint(record[0], 10, 64); err == nil {
			req.ColumnIDs = append(req.ColumnIDs, columnID)
		} else {
			return fmt.Errorf("invalid column id on row %d: %q", rnum, record[0])
		}

		// Parse value.
		if parseAsFloat {
			value, err := strconv.ParseFloat(record[1], 64)
			if err != nil {
				return errors.Wrapf(err, "parseing value '%s' as float", record[1])
			}
			req.FloatValues = append(req.FloatValues, value)
		} else {
			value, err := strconv.ParseInt(record[1], 10, 64)
			if err != nil {
				return errors.Wrapf(err, "invalid value on row %d: %q", rnum, record[1])
			}
			req.Values = append(req.Values, value)
		}

		// If we've reached the buffer size then import the batch.
		if len(req.ColumnKeys) == cmd.BufferSize || len(req.ColumnIDs) == cmd.BufferSize {
			if err := cmd.client.ImportValue2(ctx, req, &pilosa.ImportOptions{}); err != nil {
				return errors.Wrap(err, "importing values")
			}
			req.ColumnIDs = req.ColumnIDs[:0]
			req.ColumnKeys = req.ColumnKeys[:0]
			req.Values = req.Values[:0]
			req.FloatValues = req.FloatValues[:0]
		}
	}

	// If there are still values in the buffer then flush them.
	return errors.Wrap(cmd.client.ImportValue2(ctx, req, &pilosa.ImportOptions{}), "importing values")
}

func (cmd *ImportCommand) TLSHost() string {
	return cmd.Host
}

func (cmd *ImportCommand) TLSConfiguration() server.TLSConfig {
	return cmd.TLS
}
