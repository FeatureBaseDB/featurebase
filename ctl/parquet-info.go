// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package ctl

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"

	"github.com/apache/arrow/go/v10/arrow/memory"
	"github.com/apache/arrow/go/v10/parquet/file"
	"github.com/apache/arrow/go/v10/parquet/pqarrow"
	pilosa "github.com/featurebasedb/featurebase/v3"
	"github.com/featurebasedb/featurebase/v3/logger"
)

// ParquetInfoCommand represents a command for displaying info about a parquet file
type ParquetInfoCommand struct {
	// Filepath or URL  to the parquet file.
	Path string

	// Standard input/output
	stdout  io.Writer
	logDest logger.Logger
}

// NewParquetInfoCommand returns a new instance of ParquetInfoCommand.
func NewParquetInfoCommand(logdest logger.Logger) *ParquetInfoCommand {
	return &ParquetInfoCommand{
		stdout:  os.Stdout,
		logDest: logdest,
	}
}

// Run displays schema and samples data from a parquet file
func (cmd *ParquetInfoCommand) Run(ctx context.Context) error {
	// Open database.
	var f *os.File
	_, err := url.ParseRequestURI(cmd.Path)
	if err == nil { // treat as a URL
		response, err := http.Get(cmd.Path)
		if err != nil {
			return err
		}
		if response.StatusCode != 200 {
			return fmt.Errorf("unexpected response %d", response.StatusCode)
		}
		defer response.Body.Close()
		// download to temp file first
		f, err = os.CreateTemp("", "BulkParquetFile.parquet")
		if err != nil {
			return fmt.Errorf("error creating tempfile %v", err)
		}
		_, err = io.Copy(f, response.Body)
		if err != nil {
			return fmt.Errorf("error downloading url %v %v", cmd.Path, err)
		}
		defer os.Remove(f.Name())

		_, err = f.Seek(0, io.SeekStart)
		if err != nil {
			return fmt.Errorf("error reseting file for reading %v ", err)
		}
	} else {
		f, err = os.Open(cmd.Path)
		if err != nil {
			return err
		}
	}

	pf, err := file.NewParquetReader(f)
	if err != nil {
		return err
	}
	mem := memory.NewGoAllocator()
	reader, err := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{}, mem)
	if err != nil {
		return err
	}
	table, err := reader.ReadTable(ctx)
	if err != nil {
		return err
	}
	// print file name
	fmt.Printf("\n\nName:%v\n", cmd.Path)
	// print schema
	schema := table.Schema()
	fields := schema.Fields()
	for i, field := range fields {
		fmt.Printf("%v. Name: %v\n", i, field.Name)
		fmt.Printf("%v. Type: %v\n", i, field.Type)
		fmt.Printf("%v. Nullable: %v\n\n", i, field.Nullable)
	}
	bt := pilosa.BasicTableFromArrow(table, mem)
	// print num rows
	numRows := int(bt.NumRows())
	fmt.Printf("Number of rows:%v\n", numRows)
	if numRows > 10 {
		numRows = 10
	}
	fmt.Println("Sample:")
	// print at most 10 sample rows in table format
	for _, field := range fields {
		fmt.Printf("%v\t", field.Name)
	}
	fmt.Println("")
	for i := 0; i < numRows; i++ {
		for j := 0; j < len(fields); j++ {
			fmt.Printf("%v\t", bt.Get(j, i))
		}
		fmt.Println("")
	}

	return nil
}
