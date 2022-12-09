// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package ctl

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/featurebasedb/featurebase/v3/logger"
	"github.com/featurebasedb/featurebase/v3/rbf"
)

// RBFPageCommand represents a command for printing data for a single RBF page.
type RBFPageCommand struct {
	// Filepath to the RBF database.
	Path string

	// Page numbers to print.
	Pgnos []uint32

	// Standard input/output
	stdout  io.Writer
	logDest logger.Logger
}

// NewRBFPageCommand returns a new instance of RBFPageCommand.
func NewRBFPageCommand(logdest logger.Logger) *RBFPageCommand {
	return &RBFPageCommand{
		stdout:  os.Stdout,
		logDest: logdest,
	}
}

// Run executes the export.
func (cmd *RBFPageCommand) Run(ctx context.Context) error {
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

	// Fetch the page.
	pages, err := tx.Pages(cmd.Pgnos)
	if err != nil {
		return err
	}

	for _, page := range pages {
		switch page := page.(type) {
		case *rbf.MetaPage:
			cmd.printMetaPage(page)
		case *rbf.RootRecordPage:
			cmd.printRootRecordPage(page)
		case *rbf.LeafPage:
			cmd.printLeafPage(page)
		case *rbf.BranchPage:
			cmd.printBranchPage(page)
		case *rbf.BitmapPage:
			cmd.printBitmapPage(page)
		case *rbf.FreePage:
			cmd.printFreePage(page)
		default:
			return fmt.Errorf("unexpected page type %T", page)
		}
		fmt.Fprintln(cmd.stdout, "")
	}

	return nil
}

func (cmd *RBFPageCommand) printMetaPage(page *rbf.MetaPage) {
	fmt.Fprintf(cmd.stdout, "Pgno: %d\n", page.Pgno)
	fmt.Fprintf(cmd.stdout, "Type: meta\n")
	fmt.Fprintf(cmd.stdout, "PageN: %d\n", page.PageN)
	fmt.Fprintf(cmd.stdout, "WALID: %d\n", page.WALID)
	fmt.Fprintf(cmd.stdout, "Root Record Pgno: %d\n", page.RootRecordPageNo)
	fmt.Fprintf(cmd.stdout, "Freelist Pgno: %d\n", page.FreelistPageNo)
}

func (cmd *RBFPageCommand) printRootRecordPage(page *rbf.RootRecordPage) {
	fmt.Fprintf(cmd.stdout, "Pgno: %d\n", page.Pgno)
	fmt.Fprintf(cmd.stdout, "Type: root record\n")
	fmt.Fprintf(cmd.stdout, "Next: %d\n", page.Next)
	fmt.Fprintf(cmd.stdout, "Records: n=%d\n", len(page.Records))
	for i, rec := range page.Records {
		fmt.Fprintf(cmd.stdout, "[%d]: name=%q pgno=%d\n", i, rec.Name, rec.Pgno)
	}
}

func (cmd *RBFPageCommand) printLeafPage(page *rbf.LeafPage) {
	fmt.Fprintf(cmd.stdout, "Pgno: %d\n", page.Pgno)
	fmt.Fprintf(cmd.stdout, "Type: leaf\n")
	fmt.Fprintf(cmd.stdout, "Cells: n=%d\n", len(page.Cells))
	for i, cell := range page.Cells {
		if cell.Type == rbf.ContainerTypeBitmapPtr {
			fmt.Fprintf(cmd.stdout, "[%d]: key=%d type=%s pgno=%d\n", i, cell.Key, cell.Type, cell.Pgno)
		} else {
			fmt.Fprintf(cmd.stdout, "[%d]: key=%d type=%s values=%v\n", i, cell.Key, cell.Type, cell.Values)
		}
	}
}

func (cmd *RBFPageCommand) printBranchPage(page *rbf.BranchPage) {
	fmt.Fprintf(cmd.stdout, "Pgno: %d\n", page.Pgno)
	fmt.Fprintf(cmd.stdout, "Type: branch\n")
	fmt.Fprintf(cmd.stdout, "Cells: n=%d\n", len(page.Cells))
	for i, cell := range page.Cells {
		fmt.Fprintf(cmd.stdout, "[%d]: key=%d flags=%d pgno=%d\n", i, cell.Key, cell.Flags, cell.Pgno)
	}
}

func (cmd *RBFPageCommand) printBitmapPage(page *rbf.BitmapPage) {
	fmt.Fprintf(cmd.stdout, "Pgno: %d\n", page.Pgno)
	fmt.Fprintf(cmd.stdout, "Type: bitmap\n")
	fmt.Fprintf(cmd.stdout, "Values: %v\n", page.Values)
}

func (cmd *RBFPageCommand) printFreePage(page *rbf.FreePage) {
	fmt.Fprintf(cmd.stdout, "Pgno: %d\n", page.Pgno)
	fmt.Fprintf(cmd.stdout, "Type: free\n")
}
