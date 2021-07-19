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
	"fmt"
	"io"

	"github.com/molecula/featurebase/v2"
	"github.com/molecula/featurebase/v2/rbf"
)

// RBFPageCommand represents a command for printing data for a single RBF page.
type RBFPageCommand struct {
	// Filepath to the RBF database.
	Path string

	// Page numbers to print.
	Pgnos []uint32

	// Standard input/output
	*pilosa.CmdIO
}

// NewRBFPageCommand returns a new instance of RBFPageCommand.
func NewRBFPageCommand(stdin io.Reader, stdout, stderr io.Writer) *RBFPageCommand {
	return &RBFPageCommand{
		CmdIO: pilosa.NewCmdIO(stdin, stdout, stderr),
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
		fmt.Fprintln(cmd.Stdout, "")
	}

	return nil
}

func (cmd *RBFPageCommand) printMetaPage(page *rbf.MetaPage) {
	fmt.Fprintf(cmd.Stdout, "Pgno: %d\n", page.Pgno)
	fmt.Fprintf(cmd.Stdout, "Type: meta\n")
	fmt.Fprintf(cmd.Stdout, "PageN: %d\n", page.PageN)
	fmt.Fprintf(cmd.Stdout, "WALID: %d\n", page.WALID)
	fmt.Fprintf(cmd.Stdout, "Root Record Pgno: %d\n", page.RootRecordPageNo)
	fmt.Fprintf(cmd.Stdout, "Freelist Pgno: %d\n", page.FreelistPageNo)
}

func (cmd *RBFPageCommand) printRootRecordPage(page *rbf.RootRecordPage) {
	fmt.Fprintf(cmd.Stdout, "Pgno: %d\n", page.Pgno)
	fmt.Fprintf(cmd.Stdout, "Type: root record\n")
	fmt.Fprintf(cmd.Stdout, "Next: %d\n", page.Next)
	fmt.Fprintf(cmd.Stdout, "Records: n=%d\n", len(page.Records))
	for i, rec := range page.Records {
		fmt.Fprintf(cmd.Stdout, "[%d]: name=%q pgno=%d\n", i, rec.Name, rec.Pgno)
	}
}

func (cmd *RBFPageCommand) printLeafPage(page *rbf.LeafPage) {
	fmt.Fprintf(cmd.Stdout, "Pgno: %d\n", page.Pgno)
	fmt.Fprintf(cmd.Stdout, "Type: leaf\n")
	fmt.Fprintf(cmd.Stdout, "Cells: n=%d\n", len(page.Cells))
	for i, cell := range page.Cells {
		if cell.Type == rbf.ContainerTypeBitmapPtr {
			fmt.Fprintf(cmd.Stdout, "[%d]: key=%d type=%s pgno=%d\n", i, cell.Key, cell.Type, cell.Pgno)
		} else {
			fmt.Fprintf(cmd.Stdout, "[%d]: key=%d type=%s values=%v\n", i, cell.Key, cell.Type, cell.Values)
		}
	}
}

func (cmd *RBFPageCommand) printBranchPage(page *rbf.BranchPage) {
	fmt.Fprintf(cmd.Stdout, "Pgno: %d\n", page.Pgno)
	fmt.Fprintf(cmd.Stdout, "Type: branch\n")
	fmt.Fprintf(cmd.Stdout, "Cells: n=%d\n", len(page.Cells))
	for i, cell := range page.Cells {
		fmt.Fprintf(cmd.Stdout, "[%d]: key=%d flags=%d pgno=%d\n", i, cell.Key, cell.Flags, cell.Pgno)
	}
}

func (cmd *RBFPageCommand) printBitmapPage(page *rbf.BitmapPage) {
	fmt.Fprintf(cmd.Stdout, "Pgno: %d\n", page.Pgno)
	fmt.Fprintf(cmd.Stdout, "Type: bitmap\n")
	fmt.Fprintf(cmd.Stdout, "Values: %v\n", page.Values)
}

func (cmd *RBFPageCommand) printFreePage(page *rbf.FreePage) {
	fmt.Fprintf(cmd.Stdout, "Pgno: %d\n", page.Pgno)
	fmt.Fprintf(cmd.Stdout, "Type: free\n")
}
