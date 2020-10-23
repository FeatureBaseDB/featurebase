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

	"github.com/pilosa/pilosa/v2"
	"github.com/pilosa/pilosa/v2/rbf"
	"github.com/pilosa/pilosa/v2/txkey"
)

// RBFPagesCommand represents a command for printing a list of RBF page metadata.
type RBFPagesCommand struct {
	// Filepath to the RBF database.
	Path string

	// Standard input/output
	*pilosa.CmdIO
}

// NewRBFPagesCommand returns a new instance of RBFPagesCommand.
func NewRBFPagesCommand(stdin io.Reader, stdout, stderr io.Writer) *RBFPagesCommand {
	return &RBFPagesCommand{
		CmdIO: pilosa.NewCmdIO(stdin, stdout, stderr),
	}
}

// Run executes the export.
func (cmd *RBFPagesCommand) Run(ctx context.Context) error {
	// Open database.
	db := rbf.NewDB(cmd.Path)
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

	// Iterate over each page and grab info.
	infos, err := tx.PageInfos()
	if err != nil {
		return err
	}

	// Write header.
	fmt.Fprintln(cmd.Stdout, "ID       TYPE       TREE                           EXTRA")
	fmt.Fprintln(cmd.Stdout, "======== ========== ============================== ====================")

	// Print one line for each page.
	for pgno, info := range infos {
		switch info := info.(type) {
		case *rbf.MetaPageInfo:
			fmt.Fprintf(cmd.Stdout, "%-8d %-10s %-30q pageN=%d,walid=%d,rootrec=%d,freelist=%d\n", pgno, "meta", "", info.PageN, info.WALID, info.RootRecordPageNo, info.FreelistPageNo)
		case *rbf.RootRecordPageInfo:
			fmt.Fprintf(cmd.Stdout, "%-8d %-10s %-30q next=%d\n", pgno, "rootrec", "", info.Next)
		case *rbf.LeafPageInfo:
			fmt.Fprintf(cmd.Stdout, "%-8d %-10s %-30q flags=x%x,celln=%d\n", pgno, "leaf", txkeyString(info.Tree), info.Flags, info.CellN)
		case *rbf.BranchPageInfo:
			fmt.Fprintf(cmd.Stdout, "%-8d %-10s %-30q flags=x%x,celln=%d\n", pgno, "branch", txkeyString(info.Tree), info.Flags, info.CellN)
		case *rbf.BitmapPageInfo:
			fmt.Fprintf(cmd.Stdout, "%-8d %-10s %-30q -\n", pgno, "bitmap", info.Tree)
		case *rbf.FreePageInfo:
			fmt.Fprintf(cmd.Stdout, "%-8d %-10s %-30q -\n", pgno, "free", "")
		default:
			panic(fmt.Sprintf("unexpected page info type %T", info))
		}
	}

	return nil
}

func txkeyString(s string) (ret string) {
	defer func() {
		if err := recover(); err != nil {
			ret = s
		}
	}()
	return txkey.ToString([]byte(s))
}
