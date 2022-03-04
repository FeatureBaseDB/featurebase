// Copyright 2021 Molecula Corp. All rights reserved.
package ctl

import (
	"context"
	"fmt"
	"io"

	"github.com/molecula/featurebase/v3"
	"github.com/molecula/featurebase/v3/rbf"
	"github.com/molecula/featurebase/v3/txkey"
)

// RBFPagesCommand represents a command for printing a list of RBF page metadata.
type RBFPagesCommand struct {
	// Filepath to the RBF database.
	Path string

	// Print the b-tree key with each row.
	WithTree bool

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

	// Iterate over each page and grab info.
	infos, err := tx.PageInfos()
	if err != nil {
		fmt.Fprintln(cmd.Stdout, "ERRORS:")
		switch err := err.(type) {
		case rbf.ErrorList:
			for i := range err {
				fmt.Fprintln(cmd.Stdout, err[i])
			}
		default:
			fmt.Fprintln(cmd.Stdout, err)
		}
		fmt.Fprintln(cmd.Stdout, "")
	}

	// Write header.
	fmt.Fprint(cmd.Stdout, "ID       ")
	fmt.Fprint(cmd.Stdout, "TYPE       ")
	if cmd.WithTree {
		fmt.Fprint(cmd.Stdout, "TREE                           ")
	}
	fmt.Fprintln(cmd.Stdout, "EXTRA")

	fmt.Fprint(cmd.Stdout, "======== ")
	fmt.Fprint(cmd.Stdout, "========== ")
	if cmd.WithTree {
		fmt.Fprint(cmd.Stdout, "============================== ")
	}
	fmt.Fprintln(cmd.Stdout, "====================")

	// Print one line for each page.
	for pgno, info := range infos {
		fmt.Fprintf(cmd.Stdout, "%-8d ", pgno)
		switch info := info.(type) {
		case *rbf.MetaPageInfo:
			fmt.Fprintf(cmd.Stdout, "%-10s ", "meta")
			if cmd.WithTree {
				fmt.Fprintf(cmd.Stdout, "%-30q ", "")
			}
			fmt.Fprintf(cmd.Stdout, "pageN=%d,walid=%d,rootrec=%d,freelist=%d\n", info.PageN, info.WALID, info.RootRecordPageNo, info.FreelistPageNo)

		case *rbf.RootRecordPageInfo:
			fmt.Fprintf(cmd.Stdout, "%-10s ", "rootrec")
			if cmd.WithTree {
				fmt.Fprintf(cmd.Stdout, "%-30q ", "")
			}
			fmt.Fprintf(cmd.Stdout, "next=%d\n", info.Next)

		case *rbf.LeafPageInfo:
			fmt.Fprintf(cmd.Stdout, "%-10s ", "leaf")
			if cmd.WithTree {
				fmt.Fprintf(cmd.Stdout, "%-30q ", prefixToString(info.Tree))
			}
			fmt.Fprintf(cmd.Stdout, "flags=x%x,celln=%d\n", info.Flags, info.CellN)

		case *rbf.BranchPageInfo:
			fmt.Fprintf(cmd.Stdout, "%-10s ", "branch")
			if cmd.WithTree {
				fmt.Fprintf(cmd.Stdout, "%-30q ", prefixToString(info.Tree))
			}
			fmt.Fprintf(cmd.Stdout, "flags=x%x,celln=%d\n", info.Flags, info.CellN)

		case *rbf.BitmapPageInfo:
			fmt.Fprintf(cmd.Stdout, "%-10s ", "bitmap")
			if cmd.WithTree {
				fmt.Fprintf(cmd.Stdout, "%-30q ", prefixToString(info.Tree))
			}
			fmt.Fprintf(cmd.Stdout, "-\n")

		case *rbf.FreePageInfo:
			fmt.Fprintf(cmd.Stdout, "%-10s ", "free")
			if cmd.WithTree {
				fmt.Fprintf(cmd.Stdout, "%-30q ", "")
			}
			fmt.Fprintf(cmd.Stdout, "-\n")

		default:
			fmt.Fprintf(cmd.Stdout, "unknown [%T]\n", info)
		}
	}

	return nil
}

func prefixToString(s string) (ret string) {
	defer func() {
		if err := recover(); err != nil {
			ret = s
		}
	}()
	return txkey.PrefixToString([]byte(s))
}
