// Copyright 2021 Molecula Corp. All rights reserved.
package ctl

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/molecula/featurebase/v3/logger"
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
	stdout  io.Writer
	logDest logger.Logger
}

// NewRBFPagesCommand returns a new instance of RBFPagesCommand.
func NewRBFPagesCommand(logdest logger.Logger) *RBFPagesCommand {
	return &RBFPagesCommand{
		stdout:  os.Stdout,
		logDest: logdest,
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
		fmt.Fprintln(cmd.stdout, "ERRORS:")
		switch err := err.(type) {
		case rbf.ErrorList:
			for i := range err {
				fmt.Fprintln(cmd.stdout, err[i])
			}
		default:
			fmt.Fprintln(cmd.stdout, err)
		}
		fmt.Fprintln(cmd.stdout, "")
	}

	// Write header.
	fmt.Fprint(cmd.stdout, "ID       ")
	fmt.Fprint(cmd.stdout, "TYPE       ")
	if cmd.WithTree {
		fmt.Fprint(cmd.stdout, "TREE                           ")
	}
	fmt.Fprintln(cmd.stdout, "EXTRA")

	fmt.Fprint(cmd.stdout, "======== ")
	fmt.Fprint(cmd.stdout, "========== ")
	if cmd.WithTree {
		fmt.Fprint(cmd.stdout, "============================== ")
	}
	fmt.Fprintln(cmd.stdout, "====================")

	// Print one line for each page.
	for pgno, info := range infos {
		fmt.Fprintf(cmd.stdout, "%-8d ", pgno)
		switch info := info.(type) {
		case *rbf.MetaPageInfo:
			fmt.Fprintf(cmd.stdout, "%-10s ", "meta")
			if cmd.WithTree {
				fmt.Fprintf(cmd.stdout, "%-30q ", "")
			}
			fmt.Fprintf(cmd.stdout, "pageN=%d,walid=%d,rootrec=%d,freelist=%d\n", info.PageN, info.WALID, info.RootRecordPageNo, info.FreelistPageNo)

		case *rbf.RootRecordPageInfo:
			fmt.Fprintf(cmd.stdout, "%-10s ", "rootrec")
			if cmd.WithTree {
				fmt.Fprintf(cmd.stdout, "%-30q ", "")
			}
			fmt.Fprintf(cmd.stdout, "next=%d\n", info.Next)

		case *rbf.LeafPageInfo:
			fmt.Fprintf(cmd.stdout, "%-10s ", "leaf")
			if cmd.WithTree {
				fmt.Fprintf(cmd.stdout, "%-30q ", prefixToString(info.Tree))
			}
			fmt.Fprintf(cmd.stdout, "flags=x%x,celln=%d\n", info.Flags, info.CellN)

		case *rbf.BranchPageInfo:
			fmt.Fprintf(cmd.stdout, "%-10s ", "branch")
			if cmd.WithTree {
				fmt.Fprintf(cmd.stdout, "%-30q ", prefixToString(info.Tree))
			}
			fmt.Fprintf(cmd.stdout, "flags=x%x,celln=%d\n", info.Flags, info.CellN)

		case *rbf.BitmapPageInfo:
			fmt.Fprintf(cmd.stdout, "%-10s ", "bitmap")
			if cmd.WithTree {
				fmt.Fprintf(cmd.stdout, "%-30q ", prefixToString(info.Tree))
			}
			fmt.Fprintf(cmd.stdout, "-\n")

		case *rbf.FreePageInfo:
			fmt.Fprintf(cmd.stdout, "%-10s ", "free")
			if cmd.WithTree {
				fmt.Fprintf(cmd.stdout, "%-30q ", "")
			}
			fmt.Fprintf(cmd.stdout, "-\n")

		default:
			fmt.Fprintf(cmd.stdout, "unknown [%T]\n", info)
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
