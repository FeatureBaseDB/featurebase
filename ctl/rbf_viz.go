// Copyright 2021 Molecula Corp. All rights reserved.
package ctl

import (
	"context"
	"fmt"
	"io"

	pilosa "github.com/molecula/featurebase/v3"
	"github.com/molecula/featurebase/v3/rbf"
)

// RBFVizCommand represents a command for doing a visualisation of the RBF tree.
type RBFVizCommand struct {
	// Filepath to the RBF database.
	Path string

	// Standard input/output
	*pilosa.CmdIO
}

// NewRBFVizCommand returns a new instance of RBFVizCommand.
func NewRBFVizCommand(stdin io.Reader, stdout, stderr io.Writer) *RBFVizCommand {
	return &RBFVizCommand{
		CmdIO: pilosa.NewCmdIO(stdin, stdout, stderr),
	}
}

// Run executes a consistency viz of an RBF database.
func (cmd *RBFVizCommand) Run(ctx context.Context) error {
	// Open database.
	db := rbf.NewDB(cmd.Path, nil)
	if err := db.Open(); err != nil {
		return err
	}
	defer db.Close()

	// Run viz on the database.
	if err := db.Viz(cmd.Stdout); err != nil {
		switch err := err.(type) {
		case rbf.ErrorList:
			for i := range err {
				fmt.Fprintln(cmd.Stdout, err[i])
			}
		default:
			fmt.Fprintln(cmd.Stdout, err)
		}
		return fmt.Errorf("viz failed")
	}

	return nil
}
