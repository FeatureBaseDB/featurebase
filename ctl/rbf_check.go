// Copyright 2021 Molecula Corp. All rights reserved.
package ctl

import (
	"context"
	"fmt"
	"io"

	"github.com/molecula/featurebase/v2"
	"github.com/molecula/featurebase/v2/rbf"
)

// RBFCheckCommand represents a command for running a consistency check on RBF.
type RBFCheckCommand struct {
	// Filepath to the RBF database.
	Path string

	// Standard input/output
	*pilosa.CmdIO
}

// NewRBFCheckCommand returns a new instance of RBFCheckCommand.
func NewRBFCheckCommand(stdin io.Reader, stdout, stderr io.Writer) *RBFCheckCommand {
	return &RBFCheckCommand{
		CmdIO: pilosa.NewCmdIO(stdin, stdout, stderr),
	}
}

// Run executes a consistency check of an RBF database.
func (cmd *RBFCheckCommand) Run(ctx context.Context) error {
	// Open database.
	db := rbf.NewDB(cmd.Path, nil)
	if err := db.Open(); err != nil {
		return err
	}
	defer db.Close()

	// Run check on the database.
	if err := db.Check(); err != nil {
		switch err := err.(type) {
		case rbf.ErrorList:
			for i := range err {
				fmt.Fprintln(cmd.Stdout, err[i])
			}
		default:
			fmt.Fprintln(cmd.Stdout, err)
		}
		return fmt.Errorf("check failed")
	}

	// If successful, print a success message.
	fmt.Fprintln(cmd.Stdout, "ok")

	return nil
}
