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

// Run executes the export.
func (cmd *RBFCheckCommand) Run(ctx context.Context) error {
	// Open database.
	db := rbf.NewDB(cmd.Path, nil)
	if err := db.Open(); err != nil {
		return err
	}
	defer db.Close()

	// Run check on the database.
	if err := db.Check(); err != nil {
		return err
	}

	// If successful, print a success message.
	fmt.Fprintln(cmd.Stdout, "ok")

	return nil
}
