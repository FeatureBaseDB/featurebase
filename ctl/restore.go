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
	"errors"
	"io"
	"os"

	"github.com/pilosa/pilosa"
)

// RestoreCommand represents a command for restoring a frame from a backup.
type RestoreCommand struct {
	// Destination host and port.
	Host string

	// Name of the index & frame to backup.
	Index string
	Frame string
	View  string

	// Import file to read from.
	Path string

	// Standard input/output
	*pilosa.CmdIO

	TLS pilosa.TLSConfig
}

// NewRestoreCommand returns a new instance of RestoreCommand.
func NewRestoreCommand(stdin io.Reader, stdout, stderr io.Writer) *RestoreCommand {
	return &RestoreCommand{
		CmdIO: pilosa.NewCmdIO(stdin, stdout, stderr),
	}
}

// Run executes the restore command.
func (cmd *RestoreCommand) Run(ctx context.Context) error {
	// Validate arguments.
	if cmd.Path == "" {
		return errors.New("backup file required")
	}

	// Create a client to the server.
	client, err := CommandClient(cmd)
	if err != nil {
		return err
	}

	// Open backup file.
	f, err := os.Open(cmd.Path)
	if err != nil {
		return err
	}
	defer f.Close()

	// Restore backup file to the cluster.
	if err := client.RestoreFrom(ctx, f, cmd.Index, cmd.Frame, cmd.View); err != nil {
		return err
	}

	return nil
}

func (cmd *RestoreCommand) TLSHost() string {
	return cmd.Host
}

func (cmd *RestoreCommand) TLSConfiguration() pilosa.TLSConfig {
	return cmd.TLS
}
