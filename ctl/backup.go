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

// BackupCommand represents a command for backing up a view.
type BackupCommand struct {
	// Destination host and port.
	Host string

	// Name of the index, frame, view to backup.
	Index string
	Frame string
	View  string

	// Output file to write to.
	Path string

	// Standard input/output
	*pilosa.CmdIO

	TLS pilosa.TLSConfig
}

// NewBackupCommand returns a new instance of BackupCommand.
func NewBackupCommand(stdin io.Reader, stdout, stderr io.Writer) *BackupCommand {
	return &BackupCommand{
		CmdIO: pilosa.NewCmdIO(stdin, stdout, stderr),
	}
}

// Run executes the backup.
func (cmd *BackupCommand) Run(ctx context.Context) error {
	// Validate arguments.
	if cmd.Path == "" {
		return errors.New("output file required")
	}

	// Create a client to the server.
	client, err := CommandClient(cmd)
	if err != nil {
		return err
	}

	// Open output file.
	f, err := os.Create(cmd.Path)
	if err != nil {
		return err
	}
	defer f.Close()

	// Begin streaming backup.
	if err := client.BackupTo(ctx, f, cmd.Index, cmd.Frame, cmd.View); err != nil {
		return err
	}

	// Sync & close file to ensure durability.
	if err := f.Sync(); err != nil {
		return err
	} else if err = f.Close(); err != nil {
		return err
	}

	return nil
}

func (cmd *BackupCommand) TLSHost() string {
	return cmd.Host
}

func (cmd *BackupCommand) TLSConfiguration() pilosa.TLSConfig {
	return cmd.TLS
}
