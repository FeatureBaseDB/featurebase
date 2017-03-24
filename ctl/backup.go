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

	// Name of the database, frame, view to backup.
	Database string
	Frame    string
	View     string

	// Output file to write to.
	Path string

	// Standard input/output
	*pilosa.CmdIO
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
	client, err := pilosa.NewClient(cmd.Host)
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
	if err := client.BackupTo(ctx, f, cmd.Database, cmd.Frame, cmd.View); err != nil {
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
