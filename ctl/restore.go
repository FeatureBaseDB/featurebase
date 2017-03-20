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

	// Name of the database & frame to backup.
	Database string
	Frame    string
	View     string

	// Import file to read from.
	Path string

	// Standard input/output
	*pilosa.CmdIO
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
	client, err := pilosa.NewClient(cmd.Host)
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
	if err := client.RestoreFrom(ctx, f, cmd.Database, cmd.Frame, cmd.View); err != nil {
		return err
	}

	return nil
}
