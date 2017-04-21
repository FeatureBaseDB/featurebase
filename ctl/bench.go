package ctl

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"time"

	"github.com/pilosa/pilosa"
)

// BenchCommand represents a command for benchmarking database operations.
type BenchCommand struct {
	// Destination host and port.
	Host string

	// Name of the database & frame to execute against.
	Database string
	Frame    string

	// Type of operation and number to execute.
	Op string
	N  int

	// Standard input/output
	*pilosa.CmdIO
}

// NewBenchCommand returns a new instance of BenchCommand.
func NewBenchCommand(stdin io.Reader, stdout, stderr io.Writer) *BenchCommand {
	return &BenchCommand{
		CmdIO: pilosa.NewCmdIO(stdin, stdout, stderr),
	}
}

// Run executes the bench command.
func (cmd *BenchCommand) Run(ctx context.Context) error {
	// Create a client to the server.
	client, err := pilosa.NewClient(cmd.Host)
	if err != nil {
		return err
	}

	switch cmd.Op {
	case "set-bit":
		return cmd.runSetBit(ctx, client)
	case "":
		return errors.New("op required")
	default:
		return fmt.Errorf("unknown bench op: %q", cmd.Op)
	}
}

// runSetBit executes a benchmark of random SetBit() operations.
func (cmd *BenchCommand) runSetBit(ctx context.Context, client *pilosa.Client) error {
	if cmd.N == 0 {
		return errors.New("operation count required")
	} else if cmd.Database == "" {
		return pilosa.ErrDatabaseRequired
	} else if cmd.Frame == "" {
		return pilosa.ErrFrameRequired
	}

	const maxRowID = 1000
	const maxColumnID = 100000

	startTime := time.Now()

	// Execute operation continuously.
	for i := 0; i < cmd.N; i++ {
		rowID := rand.Intn(maxRowID)
		columnID := rand.Intn(maxColumnID)

		q := fmt.Sprintf(`SetBit(id=%d, frame="%s", columnID=%d)`, rowID, cmd.Frame, columnID)

		if _, err := client.ExecuteQuery(ctx, cmd.Database, q, true); err != nil {
			return err
		}
	}

	// Print results.
	elapsed := time.Since(startTime)
	fmt.Fprintf(cmd.Stdout, "Executed %d operations in %s (%0.3f op/sec)\n", cmd.N, elapsed, float64(cmd.N)/elapsed.Seconds())

	return nil
}
