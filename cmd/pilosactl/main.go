package main

import (
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/umbel/pilosa"
)

var (
	// ErrUsage is returned when usage should be displayed for the program.
	ErrUsage = errors.New("usage")

	// ErrUnknownCommand is returned when specifying an unknown command.
	ErrUnknownCommand = errors.New("unknown command")

	// ErrQuit is returned when the program should simply quit.
	// This is used when the error message has already been printed.
	ErrQuit = errors.New("quit")

	// ErrPathRequired is returned when executing a command without a required path.
	ErrPathRequired = errors.New("path required")
)

func main() {
	m := NewMain()

	// Parse command line arguments.
	if err := m.ParseFlags(os.Args[1:]); err != nil {
		fmt.Fprintln(m.Stderr, err)
		os.Exit(2)
	}

	// Execute the program.
	if err := m.Run(); err == ErrQuit {
		os.Exit(1)
	} else if err != nil {
		fmt.Fprintln(m.Stderr, err)
		os.Exit(1)
	}
}

// Main represents the main program execution.
type Main struct {
	// Command name and arguments passed into the CLI.
	Command string
	Args    []string

	// Standard input/output
	Stdin  io.Reader
	Stdout io.Writer
	Stderr io.Writer
}

// NewMain returns a new instance of Main.
func NewMain() *Main {
	return &Main{
		Stdin:  os.Stdin,
		Stdout: os.Stdout,
		Stderr: os.Stderr,
	}
}

// Run executes the main program execution.
func (m *Main) Run() error {
	var cmd Command
	switch m.Command {
	case "", "help", "-h":
		return ErrUsage
	case "import":
		cmd = NewImportCommand(m.Stdin, m.Stdout, m.Stderr)
	default:
		return ErrUnknownCommand
	}

	// Parse command's flags.
	if err := cmd.ParseFlags(m.Args); err == ErrUsage {
		fmt.Fprintln(m.Stderr, cmd.Usage())
		return ErrQuit
	} else if err != nil {
		return err
	}

	// Execute the command.
	if err := cmd.Run(); err != nil {
		return err
	}
	return nil
}

// ParseFlags parses command line flags from args.
func (m *Main) ParseFlags(args []string) error {
	if len(args) == 0 {
		return nil
	}

	m.Command = args[0]
	m.Args = args[1:]
	return nil
}

// Command represents an executable subcommand.
type Command interface {
	Usage() string
	ParseFlags(args []string) error
	Run() error
}

// ImportCommand represents a command for bulk importing data.
type ImportCommand struct {
	// Destination host and port.
	Host string

	// Name of the database & frame to import into.
	Database string
	Frame    string

	// Filenames to import from.
	Paths []string

	// Standard input/output
	Stdin  io.Reader
	Stdout io.Writer
	Stderr io.Writer
}

// NewImportCommand returns a new instance of ImportCommand.
func NewImportCommand(stdin io.Reader, stdout, stderr io.Writer) *ImportCommand {
	return &ImportCommand{
		Stdin:  stdin,
		Stdout: stdout,
		Stderr: stderr,
	}
}

// ParseFlags parses command line flags from args.
func (cmd *ImportCommand) ParseFlags(args []string) error {
	fs := flag.NewFlagSet("pilosactl", flag.ContinueOnError)
	fs.SetOutput(cmd.Stderr)
	fs.StringVar(&cmd.Host, "host", "localhost:15000", "host:port")
	fs.StringVar(&cmd.Database, "d", "", "database")
	fs.StringVar(&cmd.Frame, "f", "", "frame")
	if err := fs.Parse(args); err != nil {
		return err
	}

	// Extract the import paths.
	cmd.Paths = fs.Args()

	return nil
}

// Usage returns the usage message to be printed.
func (cmd *ImportCommand) Usage() string {
	return strings.TrimSpace(`
usage: pilosactl import -host HOST -d database -f frame paths

Bulk imports one or more CSV files to a host's database and frame. The bits
of the CSV file are grouped by slice for the most efficient import.

The format of the CSV file is:

	BITMAPID,PROFILEID

The file should contain no headers.
`)
}

// Run executes the main program execution.
func (cmd *ImportCommand) Run() error {
	logger := log.New(cmd.Stderr, "", log.LstdFlags)

	// Validate arguments.
	// Database and frame are validated early before the files are parsed.
	if cmd.Database == "" {
		return pilosa.ErrDatabaseRequired
	} else if cmd.Frame == "" {
		return pilosa.ErrFrameRequired
	} else if len(cmd.Paths) == 0 {
		return ErrPathRequired
	}

	// Create a client to the server.
	client, err := pilosa.NewClient(cmd.Host)
	if err != nil {
		return err
	}

	// Import each path and import by slice.
	for _, path := range cmd.Paths {
		// Parse path into bits.
		logger.Printf("parsing: %s", path)
		bits, err := cmd.parsePath(path)
		if err != nil {
			return err
		}

		// Group bits by slice.
		logger.Printf("grouping %d bits", len(bits))
		bitsBySlice := pilosa.Bits(bits).GroupBySlice()
		logger.Printf("grouped into %d slices", len(bitsBySlice))

		// Parse path into bits.
		for slice, bits := range bitsBySlice {
			logger.Printf("importing slice: %d, n=%d", slice, len(bits))
			if err := client.Import(cmd.Database, cmd.Frame, slice, bits); err != nil {
				return err
			}
		}
	}

	return nil
}

// parsePath parses a path into bits.
func (cmd *ImportCommand) parsePath(path string) ([]pilosa.Bit, error) {
	var a []pilosa.Bit

	// Open file for reading.
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	// Read rows as bits.
	r := csv.NewReader(f)
	rnum := 0
	for {
		rnum++

		// Read CSV row.
		record, err := r.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}

		// Ignore blank rows.
		if record[0] == "" {
			continue
		} else if len(record) != 2 {
			return nil, fmt.Errorf("bad column count on row %d: col=%d", rnum, len(record))
		}

		// Parse bitmap id.
		bitmapID, err := strconv.ParseUint(record[0], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid bitmap id on row %d: %q", rnum, record[0])
		}

		// Parse bitmap id.
		profileID, err := strconv.ParseUint(record[1], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid profile id on row %d: %q", rnum, record[1])
		}

		a = append(a, pilosa.Bit{BitmapID: bitmapID, ProfileID: profileID})
	}

	return a, nil
}
