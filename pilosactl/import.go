package pilosactl

import (
	"context"
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/pilosa/pilosa"
)

// ImportCommand represents a command for bulk importing data.
type ImportCommand struct {
	// Destination host and port.
	Host string `json:"host"`

	// Name of the database & frame to import into.
	Database string `json:"db"`
	Frame    string `json:"frame"`

	// Filenames to import from.
	Paths []string `json:"paths"`

	// Size of buffer used to chunk import.
	BufferSize int `json:"buffer-size"`

	// Reusable client.
	Client *pilosa.Client `json:"-"`

	// Standard input/output
	Stdin  io.Reader `json:"-"`
	Stdout io.Writer `json:"-"`
	Stderr io.Writer `json:"-"`
}

// NewImportCommand returns a new instance of ImportCommand.
func NewImportCommand(stdin io.Reader, stdout, stderr io.Writer) *ImportCommand {
	return &ImportCommand{
		Stdin:  stdin,
		Stdout: stdout,
		Stderr: stderr,

		BufferSize: 10000000,
	}
}

func (cmd *ImportCommand) String() string {
	return fmt.Sprint(*cmd)
}

// ParseFlags parses command line flags from args.
func (cmd *ImportCommand) ParseFlags(args []string) error {
	fs := flag.NewFlagSet("pilosactl", flag.ContinueOnError)
	fs.SetOutput(ioutil.Discard)
	fs.StringVar(&cmd.Host, "host", "localhost:15000", "host:port")
	fs.StringVar(&cmd.Database, "d", "", "database")
	fs.StringVar(&cmd.Frame, "f", "", "frame")
	fs.IntVar(&cmd.BufferSize, "buffer-size", cmd.BufferSize, "buffer size")
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

	BITMAPID,PROFILEID,[TIME]

The file should contain no headers. The TIME column is optional and can be
omitted. If it is present then its format should be YYYY-MM-DDTHH:MM.
`)
}

// Run executes the main program execution.
func (cmd *ImportCommand) Run(ctx context.Context) error {
	logger := log.New(cmd.Stderr, "", log.LstdFlags)

	// Validate arguments.
	// Database and frame are validated early before the files are parsed.
	if cmd.Database == "" {
		return pilosa.ErrDatabaseRequired
	} else if cmd.Frame == "" {
		return pilosa.ErrFrameRequired
	} else if len(cmd.Paths) == 0 {
		return errors.New("path required")
	}

	// Create a client to the server.
	client, err := pilosa.NewClient(cmd.Host)
	if err != nil {
		return err
	}
	cmd.Client = client

	// Import each path and import by slice.
	for _, path := range cmd.Paths {
		// Parse path into bits.
		logger.Printf("parsing: %s", path)
		if err := cmd.importPath(ctx, path); err != nil {
			return err
		}
	}

	return nil
}

// importPath parses a path into bits and imports it to the server.
func (cmd *ImportCommand) importPath(ctx context.Context, path string) error {
	a := make([]pilosa.Bit, 0, cmd.BufferSize)

	var r *csv.Reader

	if path != "-" {
		// Open file for reading.
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		defer f.Close()

		// Read rows as bits.
		r = csv.NewReader(f)
	} else {
		r = csv.NewReader(cmd.Stdin)
	}

	r.FieldsPerRecord = -1
	rnum := 0
	for {
		rnum++

		// Read CSV row.
		record, err := r.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}

		// Ignore blank rows.
		if record[0] == "" {
			continue
		} else if len(record) < 2 {
			return fmt.Errorf("bad column count on row %d: col=%d", rnum, len(record))
		}

		var bit pilosa.Bit

		// Parse bitmap id.
		bitmapID, err := strconv.ParseUint(record[0], 10, 64)
		if err != nil {
			return fmt.Errorf("invalid bitmap id on row %d: %q", rnum, record[0])
		}
		bit.BitmapID = bitmapID

		// Parse bitmap id.
		profileID, err := strconv.ParseUint(record[1], 10, 64)
		if err != nil {
			return fmt.Errorf("invalid profile id on row %d: %q", rnum, record[1])
		}
		bit.ProfileID = profileID

		// Parse time, if exists.
		if len(record) > 2 && record[2] != "" {
			t, err := time.Parse(pilosa.TimeFormat, record[2])
			if err != nil {
				return fmt.Errorf("invalid timestamp on row %d: %q", rnum, record[2])
			}
			bit.Timestamp = t.UnixNano()
		}

		a = append(a, bit)

		// If we've reached the buffer size then import bits.
		if len(a) == cmd.BufferSize {
			if err := cmd.importBits(ctx, a); err != nil {
				return err
			}
			a = a[:0]
		}
	}

	// If there are still bits in the buffer then flush them.
	if err := cmd.importBits(ctx, a); err != nil {
		return err
	}

	return nil
}

// importPath parses a path into bits and imports it to the server.
func (cmd *ImportCommand) importBits(ctx context.Context, bits []pilosa.Bit) error {
	logger := log.New(cmd.Stderr, "", log.LstdFlags)

	// Group bits by slice.
	logger.Printf("grouping %d bits", len(bits))
	bitsBySlice := pilosa.Bits(bits).GroupBySlice()

	// Parse path into bits.
	for slice, bits := range bitsBySlice {
		logger.Printf("importing slice: %d, n=%d", slice, len(bits))
		if err := cmd.Client.Import(ctx, cmd.Database, cmd.Frame, slice, bits); err != nil {
			return err
		}
	}

	return nil
}
