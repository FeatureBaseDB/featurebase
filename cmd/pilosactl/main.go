package main

import (
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"
	"unsafe"

	"github.com/umbel/pilosa"
	"github.com/umbel/pilosa/roaring"
)

var (
	// ErrUnknownCommand is returned when specifying an unknown command.
	ErrUnknownCommand = errors.New("unknown command")

	// ErrPathRequired is returned when executing a command without a required path.
	ErrPathRequired = errors.New("path required")
)

func main() {
	m := NewMain()

	// Parse command line arguments.
	if err := m.ParseFlags(os.Args[1:]); err == flag.ErrHelp {
		os.Exit(2)
	} else if err != nil {
		fmt.Fprintln(m.Stderr, err)
		os.Exit(2)
	}

	// Execute the program.
	if err := m.Run(); err != nil {
		fmt.Fprintln(m.Stderr, err)
		os.Exit(1)
	}
}

// Main represents the main program execution.
type Main struct {
	// Subcommand to execute.
	Cmd Command

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

// Usage returns the usage message to be printed.
func (m *Main) Usage() string {
	return strings.TrimSpace(`
Pilosactl is a tool for interacting with a pilosa server.

Usage:

	pilosactl command [arguments]

The commands are:

	config     prints the default configuration
	import     imports data from a CSV file
	backup     backs up a frame to an archive file
	restore    restores a frame from an archive file
	inspect    inspects fragment data files
	bench      benchmarks operations

Use the "-h" flag with any command for more information.
`)
}

// Run executes the main program execution.
func (m *Main) Run() error { return m.Cmd.Run() }

// ParseFlags parses command line flags from args.
func (m *Main) ParseFlags(args []string) error {
	var command string
	if len(args) > 0 {
		command = args[0]
		args = args[1:]
	}

	switch command {
	case "", "help", "-h":
		fmt.Fprintln(m.Stderr, m.Usage())
		fmt.Fprintln(m.Stderr, "")
		return flag.ErrHelp
	case "config":
		m.Cmd = NewConfigCommand(m.Stdin, m.Stdout, m.Stderr)
	case "import":
		m.Cmd = NewImportCommand(m.Stdin, m.Stdout, m.Stderr)
	case "backup":
		m.Cmd = NewBackupCommand(m.Stdin, m.Stdout, m.Stderr)
	case "restore":
		m.Cmd = NewRestoreCommand(m.Stdin, m.Stdout, m.Stderr)
	case "inspect":
		m.Cmd = NewInspectCommand(m.Stdin, m.Stdout, m.Stderr)
	case "bench":
		m.Cmd = NewBenchCommand(m.Stdin, m.Stdout, m.Stderr)
	default:
		return ErrUnknownCommand
	}

	// Parse command's flags.
	if err := m.Cmd.ParseFlags(args); err == flag.ErrHelp {
		fmt.Fprintln(m.Stderr, m.Cmd.Usage())
		fmt.Fprintln(m.Stderr, "")
		return err
	} else if err != nil {
		return err
	}

	return nil
}

// Command represents an executable subcommand.
type Command interface {
	Usage() string
	ParseFlags(args []string) error
	Run() error
}

// ConfigCommand represents a command for printing a default config.
type ConfigCommand struct {
	// Standard input/output
	Stdin  io.Reader
	Stdout io.Writer
	Stderr io.Writer
}

// NewConfigCommand returns a new instance of ConfigCommand.
func NewConfigCommand(stdin io.Reader, stdout, stderr io.Writer) *ConfigCommand {
	return &ConfigCommand{
		Stdin:  stdin,
		Stdout: stdout,
		Stderr: stderr,
	}
}

// ParseFlags parses command line flags from args.
func (cmd *ConfigCommand) ParseFlags(args []string) error {
	fs := flag.NewFlagSet("pilosactl", flag.ContinueOnError)
	fs.SetOutput(ioutil.Discard)
	if err := fs.Parse(args); err != nil {
		return err
	}
	return nil
}

// Usage returns the usage message to be printed.
func (cmd *ConfigCommand) Usage() string {
	return strings.TrimSpace(`
usage: pilosactl config

Prints the default configuration file to standard out.
`)
}

// Run executes the main program execution.
func (cmd *ConfigCommand) Run() error {
	fmt.Fprintln(cmd.Stdout, strings.TrimSpace(`
data-dir = "~/.pilosa"
host = "localhost:15000"

[cluster]
replicas = 1

[[cluster.node]]
host = "localhost:15000"

[plugins]
path = ""
`)+"\n")
	return nil
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
	fs.SetOutput(ioutil.Discard)
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
		} else if len(record) < 2 {
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

// BackupCommand represents a command for backing up a frame.
type BackupCommand struct {
	// Destination host and port.
	Host string

	// Name of the database & frame to backup.
	Database string
	Frame    string

	// Output file to write to.
	Path string

	// Standard input/output
	Stdin  io.Reader
	Stdout io.Writer
	Stderr io.Writer
}

// NewBackupCommand returns a new instance of BackupCommand.
func NewBackupCommand(stdin io.Reader, stdout, stderr io.Writer) *BackupCommand {
	return &BackupCommand{
		Stdin:  stdin,
		Stdout: stdout,
		Stderr: stderr,
	}
}

// ParseFlags parses command line flags from args.
func (cmd *BackupCommand) ParseFlags(args []string) error {
	fs := flag.NewFlagSet("pilosactl", flag.ContinueOnError)
	fs.SetOutput(ioutil.Discard)
	fs.StringVar(&cmd.Host, "host", "localhost:15000", "host:port")
	fs.StringVar(&cmd.Database, "d", "", "database")
	fs.StringVar(&cmd.Frame, "f", "", "frame")
	fs.StringVar(&cmd.Path, "o", "", "output file")
	if err := fs.Parse(args); err != nil {
		return err
	}

	return nil
}

// Usage returns the usage message to be printed.
func (cmd *BackupCommand) Usage() string {
	return strings.TrimSpace(`
usage: pilosactl backup -host HOST -d database -f frame -o PATH

Backs up the database and frame from across the cluster into a single file.
`)
}

// Run executes the main program execution.
func (cmd *BackupCommand) Run() error {
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
	if err := client.BackupTo(f, cmd.Database, cmd.Frame); err != nil {
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

// RestoreCommand represents a command for restoring a frame from a backup.
type RestoreCommand struct {
	// Destination host and port.
	Host string

	// Name of the database & frame to backup.
	Database string
	Frame    string

	// Import file to read from.
	Path string

	// Standard input/output
	Stdin  io.Reader
	Stdout io.Writer
	Stderr io.Writer
}

// NewRestoreCommand returns a new instance of RestoreCommand.
func NewRestoreCommand(stdin io.Reader, stdout, stderr io.Writer) *RestoreCommand {
	return &RestoreCommand{
		Stdin:  stdin,
		Stdout: stdout,
		Stderr: stderr,
	}
}

// ParseFlags parses command line flags from args.
func (cmd *RestoreCommand) ParseFlags(args []string) error {
	fs := flag.NewFlagSet("pilosactl", flag.ContinueOnError)
	fs.SetOutput(ioutil.Discard)
	fs.StringVar(&cmd.Host, "host", "localhost:15000", "host:port")
	fs.StringVar(&cmd.Database, "d", "", "database")
	fs.StringVar(&cmd.Frame, "f", "", "frame")
	if err := fs.Parse(args); err != nil {
		return err
	}

	// Read input path from the args.
	if fs.NArg() == 0 {
		return errors.New("path required")
	} else if fs.NArg() > 1 {
		return errors.New("too many paths specified")
	}
	cmd.Path = fs.Arg(0)

	return nil
}

// Usage returns the usage message to be printed.
func (cmd *RestoreCommand) Usage() string {
	return strings.TrimSpace(`
usage: pilosactl restore -host HOST -d database -f frame PATH

Restores a frame to the cluster from a backup file.
`)
}

// Run executes the main program execution.
func (cmd *RestoreCommand) Run() error {
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
	if err := client.RestoreFrom(f, cmd.Database, cmd.Frame); err != nil {
		return err
	}

	return nil
}

// InspectCommand represents a command for inspecting fragment data files.
type InspectCommand struct {
	// Path to data file
	Path string

	// Standard input/output
	Stdin  io.Reader
	Stdout io.Writer
	Stderr io.Writer
}

// NewInspectCommand returns a new instance of InspectCommand.
func NewInspectCommand(stdin io.Reader, stdout, stderr io.Writer) *InspectCommand {
	return &InspectCommand{
		Stdin:  stdin,
		Stdout: stdout,
		Stderr: stderr,
	}
}

// ParseFlags parses command line flags from args.
func (cmd *InspectCommand) ParseFlags(args []string) error {
	fs := flag.NewFlagSet("pilosactl", flag.ContinueOnError)
	fs.SetOutput(ioutil.Discard)
	if err := fs.Parse(args); err != nil {
		return err
	}

	// Parse path.
	if fs.NArg() == 0 {
		return errors.New("path required")
	} else if fs.NArg() > 1 {
		return errors.New("only one path allowed")
	}
	cmd.Path = fs.Arg(0)

	return nil
}

// Usage returns the usage message to be printed.
func (cmd *InspectCommand) Usage() string {
	return strings.TrimSpace(`
usage: pilosactl inspect PATH 

Inspects a data file and provides stats.

`)
}

// Run executes the main program execution.
func (cmd *InspectCommand) Run() error {
	// Open file handle.
	f, err := os.Open(cmd.Path)
	if err != nil {
		return err
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return err
	}

	// Memory map the file.
	data, err := syscall.Mmap(int(f.Fd()), 0, int(fi.Size()), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return err
	}
	defer syscall.Munmap(data)

	// Attach the mmap file to the bitmap.
	t := time.Now()
	fmt.Fprintf(cmd.Stderr, "unmarshaling bitmap...")
	bm := roaring.NewBitmap()
	buf := (*[0x7FFFFFFF]byte)(unsafe.Pointer(&data[0]))[:fi.Size()]
	if err := bm.UnmarshalBinary(buf); err != nil {
		return err
	}
	fmt.Fprintf(cmd.Stderr, " (%s)\n", time.Since(t))

	// Retrieve stats.
	t = time.Now()
	fmt.Fprintf(cmd.Stderr, "calculating stats...")
	info := bm.Info()
	fmt.Fprintf(cmd.Stderr, " (%s)\n", time.Since(t))

	// Print top-level info.
	fmt.Fprintf(cmd.Stdout, "== Bitmap Info ==\n")
	fmt.Fprintf(cmd.Stdout, "Containers: %d\n", len(info.Containers))
	fmt.Fprintf(cmd.Stdout, "Operations: %d\n", info.OpN)
	fmt.Fprintln(cmd.Stdout, "")

	// Print info for each container.
	fmt.Fprintln(cmd.Stdout, "== Containers ==")
	tw := tabwriter.NewWriter(cmd.Stdout, 0, 8, 0, '\t', 0)
	fmt.Fprintf(tw, "%s\t%s\t% 8s \t% 8s\t%s\n", "KEY", "TYPE", "N", "ALLOC", "OFFSET")
	for _, ci := range info.Containers {
		fmt.Fprintf(tw, "%d\t%s\t% 8d \t% 8d \t0x%08x\n",
			ci.Key,
			ci.Type,
			ci.N,
			ci.Alloc,
			uintptr(ci.Pointer)-uintptr(unsafe.Pointer(&data[0])),
		)
	}
	tw.Flush()

	return nil
}

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
	Stdin  io.Reader
	Stdout io.Writer
	Stderr io.Writer
}

// NewBenchCommand returns a new instance of BenchCommand.
func NewBenchCommand(stdin io.Reader, stdout, stderr io.Writer) *BenchCommand {
	return &BenchCommand{
		Stdin:  stdin,
		Stdout: stdout,
		Stderr: stderr,
	}
}

// ParseFlags parses command line flags from args.
func (cmd *BenchCommand) ParseFlags(args []string) error {
	fs := flag.NewFlagSet("pilosactl", flag.ContinueOnError)
	fs.SetOutput(ioutil.Discard)
	fs.StringVar(&cmd.Host, "host", "localhost:15000", "host:port")
	fs.StringVar(&cmd.Database, "d", "", "database")
	fs.StringVar(&cmd.Frame, "f", "", "frame")
	fs.StringVar(&cmd.Op, "op", "", "operation")
	fs.IntVar(&cmd.N, "n", 0, "op count")

	if err := fs.Parse(args); err != nil {
		return err
	}
	return nil
}

// Usage returns the usage message to be printed.
func (cmd *BenchCommand) Usage() string {
	return strings.TrimSpace(`
usage: pilosactl bench [args]

Executes a benchmark for a given operation against the database.

The following flags are allowed:

	-host HOSTPORT
		hostname and port of running pilosa server

	-d DATABASE
		database to execute operation against

	-f FRAME
		frame to execute operation against

	-op OP
		name of operation to execute

	-n COUNT
		number of iterations to execute

The following operations are available:

	set-bit
		Sets a single random bit on the frame

`)
}

// Run executes the main program execution.
func (cmd *BenchCommand) Run() error {
	// Create a client to the server.
	client, err := pilosa.NewClient(cmd.Host)
	if err != nil {
		return err
	}

	switch cmd.Op {
	case "set-bit":
		return cmd.runSetBit(client)
	case "":
		return errors.New("op required")
	default:
		return fmt.Errorf("unknown bench op: %q", cmd.Op)
	}
}

// runSetBit executes a benchmark of random SetBit() operations.
func (cmd *BenchCommand) runSetBit(client *pilosa.Client) error {
	if cmd.N == 0 {
		return errors.New("operation count required")
	} else if cmd.Database == "" {
		return pilosa.ErrDatabaseRequired
	} else if cmd.Frame == "" {
		return pilosa.ErrFrameRequired
	}

	const maxBitmapID = 1000
	const maxProfileID = 100000

	startTime := time.Now()

	// Execute operation continuously.
	for i := 0; i < cmd.N; i++ {
		bitmapID := rand.Intn(maxBitmapID)
		profileID := rand.Intn(maxProfileID)

		q := fmt.Sprintf(`SetBit(id=%d, frame="%s", profileID=%d)`, bitmapID, cmd.Frame, profileID)

		if _, err := client.ExecuteQuery(cmd.Database, q, true); err != nil {
			return err
		}
	}

	// Print results.
	elapsed := time.Since(startTime)
	fmt.Fprintf(cmd.Stdout, "Executed %d operations in %s (%0.3f op/sec)\n", cmd.N, elapsed, float64(cmd.N)/elapsed.Seconds())

	return nil
}
