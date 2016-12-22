package main

import (
	"bufio"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"text/tabwriter"
	"time"
	"unsafe"

	"golang.org/x/crypto/ssh"

	"github.com/pilosa/pilosa"
	"github.com/pilosa/pilosa/bench"
	"github.com/pilosa/pilosa/creator"
	"github.com/pilosa/pilosa/pilosactl"
	"github.com/pilosa/pilosa/roaring"

	"github.com/satori/go.uuid"
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
	export     exports data to a CSV file
	sort       sorts a data file for optimal import speed
	backup     backs up a frame to an archive file
	restore    restores a frame from an archive file
	inspect    inspects fragment data files
	check      performs a consistency check of data files
	bench      benchmarks operations
	create     create pilosa clusters
	bagent     run a benchmarking agent
	bspawn     create a cluster and agents and run benchmarks based on config file

Use the "-h" flag with any command for more information.
`)
}

// Run executes the main program execution.
func (m *Main) Run() error { return m.Cmd.Run(context.Background()) }

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
		m.Cmd = pilosactl.NewImportCommand(m.Stdin, m.Stdout, m.Stderr)
	case "export":
		m.Cmd = NewExportCommand(m.Stdin, m.Stdout, m.Stderr)
	case "sort":
		m.Cmd = NewSortCommand(m.Stdin, m.Stdout, m.Stderr)
	case "backup":
		m.Cmd = NewBackupCommand(m.Stdin, m.Stdout, m.Stderr)
	case "restore":
		m.Cmd = NewRestoreCommand(m.Stdin, m.Stdout, m.Stderr)
	case "inspect":
		m.Cmd = NewInspectCommand(m.Stdin, m.Stdout, m.Stderr)
	case "check":
		m.Cmd = NewCheckCommand(m.Stdin, m.Stdout, m.Stderr)
	case "bench":
		m.Cmd = NewBenchCommand(m.Stdin, m.Stdout, m.Stderr)
	case "create":
		m.Cmd = NewCreateCommand(m.Stdin, m.Stdout, m.Stderr)
	case "bagent":
		m.Cmd = NewBagentCommand(m.Stdin, m.Stdout, m.Stderr)
	case "bspawn":
		m.Cmd = NewBspawnCommand(m.Stdin, m.Stdout, m.Stderr)
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
	Run(context.Context) error
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
func (cmd *ConfigCommand) Run(ctx context.Context) error {
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

// ExportCommand represents a command for bulk exporting data from a server.
type ExportCommand struct {
	// Remote host and port.
	Host string

	// Name of the database & frame to export from.
	Database string
	Frame    string

	// Filename to export to.
	Path string

	// Standard input/output
	Stdin  io.Reader
	Stdout io.Writer
	Stderr io.Writer
}

// NewExportCommand returns a new instance of ExportCommand.
func NewExportCommand(stdin io.Reader, stdout, stderr io.Writer) *ExportCommand {
	return &ExportCommand{
		Stdin:  stdin,
		Stdout: stdout,
		Stderr: stderr,
	}
}

// ParseFlags parses command line flags from args.
func (cmd *ExportCommand) ParseFlags(args []string) error {
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
func (cmd *ExportCommand) Usage() string {
	return strings.TrimSpace(`
usage: pilosactl export -host HOST -d database -f frame -o OUTFILE

Bulk exports a fragment to a CSV file. If the OUTFILE is not specified then
the output is written to STDOUT.

The format of the CSV file is:

	BITMAPID,PROFILEID

The file does not contain any headers.
`)
}

// Run executes the main program execution.
func (cmd *ExportCommand) Run(ctx context.Context) error {
	logger := log.New(cmd.Stderr, "", log.LstdFlags)

	// Validate arguments.
	if cmd.Database == "" {
		return pilosa.ErrDatabaseRequired
	} else if cmd.Frame == "" {
		return pilosa.ErrFrameRequired
	}

	// Use output file, if specified.
	// Otherwise use STDOUT.
	var w io.Writer = cmd.Stdout
	if cmd.Path != "" {
		f, err := os.Create(cmd.Path)
		if err != nil {
			return err
		}
		defer f.Close()

		w = f
	}

	// Create a client to the server.
	client, err := pilosa.NewClient(cmd.Host)
	if err != nil {
		return err
	}

	// Determine slice count.
	sliceN, err := client.SliceN(ctx)
	if err != nil {
		return err
	}

	// Export each slice.
	for slice := uint64(0); slice <= sliceN; slice++ {
		logger.Printf("exporting slice: %d", slice)
		if err := client.ExportCSV(ctx, cmd.Database, cmd.Frame, slice, w); err != nil {
			return err
		}
	}

	// Close writer, if applicable.
	if w, ok := w.(io.Closer); ok {
		if err := w.Close(); err != nil {
			return err
		}
	}

	return nil
}

// SortCommand represents a command for sorting import data.
type SortCommand struct {
	// Filename to sort
	Path string

	// Standard input/output
	Stdin  io.Reader
	Stdout io.Writer
	Stderr io.Writer
}

// NewSortCommand returns a new instance of SortCommand.
func NewSortCommand(stdin io.Reader, stdout, stderr io.Writer) *SortCommand {
	return &SortCommand{
		Stdin:  stdin,
		Stdout: stdout,
		Stderr: stderr,
	}
}

// ParseFlags parses command line flags from args.
func (cmd *SortCommand) ParseFlags(args []string) error {
	fs := flag.NewFlagSet("pilosactl", flag.ContinueOnError)
	fs.SetOutput(ioutil.Discard)
	if err := fs.Parse(args); err != nil {
		return err
	}

	// Extract the data path.
	if fs.NArg() == 0 {
		return errors.New("path required")
	} else if fs.NArg() > 1 {
		return errors.New("only one path allowed")
	}
	cmd.Path = fs.Arg(0)

	return nil
}

// Usage returns the usage message to be printed.
func (cmd *SortCommand) Usage() string {
	return strings.TrimSpace(`
usage: pilosactl sort PATH

Sorts the import data at PATH into the optimal sort order for importing.

The format of the CSV file is:

	BITMAPID,PROFILEID

The file should contain no headers.
`)
}

// Run executes the main program execution.
func (cmd *SortCommand) Run(ctx context.Context) error {
	// Open file for reading.
	f, err := os.Open(cmd.Path)
	if err != nil {
		return err
	}
	defer f.Close()

	// Read rows as bits.
	r := csv.NewReader(f)
	a := make([]pilosa.Bit, 0, 1000000)
	for {
		bitmapID, profileID, err := readCSVRow(r)
		if err == io.EOF {
			break
		} else if err == errBlank {
			continue
		} else if err != nil {
			return err
		}
		a = append(a, pilosa.Bit{BitmapID: bitmapID, ProfileID: profileID})
	}

	// Sort bits by position.
	sort.Sort(pilosa.BitsByPos(a))

	// Rewrite to STDOUT.
	w := bufio.NewWriter(cmd.Stdout)
	buf := make([]byte, 0, 1024)
	for _, bit := range a {
		// Write CSV to buffer.
		buf = buf[:0]
		buf = strconv.AppendUint(buf, bit.BitmapID, 10)
		buf = append(buf, ',')
		buf = strconv.AppendUint(buf, bit.ProfileID, 10)
		buf = append(buf, '\n')

		// Write to output.
		if _, err := w.Write(buf); err != nil {
			return err
		}
	}

	// Ensure buffer is flushed before exiting.
	if err := w.Flush(); err != nil {
		return err
	}

	return nil
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
	if err := client.BackupTo(ctx, f, cmd.Database, cmd.Frame); err != nil {
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
	if err := client.RestoreFrom(ctx, f, cmd.Database, cmd.Frame); err != nil {
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
func (cmd *InspectCommand) Run(ctx context.Context) error {
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
	if err := bm.UnmarshalBinary(data); err != nil {
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

// CheckCommand represents a command for performing consistency checks on data files.
type CheckCommand struct {
	// Data file paths.
	Paths []string

	// Standard input/output
	Stdin  io.Reader
	Stdout io.Writer
	Stderr io.Writer
}

// NewCheckCommand returns a new instance of CheckCommand.
func NewCheckCommand(stdin io.Reader, stdout, stderr io.Writer) *CheckCommand {
	return &CheckCommand{
		Stdin:  stdin,
		Stdout: stdout,
		Stderr: stderr,
	}
}

// ParseFlags parses command line flags from args.
func (cmd *CheckCommand) ParseFlags(args []string) error {
	fs := flag.NewFlagSet("pilosactl", flag.ContinueOnError)
	fs.SetOutput(ioutil.Discard)
	if err := fs.Parse(args); err != nil {
		return err
	}

	// Parse path.
	if fs.NArg() == 0 {
		return errors.New("path required")
	}
	cmd.Paths = fs.Args()

	return nil
}

// Usage returns the usage message to be printed.
func (cmd *CheckCommand) Usage() string {
	return strings.TrimSpace(`
usage: pilosactl check PATHS...

Performs a consistency check on data files.

`)
}

// Run executes the main program execution.
func (cmd *CheckCommand) Run(ctx context.Context) error {
	for _, path := range cmd.Paths {
		switch filepath.Ext(path) {
		case "":
			if err := cmd.checkBitmapFile(path); err != nil {
				return err
			}

		case ".cache":
			if err := cmd.checkCacheFile(path); err != nil {
				return err
			}

		case ".snapshotting":
			if err := cmd.checkSnapshotFile(path); err != nil {
				return err
			}
		}
	}

	return nil
}

// checkBitmapFile performs a consistency check on path for a roaring bitmap file.
func (cmd *CheckCommand) checkBitmapFile(path string) error {
	// Open file handle.
	f, err := os.Open(path)
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
	bm := roaring.NewBitmap()
	if err := bm.UnmarshalBinary(data); err != nil {
		return err
	}

	// Perform consistency check.
	if err := bm.Check(); err != nil {
		// Print returned errors.
		switch err := err.(type) {
		case roaring.ErrorList:
			for i := range err {
				fmt.Fprintf(cmd.Stdout, "%s: %s\n", path, err[i].Error())
			}
		default:
			fmt.Fprintf(cmd.Stdout, "%s: %s\n", path, err.Error())
		}
	}

	// Print success message if no errors were found.
	fmt.Fprintf(cmd.Stdout, "%s: ok\n", path)

	return nil
}

// checkCacheFile performs a consistency check on path for a cache file.
func (cmd *CheckCommand) checkCacheFile(path string) error {
	fmt.Fprintf(cmd.Stderr, "%s: ignoring cache file\n", path)
	return nil
}

// checkSnapshotFile performs a consistency check on path for a snapshot file.
func (cmd *CheckCommand) checkSnapshotFile(path string) error {
	fmt.Fprintf(cmd.Stderr, "%s: ignoring snapshot file\n", path)
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

	const maxBitmapID = 1000
	const maxProfileID = 100000

	startTime := time.Now()

	// Execute operation continuously.
	for i := 0; i < cmd.N; i++ {
		bitmapID := rand.Intn(maxBitmapID)
		profileID := rand.Intn(maxProfileID)

		q := fmt.Sprintf(`SetBit(id=%d, frame="%s", profileID=%d)`, bitmapID, cmd.Frame, profileID)

		if _, err := client.ExecuteQuery(ctx, cmd.Database, q, true); err != nil {
			return err
		}
	}

	// Print results.
	elapsed := time.Since(startTime)
	fmt.Fprintf(cmd.Stdout, "Executed %d operations in %s (%0.3f op/sec)\n", cmd.N, elapsed, float64(cmd.N)/elapsed.Seconds())

	return nil
}

// CreateCommand represents a command for creating a pilosa cluster.
type CreateCommand struct {
	Type          string
	ServerN       int
	ReplicaN      int
	LogFilePrefix string
	Hosts         []string
	GoMaxProcs    int
	RunUUID       string

	SSHUser string

	// Standard input/output
	Stdin  io.Reader
	Stdout io.Writer
	Stderr io.Writer
}

// NewCreateCommand returns a new instance of CreateCommand.
func NewCreateCommand(stdin io.Reader, stdout, stderr io.Writer) *CreateCommand {
	return &CreateCommand{
		Stdin:  stdin,
		Stdout: stdout,
		Stderr: stderr,
	}
}

// ParseFlags parses command line flags from args.
func (cmd *CreateCommand) ParseFlags(args []string) error {
	fs := flag.NewFlagSet("pilosactl", flag.ContinueOnError)
	fs.SetOutput(ioutil.Discard)
	fs.StringVar(&cmd.Type, "type", "", "")
	fs.IntVar(&cmd.ServerN, "serverN", 3, "")
	fs.IntVar(&cmd.ReplicaN, "replicaN", 1, "")
	fs.StringVar(&cmd.LogFilePrefix, "log-file-prefix", "", "")
	var hosts string
	fs.IntVar(&cmd.GoMaxProcs, "gomaxprocs", 0, "")
	fs.StringVar(&hosts, "hosts", "", "")
	fs.StringVar(&cmd.SSHUser, "ssh-user", "", "")
	fs.StringVar(&cmd.RunUUID, "run-uuid", "", "")

	if err := fs.Parse(args); err != nil {
		return err
	}
	cmd.Hosts = strings.Split(hosts, ",")
	return nil
}

// Usage returns the usage message to be printed.
func (cmd *CreateCommand) Usage() string {
	return strings.TrimSpace(`
usage: pilosactl create [args]

Creates a cluster based on the arguments.

The following flags are allowed:

	-type
		type of cluster - local, AWS, etc.

	-serverN
		number of hosts in cluster

	-replicaN
		replication factor for cluster

	-hosts
		Comma separated host:port list. Instead of
		creating hosts, just start pilosa on these
		pre-existing hosts. The same host may be
		listed multiple times with different ports.

	-log-file-prefix
		output from the started cluster will go
		into files with this prefix (one per node)

	-ssh-user
		username to use when contacting remote hosts

	-gomaxprocs
		when starting a cluster on remote hosts, this
		will set the value of GOMAXPROCS.

`)
}

type CreateOutput struct {
	Hosts    []string `json:"hosts"`
	LogFiles []string `json:"log-files"`
	RunUUID  string   `json:"run-uuid"`
}

// Run executes cluster creation.
func (cmd *CreateCommand) Run(ctx context.Context) error {
	var clus creator.Cluster
	switch cmd.Type {
	case "local":
		clus = &creator.LocalCluster{
			ReplicaN: cmd.ReplicaN,
			ServerN:  cmd.ServerN,
		}
	case "AWS":
		return fmt.Errorf("AWS cluster type is not yet implemented")
	case "":
		clus = &creator.RemoteCluster{
			ClusterHosts: cmd.Hosts,
			ReplicaN:     cmd.ReplicaN,
			SSHUser:      cmd.SSHUser,
			Stderr:       cmd.Stderr,
			GoMaxProcs:   cmd.GoMaxProcs,
		}
	default:
		return fmt.Errorf("Unknown cluster type %v", cmd.Type)
	}

	err := clus.Start()
	if err != nil {
		return fmt.Errorf("starting cluster: %v", err)
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		for range c {
			fmt.Fprintf(cmd.Stderr, "\ncaught signal - shutting down\n")
			err := clus.Shutdown()
			code := 0
			if err != nil {
				code = 1
			}
			os.Exit(code)
		}
	}()

	defer clus.Shutdown()
	output := &CreateOutput{
		RunUUID: cmd.RunUUID,
		Hosts:   clus.Hosts(),
	}

	logReaders := clus.Logs()
	if cmd.LogFilePrefix != "" {
		output.LogFiles = make([]string, len(clus.Hosts()))
	}
	for i, _ := range clus.Hosts() {
		var f io.Writer = cmd.Stderr
		var err error
		if cmd.LogFilePrefix != "" {
			f, err = os.Create(cmd.LogFilePrefix + strconv.Itoa(i))
			if err != nil {
				return err
			}
			output.LogFiles[i] = f.(*os.File).Name()
		}

		go func(i int, f io.Writer) {
			_, err := io.Copy(f, logReaders[i])
			if err != nil {
				fmt.Fprintf(cmd.Stderr, "Error copying cluster logs: '%v'", err)
			}
		}(i, f)
	}

	enc := json.NewEncoder(cmd.Stdout)
	err = enc.Encode(output)
	if err != nil {
		return err
	}
	select {}

}

// BagentCommand represents a command for running a benchmark agent. A benchmark
// agent runs multiple benchmarks in series in the order that they are specified
// on the command line.
type BagentCommand struct {
	// Slice of Benchmarks which will be run serially.
	Benchmarks []bench.Benchmark `json:"benchmarks"`
	// AgentNum will be passed to each benchmark's Run method so that it can
	// parameterize its behavior.
	AgentNum int `json:"agent-num"`

	// Enable pretty printing of results, for human consumption.
	HumanReadable bool `json:"human-readable"`

	RunUUID string `json:"-"` // ignoring this here because we add it to the top level of the output

	// Slice of pilosa hosts to run the Benchmarks against.
	Hosts []string `json:"hosts"`

	Stdin  io.Reader `json:"-"`
	Stdout io.Writer `json:"-"`
	Stderr io.Writer `json:"-"`
}

// NewBagentCommand returns a new instance of BagentCommand.
func NewBagentCommand(stdin io.Reader, stdout, stderr io.Writer) *BagentCommand {
	return &BagentCommand{
		Benchmarks:    []bench.Benchmark{},
		Hosts:         []string{},
		AgentNum:      0,
		HumanReadable: false,

		Stdin:  stdin,
		Stdout: stdout,
		Stderr: stderr,
	}
}

// ParseFlags parses command line flags for the BagentCommand. First the command
// wide flags `hosts` and `agentNum` are parsed. The rest of the flags should be
// a series of subcommands along with their flags. ParseFlags runs each
// subcommand's `ConsumeFlags` method which parses the flags for that command
// and returns the rest of the argument slice which should contain further
// subcommands.
func (cmd *BagentCommand) ParseFlags(args []string) error {
	fs := flag.NewFlagSet("pilosactl", flag.ContinueOnError)
	fs.SetOutput(ioutil.Discard)

	var pilosaHosts string
	fs.StringVar(&pilosaHosts, "hosts", "localhost:15000", "")
	fs.IntVar(&cmd.AgentNum, "agentNum", 0, "")
	fs.BoolVar(&cmd.HumanReadable, "human", false, "")
	fs.StringVar(&cmd.RunUUID, "run-uuid", "", "")

	if err := fs.Parse(args); err != nil {
		return err
	}
	remArgs := fs.Args()
	if len(remArgs) == 0 {
		return flag.ErrHelp
	}
	for len(remArgs) > 0 {
		var bm bench.Command
		var err error
		switch remArgs[0] {
		case "-help", "-h":
			return flag.ErrHelp
		case "diagonal-set-bits":
			bm = &bench.DiagonalSetBits{}
		case "random-set-bits":
			bm = &bench.RandomSetBits{}
		case "zipf-set-bits":
			bm = &bench.ZipfSetBits{}
		case "multi-db-set-bits":
			bm = &bench.MultiDBSetBits{}
		case "random-query":
			bm = &bench.RandomQuery{}
		case "import":
			bm = bench.NewImport(cmd.Stdin, cmd.Stdout, cmd.Stderr)
		case "slice-height":
			bm = bench.NewSliceHeight(cmd.Stdin, cmd.Stdout, cmd.Stderr)
		default:
			return fmt.Errorf("Unknown benchmark cmd: %v", remArgs[0])
		}
		remArgs, err = bm.ConsumeFlags(remArgs[1:])
		cmd.Benchmarks = append(cmd.Benchmarks, bm)
		if err != nil {
			if err == flag.ErrHelp {
				fmt.Fprintln(cmd.Stderr, bm.Usage())
				return fmt.Errorf("")
			}
			return fmt.Errorf("BagentCommand.ParseFlags: %v", err)
		}
	}
	cmd.Hosts = strings.Split(pilosaHosts, ",")

	return nil
}

// Usage returns the usage message to be printed.
func (cmd *BagentCommand) Usage() string {
	return strings.TrimSpace(`
pilosactl bagent is a tool for running benchmarks against a pilosa cluster.

Usage:

pilosactl bagent [options] <subcommand [options]>...

The following arguments are available:

	-hosts
		Comma separated list of host:port describing all hosts in the cluster.

	-agentNum N
		An integer differentiating this agent from others in the fleet.

	-human
		Boolean to enable human-readable format.

	subcommands:
		diagonal-set-bits
		random-set-bits
		zipf-set-bits
		multi-db-set-bits
		random-query
		import
		slice-height
`)
}

// Run executes the benchmark agent.
func (cmd *BagentCommand) Run(ctx context.Context) error {
	sbm := bench.Serial(cmd.Benchmarks...)
	err := sbm.Init(cmd.Hosts, cmd.AgentNum)
	if err != nil {
		return fmt.Errorf("in cmd.Run initialization: %v", err)
	}

	res := sbm.Run(ctx, cmd.AgentNum)
	res["metadata"] = cmd
	if cmd.RunUUID != "" {
		res["run-uuid"] = cmd.RunUUID
	}
	enc := json.NewEncoder(cmd.Stdout)
	if cmd.HumanReadable {
		enc.SetIndent("", "  ")
		res = bench.Prettify(res)
	}
	fmt.Println("  agent encoder setup")
	err = enc.Encode(res)
	fmt.Println("  agent encoder done")
	if err != nil {
		fmt.Fprintln(cmd.Stderr, err)
	}
	// fmt.Fprintln(cmd.Stdout, res)

	return nil
}

// BspawnCommand represents a command for spawning complex benchmarks. This
// includes cluster creation and teardown, agent creation and teardown, running
// multiple benchmarks in series and/or parallel, and collecting all the
// results.
type BspawnCommand struct {
	// If PilosaHosts is specified, CreatorArgs is ignored and the existing
	// cluster specified here is used.
	PilosaHosts []string

	// CreateCommand will be used with these arguments to create a cluster -
	// the cluster will be used to populate the PilosaHosts field. This
	// should include everything that comes after `pilosactl create`
	CreatorArgs []string

	// If AgentHosts is specified, Agents is ignored, and the existing
	// agents specified here are used.
	AgentHosts []string

	// Benchmarks is a slice of Spawns which specifies all of the bagent
	// commands to run. These will all be run in parallel, started on each
	// of the agents in a round robin fashion.
	Benchmarks []Spawn

	SSHUser string

	Stdin  io.Reader `json:"-"`
	Stdout io.Writer `json:"-"`
	Stderr io.Writer `json:"-"`
}

// Spawn represents a bagent command run in parallel across Num agents. The
// bagent command can run multiple Benchmarks serially within itself.
type Spawn struct {
	Num  int      // number of agents to run
	Args []string // everything that comes after `pilosactl bagent [arguments]`
}

// NewBspawnCommand returns a new instance of BspawnCommand.
func NewBspawnCommand(stdin io.Reader, stdout, stderr io.Writer) *BspawnCommand {
	return &BspawnCommand{
		Stdin:  stdin,
		Stdout: stdout,
		Stderr: stderr,
	}
}

// ParseFlags parses command line flags from args.
func (cmd *BspawnCommand) ParseFlags(args []string) error {
	if len(args) != 1 {
		return flag.ErrHelp
	}
	f, err := os.Open(args[0])
	if err != nil {
		return err
	}
	dec := json.NewDecoder(f)
	err = dec.Decode(cmd)
	if err != nil {
		return err
	}

	// handle pilosa creation
	// handle agent creation

	return nil
}

// Usage returns the usage message to be printed.
func (cmd *BspawnCommand) Usage() string {
	return strings.TrimSpace(`
pilosactl bspawn is a tool for running multiple instances of bagent against a cluster.

Usage:

pilosactl spawn configfile
`)
}

// Run executes the main program execution.
func (cmd *BspawnCommand) Run(ctx context.Context) error {
	runUUID := uuid.NewV1()
	if len(cmd.PilosaHosts) == 0 {
		// must create cluster
		r, w := io.Pipe()
		createCmd := NewCreateCommand(cmd.Stdin, w, cmd.Stderr)
		err := createCmd.ParseFlags(append(cmd.CreatorArgs, []string{"-run-uuid", runUUID.String()}...))
		if err != nil {
			return err
		}
		go func() {
			err := createCmd.Run(ctx)
			if err != nil {
				fmt.Fprintf(cmd.Stderr, "Cluster creation error while spawning: %v", err)
			}
		}()
		clus := &CreateOutput{}
		dec := json.NewDecoder(r)
		err = dec.Decode(clus)
		if err != nil {
			return err
		}
		cmd.PilosaHosts = clus.Hosts

		// print createOutput to stdout
		enc := json.NewEncoder(cmd.Stdout)
		enc.SetIndent("", " ")
		err = enc.Encode(clus)
		if err != nil {
			return err
		}
	}

	readers := []io.Reader{}
	err := fmt.Errorf("ce n'est pas une erreur")
	if len(cmd.AgentHosts) > 0 {
		// these can return a slice of readers, which then go to either stdout or somewhere else
		err = cmd.spawnRemote(ctx, runUUID)
	} else {
		fmt.Println("spawn local")
		readers, err = cmd.spawnLocal(ctx, runUUID)
	}
	if err != nil {
		return err
	}

	fmt.Println(readers)
	for i, r := range readers {
		fmt.Println(i)
		b, err := ioutil.ReadAll(r)
		if err != nil {
			return err
		}
		log.Printf("%s\n", b)
	}

	return nil

}

func (cmd *BspawnCommand) spawnRemote(ctx context.Context, runUUID uuid.UUID) error {
	agentIndex := 0
	agentConnections, err := pilosactl.SSHClients(cmd.AgentHosts, cmd.SSHUser, "")
	if err != nil {
		return err
	}
	sessions := make([]*ssh.Session, 0)
	for _, sp := range cmd.Benchmarks {
		for i := 0; i < sp.Num; i++ {
			sess, err := agentConnections[agentIndex].NewSession()
			if err != nil {
				return err
			}
			sessions = append(sessions, sess)
			sess.Stdout = cmd.Stdout
			sess.Stderr = cmd.Stderr
			err = sess.Start("pilosactl bagent -agentNum=" + strconv.Itoa(i) + " -hosts=" + strings.Join(cmd.PilosaHosts, ",") + " -run-uuid=" + runUUID.String() + " " + strings.Join(sp.Args, " "))
			if err != nil {
				return err
			}
		}
	}

	for _, sess := range sessions {
		err = sess.Wait()
		if err != nil {
			return fmt.Errorf("error waiting for remote bagent: %v", err)
		}
	}
	return nil
}

func (cmd *BspawnCommand) spawnLocal(ctx context.Context, runUUID uuid.UUID) ([]io.Reader, error) {
	agents := []*BagentCommand{}
	readers := make([]io.Reader, len(cmd.Benchmarks))
	writers := make([]io.Writer, len(cmd.Benchmarks))
	for _, sp := range cmd.Benchmarks {
		for i := 0; i < sp.Num; i++ {
			r, w := io.Pipe()
			readers = append(readers, r)
			writers = append(writers, w)
			defer w.Close()
			//fmt.Printf("%T, %T\n", w, writers[i])
			agentCmd := NewBagentCommand(cmd.Stdin, w, cmd.Stderr)
			agents = append(agents, agentCmd)
			err := agentCmd.ParseFlags(append([]string{"-agentNum", strconv.Itoa(i), "-hosts", strings.Join(cmd.PilosaHosts, ","), "-run-uuid", runUUID.String()}, sp.Args...))
			if err != nil {
				return nil, err
			}
		}
	}
	errors := make([]error, len(agents))

	fmt.Println(readers)

	wg := sync.WaitGroup{}
	for i, agent := range agents {
		wg.Add(1)
		go func(i int, agent *BagentCommand) {
			defer wg.Done()
			fmt.Printf("go func %d b\n", i)
			errors[i] = agent.Run(ctx)
		}(i, agent)
	}
	wg.Wait()
	for _, err := range errors {
		if err != nil {
			return nil, fmt.Errorf("%v", errors)
		}
	}
	return readers, nil
}

// readCSVRow reads a bitmap/profile pair from a CSV row.
func readCSVRow(r *csv.Reader) (bitmapID, profileID uint64, err error) {
	// Read CSV row.
	record, err := r.Read()
	if err != nil {
		return 0, 0, err
	}

	// Ignore blank rows.
	if record[0] == "" {
		return 0, 0, errBlank
	} else if len(record) < 2 {
		return 0, 0, fmt.Errorf("bad column count: %d", len(record))
	}

	// Parse bitmap id.
	bitmapID, err = strconv.ParseUint(record[0], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid bitmap id: %q", record[0])
	}

	// Parse bitmap id.
	profileID, err = strconv.ParseUint(record[1], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid profile id: %q", record[1])
	}

	return bitmapID, profileID, nil
}

// errBlank indicates a blank row in a CSV file.
var errBlank = errors.New("blank row")
