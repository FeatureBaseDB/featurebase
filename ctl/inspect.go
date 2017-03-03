package ctl

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"
	"unsafe"

	"github.com/pilosa/pilosa/roaring"
)

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
