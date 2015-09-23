package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
)

func main() {
	m := NewMain()

	// Parse command line arguments.
	if err := m.ParseFlags(os.Args[1:]); err != nil {
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
	Command string

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
	panic("FIXME: implement commands")
}

// ParseFlags parses command line flags from args.
func (m *Main) ParseFlags(args []string) error {
	fs := flag.NewFlagSet("pilosactl", flag.ContinueOnError)
	fs.SetOutput(m.Stderr)
	if err := fs.Parse(args); err != nil {
		return err
	}

	// Require command.
	if fs.NArg() == 0 {
		return errors.New("command required")
	}

	return nil
}
