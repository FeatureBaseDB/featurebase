package ctl

import (
	"context"
	"fmt"
	"io"
	"strings"
)

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
