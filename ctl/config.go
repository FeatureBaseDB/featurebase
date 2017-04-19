package ctl

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/pilosa/pilosa"
)

// ConfigCommand represents a command for printing a default config.
type ConfigCommand struct {
	*pilosa.CmdIO
}

// NewConfigCommand returns a new instance of ConfigCommand.
func NewConfigCommand(stdin io.Reader, stdout, stderr io.Writer) *ConfigCommand {
	return &ConfigCommand{
		CmdIO: pilosa.NewCmdIO(stdin, stdout, stderr),
	}
}

// Run prints out the default config.
func (cmd *ConfigCommand) Run(ctx context.Context) error {
	fmt.Fprintln(cmd.Stdout, strings.TrimSpace(`
data-dir = "~/.pilosa"
bind = "localhost:10101"

[cluster]
  poll-interval = "2m0s"
  replicas = 1
  hosts = [
    "localhost:10101",
  ]

[anti-entropy]
  interval = "10m0s"

[metrics]
	service = "statsd"
	host = "127.0.0.1:8125"

[profile]
  cpu = ""
  cpu-time = "30s"

[plugins]
  path = ""
`)+"\n")
	return nil
}
