package ctl

import (
	"context"
	"fmt"
	"io"

	"github.com/molecula/featurebase/v2"
	"github.com/molecula/featurebase/v2/server"
	toml "github.com/pelletier/go-toml"
)

// ConfigCommand represents a command for printing a default config.
type ConfigCommand struct {
	*pilosa.CmdIO
	Config *server.Config
}

// NewConfigCommand returns a new instance of ConfigCommand.
func NewConfigCommand(stdin io.Reader, stdout, stderr io.Writer) *ConfigCommand {
	return &ConfigCommand{
		CmdIO: pilosa.NewCmdIO(stdin, stdout, stderr),
	}
}

// Run prints out the default config.
func (cmd *ConfigCommand) Run(_ context.Context) error {
	buf, err := toml.Marshal(*cmd.Config)
	if err != nil {
		return err
	}
	fmt.Fprintln(cmd.Stdout, string(buf))
	return nil
}
