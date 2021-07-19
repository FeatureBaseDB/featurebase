// Copyright 2017 Pilosa Corp.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ctl

import (
	"context"
	"fmt"
	"io"

	toml "github.com/pelletier/go-toml"
	"github.com/molecula/featurebase/v2"
	"github.com/molecula/featurebase/v2/server"
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
