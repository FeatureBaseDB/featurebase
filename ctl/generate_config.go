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

	"github.com/molecula/featurebase/v2"
	"github.com/molecula/featurebase/v2/server"
	"github.com/pelletier/go-toml"
	"github.com/pkg/errors"
)

// GenerateConfigCommand represents a command for printing a default config.
type GenerateConfigCommand struct {
	*pilosa.CmdIO
}

// NewGenerateConfigCommand returns a new instance of GenerateConfigCommand.
func NewGenerateConfigCommand(stdin io.Reader, stdout, stderr io.Writer) *GenerateConfigCommand {
	return &GenerateConfigCommand{
		CmdIO: pilosa.NewCmdIO(stdin, stdout, stderr),
	}
}

// Run prints out the default config.
func (cmd *GenerateConfigCommand) Run(_ context.Context) error {
	conf := server.NewConfig()
	ret, err := toml.Marshal(*conf)
	if err != nil {
		return errors.Wrap(err, "unmarshalling default config")
	}
	fmt.Fprintf(cmd.Stdout, "%s\n", ret)
	return nil
}
