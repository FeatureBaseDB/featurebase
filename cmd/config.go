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

package cmd

import (
	"context"
	"io"
	"os"

	"github.com/spf13/cobra"

	"github.com/pilosa/pilosa/ctl"
	"github.com/pilosa/pilosa/server"
)

var Conf *ctl.ConfigCommand

func NewConfigCommand(stdin io.Reader, stdout, stderr io.Writer) *cobra.Command {
	Conf = ctl.NewConfigCommand(os.Stdin, os.Stdout, os.Stderr)
	Server := server.NewCommand(stdin, stdout, stderr)
	confCmd := &cobra.Command{
		Use:   "config",
		Short: "Print the current configuration.",
		Long:  `config prints the current configuration to stdout`,

		RunE: func(cmd *cobra.Command, args []string) error {
			Conf.Config = Server.Config
			if err := Conf.Run(context.Background()); err != nil {
				return err
			}
			return nil
		},
	}

	// Attach flags to the command.
	ctl.BuildServerFlags(confCmd, Server)

	return confCmd
}

func init() {
	subcommandFns["config"] = NewConfigCommand
}
