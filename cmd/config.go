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

	"github.com/spf13/cobra"

	"github.com/pilosa/pilosa/ctl"
	"github.com/pilosa/pilosa/server"
)

var conf *ctl.ConfigCommand

func newConfigCommand(stdin io.Reader, stdout, stderr io.Writer) *cobra.Command {
	conf = ctl.NewConfigCommand(stdin, stdout, stderr)
	Server := server.NewCommand(stdin, stdout, stderr)
	confCmd := &cobra.Command{
		Use:   "config",
		Short: "Print the current configuration.",
		Long:  `config prints the current configuration to stdout`,

		RunE: func(cmd *cobra.Command, args []string) error {
			conf.Config = Server.Config
			return conf.Run(context.Background())
		},
	}

	// Attach flags to the command.
	ctl.BuildServerFlags(confCmd, Server)

	return confCmd
}
