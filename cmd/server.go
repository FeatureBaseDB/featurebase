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
	"fmt"
	"io"
	"os"
	"os/signal"
	"runtime/pprof"
	"time"

	"github.com/spf13/cobra"

	"github.com/pilosa/pilosa/ctl"
	"github.com/pilosa/pilosa/server"
)

// Server is global so that tests can control and verify it.
var Server *server.Command

func NewServeCmd(stdin io.Reader, stdout, stderr io.Writer) *cobra.Command {
	Server = server.NewCommand(stdin, stdout, stderr)
	serveCmd := &cobra.Command{
		Use:   "server",
		Short: "Run Pilosa.",
		Long: `pilosa server runs Pilosa.

It will load existing data from the configured
directory, and start listening client connections
on the configured port.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			Server.Server.Handler.Version = Version
			fmt.Fprintf(Server.Stderr, "Pilosa %s, build time %s\n", Version, BuildTime)

			// Start CPU profiling.
			if Server.CPUProfile != "" {
				f, err := os.Create(Server.CPUProfile)
				if err != nil {
					return fmt.Errorf("create cpu profile: %v", err)
				}
				defer f.Close()

				fmt.Fprintln(Server.Stderr, "Starting cpu profile")
				pprof.StartCPUProfile(f)
				time.AfterFunc(Server.CPUTime, func() {
					fmt.Fprintln(Server.Stderr, "Stopping cpu profile")
					pprof.StopCPUProfile()
					f.Close()
				})
			}

			// Execute the program.
			if err := Server.Run(); err != nil {
				return fmt.Errorf("error running server: %v", err)
			}

			// First SIGKILL causes server to shut down gracefully.
			c := make(chan os.Signal, 2)
			signal.Notify(c, os.Interrupt)
			select {
			case sig := <-c:
				fmt.Fprintf(Server.Stderr, "Received %s; gracefully shutting down...\n", sig.String())

				// Second signal causes a hard shutdown.
				go func() { <-c; os.Exit(1) }()

				if err := Server.Close(); err != nil {
					return err
				}
			case <-Server.Done:
				fmt.Fprintf(Server.Stderr, "Server closed externally")
			}
			return nil
		},
	}

	// Attach flags to the command.
	ctl.BuildServerFlags(serveCmd, Server)

	return serveCmd
}

func init() {
	subcommandFns["server"] = NewServeCmd
}
