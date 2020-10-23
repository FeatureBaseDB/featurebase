// Copyright 2020 Pilosa Corp.
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
	"errors"
	"io"
	"strconv"

	"github.com/pilosa/pilosa/v2/ctl"
	"github.com/spf13/cobra"
)

func newRBFCommand(stdin io.Reader, stdout, stderr io.Writer) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "rbf",
		Short: "Inspect RBF data files.",
		Long: `
Provides a set of commands for inspecting RBF data files.
`,
	}
	cmd.AddCommand(newRBFCheckCommand(stdin, stdout, stderr))
	cmd.AddCommand(newRBFDumpCommand(stdin, stdout, stderr))
	cmd.AddCommand(newRBFPagesCommand(stdin, stdout, stderr))
	cmd.AddCommand(newRBFPageCommand(stdin, stdout, stderr))
	return cmd
}

func newRBFCheckCommand(stdin io.Reader, stdout, stderr io.Writer) *cobra.Command {
	c := ctl.NewRBFCheckCommand(stdin, stdout, stderr)
	cmd := &cobra.Command{
		Use:   "check",
		Short: "Run consistency check on RBF data.",
		Long: `
Executes a consistency check on an RBF data directory.
`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c.Path = args[0]
			return c.Run(context.Background())
		},
	}
	return cmd
}

func newRBFDumpCommand(stdin io.Reader, stdout, stderr io.Writer) *cobra.Command {
	c := ctl.NewRBFDumpCommand(stdin, stdout, stderr)
	cmd := &cobra.Command{
		Use:   "dump",
		Short: "Prints RBF raw page data",
		Long: `
Dumps the raw hex data for one or more RBF pages.
`,
		Args: cobra.MinimumNArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			c.Path = args[0]

			for _, arg := range args[1:] {
				pgno, err := strconv.Atoi(arg)
				if err != nil {
					return errors.New("invalid page number")
				}
				c.Pgnos = append(c.Pgnos, uint32(pgno))
			}

			return c.Run(context.Background())
		},
	}
	return cmd
}

func newRBFPagesCommand(stdin io.Reader, stdout, stderr io.Writer) *cobra.Command {
	c := ctl.NewRBFPagesCommand(stdin, stdout, stderr)
	cmd := &cobra.Command{
		Use:   "pages",
		Short: "Prints metadata for the list of all pages",
		Long: `
Prints a line for every page in the database with its type/status.
`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c.Path = args[0]
			return c.Run(context.Background())
		},
	}
	return cmd
}

func newRBFPageCommand(stdin io.Reader, stdout, stderr io.Writer) *cobra.Command {
	c := ctl.NewRBFPageCommand(stdin, stdout, stderr)
	cmd := &cobra.Command{
		Use:   "page",
		Short: "Prints data for a single page",
		Long: `
Prints the header & cell data for a single page.
`,
		Args: cobra.MinimumNArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			c.Path = args[0]

			for _, arg := range args[1:] {
				pgno, err := strconv.Atoi(arg)
				if err != nil {
					return errors.New("invalid page number")
				}
				c.Pgnos = append(c.Pgnos, uint32(pgno))
			}

			return c.Run(context.Background())
		},
	}
	return cmd
}
