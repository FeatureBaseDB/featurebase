// Copyright 2021 Molecula Corp. All rights reserved.
package cmd

import (
	"errors"
	"fmt"
	"io"
	"strconv"

	"github.com/molecula/featurebase/v3/ctl"
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
		Use:   "check [flags] PATH",
		Short: "Run consistency check on RBF data.",
		Long: `
Executes a consistency check on an RBF data directory.
`,
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) == 0 {
				return fmt.Errorf("data directory path required")
			} else if len(args) > 1 {
				return fmt.Errorf("too many command line arguments")
			}
			c.Path = args[0]
			return nil
		},
		RunE: usageErrorWrapper(c),
	}
	return cmd
}

func newRBFDumpCommand(stdin io.Reader, stdout, stderr io.Writer) *cobra.Command {
	c := ctl.NewRBFDumpCommand(stdin, stdout, stderr)
	cmd := &cobra.Command{
		Use:   "dump [flags] PATH PGNO [PGNO...]",
		Short: "Prints RBF raw page data",
		Long: `
Dumps the raw hex data for one or more RBF pages.
`,
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) == 0 {
				return fmt.Errorf("data directory path required")
			} else if len(args) == 1 {
				return fmt.Errorf("page number required")
			}

			c.Path = args[0]

			for _, arg := range args[1:] {
				pgno, err := strconv.Atoi(arg)
				if err != nil {
					return errors.New("invalid page number")
				}
				c.Pgnos = append(c.Pgnos, uint32(pgno))
			}

			return nil
		},
		RunE: usageErrorWrapper(c),
	}
	return cmd
}

func newRBFPagesCommand(stdin io.Reader, stdout, stderr io.Writer) *cobra.Command {
	c := ctl.NewRBFPagesCommand(stdin, stdout, stderr)
	cmd := &cobra.Command{
		Use:   "pages [flags] PATH",
		Short: "Prints metadata for the list of all pages",
		Long: `
Prints a line for every page in the database with its type/status.
`,
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) == 0 {
				return fmt.Errorf("data directory path required")
			} else if len(args) > 1 {
				return fmt.Errorf("too many command line arguments")
			}
			c.Path = args[0]
			return nil
		},
		RunE: usageErrorWrapper(c),
	}

	flags := cmd.Flags()
	flags.BoolVar(&c.WithTree, "with-tree", false, "Display b-tree name for each row")
	return cmd
}

func newRBFPageCommand(stdin io.Reader, stdout, stderr io.Writer) *cobra.Command {
	c := ctl.NewRBFPageCommand(stdin, stdout, stderr)
	cmd := &cobra.Command{
		Use:   "page [flags] PATH PGNO [PGNO...]",
		Short: "Prints data for a page(s)",
		Long: `
Prints the header & cell data for one or more pages.
`,
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) == 0 {
				return fmt.Errorf("data directory path required")
			} else if len(args) == 1 {
				return fmt.Errorf("page number required")
			}

			c.Path = args[0]

			for _, arg := range args[1:] {
				pgno, err := strconv.Atoi(arg)
				if err != nil {
					return errors.New("invalid page number")
				}
				c.Pgnos = append(c.Pgnos, uint32(pgno))
			}

			return nil
		},
		RunE: usageErrorWrapper(c),
	}
	return cmd
}
