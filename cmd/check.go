// Copyright 2021 Molecula Corp. All rights reserved.
package cmd

import (
	"context"
	"fmt"
	"io"

	"github.com/spf13/cobra"

	"github.com/molecula/featurebase/v3/ctl"
)

var checker *ctl.CheckCommand

func newCheckCommand(stdin io.Reader, stdout io.Writer, stderr io.Writer) *cobra.Command {
	checker = ctl.NewCheckCommand(stdin, stdout, stderr)
	checkCmd := &cobra.Command{
		Use:   "check <path> [path2]...",
		Short: "Do a consistency check on a FeatureBase data file.",
		Long: `
Performs a consistency check on data files.
`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) == 0 {
				return fmt.Errorf("path required")
			}
			checker.Paths = args
			return checker.Run(context.Background())
		},
	}
	return checkCmd
}
