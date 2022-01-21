// Copyright 2021 Molecula Corp. All rights reserved.
package cmd

import (
	"context"
	"fmt"
	"io"

	"github.com/spf13/cobra"

	"github.com/molecula/featurebase/v3/ctl"
)

var inspector *ctl.InspectCommand

func newInspectCommand(stdin io.Reader, stdout, stderr io.Writer) *cobra.Command {
	inspector = ctl.NewInspectCommand(stdin, stdout, stderr)

	inspectCmd := &cobra.Command{
		Use:   "inspect",
		Short: "Get stats on a FeatureBase data file.",
		Long: `
Inspects a data file and provides stats.
`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) == 0 {
				return fmt.Errorf("path required")
			} else if len(args) > 1 {
				return fmt.Errorf("only one path allowed")
			}
			inspector.Path = args[0]
			return inspector.Run(context.Background())
		},
	}
	flags := inspectCmd.Flags()
	flags.BoolVarP(&inspector.Quiet, "quiet", "q", false, "don't list details of containers")
	flags.IntVarP(&inspector.Max, "max", "n", 0, "list at most max items (0 = unlimited)")
	flags.StringVarP(&inspector.InspectOpts.Indexes, "index", "i", "", "filter indexes")
	flags.StringVarP(&inspector.InspectOpts.Views, "view", "v", "", "filter views")
	flags.StringVarP(&inspector.InspectOpts.Fields, "field", "f", "", "filter fields")
	flags.StringVarP(&inspector.InspectOpts.Shards, "shard", "s", "", "filter shards")
	return inspectCmd
}
