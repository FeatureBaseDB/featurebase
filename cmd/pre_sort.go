// Copyright 2021 Molecula Corp. All rights reserved.
package cmd

import (
	"github.com/molecula/featurebase/v3/ctl"
	"github.com/molecula/featurebase/v3/logger"
	"github.com/spf13/cobra"
)

func newPreSortCommand(logdest logger.Logger) *cobra.Command {
	cmd := ctl.NewPreSortCommand(logdest)
	ccmd := &cobra.Command{
		Use:   "pre_sort",
		Short: "Sort records within files into files by FB partition for more efficient ingest",
		Long: `
Takes all input files and writes PartitionN numbered files to a directory, where each file contains only records that will go into the partition it is named for.
`,
		RunE: usageErrorWrapper(cmd),
	}

	flags := ccmd.Flags()
	flags.StringVarP(&cmd.File, "file", "", "", "Input file or directory.")
	flags.StringVarP(&cmd.Table, "table", "", "", "Name of table (used to hash keys to determine partition)")
	flags.StringArrayVar(&cmd.PrimaryKeyFields, "primary-key-fields", []string{}, "names of primary key fields")
	flags.IntVar(&cmd.PartitionN, "partition-n", cmd.PartitionN, "Number of partitions")
	flags.StringVarP(&cmd.OutputDir, "output-dir", "", cmd.OutputDir, "Directory name to write output to.")
	return ccmd
}
