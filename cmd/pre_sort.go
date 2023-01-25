// Copyright 2021 Molecula Corp. All rights reserved.
package cmd

import (
	"github.com/featurebasedb/featurebase/v3/ctl"
	"github.com/featurebasedb/featurebase/v3/logger"
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
	flags.StringVarP(&cmd.Table, "table", "", "", "Name of table (used to hash keys to determine partition).")
	flags.StringVarP(&cmd.Type, "type", "", cmd.Type, "Input file type (csv or ndjson).")
	flags.StringSliceVar(&cmd.PrimaryKeyFields, "primary-key-fields", []string{}, "Names of primary key fields. For CSV there must be a header row and these are pulled from there.")
	flags.IntVar(&cmd.PartitionN, "partition-n", cmd.PartitionN, "Number of partitions.")
	flags.StringVarP(&cmd.OutputDir, "output-dir", "", cmd.OutputDir, "Directory name to write output to.")
	flags.StringVarP(&cmd.PrimaryKeySeparator, "primary-key-separator", "", cmd.PrimaryKeySeparator, "Separator to write in between primary key fields, can be empty.")
	flags.IntVar(&cmd.JobSize, "job-size", cmd.JobSize, "Number of lines to put into each job (purely a performance tuning parameter, only supported by ndjson mode).")
	flags.IntVar(&cmd.NumWorkers, "num-workers", cmd.NumWorkers, "Number of parallel worker routines doing decode->hash->encode. Only supported by ndjson mode.")
	return ccmd
}
