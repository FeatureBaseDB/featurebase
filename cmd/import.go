// Copyright 2021 Molecula Corp. All rights reserved.
package cmd

import (
	"context"
	"io"

	pilosa "github.com/molecula/featurebase/v3"
	"github.com/molecula/featurebase/v3/ctl"
	"github.com/spf13/cobra"
)

var Importer *ctl.ImportCommand

// newImportCommand runs the FeatureBase import subcommand for ingesting bulk data.
func newImportCommand(stdin io.Reader, stdout, stderr io.Writer) *cobra.Command {
	Importer = ctl.NewImportCommand(stdin, stdout, stderr)
	importCmd := &cobra.Command{
		Use:   "import",
		Short: "Bulk load data into FeatureBase.",
		Long: `Bulk imports one or more CSV files to a host's index and field. The data
of the CSV file are grouped by shard for the most efficient import.

The format of the CSV file is:

	ROWID,COLUMNID,[TIME]

The file should contain no headers. The TIME column is optional and can be
omitted. If it is present then its format should be YYYY-MM-DDTHH:MM.
`,
		RunE: func(cmd *cobra.Command, args []string) error {
			Importer.Paths = args
			return Importer.Run(context.Background())
		},
	}

	flags := importCmd.Flags()
	flags.StringVarP(&Importer.Host, "host", "", "localhost:10101", "host:port of FeatureBase.")
	flags.StringVarP(&Importer.Index, "index", "i", "", "FeatureBase index to import into.")
	flags.StringVarP(&Importer.Field, "field", "f", "", "Field to import into.")
	flags.BoolVar(&Importer.IndexOptions.Keys, "index-keys", false, "Specify keys=true when creating an index")
	flags.BoolVar(&Importer.FieldOptions.Keys, "field-keys", false, "Specify keys=true when creating a field")
	flags.StringVar(&Importer.FieldOptions.Type, "field-type", "", "Specify the field type when creating a field. One of: set, int, decimal, time, bool, mutex")
	flags.Int64Var(&Importer.FieldOptions.Min.Value, "field-min", 0, "Specify the minimum for an int field on creation") // TODO: noting that decimal field min/max are not supported here.
	flags.Int64Var(&Importer.FieldOptions.Max.Value, "field-max", 0, "Specify the maximum for an int field on creation")
	flags.StringVar(&Importer.FieldOptions.CacheType, "field-cache-type", pilosa.CacheTypeRanked, "Specify the cache type for a set field on creation. One of: none, lru, ranked")
	flags.Uint32Var(&Importer.FieldOptions.CacheSize, "field-cache-size", 50000, "Specify the cache size for a set field on creation")
	flags.Var(&Importer.FieldOptions.TimeQuantum, "field-time-quantum", "Specify the time quantum for a time field on creation. One of: D, DH, H, M, MD, MDH, Y, YM, YMD, YMDH")
	flags.IntVarP(&Importer.BufferSize, "buffer-size", "s", 10000000, "Number of bits to buffer/sort before importing.")
	flags.BoolVarP(&Importer.Sort, "sort", "", false, "Enables sorting before import.")
	flags.BoolVarP(&Importer.CreateSchema, "create", "e", false, "Create the schema if it does not exist before import.")
	flags.BoolVarP(&Importer.Clear, "clear", "", false, "Clear the data provided in the import.")
	ctl.SetTLSConfig(flags, "", &Importer.TLS.CertificatePath, &Importer.TLS.CertificateKeyPath, &Importer.TLS.CACertPath, &Importer.TLS.SkipVerify, &Importer.TLS.EnableClientVerification)
	flags.StringVar(&Importer.AuthToken, "auth-token", "", "Authentication token")

	return importCmd
}
