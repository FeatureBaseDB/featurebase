// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package cmd

import (
	"context"
	"fmt"
	"strconv"

	pilosa "github.com/molecula/featurebase/v3"
	"github.com/molecula/featurebase/v3/ctl"
	"github.com/molecula/featurebase/v3/logger"
	"github.com/molecula/featurebase/v3/pql"
	"github.com/spf13/cobra"
)

var Importer *ctl.ImportCommand

// DecimalFlagValue is used to set the unexported value field in a decimal. It also
// fulfills the flag.Value interface.
type DecimalFlagValue struct {
	dec *pql.Decimal
}

func (dfv *DecimalFlagValue) String() string {
	return fmt.Sprintf("%v", dfv.dec.Value())
}

func (dfv *DecimalFlagValue) Set(s string) error {
	i, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return err
	}
	if dfv.dec == nil {
		d := pql.NewDecimal(0, 0)
		dfv.dec = &d
	}
	dfv.dec.SetValue(i)
	return nil
}

func (dfv *DecimalFlagValue) Type() string {
	return fmt.Sprintf("%T", int64(0))
}

// newImportCommand runs the FeatureBase import subcommand for ingesting bulk data.
func newImportCommand(logdest logger.Logger) *cobra.Command {
	Importer = ctl.NewImportCommand(logdest)
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
			return considerUsageError(cmd, Importer.Run(context.Background()))
		},
	}

	fieldMin := DecimalFlagValue{dec: &Importer.FieldOptions.Min}
	fieldMax := DecimalFlagValue{dec: &Importer.FieldOptions.Max}
	flags := importCmd.Flags()
	flags.StringVarP(&Importer.Host, "host", "", "localhost:10101", "host:port of FeatureBase.")
	flags.StringVarP(&Importer.Index, "index", "i", "", "FeatureBase index to import into.")
	flags.StringVarP(&Importer.Field, "field", "f", "", "Field to import into.")
	flags.BoolVar(&Importer.IndexOptions.Keys, "index-keys", false, "Specify keys=true when creating an index")
	flags.BoolVar(&Importer.RowColMode, "row-col-mode", false, "Specify row-col-mode=true to read csv files as <row id>,<col id>")
	flags.BoolVar(&Importer.FieldOptions.Keys, "field-keys", false, "Specify keys=true when creating a field")
	flags.StringVar(&Importer.FieldOptions.Type, "field-type", "", "Specify the field type when creating a field. One of: set, int, decimal, time, bool, mutex")
	flags.Var(&fieldMin, "field-min", "Specify the minimum for an int field on creation") // TODO: noting that decimal field min/max are not supported here.
	flags.Var(&fieldMax, "field-max", "Specify the maximum for an int field on creation")
	flags.StringVar(&Importer.FieldOptions.CacheType, "field-cache-type", pilosa.CacheTypeRanked, "Specify the cache type for a set field on creation. One of: none, lru, ranked")
	flags.Uint32Var(&Importer.FieldOptions.CacheSize, "field-cache-size", 50000, "Specify the cache size for a set field on creation")
	flags.Var(&Importer.FieldOptions.TimeQuantum, "field-time-quantum", "Specify the time quantum for a time field on creation. One of: D, DH, H, M, MD, MDH, Y, YM, YMD, YMDH")
	flags.DurationVarP(&Importer.FieldOptions.TTL, "time-to-live", "t", 0, "Specify the time to live for views created by time quantum. Supported time unit: \"s\", \"m\", \"h\"") // \"ns\", \"us\" (or \"µs\"), \"ms\" also supported but ommitted for simplicity
	flags.IntVarP(&Importer.BufferSize, "buffer-size", "s", 10000000, "Number of bits to buffer/sort before importing.")
	flags.BoolVarP(&Importer.Sort, "sort", "", false, "Enables sorting before import.")
	flags.BoolVarP(&Importer.CreateSchema, "create", "e", false, "Create the schema if it does not exist before import.")
	flags.BoolVarP(&Importer.Clear, "clear", "", false, "Clear the data provided in the import.")
	ctl.SetTLSConfig(flags, "", &Importer.TLS.CertificatePath, &Importer.TLS.CertificateKeyPath, &Importer.TLS.CACertPath, &Importer.TLS.SkipVerify, &Importer.TLS.EnableClientVerification)
	flags.StringVar(&Importer.AuthToken, "auth-token", "", "Authentication token")

	return importCmd
}
