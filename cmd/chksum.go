// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package cmd

import (
	"context"
	"io"

	"github.com/featurebasedb/featurebase/v3/ctl"
	"github.com/spf13/cobra"
)

func newChkSumCommand(stdin io.Reader, stdout io.Writer, stderr io.Writer) *cobra.Command {
	cmd := ctl.NewChkSumCommand(stdin, stdout, stderr)
	ccmd := &cobra.Command{
		Use:   "chksum",
		Short: "Digital signature of FeatureBase data",
		Long: `
			Generates a digital signature of all the data associated with a provided FeatureBase server
			WARNING: could be slow if high cardinality fields exist
`,
		RunE: func(c *cobra.Command, args []string) error {
			return cmd.Run(context.Background())
		},
	}

	flags := ccmd.Flags()
	flags.StringVar(&cmd.Host, "host", "localhost:10101", "host:port of FeatureBase.")
	ctl.SetTLSConfig(flags, "", &cmd.TLS.CertificatePath, &cmd.TLS.CertificateKeyPath, &cmd.TLS.CACertPath, &cmd.TLS.SkipVerify, &cmd.TLS.EnableClientVerification)
	return ccmd
}
