// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package cmd

import (
	"os"

	"github.com/featurebasedb/featurebase/v3/ctl"
	"github.com/featurebasedb/featurebase/v3/logger"
	"github.com/spf13/cobra"
)

func newChkSumCommand(logdest logger.Logger) *cobra.Command {
	cmd := ctl.NewChkSumCommand(logdest, os.Stdout)
	ccmd := &cobra.Command{
		Use:   "chksum",
		Short: "Digital signature of FeatureBase data",
		Long: `
			Generates a digital signature of all the data associated with a provided FeatureBase server
			WARNING: could be slow if high cardinality fields exist
`,
		RunE: UsageErrorWrapper(cmd),
	}

	flags := ccmd.Flags()
	flags.StringVar(&cmd.Host, "host", "localhost:10101", "host:port of FeatureBase.")
	ctl.SetTLSConfig(flags, "", &cmd.TLS.CertificatePath, &cmd.TLS.CertificateKeyPath, &cmd.TLS.CACertPath, &cmd.TLS.SkipVerify, &cmd.TLS.EnableClientVerification)
	return ccmd
}
