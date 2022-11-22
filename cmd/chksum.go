// Copyright 2021 Molecula Corp. All rights reserved.
package cmd

import (
	"github.com/molecula/featurebase/v3/ctl"
	"github.com/molecula/featurebase/v3/logger"
	"github.com/spf13/cobra"
)

func newChkSumCommand(logdest logger.Logger) *cobra.Command {
	cmd := ctl.NewChkSumCommand(logdest)
	ccmd := &cobra.Command{
		Use:   "chksum",
		Short: "Digital signature of FeatureBase data",
		Long: `
			Generates a digital signature of all the data associated with a provided FeatureBase server
			WARNING: could be slow if high cardinality fields exist
`,
		RunE: usageErrorWrapper(cmd),
	}

	flags := ccmd.Flags()
	flags.StringVar(&cmd.Host, "host", "localhost:10101", "host:port of FeatureBase.")
	ctl.SetTLSConfig(flags, "", &cmd.TLS.CertificatePath, &cmd.TLS.CertificateKeyPath, &cmd.TLS.CACertPath, &cmd.TLS.SkipVerify, &cmd.TLS.EnableClientVerification)
	return ccmd
}
