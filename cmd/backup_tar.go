// Copyright 2022 Molecula Corp. All rights reserved.
package cmd

import (
	"context"
	"io"

	"github.com/molecula/featurebase/v3/ctl"
	"github.com/spf13/cobra"
)

func newBackupTarCommand(stdin io.Reader, stdout io.Writer, stderr io.Writer) *cobra.Command {
	cmd := ctl.NewBackupTarCommand(stdin, stdout, stderr)
	ccmd := &cobra.Command{
		Use:   "backuptar",
		Short: "Back up FeatureBase server in tar format",
		Long: `
Backs up a FeatureBase server to a local, tar-formatted snapshot file.
`,
		RunE: func(c *cobra.Command, args []string) error {
			return cmd.Run(context.Background())
		},
	}

	flags := ccmd.Flags()
	flags.StringVarP(&cmd.OutputPath, "output", "o", "", "Output directory to write to.")
	flags.StringVar(&cmd.Host, "host", "localhost:10101", "The address (host:port) of FeatureBase (HTTP).")
	flags.StringVar(&cmd.Index, "index", "", "Index to backup, default backs up all indexes. ")
	flags.DurationVar(&cmd.RetryPeriod, "retry-period", cmd.RetryPeriod, "Length of time after HTTP request failure to continue retrying request.")
	flags.StringVar(&cmd.Pprof, "pprof", cmd.Pprof, "host:port to listen for profiling requests at /debug/pprof and /debug/fgprof.")
	ctl.SetTLSConfig(flags, "", &cmd.TLS.CertificatePath, &cmd.TLS.CertificateKeyPath, &cmd.TLS.CACertPath, &cmd.TLS.SkipVerify, &cmd.TLS.EnableClientVerification)
	flags.StringVar(&cmd.AuthToken, "auth-token", "", "Authentication token")
	flags.StringVar(&cmd.HeaderTimeoutStr, "header-timeout", cmd.HeaderTimeoutStr, "Length of time to wait for initial HTTP response before giving up.")

	return ccmd
}
