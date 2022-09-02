// Copyright 2022 Molecula Corp. All rights reserved.
package cmd

import (
	"context"
	"io"

	"github.com/featurebasedb/featurebase/v3/ctl"
	"github.com/spf13/cobra"
)

func newRestoreTarCommand(stdin io.Reader, stdout, stderr io.Writer) *cobra.Command {
	cmd := ctl.NewRestoreTarCommand(stdin, stdout, stderr)
	restoreCmd := &cobra.Command{
		Use:   "restoretar",
		Short: "Restore from a backup in tar format",
		Long: `
The Restore command will take a tar-formatted backup archive and restore it to a new, clean cluster.
`,
		RunE: func(c *cobra.Command, args []string) error {
			return cmd.Run(context.Background())
		},
	}
	flags := restoreCmd.Flags()
	flags.StringVarP(&cmd.Path, "source", "s", "", "backup file; specify '-' to restore from stdin tar stream")
	flags.StringVar(&cmd.Host, "host", "localhost:10101", "host:port of FeatureBase.")
	flags.DurationVar(&cmd.RetryPeriod, "retry-period", cmd.RetryPeriod, "Length of time after HTTP request failure to continue retrying request.")
	flags.StringVar(&cmd.Pprof, "pprof", cmd.Pprof, "host:port to listen for profiling requests at /debug/pprof and /debug/fgprof.")
	flags.StringVar(&cmd.AuthToken, "auth-token", "", "Authentication token")
	ctl.SetTLSConfig(
		flags, "",
		&cmd.TLS.CertificatePath,
		&cmd.TLS.CertificateKeyPath,
		&cmd.TLS.CACertPath,
		&cmd.TLS.SkipVerify,
		&cmd.TLS.EnableClientVerification,
	)

	return restoreCmd
}
