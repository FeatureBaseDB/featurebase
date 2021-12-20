// Copyright 2021 Molecula Corp. All rights reserved.
package cmd

import (
	"context"
	"io"

	"github.com/molecula/featurebase/v2/ctl"
	"github.com/spf13/cobra"
)

func newRestoreCommand(stdin io.Reader, stdout, stderr io.Writer) *cobra.Command {
	cmd := ctl.NewRestoreCommand(stdin, stdout, stderr)
	restoreCmd := &cobra.Command{
		Use:   "restore",
		Short: "Restore from a backup",
		Long: `
The Restore command will take a backup archive and restore it to a new, clean cluster.
`,
		RunE: func(c *cobra.Command, args []string) error {
			return cmd.Run(context.Background())
		},
	}
	flags := restoreCmd.Flags()
	flags.StringVarP(&cmd.Path, "source", "s", "", "backup file; specify '-' to restore from stdin tar stream")
	flags.StringVar(&cmd.Host, "host", "localhost:10101", "host:port of FeatureBase.")
	flags.IntVar(&cmd.Concurrency, "concurrency", 1, "number of concurrent uploads")
	flags.DurationVar(&cmd.RetryPeriod, "retry-period", cmd.RetryPeriod, "Length of time after HTTP request failure to continue retrying request.")
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
