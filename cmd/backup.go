// Copyright 2021 Molecula Corp. All rights reserved.
package cmd

import (
	"context"
	"io"

	"github.com/molecula/featurebase/v3/ctl"
	"github.com/spf13/cobra"
)

func newBackupCommand(stdin io.Reader, stdout io.Writer, stderr io.Writer) *cobra.Command {
	cmd := ctl.NewBackupCommand(stdin, stdout, stderr)
	ccmd := &cobra.Command{
		Use:   "backup",
		Short: "Back up FeatureBase server",
		Long: `
Backs up a FeatureBase server to a local, tar-formatted snapshot file.
`,
		RunE: func(c *cobra.Command, args []string) error {
			return cmd.Run(context.Background())
		},
	}

	flags := ccmd.Flags()
	flags.StringVarP(&cmd.OutputDir, "output", "o", "", "Output directory to write to.")
	flags.BoolVar(&cmd.NoSync, "no-sync", false, "Disable file sync")
	flags.IntVar(&cmd.Concurrency, "concurrency", cmd.Concurrency, "Number of concurrent backup goroutines.")
	flags.StringVar(&cmd.Host, "host", "localhost:10101", "The address (host:port) of FeatureBase (HTTP).")
	flags.StringVar(&cmd.Index, "index", "", "Index to backup, default backs up all indexes. ")
	flags.DurationVar(&cmd.RetryPeriod, "retry-period", cmd.RetryPeriod, "Length of time after HTTP request failure to continue retrying request.")
	flags.StringVar(&cmd.Pprof, "pprof", cmd.Pprof, "host:port to listen for profiling requests at /debug/pprof and /debug/fgprof.")
	ctl.SetTLSConfig(flags, "", &cmd.TLS.CertificatePath, &cmd.TLS.CertificateKeyPath, &cmd.TLS.CACertPath, &cmd.TLS.SkipVerify, &cmd.TLS.EnableClientVerification)
	flags.StringVar(&cmd.AuthToken, "auth-token", "", "Authentication token")
	return ccmd
}
