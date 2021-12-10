// Copyright 2021 Molecula Corp. All rights reserved.
package cmd

import (
	"context"
	"io"

	"github.com/molecula/featurebase/v2/ctl"
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
	flags.StringVarP(&cmd.OutputDir, "output", "o", "", "output dir to write to")
	flags.BoolVar(&cmd.NoSync, "no-sync", false, "disable file sync")
	flags.IntVar(&cmd.Concurrency, "concurrency", cmd.Concurrency, "number of concurrent backup goroutines")
	flags.StringVar(&cmd.Host, "host", "localhost:10101", "host:port of FeatureBase.")
	flags.StringVar(&cmd.Index, "index", "", "index to backup, default backs up all indexes. ")
	ctl.SetTLSConfig(flags, "", &cmd.TLS.CertificatePath, &cmd.TLS.CertificateKeyPath, &cmd.TLS.CACertPath, &cmd.TLS.SkipVerify, &cmd.TLS.EnableClientVerification)
	return ccmd
}
