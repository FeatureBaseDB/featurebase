// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package cmd

import (
	"github.com/featurebasedb/featurebase/v3/ctl"
	"github.com/featurebasedb/featurebase/v3/logger"
	"github.com/spf13/cobra"
)

func newRestoreCommand(logdest logger.Logger) *cobra.Command {
	cmd := ctl.NewRestoreCommand(logdest)
	restoreCmd := &cobra.Command{
		Use:   "restore",
		Short: "Restore from a backup",
		Long: `
The Restore command will take a backup archive and restore it to a new, clean cluster.
`,
		RunE: UsageErrorWrapper(cmd),
	}
	flags := restoreCmd.Flags()
	flags.StringVarP(&cmd.Path, "source", "s", "", "backup file; specify '-' to restore from stdin tar stream")
	flags.StringVar(&cmd.Host, "host", "localhost:10101", "host:port of FeatureBase.")
	flags.IntVar(&cmd.Concurrency, "concurrency", 1, "number of concurrent uploads")
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
