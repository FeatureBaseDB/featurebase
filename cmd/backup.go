// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package cmd

import (
	"github.com/featurebasedb/featurebase/v3/ctl"
	"github.com/featurebasedb/featurebase/v3/logger"
	"github.com/spf13/cobra"
)

func newBackupCommand(logdest logger.Logger) *cobra.Command {
	cmd := ctl.NewBackupCommand(logdest)
	ccmd := &cobra.Command{
		Use:   "backup",
		Short: "Back up FeatureBase server",
		Long: `
Backs up a FeatureBase server to a local, tar-formatted snapshot file.
`,
		RunE: UsageErrorWrapper(cmd),
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
	flags.StringVar(&cmd.HeaderTimeoutStr, "header-timeout", cmd.HeaderTimeoutStr, "Length of time to wait for initial HTTP response before giving up.")
	flags.BoolVar(&cmd.IgnoreSpaceCheck, "ignore-space-check", false, "Disable disk space check")
	return ccmd
}
