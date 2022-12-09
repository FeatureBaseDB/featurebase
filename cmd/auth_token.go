// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package cmd

import (
	"github.com/featurebasedb/featurebase/v3/ctl"
	"github.com/featurebasedb/featurebase/v3/logger"
	"github.com/spf13/cobra"
)

func newAuthTokenCommand(logdest logger.Logger) *cobra.Command {
	cmd := ctl.NewAuthTokenCommand(logdest)
	ccmd := &cobra.Command{
		Use:   "auth-token",
		Short: "Get an auth-token",
		Long: `
Retrieves an auth-token for use in authenticating with FeatureBase from the configured identity provider.
`,
		RunE: usageErrorWrapper(cmd),
	}

	flags := ccmd.Flags()
	flags.StringVar(&cmd.Host, "host", "https://localhost:10101", "The address (host:port) of FeatureBase (HTTPs).")
	ctl.SetTLSConfig(flags, "", &cmd.TLS.CertificatePath, &cmd.TLS.CertificateKeyPath, &cmd.TLS.CACertPath, &cmd.TLS.SkipVerify, &cmd.TLS.EnableClientVerification)
	return ccmd
}
