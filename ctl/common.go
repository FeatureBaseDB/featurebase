package ctl

import (
	"github.com/molecula/featurebase/v2/http"
	"github.com/molecula/featurebase/v2/logger"
	"github.com/molecula/featurebase/v2/server"
	"github.com/pkg/errors"
	"github.com/spf13/pflag"
)

// CommandWithTLSSupport is the interface for commands which has TLS settings
type CommandWithTLSSupport interface {
	TLSHost() string
	TLSConfiguration() server.TLSConfig
	Logger() logger.Logger
}

// SetTLSConfig creates common TLS flags
func SetTLSConfig(flags *pflag.FlagSet, prefix string, certificatePath *string, certificateKeyPath *string, caCertPath *string, skipVerify *bool, enableClientVerification *bool) {
	flags.StringVarP(certificatePath, prefix+"tls.certificate", "", "", "TLS certificate path (usually has the .crt or .pem extension)")
	flags.StringVarP(certificateKeyPath, prefix+"tls.key", "", "", "TLS certificate key path (usually has the .key extension)")
	flags.StringVarP(caCertPath, prefix+"tls.ca-certificate", "", "", "TLS CA certificate path (usually has the .pem extension)")
	flags.BoolVarP(skipVerify, prefix+"tls.skip-verify", "", false, "Skip TLS certificate server verification (not secure)")
	flags.BoolVarP(enableClientVerification, prefix+"tls.enable-client-verification", "", false, "Enable TLS certificate client verification for incoming connections")
}

// commandClient returns a pilosa.InternalHTTPClient for the command
func commandClient(cmd CommandWithTLSSupport) (*http.InternalClient, error) {
	tls := cmd.TLSConfiguration()
	tlsConfig, err := server.GetTLSConfig(&tls, cmd.Logger())
	if err != nil {
		return nil, errors.Wrap(err, "getting tls config")
	}
	client, err := http.NewInternalClient(cmd.TLSHost(), http.GetHTTPClient(tlsConfig))
	if err != nil {
		return nil, errors.Wrap(err, "getting internal client")
	}
	return client, err
}
