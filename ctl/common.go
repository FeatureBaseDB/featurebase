// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package ctl

import (
	"time"

	pilosa "github.com/featurebasedb/featurebase/v3"
	"github.com/featurebasedb/featurebase/v3/encoding/proto"
	"github.com/featurebasedb/featurebase/v3/logger"
	"github.com/featurebasedb/featurebase/v3/server"
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

// AnyClientOption can be either pilosa.InternalClientOption or
// pilosa.ClientOption. The internal options are specific to the
// featurebase client, whereas the client options are applied to the
// Go HTTP client that gets used under the hood.
type AnyClientOption interface{}

// commandClient returns a pilosa.InternalHTTPClient for the command
func commandClient(cmd CommandWithTLSSupport, opts ...AnyClientOption) (*pilosa.InternalClient, error) {
	internalopts, clientopts, err := separateOptions(opts...)
	if err != nil {
		return nil, errors.Wrap(err, "separating client options")
	}

	// we default dial timeout to 3s in commandClient, but prepend it
	// to the option list so other options can override it.
	clientopts = append([]pilosa.ClientOption{pilosa.ClientDialTimeoutOption(time.Second * 3)}, clientopts...)
	internalopts = append([]pilosa.InternalClientOption{pilosa.WithSerializer(proto.Serializer{})}, internalopts...)
	tls := cmd.TLSConfiguration()
	tlsConfig, err := server.GetTLSConfig(&tls, cmd.Logger())
	if err != nil {
		return nil, errors.Wrap(err, "getting tls config")
	}
	client, err := pilosa.NewInternalClient(cmd.TLSHost(), pilosa.GetHTTPClient(tlsConfig, clientopts...), internalopts...)
	if err != nil {
		return nil, errors.Wrap(err, "getting internal client")
	}
	return client, err
}

// separateOptions splits the list of AnyClientOption into the two
// possible types.
func separateOptions(opts ...AnyClientOption) ([]pilosa.InternalClientOption, []pilosa.ClientOption, error) {
	internalopts := []pilosa.InternalClientOption{}
	clientopts := []pilosa.ClientOption{}
	for _, opt := range opts {
		if iopt, ok := opt.(pilosa.InternalClientOption); ok {
			internalopts = append(internalopts, iopt)
			continue
		}
		if copt, ok := opt.(pilosa.ClientOption); ok {
			clientopts = append(clientopts, copt)
			continue
		}
		return nil, nil, errors.Errorf("opt: %+v of type %[1]T must be an InternalClientOption or a ClientOption", opt)
	}
	return internalopts, clientopts, nil
}
