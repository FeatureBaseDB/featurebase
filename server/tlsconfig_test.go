package server_test

import (
	"crypto/tls"
	"fmt"
	"reflect"
	"testing"

	"github.com/molecula/featurebase/v3/logger"
	"github.com/molecula/featurebase/v3/server"
)

func TestGetTLSConfig(t *testing.T) {
	type testCase struct {
		config *server.TLSConfig
		exp    *tls.Config
		err    error
	}
	for name, test := range map[string]testCase{
		"nil": {
			config: nil,
			exp:    nil,
			err:    fmt.Errorf("cannot parse nil tls config"),
		},
		"hasCASkip": {
			config: &server.TLSConfig{
				CACertPath: "blah",
				SkipVerify: true,
			},
			exp: nil,
			err: fmt.Errorf("cannot specify root certificate and disable server certificate verification"),
		},
		"hasCertSkip": {
			config: &server.TLSConfig{
				CertificatePath:    "blah",
				CertificateKeyPath: "blah",
				SkipVerify:         true,
			},
			exp: nil,
			err: fmt.Errorf("cannot specify TLS certificate and disable server certificate verification"),
		},
	} {
		t.Run(name, func(t *testing.T) {
			got, err := server.GetTLSConfig(test.config, logger.NopLogger)
			if errStr(err) != errStr(test.err) {
				t.Fatalf("expected %v, got %v", test.err, err)
			}
			if !reflect.DeepEqual(got, test.exp) {
				t.Fatalf("expected %v, got %v", test.exp, got)
			}
		})
	}
}

func errStr(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
