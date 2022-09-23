// Copyright 2021 Molecula Corp. All rights reserved.
//
// This file contains source code from bridge
// (https://github.com/robustirc/bridge); which is governed by the following
// license notice:
//
// Copyright © 2014-2015 The RobustIRC Authors. All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
//
//     * Redistributions of source code must retain the above copyright
//       notice, this list of conditions and the following disclaimer.
//
//     * Redistributions in binary form must reproduce the above copyright
//       notice, this list of conditions and the following disclaimer in the
//       documentation and/or other materials provided with the distribution.
//
//     * Neither the name of RobustIRC nor the names of contributors may be used
//       to endorse or promote products derived from this software without
//       specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
// ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
// WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
// DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDERS OR CONTRIBUTORS BE LIABLE
// FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
// DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
// SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
// CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
// OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

package server

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/molecula/featurebase/v3/logger"
	"github.com/pkg/errors"
)

type keypairReloader struct {
	certMu   sync.RWMutex
	cert     *tls.Certificate
	certPath string
	keyPath  string
}

func NewKeypairReloader(certPath, keyPath string, logger logger.Logger) (*keypairReloader, error) {
	result := &keypairReloader{
		certPath: certPath,
		keyPath:  keyPath,
	}
	cert, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		return nil, err
	}
	result.cert = &cert
	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGHUP)
		for range c {
			logger.Infof("Received SIGHUP, reloading TLS certificate and key from %q and %q", certPath, keyPath)
			if err := result.maybeReload(); err != nil {
				logger.Printf("ERROR: Keeping old TLS certificate because the new one could not be loaded: %v", err)
			}
		}
	}()
	return result, nil
}

func (kpr *keypairReloader) maybeReload() error {
	newCert, err := tls.LoadX509KeyPair(kpr.certPath, kpr.keyPath)
	if err != nil {
		return err
	}
	kpr.certMu.Lock()
	defer kpr.certMu.Unlock()
	kpr.cert = &newCert
	return nil
}

func (kpr *keypairReloader) GetCertificateFunc() func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
	return func(clientHello *tls.ClientHelloInfo) (*tls.Certificate, error) {
		kpr.certMu.RLock()
		defer kpr.certMu.RUnlock()
		return kpr.cert, nil
	}
}

func (kpr *keypairReloader) GetClientCertificateFunc() func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
	return func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
		kpr.certMu.RLock()
		defer kpr.certMu.RUnlock()
		return kpr.cert, nil
	}
}

func GetTLSConfig(tlsConfig *TLSConfig, logger logger.Logger) (TLSConfig *tls.Config, err error) {
	if tlsConfig == nil {
		return nil, fmt.Errorf("cannot parse nil tls config")
	}

	hasCA := len(tlsConfig.CACertPath) > 0
	hasCert := len(tlsConfig.CertificatePath) > 0 && len(tlsConfig.CertificateKeyPath) > 0

	if hasCA && tlsConfig.SkipVerify {
		return nil, fmt.Errorf("cannot specify root certificate and disable server certificate verification")
	}

	if hasCert && tlsConfig.SkipVerify {
		return nil, fmt.Errorf("cannot specify TLS certificate and disable server certificate verification")
	}

	if hasCert {
		kpr, err := NewKeypairReloader(tlsConfig.CertificatePath, tlsConfig.CertificateKeyPath, logger)
		if err != nil {
			return nil, errors.Wrap(err, "loading keypair")
		}

		TLSConfig = &tls.Config{
			InsecureSkipVerify:       tlsConfig.SkipVerify,
			PreferServerCipherSuites: true,
			MinVersion:               tls.VersionTLS12,
			GetCertificate:           kpr.GetCertificateFunc(),
			GetClientCertificate:     kpr.GetClientCertificateFunc(),
		}

		if hasCA {
			b, err := os.ReadFile(tlsConfig.CACertPath)
			if err != nil {
				return nil, errors.Wrap(err, "loading tls ca key")
			}
			certPool := x509.NewCertPool()

			ok := certPool.AppendCertsFromPEM(b)
			if !ok {
				return nil, errors.New("error parsing CA certificate")
			}
			TLSConfig.ClientCAs = certPool
			TLSConfig.RootCAs = certPool
		}

		if tlsConfig.EnableClientVerification {
			TLSConfig.ClientAuth = tls.RequireAndVerifyClientCert
		}
	}
	return TLSConfig, nil
}
