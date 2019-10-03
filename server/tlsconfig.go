package server

import (
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/pkg/errors"
)

type keypairReloader struct {
	certMu   sync.RWMutex
	cert     *tls.Certificate
	certPath string
	keyPath  string
}

func NewKeypairReloader(certPath, keyPath string, logger *log.Logger) (*keypairReloader, error) {
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
		for _ = range c {
			logger.Printf("Received SIGHUP, reloading TLS certificate and key from %q and %q", certPath, keyPath)
			if err := result.maybeReload(); err != nil {
				logger.Printf("Keeping old TLS certificate because the new one could not be loaded: %v", err)
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

func GetTLSConfig(tlsConfig *TLSConfig, logger *log.Logger) (TLSConfig *tls.Config, err error) {
	if tlsConfig.CertificatePath != "" && tlsConfig.CertificateKeyPath != "" {
		kpr, err := NewKeypairReloader(tlsConfig.CertificatePath, tlsConfig.CertificateKeyPath, logger)
		if err != nil {
			return nil, errors.Wrap(err, "loading keypair")
		}
		TLSConfig = &tls.Config{
			InsecureSkipVerify:       tlsConfig.SkipVerify,
			PreferServerCipherSuites: true,
			MinVersion:               tls.VersionTLS12,
			GetCertificate:           kpr.GetCertificateFunc(),
		}
		if tlsConfig.CACertPath != "" {
			b, err := ioutil.ReadFile(tlsConfig.CACertPath)
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
