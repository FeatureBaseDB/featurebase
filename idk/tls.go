package idk

import (
	"crypto/tls"
	"crypto/x509"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/featurebasedb/featurebase/v3/logger"
	"github.com/pkg/errors"
)

// TLSConfig contains TLS configuration.
// *Path elements can be set with local file paths, OR with
// literal PEM data, which is detected automatically (naively). Passing
// cert data directly via environment variables is used for
// m-cloud deployment, secured via SSM.
// This makes the parameter names slightly inaccurate, but it avoids
// introducing yet another configuration parameter.
type TLSConfig struct {
	// CertificatePath contains the path to the certificate (.crt or .pem file)
	CertificatePath string `json:"certificate" help:"Path to certificate file, or literal PEM data."`
	// CertificateKeyPath contains the path to the certificate key (.key file)
	CertificateKeyPath string `json:"key" help:"Path to certificate key file, or literal PEM data."`
	// CACertPath is the path to a CA certificate (.crt or .pem file)
	CACertPath string `json:"ca-certificate" help:"Path to CA certificate file, or literal PEM data."`
	// SkipVerify disables verification of server certificates.
	SkipVerify bool `json:"skip-verify" help:"Disables verification of server certificates."`
	// EnableClientVerification enables verification of client TLS certificates (Mutual TLS)
	EnableClientVerification bool `json:"enable-client-verification" help:"Enable verification of client certificates."`
}

type keypairReloader struct {
	certMu   sync.RWMutex
	cert     *tls.Certificate
	certPath string
	keyPath  string
}

func NewKeypairReloader(certPath, keyPath string, log logger.Logger) (*keypairReloader, error) {
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
			log.Printf("Received SIGHUP, reloading TLS certificate and key from %q and %q", certPath, keyPath)
			if err := result.maybeReload(); err != nil {
				log.Printf("Keeping old TLS certificate because the new one could not be loaded: %v", err)
			}
		}
	}()
	return result, nil
}

func NewStaticKeypair(certPemData, keyPemData []byte, log logger.Logger) (*keypairReloader, error) {
	cert, err := tls.X509KeyPair(certPemData, keyPemData)
	if err != nil {
		return nil, err
	}
	return &keypairReloader{
		certPath: "",
		keyPath:  "",
		cert:     &cert,
	}, nil
}

func (kpr *keypairReloader) maybeReload() error {
	if kpr.certPath == "" {
		return nil
	}
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

func truncateString(in string, prefixLen int) string {
	out := in
	if len(out) >= prefixLen {
		out = out[:prefixLen] + "..."
	}
	return out
}

func getKPR(CertificatePath, KeyPath string, log logger.Logger) (*keypairReloader, error) {
	var err error
	var kpr *keypairReloader

	_, certPathErr := os.Stat(CertificatePath)
	_, keyPathErr := os.Stat(KeyPath)
	if certPathErr != nil && keyPathErr != nil {
		log.Printf("CertificatePath stat unsuccessful, treating as literal PEM data: %s", certPathErr)
		log.Printf("CertificateKeyPath stat unsuccessful, treating as literal PEM data: %s", keyPathErr)

		kpr, err = NewStaticKeypair([]byte(CertificatePath), []byte(KeyPath), log)
		if err != nil {
			return nil, errors.Wrap(err, "creating static keypair")
		}
	} else {
		kpr, err = NewKeypairReloader(CertificatePath, KeyPath, log)
		if err != nil {
			return nil, errors.Wrap(err, "creating keypair reloader")
		}
	}
	return kpr, err
}

func getCertPool(capath string) (*x509.CertPool, error) {
	var caCertData []byte
	var err error
	_, caCertPathErr := os.Stat(capath)
	if caCertPathErr != nil {
		// CACertPath refers to nonexistent files; it contains literal PEM data
		log.Printf("CACertPath (%s) not found, treating as literal PEM data",
			truncateString(capath, 4),
		)
		caCertData = []byte(capath)
	} else {
		caCertData, err = os.ReadFile(capath)
		if err != nil {
			return nil, errors.Wrap(err, "loading tls ca key")
		}
	}
	certPool := x509.NewCertPool()
	ok := certPool.AppendCertsFromPEM(caCertData)
	if !ok {
		return nil, errors.New("error parsing CA certificate")
	}
	return certPool, nil
}

func GetTLSConfigFromConfluent(config *ConfluentCommand, log logger.Logger) (TLSConfig *tls.Config, err error) {
	var kpr *keypairReloader
	if config.KafkaSslCertificateLocation != "" && config.KafkaSslKeyLocation != "" {
		kpr, err = getKPR(config.KafkaSslCertificateLocation, config.KafkaSslKeyLocation, log)
		if err != nil {
			return nil, err
		}
	} else {
		return TLSConfig, nil
	}

	TLSConfig = &tls.Config{
		InsecureSkipVerify:       !config.KafkaEnableSslCertificateVerification,
		PreferServerCipherSuites: true,
		MinVersion:               tls.VersionTLS12,
		GetCertificate:           kpr.GetCertificateFunc(),
		GetClientCertificate:     kpr.GetClientCertificateFunc(),
	}

	if config.KafkaSslCaLocation != "" {
		certPool, err := getCertPool(config.KafkaSslCaLocation)
		if err != nil {
			return nil, errors.New("error building CA cert pool")
		}
		TLSConfig.ClientCAs = certPool
		TLSConfig.RootCAs = certPool
	}

	if config.KafkaEnableSslCertificateVerification {
		TLSConfig.ClientAuth = tls.RequireAndVerifyClientCert
	}
	return TLSConfig, nil
}
func GetTLSConfig(tlsConfig *TLSConfig, log logger.Logger) (TLSConfig *tls.Config, err error) {
	if tlsConfig == nil {
		return nil, nil
	}

	var kpr *keypairReloader
	if tlsConfig.CertificatePath != "" && tlsConfig.CertificateKeyPath != "" {
		kpr, err = getKPR(tlsConfig.CertificatePath, tlsConfig.CertificateKeyPath, log)
		if err != nil {
			return nil, err
		}
	} else {
		return TLSConfig, nil
	}

	TLSConfig = &tls.Config{
		InsecureSkipVerify:       tlsConfig.SkipVerify,
		PreferServerCipherSuites: true,
		MinVersion:               tls.VersionTLS12,
		GetCertificate:           kpr.GetCertificateFunc(),
		GetClientCertificate:     kpr.GetClientCertificateFunc(),
	}

	if tlsConfig.CACertPath != "" {
		certPool, err := getCertPool(tlsConfig.CACertPath)
		if err != nil {
			return nil, err
		}

		TLSConfig.ClientCAs = certPool
		TLSConfig.RootCAs = certPool
	}

	if tlsConfig.EnableClientVerification {
		TLSConfig.ClientAuth = tls.RequireAndVerifyClientCert
	}
	return TLSConfig, nil
}
