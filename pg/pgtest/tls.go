package pgtest

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"time"

	"github.com/molecula/featurebase/v2/pg"
	"github.com/pkg/errors"
)

// SetupTLS generates a TLS certificate and installs it into the server.
// TODO: have the client properly trust this (generate a CA to install instead of using self-signed).
func SetupTLS(server *pg.Server) error {
	// Generate an ecdsa key for the cert.
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return errors.Wrap(err, "generating TLS key")
	}

	// Generate a random 128-bit serial number.
	serialNumber, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return errors.Wrap(err, "generating serial number")
	}

	// Make the certificate valid starting now.
	now := time.Now()

	// Create a certificate template.
	template := x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			Organization: []string{"Molecula"},
		},
		NotBefore: now,
		NotAfter:  now.Add(time.Hour),

		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}

	// Generate a self-signed x509 cert from the template and the key.
	certData, err := x509.CreateCertificate(rand.Reader, &template, &template, key.Public(), key)
	if err != nil {
		return errors.Wrap(err, "encoding certificate x509")
	}

	// Encode the cert to PEM so that the TLS package can load it.
	certPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: certData,
	})

	// Encode the private key to x509.
	keyData, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return errors.Wrap(err, "encoding key x509")
	}

	// Encode the key to PEM so that the TLS package can load it.
	keyPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "PRIVATE KEY",
		Bytes: keyData,
	})

	// Load the certificate and key from their PEM encodings.
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return errors.Wrap(err, "loading TLS key pair")
	}

	// Install the certificate into the server.
	if server.TLSConfig == nil {
		server.TLSConfig = &tls.Config{}
	}
	server.TLSConfig.Certificates = append(server.TLSConfig.Certificates, cert)

	return nil
}
