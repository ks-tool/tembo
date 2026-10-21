package postgres

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// writeCert generates a self-signed CA certificate and writes it as the CA
// bundle, the server certificate and its key into dir.
func writeCert(t *testing.T, dir string) (caFile, certFile, keyFile string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "tembo-test"},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		DNSNames:              []string{"localhost"},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	caFile = filepath.Join(dir, "ca.pem")
	certFile = filepath.Join(dir, "cert.pem")
	keyFile = filepath.Join(dir, "key.pem")
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	for file, data := range map[string][]byte{
		caFile:   certPEM,
		certFile: certPEM,
		keyFile:  pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}),
	} {
		if err := os.WriteFile(file, data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	return caFile, certFile, keyFile
}

// withTLSFlags sets the --tls-* fields of rc for the duration of the test.
func withTLSFlags(t *testing.T, cert, key, ca string) {
	t.Helper()
	old := rc
	rc.TLSCertFile, rc.TLSKeyFile, rc.TLSCAFile = cert, key, ca
	t.Cleanup(func() { rc = old })
}

func TestAPITLSConfig(t *testing.T) {
	ca, cert, key := writeCert(t, t.TempDir())

	withTLSFlags(t, "", "", "")
	if cfg, err := apiTLSConfig(); cfg != nil || err != nil {
		t.Errorf("no flags: got %v, %v", cfg, err)
	}

	rc.TLSCertFile, rc.TLSKeyFile = cert, ""
	if _, err := apiTLSConfig(); err == nil {
		t.Error("a certificate without a key must fail")
	}
	rc.TLSCertFile, rc.TLSKeyFile = "", key
	if _, err := apiTLSConfig(); err == nil {
		t.Error("a key without a certificate must fail")
	}
	rc.TLSKeyFile, rc.TLSCAFile = "", ca
	if _, err := apiTLSConfig(); err == nil {
		t.Error("a CA without a certificate must fail")
	}

	rc.TLSCertFile, rc.TLSKeyFile, rc.TLSCAFile = cert, key, ""
	cfg, err := apiTLSConfig()
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Certificates) != 1 || cfg.ClientAuth != tls.NoClientCert || cfg.ClientCAs != nil {
		t.Errorf("cert and key: got %+v", cfg)
	}

	rc.TLSCAFile = ca
	cfg, err = apiTLSConfig()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ClientAuth != tls.VerifyClientCertIfGiven || cfg.ClientCAs == nil {
		t.Errorf("with a CA: got ClientAuth %v, ClientCAs %v", cfg.ClientAuth, cfg.ClientCAs)
	}

	bad := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(bad, []byte("not pem"), 0600); err != nil {
		t.Fatal(err)
	}
	rc.TLSCAFile = bad
	if _, err := apiTLSConfig(); err == nil {
		t.Error("a CA file without certificates must fail")
	}
}
