package auth

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

func tokenHS(t *testing.T, secret []byte, claims jwt.MapClaims) string {
	t.Helper()
	tok, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString(secret)
	if err != nil {
		t.Fatal(err)
	}
	return tok
}

func rsaKeys(t *testing.T) (*rsa.PrivateKey, []byte) {
	t.Helper()
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.MarshalPKIXPublicKey(&priv.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	return priv, pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der})
}

func TestJWTHMAC(t *testing.T) {
	secret := []byte("a sufficiently long secret")
	f := filepath.Join(t.TempDir(), "key")
	if err := os.WriteFile(f, append(secret, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
	a, err := New(Config{JWTKeyFile: f, JWTIssuer: "test"})
	if err != nil {
		t.Fatal(err)
	}
	ok := tokenHS(t, secret, jwt.MapClaims{"sub": "alice", "iss": "test", "exp": time.Now().Add(time.Hour).Unix()})
	id, valid := a.jwt.authenticate(ok)
	if !valid || id.String() != "jwt:alice" {
		t.Errorf("valid token: %v %v", id, valid)
	}
	for _, bad := range []string{
		tokenHS(t, secret, jwt.MapClaims{"sub": "alice", "iss": "test", "exp": time.Now().Add(-time.Hour).Unix()}), // expired
		tokenHS(t, secret, jwt.MapClaims{"sub": "alice", "iss": "other", "exp": time.Now().Add(time.Hour).Unix()}), // wrong issuer
		tokenHS(t, secret, jwt.MapClaims{"iss": "test", "exp": time.Now().Add(time.Hour).Unix()}),                  // no sub
		tokenHS(t, []byte("another secret entirely"), jwt.MapClaims{"sub": "alice", "iss": "test"}),                // wrong key
	} {
		if _, valid := a.jwt.authenticate(bad); valid {
			t.Errorf("invalid token accepted: %s", bad)
		}
	}
}

func TestJWTRSAAndAlgConfusion(t *testing.T) {
	priv, pub := rsaKeys(t)
	f := filepath.Join(t.TempDir(), "key.pem")
	if err := os.WriteFile(f, pub, 0600); err != nil {
		t.Fatal(err)
	}
	a, err := New(Config{JWTKeyFile: f})
	if err != nil {
		t.Fatal(err)
	}
	tok, err := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.MapClaims{"sub": "bob", "exp": time.Now().Add(time.Hour).Unix()}).SignedString(priv)
	if err != nil {
		t.Fatal(err)
	}
	if id, valid := a.jwt.authenticate(tok); !valid || id.String() != "jwt:bob" {
		t.Errorf("RS256 token: %v %v", id, valid)
	}
	// An HMAC token signed with the public key bytes must not pass an RSA key.
	confused := tokenHS(t, pub, jwt.MapClaims{"sub": "bob", "exp": time.Now().Add(time.Hour).Unix()})
	if _, valid := a.jwt.authenticate(confused); valid {
		t.Error("alg confusion: an HS256 token passed an RSA key")
	}
}

func TestJWTJWKS(t *testing.T) {
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	jwks, _ := json.Marshal(map[string]any{"keys": []map[string]string{{
		"kty": "EC", "crv": "P-256", "kid": "k1",
		"x": base64url(priv.PublicKey.X.Bytes()), "y": base64url(priv.PublicKey.Y.Bytes()),
	}}})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(jwks)
	}))
	defer srv.Close()

	a, err := New(Config{JWTJWKS: srv.URL})
	if err != nil {
		t.Fatal(err)
	}
	tok := jwt.NewWithClaims(jwt.SigningMethodES256, jwt.MapClaims{"sub": "carol", "exp": time.Now().Add(time.Hour).Unix()})
	tok.Header["kid"] = "k1"
	signed, err := tok.SignedString(priv)
	if err != nil {
		t.Fatal(err)
	}
	if id, valid := a.jwt.authenticate(signed); !valid || id.String() != "jwt:carol" {
		t.Errorf("jwks token: %v %v", id, valid)
	}
	// An unknown kid is refetched and refused.
	tok.Header["kid"] = "gone"
	signed, _ = tok.SignedString(priv)
	if _, valid := a.jwt.authenticate(signed); valid {
		t.Error("unknown kid accepted")
	}
}

func base64url(b []byte) string {
	return base64.RawURLEncoding.EncodeToString(b)
}

func TestMiddleware(t *testing.T) {
	secret := []byte("a sufficiently long secret")
	f := filepath.Join(t.TempDir(), "key")
	if err := os.WriteFile(f, secret, 0600); err != nil {
		t.Fatal(err)
	}
	a, err := New(Config{JWTKeyFile: f, ReadOnly: []string{"jwt:ro"}})
	if err != nil {
		t.Fatal(err)
	}
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	nodes := map[string]bool{"10.0.0.2": true}
	h := a.Wrap(next, func(ip net.IP) bool { return ip.IsLoopback() || nodes[ip.String()] })

	claims := func(sub string) jwt.MapClaims {
		return jwt.MapClaims{"sub": sub, "exp": time.Now().Add(time.Hour).Unix()}
	}
	for _, tc := range []struct {
		name   string
		remote string
		method string
		token  string
		want   int
	}{
		{"peer is exempt", "10.0.0.2:1", http.MethodPost, "", 200},
		{"loopback is exempt", "127.0.0.1:1", http.MethodPost, "", 200},
		{"no credentials", "10.9.9.9:1", http.MethodGet, "", 401},
		{"bad token", "10.9.9.9:1", http.MethodGet, tokenHS(t, []byte("wrong"), claims("x")), 401},
		{"full can write", "10.9.9.9:1", http.MethodPost, tokenHS(t, secret, claims("admin")), 200},
		{"ro can read", "10.9.9.9:1", http.MethodGet, tokenHS(t, secret, claims("ro")), 200},
		{"ro cannot write", "10.9.9.9:1", http.MethodPost, tokenHS(t, secret, claims("ro")), 403},
	} {
		r := httptest.NewRequest(tc.method, "http://x/status", nil)
		r.RemoteAddr = tc.remote
		if tc.token != "" {
			r.Header.Set("Authorization", "Bearer "+tc.token)
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != tc.want {
			t.Errorf("%s: got %d, want %d", tc.name, w.Code, tc.want)
		}
	}
}

func TestMiddlewareChallenges(t *testing.T) {
	secret := []byte("a sufficiently long secret")
	f := filepath.Join(t.TempDir(), "key")
	_ = os.WriteFile(f, secret, 0600)
	a, err := New(Config{JWTKeyFile: f, LDAPURL: "ldap://127.0.0.1:1", LDAPDNTemplate: "cn=%s", MTLS: true})
	if err != nil {
		t.Fatal(err)
	}
	h := a.Wrap(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}), nil)
	r := httptest.NewRequest(http.MethodGet, "http://x/", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("want 401, got %d", w.Code)
	}
	got := w.Header().Values("WWW-Authenticate")
	if len(got) != 2 { // Bearer + Basic; Negotiate is not configured
		t.Errorf("WWW-Authenticate: %v", got)
	}
}

func TestMiddlewareMTLS(t *testing.T) {
	a := &Auth{mtls: true, ro: map[string]bool{}}
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	h := a.Wrap(next, nil)

	r := httptest.NewRequest(http.MethodGet, "https://x/", nil)
	r.TLS = &tls.ConnectionState{VerifiedChains: [][]*x509.Certificate{{{Subject: pkix.Name{CommonName: "monitor"}}}}}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Errorf("verified client cert: got %d", w.Code)
	}

	r = httptest.NewRequest(http.MethodGet, "https://x/", nil) // no cert
	w = httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusUnauthorized {
		t.Errorf("no cert: got %d, want 401", w.Code)
	}
}

func TestEscapeDNValue(t *testing.T) {
	for in, want := range map[string]string{
		"alice":       "alice",
		"a,b":         `a\,b`,
		" a":          `\ a`,
		"a ":          `a\ `,
		"#hash":       `\#hash`,
		`quo"te`:      `quo\"te`,
		`back\slash`:  `back\\slash`,
		"<tag>;plus+": `\<tag\>\;plus\+`,
	} {
		if got := escapeDNValue(in); got != want {
			t.Errorf("escapeDNValue(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestNewLDAPRequiresACompleteConfig(t *testing.T) {
	if _, err := New(Config{LDAPURL: "ldap://127.0.0.1"}); err == nil {
		t.Error("no template and no search-bind: want an error")
	}
	if _, err := New(Config{LDAPURL: "ldap://127.0.0.1", LDAPDNTemplate: "cn=%s"}); err != nil {
		t.Errorf("dn template alone: %v", err)
	}
}

func TestParseKey(t *testing.T) {
	if _, err := parseKey([]byte("short")); err == nil {
		t.Error("short HMAC secret: want an error")
	}
	if _, err := parseKey([]byte("-----BEGIN NONSENSE-----\nAAAA\n-----END NONSENSE-----")); err == nil {
		t.Error("unreadable PEM: want an error")
	}
	_, pub := rsaKeys(t)
	if key, err := parseKey(pub); err != nil {
		t.Fatalf("PEM public key: %v", err)
	} else if _, ok := key.(*rsa.PublicKey); !ok {
		t.Errorf("key type %T", key)
	}
}
