package auth

import (
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// jwtAuth validates Bearer tokens against a static key file or a JWKS URL.
type jwtAuth struct {
	key      any // static key: []byte (HMAC), *rsa.PublicKey, *ecdsa.PublicKey, ed25519.PublicKey
	jwks     string
	issuer   string
	audience string

	mu        sync.Mutex
	keys      map[string]any // kid -> public key
	fetchedAt time.Time
	hc        *http.Client
}

func newJWT(cfg Config) (*jwtAuth, error) {
	a := &jwtAuth{
		jwks:     cfg.JWTJWKS,
		issuer:   cfg.JWTIssuer,
		audience: cfg.JWTAudience,
		keys:     make(map[string]any),
		hc:       &http.Client{Timeout: 10 * time.Second},
	}
	if cfg.JWTKeyFile != "" {
		b, err := os.ReadFile(cfg.JWTKeyFile)
		if err != nil {
			return nil, err
		}
		key, err := parseKey([]byte(strings.TrimSpace(string(b))))
		if err != nil {
			return nil, fmt.Errorf("%s: %w", cfg.JWTKeyFile, err)
		}
		a.key = key
	}
	if a.jwks != "" {
		if err := a.fetchJWKS(); err != nil {
			return nil, err
		}
	}
	return a, nil
}

// authenticate validates the token and returns the sub identity.
func (a *jwtAuth) authenticate(token string) (Identity, bool) {
	opts := []jwt.ParserOption{jwt.WithValidMethods([]string{
		"HS256", "HS384", "HS512",
		"RS256", "RS384", "RS512",
		"ES256", "ES384", "ES512",
		"EdDSA",
	})}
	if a.issuer != "" {
		opts = append(opts, jwt.WithIssuer(a.issuer))
	}
	if a.audience != "" {
		opts = append(opts, jwt.WithAudience(a.audience))
	}
	claims := jwt.MapClaims{}
	t, err := jwt.ParseWithClaims(token, claims, func(t *jwt.Token) (any, error) {
		return a.keyFor(t)
	}, opts...)
	if err != nil || !t.Valid {
		return Identity{}, false
	}
	sub, _ := claims["sub"].(string)
	if sub == "" {
		return Identity{}, false
	}
	return Identity{"jwt", sub}, true
}

// keyFor resolves the signing key of the token, checking the algorithm
// against the key type, so a token cannot downgrade to another family.
func (a *jwtAuth) keyFor(t *jwt.Token) (any, error) {
	key, err := a.lookupKey(t)
	if err != nil {
		return nil, err
	}
	alg := t.Method.Alg()
	switch key.(type) {
	case []byte:
		if !strings.HasPrefix(alg, "HS") {
			return nil, fmt.Errorf("alg %s does not fit an HMAC key", alg)
		}
	case *rsa.PublicKey:
		if !strings.HasPrefix(alg, "RS") {
			return nil, fmt.Errorf("alg %s does not fit an RSA key", alg)
		}
	case *ecdsa.PublicKey:
		if !strings.HasPrefix(alg, "ES") {
			return nil, fmt.Errorf("alg %s does not fit an ECDSA key", alg)
		}
	case ed25519.PublicKey:
		if alg != "EdDSA" {
			return nil, fmt.Errorf("alg %s does not fit an Ed25519 key", alg)
		}
	}
	return key, nil
}

func (a *jwtAuth) lookupKey(t *jwt.Token) (any, error) {
	if a.key != nil {
		return a.key, nil
	}
	kid, _ := t.Header["kid"].(string)
	a.mu.Lock()
	defer a.mu.Unlock()
	if key, ok := a.keys[kid]; ok {
		return key, nil
	}
	// An unknown kid may mean the issuer rotated; refetch, throttled.
	if time.Since(a.fetchedAt) > 10*time.Second {
		if err := a.fetchJWKSLocked(); err != nil {
			return nil, err
		}
	}
	key, ok := a.keys[kid]
	if !ok {
		return nil, fmt.Errorf("unknown kid %q", kid)
	}
	return key, nil
}

func (a *jwtAuth) fetchJWKS() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.fetchJWKSLocked()
}

type jwksDoc struct {
	Keys []struct {
		Kty string `json:"kty"`
		Kid string `json:"kid"`
		N   string `json:"n"`
		E   string `json:"e"`
		Crv string `json:"crv"`
		X   string `json:"x"`
		Y   string `json:"y"`
	} `json:"keys"`
}

func (a *jwtAuth) fetchJWKSLocked() error {
	resp, err := a.hc.Get(a.jwks)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("jwks: %s", resp.Status)
	}
	b, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return err
	}
	var doc jwksDoc
	if err := json.Unmarshal(b, &doc); err != nil {
		return fmt.Errorf("jwks: %w", err)
	}
	keys := make(map[string]any, len(doc.Keys))
	for _, k := range doc.Keys {
		key, err := jwkKey(k.Kty, k.Crv, k.N, k.E, k.X, k.Y)
		if err != nil {
			continue // a key type we do not serve
		}
		keys[k.Kid] = key
	}
	if len(keys) == 0 {
		return errors.New("jwks: no usable keys")
	}
	a.keys = keys
	a.fetchedAt = time.Now()
	return nil
}

func jwkKey(kty, crv, ns, es, xs, ys string) (any, error) {
	decode := func(s string) ([]byte, error) { return base64.RawURLEncoding.DecodeString(s) }
	switch kty {
	case "RSA":
		nb, err := decode(ns)
		if err != nil {
			return nil, err
		}
		eb, err := decode(es)
		if err != nil {
			return nil, err
		}
		e := 0
		for _, b := range eb {
			e = e<<8 | int(b)
		}
		return &rsa.PublicKey{N: new(big.Int).SetBytes(nb), E: e}, nil
	case "EC":
		xb, err := decode(xs)
		if err != nil {
			return nil, err
		}
		yb, err := decode(ys)
		if err != nil {
			return nil, err
		}
		var curve elliptic.Curve
		switch crv {
		case "P-256":
			curve = elliptic.P256()
		case "P-384":
			curve = elliptic.P384()
		case "P-521":
			curve = elliptic.P521()
		default:
			return nil, fmt.Errorf("jwks: unsupported curve %q", crv)
		}
		return &ecdsa.PublicKey{Curve: curve, X: new(big.Int).SetBytes(xb), Y: new(big.Int).SetBytes(yb)}, nil
	case "OKP":
		kb, err := decode(xs)
		if err != nil {
			return nil, err
		}
		return ed25519.PublicKey(kb), nil
	}
	return nil, fmt.Errorf("jwks: unsupported kty %q", kty)
}

// parseKey reads a PEM public key or takes the content as an HMAC secret.
func parseKey(b []byte) (any, error) {
	if block, _ := pem.Decode(b); block != nil {
		if key, err := x509.ParsePKIXPublicKey(block.Bytes); err == nil {
			switch key.(type) {
			case *rsa.PublicKey, *ecdsa.PublicKey, ed25519.PublicKey:
				return key, nil
			}
			return nil, fmt.Errorf("unsupported public key type %T", key)
		}
		if key, err := x509.ParsePKCS1PublicKey(block.Bytes); err == nil {
			return key, nil
		}
		return nil, errors.New("unreadable public key")
	}
	if len(b) < 16 {
		return nil, errors.New("an HMAC secret shorter than 16 bytes is too weak")
	}
	return b, nil
}
