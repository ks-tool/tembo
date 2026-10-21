// Package auth authenticates the clients of the agent's API: JWT (Bearer),
// Kerberos (SPNEGO Negotiate), LDAP (Basic) and mTLS, and splits them into
// read-only and full access. Cluster nodes and the loopback are exempt — the
// address allowlist already covers them; see run.go.
package auth

import (
	"fmt"
	"net"
	"net/http"
	"strings"
)

// Identity is an authenticated client: the mechanism and the name it gave.
type Identity struct {
	Mechanism string // jwt, krb5, ldap, cert
	Name      string
}

func (id Identity) String() string { return id.Mechanism + ":" + id.Name }

// Config describes the enabled mechanisms; all empty disables authentication.
type Config struct {
	JWTKeyFile  string // PEM public key (RS*/ES*) or a plain HMAC secret (HS*)
	JWTJWKS     string // JWKS URL; refetched when a token names an unknown kid
	JWTIssuer   string
	JWTAudience string

	Krb5Keytab string // enables SPNEGO (Negotiate)
	Krb5Config string // krb5.conf; default library paths when empty

	LDAPURL              string // ldap:// or ldaps://; enables Basic auth against LDAP
	LDAPDNTemplate       string // direct bind: "cn=%s,ou=users,dc=example,dc=com"
	LDAPBindDN           string // or search-bind: bind with this account,
	LDAPBindPassword     string // with this password (wins over the file),
	LDAPBindPasswordFile string // or the file that holds it
	LDAPBaseDN           string // search the user under this base
	LDAPFilter           string // with this filter, e.g. "(uid=%s)"

	MTLS bool // require a verified client certificate (the listener verifies it)

	ReadOnly []string // identities with read-only (GET) access; the rest are full
}

// Auth authenticates requests against the enabled mechanisms.
type Auth struct {
	jwt    *jwtAuth
	spnego *spnegoAuth
	ldap   *ldapAuth
	mtls   bool
	ro     map[string]bool
}

// New builds the authenticators of the configuration.
func New(cfg Config) (*Auth, error) {
	a := &Auth{mtls: cfg.MTLS, ro: make(map[string]bool, len(cfg.ReadOnly))}
	for _, id := range cfg.ReadOnly {
		a.ro[strings.TrimSpace(id)] = true
	}
	if cfg.JWTKeyFile != "" || cfg.JWTJWKS != "" {
		j, err := newJWT(cfg)
		if err != nil {
			return nil, fmt.Errorf("jwt: %w", err)
		}
		a.jwt = j
	}
	if cfg.Krb5Keytab != "" {
		s, err := newSPNEGO(cfg.Krb5Keytab, cfg.Krb5Config)
		if err != nil {
			return nil, fmt.Errorf("kerberos: %w", err)
		}
		a.spnego = s
	}
	if cfg.LDAPURL != "" {
		l, err := newLDAP(cfg)
		if err != nil {
			return nil, fmt.Errorf("ldap: %w", err)
		}
		a.ldap = l
	}
	if a.jwt == nil && a.spnego == nil && a.ldap == nil && !a.mtls {
		return nil, nil
	}
	return a, nil
}

// Wrap protects next with authentication. exempt reports whether a remote
// address is a cluster node or the loopback — those pass unauthenticated.
func (a *Auth) Wrap(next http.Handler, exempt func(net.IP) bool) http.Handler {
	if a == nil {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		host, _, err := net.SplitHostPort(r.RemoteAddr)
		if err != nil {
			host = r.RemoteAddr
		}
		if exempt != nil && exempt(net.ParseIP(host)) {
			next.ServeHTTP(w, r)
			return
		}
		id, ok := a.authenticate(w, r)
		if !ok {
			return
		}
		if a.ro[id.String()] && r.Method != http.MethodGet && r.Method != http.MethodHead {
			http.Error(w, id.String()+" is read-only", http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// authenticate runs the mechanism the request carries credentials for; on a
// failure the response is already written and ok is false.
func (a *Auth) authenticate(w http.ResponseWriter, r *http.Request) (id Identity, ok bool) {
	if a.mtls && r.TLS != nil && len(r.TLS.VerifiedChains) > 0 {
		cert := r.TLS.VerifiedChains[0][0]
		name := cert.Subject.CommonName
		if name == "" && len(cert.DNSNames) > 0 {
			name = cert.DNSNames[0]
		}
		return Identity{"cert", name}, true
	}
	h := r.Header.Get("Authorization")
	switch {
	case strings.HasPrefix(h, "Bearer ") && a.jwt != nil:
		if id, ok = a.jwt.authenticate(strings.TrimPrefix(h, "Bearer ")); ok {
			return id, true
		}
	case strings.HasPrefix(h, "Negotiate ") && a.spnego != nil:
		return a.spnego.authenticate(w, r)
	case strings.HasPrefix(h, "Basic ") && a.ldap != nil:
		user, password, _ := r.BasicAuth()
		if id, ok = a.ldap.authenticate(user, password); ok {
			return id, true
		}
	}
	a.challenge(w)
	return Identity{}, false
}

// challenge answers 401, naming the enabled mechanisms.
func (a *Auth) challenge(w http.ResponseWriter) {
	var v []string
	if a.spnego != nil {
		v = append(v, "Negotiate")
	}
	if a.jwt != nil {
		v = append(v, "Bearer")
	}
	if a.ldap != nil {
		v = append(v, `Basic realm="tembo", charset="UTF-8"`)
	}
	for _, h := range v {
		w.Header().Add("WWW-Authenticate", h)
	}
	http.Error(w, "authentication required", http.StatusUnauthorized)
}
