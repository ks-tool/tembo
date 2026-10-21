package auth

import (
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"github.com/go-ldap/ldap/v3"
)

// ldapAuth verifies Basic credentials with an LDAP bind: directly against a
// DN template, or by finding the user's DN with a service account first.
type ldapAuth struct {
	url      string
	template string // DN template with one %s, direct bind
	bindDN   string // search-bind: service account
	bindPass string
	baseDN   string
	filter   string // search filter with one %s
}

func newLDAP(cfg Config) (*ldapAuth, error) {
	a := &ldapAuth{url: cfg.LDAPURL, template: cfg.LDAPDNTemplate}
	if a.template == "" {
		if cfg.LDAPBindDN == "" || (cfg.LDAPBindPassword == "" && cfg.LDAPBindPasswordFile == "") || cfg.LDAPBaseDN == "" || cfg.LDAPFilter == "" {
			return nil, errors.New("either --auth-ldap-dn-template or all of --auth-ldap-bind-dn, --auth-ldap-bind-password(-file), --auth-ldap-base-dn and --auth-ldap-filter are required")
		}
		password := cfg.LDAPBindPassword
		if password == "" {
			b, err := os.ReadFile(cfg.LDAPBindPasswordFile)
			if err != nil {
				return nil, err
			}
			password = strings.TrimRight(string(b), "\r\n")
		}
		a.bindDN = cfg.LDAPBindDN
		a.bindPass = password
		a.baseDN = cfg.LDAPBaseDN
		a.filter = cfg.LDAPFilter
	}
	return a, nil
}

// authenticate binds as the user; a bound DN is the identity.
func (a *ldapAuth) authenticate(user, password string) (Identity, bool) {
	if user == "" || password == "" {
		return Identity{}, false
	}
	dn := ""
	if a.template != "" {
		dn = strings.Replace(a.template, "%s", escapeDNValue(user), 1)
	} else {
		found, err := a.findDN(user)
		if err != nil {
			return Identity{}, false
		}
		dn = found
	}
	conn, err := ldap.DialURL(a.url, ldap.DialWithDialer(&net.Dialer{Timeout: 5 * time.Second}))
	if err != nil {
		return Identity{}, false
	}
	defer func() { _ = conn.Close() }()
	if err := conn.Bind(dn, password); err != nil {
		return Identity{}, false
	}
	return Identity{"ldap", dn}, true
}

// findDN resolves the user's DN with the service account.
func (a *ldapAuth) findDN(user string) (string, error) {
	conn, err := ldap.DialURL(a.url, ldap.DialWithDialer(&net.Dialer{Timeout: 5 * time.Second}))
	if err != nil {
		return "", err
	}
	defer func() { _ = conn.Close() }()
	if err := conn.Bind(a.bindDN, a.bindPass); err != nil {
		return "", fmt.Errorf("service bind: %w", err)
	}
	filter := strings.Replace(a.filter, "%s", ldap.EscapeFilter(user), 1)
	res, err := conn.Search(ldap.NewSearchRequest(a.baseDN, ldap.ScopeWholeSubtree,
		ldap.NeverDerefAliases, 2, 5, false, filter, []string{"dn"}, nil))
	if err != nil {
		return "", err
	}
	if len(res.Entries) != 1 {
		return "", fmt.Errorf("%d entries match %s", len(res.Entries), filter)
	}
	return res.Entries[0].DN, nil
}

// escapeDNValue escapes a value for a DN, RFC 4514 §2.4.
func escapeDNValue(s string) string {
	var b strings.Builder
	for i, r := range s {
		switch {
		case strings.ContainsRune(`,+"\<>;=`, r):
			b.WriteByte('\\')
			b.WriteRune(r)
		case i == 0 && (r == '#' || r == ' '):
			b.WriteByte('\\')
			b.WriteRune(r)
		case i == len(s)-1 && r == ' ':
			b.WriteString(`\ `)
		default:
			b.WriteRune(r)
		}
	}
	return b.String()
}
