package auth

import (
	"net/http"

	"github.com/go-krb5/krb5/credentials"
	"github.com/go-krb5/krb5/keytab"
	"github.com/go-krb5/krb5/spnego"
	"github.com/go-krb5/x/identity"
)

// spnegoAuth accepts Kerberos tickets over the Negotiate scheme.
type spnegoAuth struct {
	kt *keytab.Keytab
}

func newSPNEGO(keytabPath, _ string) (*spnegoAuth, error) {
	kt, err := keytab.Load(keytabPath)
	if err != nil {
		return nil, err
	}
	return &spnegoAuth{kt: kt}, nil
}

// authenticate runs the SPNEGO exchange; on a failure the acceptor handler
// has already answered (401 with WWW-Authenticate: Negotiate) and ok is
// false.
func (a *spnegoAuth) authenticate(w http.ResponseWriter, r *http.Request) (Identity, bool) {
	var id Identity
	ok := false
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// The acceptor stores the credentials under the identity package's
		// context key, not spnego.CTXKey.
		creds, valid := identity.FromHTTPRequestContext(r).(*credentials.Credentials)
		if !valid {
			return
		}
		id = Identity{"krb5", creds.UserName() + "@" + creds.Domain()}
		ok = true
	})
	spnego.SPNEGOKRB5Authenticate(inner, a.kt).ServeHTTP(w, r)
	return id, ok
}
