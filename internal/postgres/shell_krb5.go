package postgres

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/go-krb5/krb5/client"
	"github.com/go-krb5/krb5/credentials"
	"github.com/go-krb5/krb5/iana/nametype"
	"github.com/go-krb5/krb5/keytab"
	"github.com/go-krb5/krb5/types"
)

// shellKerberos lets the shell log in as the Kerberos user who opened the
// session. The agent impersonates the user to the Postgres service (S4U2Self,
// then S4U2Proxy, which the KDC allows per principal), and psql gets the
// resulting service ticket in a credential cache of its own. No ticket-granting
// ticket of the user leaves the KDC, and Postgres's gss rules decide the rest.
type shellKerberos struct {
	cl    *client.Client
	realm string
	spn   string // service/host, the ticket's service: psql connects to host
	host  string
}

// newShellKerberos logs the agent in with its own principal, the one whose
// key is in the keytab, and records the Postgres service it impersonates to.
func newShellKerberos(keytabPath, principal, spn string) (*shellKerberos, error) {
	user, realm, ok := strings.Cut(principal, "@")
	if !ok || user == "" || realm == "" {
		return nil, fmt.Errorf("bad --shell-krb5-principal %q, want user@REALM", principal)
	}
	service, host, ok := strings.Cut(spn, "/")
	if !ok || service == "" || host == "" {
		return nil, fmt.Errorf("bad --shell-krb5-spn %q, want service/host, e.g. postgres/db.example.com", spn)
	}
	kt, err := keytab.Load(keytabPath)
	if err != nil {
		return nil, err
	}
	cfg, err := krb5Config("")
	if err != nil {
		return nil, err
	}
	cl := client.NewWithKeytab(user, realm, kt, cfg)
	if err := cl.Login(); err != nil {
		return nil, fmt.Errorf("agent principal %s: %w", principal, err)
	}
	return &shellKerberos{cl: cl, realm: realm, spn: spn, host: host}, nil
}

// ccache impersonates the Kerberos user (name user@realm) to the Postgres
// service and writes the service ticket to a credential cache of a fresh
// directory. cleanup removes the directory.
func (k *shellKerberos) ccache(name string) (path string, cleanup func(), err error) {
	user, realm, ok := strings.Cut(name, "@")
	if !ok || user == "" || realm == "" {
		return "", nil, fmt.Errorf("bad Kerberos identity %q", name)
	}
	princ := types.NewPrincipalName(nametype.KRB_NT_PRINCIPAL, user)
	imp, err := k.cl.Impersonate(princ, realm, k.spn)
	if err != nil {
		return "", nil, fmt.Errorf("impersonating %s to %s: %w", name, k.spn, err)
	}
	return writeCCache(imp)
}

// writeCCache stores the impersonated service ticket in a credential cache of
// a fresh directory. cleanup removes the directory.
func writeCCache(imp client.Impersonation) (path string, cleanup func(), err error) {
	tkt, err := imp.Ticket.Marshal()
	if err != nil {
		return "", nil, err
	}
	now := time.Now()
	cc := &credentials.CCache{
		Version:          4,
		DefaultPrincipal: credentials.NewPrincipal(imp.CName, imp.CRealm),
		Credentials: []*credentials.Credential{{
			Client:    credentials.NewPrincipal(imp.CName, imp.CRealm),
			Server:    credentials.NewPrincipal(imp.Ticket.SName, imp.Ticket.Realm),
			Key:       imp.SessionKey,
			AuthTime:  now,
			StartTime: now,
			EndTime:   imp.EndTime,
			RenewTill: imp.EndTime,
			Ticket:    tkt,
		}},
	}
	data, err := cc.Marshal()
	if err != nil {
		return "", nil, err
	}
	dir, err := os.MkdirTemp("", "tembo-shell-*")
	if err != nil {
		return "", nil, err
	}
	cleanup = func() { _ = os.RemoveAll(dir) }
	path = filepath.Join(dir, "ccache")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		cleanup()
		return "", nil, err
	}
	return "FILE:" + path, cleanup, nil
}
