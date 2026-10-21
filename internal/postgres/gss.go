package postgres

import (
	"fmt"
	"net"
	"strings"

	"github.com/go-krb5/krb5/client"
	"github.com/go-krb5/krb5/credentials"
	"github.com/go-krb5/krb5/keytab"
	"github.com/go-krb5/krb5/spnego"
	"github.com/jackc/pgx/v5/pgconn"
)

// registerGSS makes pgx authenticate with Kerberos where pg_hba asks for it.
// pgconn calls the provider on every GSS connection, and building one
// re-reads the credentials, so a refreshed keytab or credential cache takes
// effect without a restart of the agent.
func registerGSS() {
	p := gssProvider{keytab: rc.PGKrb5Keytab, principal: rc.PGKrb5Principal, config: rc.PGKrb5Config}
	pgconn.RegisterGSSProvider(p.new)
}

// gssProvider builds the Kerberos client of one connection, from
// --pg-krb5-keytab and --pg-krb5-principal or from the credential cache.
type gssProvider struct {
	keytab    string
	principal string
	config    string // krb5.conf; $KRB5_CONFIG or /etc/krb5.conf when empty
}

func (p gssProvider) new() (pgconn.GSS, error) {
	cfg, err := krb5Config(p.config)
	if err != nil {
		return nil, err
	}
	var cl *client.Client
	if p.keytab == "" {
		ccache, err := credentials.LoadCCache(ccachePath())
		if err != nil {
			return nil, err
		}
		cl, err = client.NewFromCCache(ccache, cfg, client.DisablePAFXFAST(true))
		if err != nil {
			return nil, err
		}
	} else {
		user, realm, ok := strings.Cut(p.principal, "@")
		if !ok {
			user, realm = p.principal, cfg.LibDefaults.DefaultRealm
		}
		kt, err := keytab.Load(p.keytab)
		if err != nil {
			return nil, err
		}
		cl = client.NewWithKeytab(user, realm, kt, cfg, client.DisablePAFXFAST(true))
		if err := cl.Login(); err != nil {
			return nil, err
		}
	}
	return &gssClient{cl}, nil
}

// gssClient implements pgconn.GSS over go-krb5.
type gssClient struct {
	cl *client.Client
}

// GetInitToken implements pgconn.GSS.
func (g *gssClient) GetInitToken(host, service string) ([]byte, error) {
	if g.cl.Config.LibDefaults.DNSCanonicalizeHostname {
		// The KDC knows a host by one principal, not by its aliases.
		var err error
		host, err = canonicalizeHostname(host)
		if err != nil {
			return nil, err
		}
	}
	return g.GetInitTokenFromSPN(service + "/" + host)
}

// GetInitTokenFromSPN implements pgconn.GSS.
func (g *gssClient) GetInitTokenFromSPN(spn string) ([]byte, error) {
	t, err := spnego.SPNEGOClient(g.cl, spn).InitSecContext()
	if err != nil {
		return nil, fmt.Errorf("kerberos InitSecContext: %w", err)
	}
	b, err := t.Marshal()
	if err != nil {
		return nil, fmt.Errorf("kerberos token: %w", err)
	}
	return b, nil
}

// Continue implements pgconn.GSS: the acceptor's reply must report the
// context completed.
func (g *gssClient) Continue(inToken []byte) (bool, []byte, error) {
	t := &spnego.SPNEGOToken{}
	if err := t.Unmarshal(inToken); err != nil {
		return true, nil, fmt.Errorf("kerberos token: %w", err)
	}
	if state := t.NegTokenResp.State(); state != spnego.NegStateAcceptCompleted {
		return true, nil, fmt.Errorf("kerberos: want state accept-completed, got %d", state)
	}
	return true, nil, nil
}

// canonicalizeHostname resolves host to its canonical name, for the SPN of
// GetInitToken.
func canonicalizeHostname(host string) (string, error) {
	name, err := net.LookupCNAME(host)
	if err != nil {
		return "", err
	}
	if name = strings.TrimSuffix(name, "."); name != "" {
		return name, nil
	}
	return host, nil
}
