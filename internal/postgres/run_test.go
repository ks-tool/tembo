package postgres

import (
	"os"
	"path/filepath"
	"testing"
)

func TestValidatePGAuth(t *testing.T) {
	dir := t.TempDir()
	keytab := filepath.Join(dir, "krb5.keytab")
	if err := os.WriteFile(keytab, []byte("not a real keytab"), 0o600); err != nil {
		t.Fatal(err)
	}
	// The credential cache must not leak in from the environment.
	t.Setenv("KRB5CCNAME", filepath.Join(dir, "missing-ccache"))

	old := rc
	t.Cleanup(func() { rc = old })

	for _, tc := range []struct {
		name         string
		local, host  string
		opts, keytab string
		wantErr      bool
	}{
		{"defaults", "peer", "scram-sha-256", "", "", false},
		{"bad local", "bogus", "scram-sha-256", "", "", true},
		{"bad host", "peer", "bogus", "", "", true},
		{"ldap needs opts", "peer", "ldap", "", "", true},
		{"ldap with opts", "peer", "ldap", "ldapbasedn=dc=x", "", false},
		{"gss needs credentials", "peer", "gss", "", "", true},
		{"gss needs a keytab that exists", "peer", "gss", "", filepath.Join(dir, "missing"), true},
		{"gss with keytab", "peer", "gss", "", keytab, false},
		{"password excludes gss", "password", "gss", "", keytab, true},
		{"gss excludes ldap", "gss", "ldap", "ldapbasedn=dc=x", keytab, true},
		{"gss with gss", "gss", "gss", "", keytab, false},
		{"password with ldap", "password", "ldap", "ldapbasedn=dc=x", "", false},
	} {
		rc.PGLocalAuth, rc.PGHostAuth, rc.PGHostAuthOpts, rc.PGKrb5Keytab = tc.local, tc.host, tc.opts, tc.keytab
		if err := validatePGAuth(); (err != nil) != tc.wantErr {
			t.Errorf("%s: got %v, wantErr %v", tc.name, err, tc.wantErr)
		}
	}
}

func TestGSSContinue(t *testing.T) {
	g := &gssClient{}
	if _, _, err := g.Continue([]byte("not a token")); err == nil {
		t.Error("a broken token must fail")
	}
}
