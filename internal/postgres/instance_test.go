package postgres

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
)

func TestConfigureEscapesValues(t *testing.T) {
	dir := t.TempDir()
	i := &Instance{DataDir: dir, LogDir: dir}
	i.SetConfig(Config{Settings: map[string]string{
		"work_mem":  `C:\temp`,
		"datestyle": `it's`,
	}})
	if err := i.configure("", false); err != nil {
		t.Fatalf("configure: %v", err)
	}
	conf, err := os.ReadFile(filepath.Join(dir, "postgresql.conf"))
	if err != nil {
		t.Fatalf("read postgresql.conf: %v", err)
	}
	for _, want := range []string{
		`work_mem = 'C:\\temp'`, // a lone backslash would read as an escape
		`datestyle = 'it''s'`,   // a lone quote would end the string
	} {
		if !strings.Contains(string(conf), want+"\n") {
			t.Errorf("postgresql.conf has no %q:\n%s", want, conf)
		}
	}
}

func TestFollowsComparesParsedConninfo(t *testing.T) {
	pw := filepath.Join(t.TempDir(), "pw")
	if err := os.WriteFile(pw, []byte("secret\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	i := &Instance{Port: 5433, PasswordFile: pw}

	asWritten, err := i.conninfo("pg1")
	if err != nil {
		t.Fatalf("conninfo: %v", err)
	}

	for _, tc := range []struct {
		conninfo string
		want     bool
	}{
		{asWritten, true},
		{"host=pg1 port=5433 user=tembo_repl password=secret", true},   // re-serialized by the server
		{"user=tembo_repl password=secret host=pg1 port=5433", true},   // fields reordered
		{"host=pg2 port=5433 user=tembo_repl password=secret", false},  // another primary
		{"host=pg1 port=5434 user=tembo_repl password=secret", false},  // another port
		{"host=pg1 port=5433 user=tembo_repl password=rotated", false}, // another password
		{"", false}, // primary_conninfo not set
	} {
		if got := i.Follows("pg1", tc.conninfo); got != tc.want {
			t.Errorf("Follows(%q) = %v, want %v", tc.conninfo, got, tc.want)
		}
	}
}

func TestConfigureWritesSlotAndApplicationName(t *testing.T) {
	dir := t.TempDir()
	pw := filepath.Join(t.TempDir(), "pw")
	if err := os.WriteFile(pw, []byte("secret\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	i := &Instance{ID: "b", DataDir: dir, LogDir: dir, Port: 5433, PasswordFile: pw}
	if err := i.configure("pg1", true); err != nil {
		t.Fatalf("configure: %v", err)
	}
	conf, err := os.ReadFile(filepath.Join(dir, "postgresql.conf"))
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		`primary_slot_name = 'tembo_b'`,
		`application_name=b`,
	} {
		if !strings.Contains(string(conf), want) {
			t.Errorf("postgresql.conf has no %q:\n%s", want, conf)
		}
	}

	// A cascaded or foreign upstream keeps no slot of this node.
	if err := i.configure("pg2", false); err != nil {
		t.Fatalf("configure: %v", err)
	}
	conf, err = os.ReadFile(filepath.Join(dir, "postgresql.conf"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(conf), "primary_slot_name") {
		t.Errorf("postgresql.conf names a slot a cascaded upstream does not have:\n%s", conf)
	}
}

func TestHBAWritesNodesAndExtraLines(t *testing.T) {
	i := &Instance{Hosts: []string{"10.0.0.1"}, ExtraHBA: []string{
		"host all all 192.168.0.0/24 scram-sha-256",
		"hostssl db1 app 10.0.0.9 scram-sha-256",
	}}
	hba := string(i.hba())
	for _, want := range []string{
		"local all all peer\n",
		"local replication postgres peer\n",        // a local pg_basebackup, GET /backup
		"host all all 10.0.0.1/32 scram-sha-256\n", // a node
		"host replication tembo_repl 10.0.0.1/32 scram-sha-256\n",
		"host all all 192.168.0.0/24 scram-sha-256\n", // a setting's line, verbatim
		"hostssl db1 app 10.0.0.9 scram-sha-256\n",
	} {
		if !strings.Contains(hba, want) {
			t.Errorf("pg_hba.conf has no %q:\n%s", want, hba)
		}
	}
	if strings.Contains(hba, "host replication all") {
		t.Errorf("pg_hba.conf opens replication to every role:\n%s", hba)
	}
	// The managed lines come first: the setting cannot shadow them.
	if strings.Index(hba, "host all all 10.0.0.1/32") > strings.Index(hba, "host all all 192.168.0.0/24") {
		t.Errorf("pg_hba.conf puts the setting's lines before the nodes':\n%s", hba)
	}
}

func TestHBAAuthMethods(t *testing.T) {
	// ldap with its verbatim options
	i := &Instance{Hosts: []string{"10.0.0.1"}, HostAuth: "ldap", HostAuthOpts: `ldapbasedn="dc=tembo,dc=test"`}
	hba := string(i.hba())
	for _, want := range []string{
		`host all all 10.0.0.1/32 ldap ldapbasedn="dc=tembo,dc=test"` + "\n",
		`host replication tembo_repl 10.0.0.1/32 ldap ldapbasedn="dc=tembo,dc=test"` + "\n",
	} {
		if !strings.Contains(hba, want) {
			t.Errorf("pg_hba.conf has no %q:\n%s", want, hba)
		}
	}

	// gss, without options: ordinary connections by Kerberos, replication by
	// tembo_repl's password, on a line of its own
	i = &Instance{Hosts: []string{"10.0.0.1"}, HostAuth: "gss"}
	hba = string(i.hba())
	for _, want := range []string{
		"host all all 10.0.0.1/32 gss\n",
		"host replication tembo_repl 10.0.0.1/32 scram-sha-256\n",
	} {
		if !strings.Contains(hba, want) {
			t.Errorf("pg_hba.conf has no %q:\n%s", want, hba)
		}
	}

	// gss options go to the Kerberos line only; the replication line takes none
	i = &Instance{Hosts: []string{"10.0.0.1"}, HostAuth: "gss", HostAuthOpts: "include_realm=0 map=tembo"}
	hba = string(i.hba())
	for _, want := range []string{
		"host all all 10.0.0.1/32 gss include_realm=0 map=tembo\n",
		"host replication tembo_repl 10.0.0.1/32 scram-sha-256\n",
	} {
		if !strings.Contains(hba, want) {
			t.Errorf("pg_hba.conf has no %q:\n%s", want, hba)
		}
	}

	// a TCP line for the agent appears when it does not use the socket; its
	// method follows the nodes'
	for _, tc := range []struct {
		local, host, want string
	}{
		{"password", "scram-sha-256", "host all postgres 127.0.0.1/32 scram-sha-256\n"},
		{"password", "ldap", "host all postgres 127.0.0.1/32 ldap\n"},
		{"gss", "gss", "host all postgres 127.0.0.1/32 gss\n"},
	} {
		i = &Instance{LocalAuth: tc.local, HostAuth: tc.host}
		if hba = string(i.hba()); !strings.Contains(hba, tc.want) {
			t.Errorf("local %s, host %s: pg_hba.conf has no %q:\n%s", tc.local, tc.host, tc.want, hba)
		}
	}
	i = &Instance{}
	if hba = string(i.hba()); strings.Contains(hba, "127.0.0.1/32") {
		t.Errorf("peer needs no TCP line for the agent:\n%s", hba)
	}
}

func TestConninfoGSSHasNoPassword(t *testing.T) {
	i := &Instance{ID: "b", Port: 5433, HostAuth: "gss"} // no PasswordFile at all
	ci, err := i.conninfo("pg1")
	if err != nil {
		t.Fatalf("conninfo: %v", err)
	}
	if strings.Contains(ci, "password") {
		t.Errorf("gss conninfo must not carry a password: %s", ci)
	}
	if strings.Contains(ci, "gssencmode") {
		t.Errorf("pgconn does not know gssencmode, it would land in the startup packet: %s", ci)
	}
	// ensureSlot parses the same string with pgx.
	cfg, err := pgx.ParseConfig(ci)
	if err != nil {
		t.Errorf("pgx cannot parse the gss conninfo: %v", err)
	}
	if _, bad := cfg.Config.RuntimeParams["gssencmode"]; bad {
		t.Error("gssencmode leaked into the runtime parameters")
	}
}

func TestShutdownLSN(t *testing.T) {
	const out = "pg_control version number:            1700\nDatabase cluster state:               %s\nLatest checkpoint location:           0/3000060\n"
	for state, want := range map[string]uint64{
		"shut down":             0x3000060,
		"shut down in recovery": 0,
		"in production":         0,
	} {
		if got := shutdownLSN(fmt.Sprintf(out, state)); got != want {
			t.Errorf("state %q: got %#x, want %#x", state, got, want)
		}
	}
	if got := shutdownLSN("Database cluster state: shut down\nLatest checkpoint location: 1/2\n"); got != 1<<32|2 {
		t.Errorf("got %#x, want the high word kept", got)
	}
}

func TestWipeDataKeepsDirectory(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "base", "1"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "PG_VERSION"), []byte("17\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := (&Instance{DataDir: dir}).wipeData(); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 0 {
		t.Fatalf("got %v, %v, want an empty directory", entries, err)
	}
}

// Under gss the primary_conninfo of a standby is the replication role's
// password connection; the superuser-capable connection keeps Kerberos.
func TestReplConninfoIsPasswordUnderGSS(t *testing.T) {
	i := &Instance{Port: 5433, HostAuth: "gss", Password: "secret"}
	conninfo, err := i.replConninfo("db1")
	if err != nil {
		t.Fatalf("replConninfo: %v", err)
	}
	if !strings.Contains(conninfo, "user=tembo_repl") || !strings.Contains(conninfo, "password='secret'") {
		t.Fatalf("replication conninfo under gss: %q", conninfo)
	}
	gssConn, err := i.conninfo("db1")
	if err != nil {
		t.Fatalf("conninfo: %v", err)
	}
	if strings.Contains(gssConn, "password=") {
		t.Fatalf("gss conninfo carries a password: %q", gssConn)
	}
}
