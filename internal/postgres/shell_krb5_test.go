package postgres

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/go-krb5/krb5/client"
	"github.com/go-krb5/krb5/credentials"
	"github.com/go-krb5/krb5/iana/nametype"
	"github.com/go-krb5/krb5/messages"
	"github.com/go-krb5/krb5/types"
)

// The credential cache must read back as the library reads its own files:
// the service ticket for the Postgres service, as the user's own entry.
func TestWriteCCacheReadsBack(t *testing.T) {
	imp := client.Impersonation{
		Ticket: messages.Ticket{
			TktVNO:  5,
			Realm:   "EXAMPLE.COM",
			SName:   types.NewPrincipalName(nametype.KRB_NT_SRV_INST, "postgres/db.example.com"),
			EncPart: types.EncryptedData{EType: 18, KVNO: 1, Cipher: []byte("cipher")},
		},
		SessionKey: types.EncryptionKey{KeyType: 18, KeyValue: make([]byte, 32)},
		CName:      types.NewPrincipalName(nametype.KRB_NT_PRINCIPAL, "alice"),
		CRealm:     "EXAMPLE.COM",
		EndTime:    time.Now().Add(time.Hour),
	}
	path, cleanup, err := writeCCache(imp)
	if err != nil {
		t.Fatalf("writeCCache: %v", err)
	}
	defer cleanup()

	if !strings.HasPrefix(path, "FILE:") {
		t.Fatalf("path %q is not a FILE: name", path)
	}
	if info, err := os.Stat(strings.TrimPrefix(path, "FILE:")); err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("ccache must be 0600: %v %v", info, err)
	}

	cc, err := credentials.LoadCCache(strings.TrimPrefix(path, "FILE:"))
	if err != nil {
		t.Fatalf("LoadCCache: %v", err)
	}
	if got := cc.GetClientPrincipalName().PrincipalNameString(); got != "alice" {
		t.Errorf("default principal %q, want alice", got)
	}
	if _, ok := cc.GetEntry(types.NewPrincipalName(nametype.KRB_NT_SRV_INST, "postgres/db.example.com")); !ok {
		t.Error("the Postgres service ticket is not in the cache")
	}
}
