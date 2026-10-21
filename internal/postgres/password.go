package postgres

import (
	"crypto/rand"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/ks-tool/tembo/internal/agent"
	"github.com/ks-tool/tembo/internal/store"

	"github.com/spf13/cobra"
)

// passwordHandlers serves PUT /password: rotate the Postgres password of the
// whole cluster. The new value commits through raft; every agent starts
// connecting with it on its next reconcile tick, and the primary alters the
// roles then — standbys receive them by WAL replay. The stored value is
// never handed back by any GET. With the OpenBao integration the answer is
// 403: the secret at --bao-password-path is the source of truth.
func passwordHandlers(mux *http.ServeMux, srv *agent.Server) {
	mux.HandleFunc("PUT /password", func(w http.ResponseWriter, r *http.Request) {
		if rc.BaoPasswordPath != "" {
			http.Error(w, "managed by OpenBao: change the secret at --bao-password-path", http.StatusForbidden)
			return
		}
		body, err := io.ReadAll(io.LimitReader(r.Body, 1<<10))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		// A trailing newline ends a password file; it is not the password.
		pw := strings.TrimSuffix(strings.TrimSuffix(string(body), "\n"), "\r")
		if pw == "" {
			http.Error(w, "the request body is the new password", http.StatusBadRequest)
			return
		}
		srv.Propose(w, r, store.Command{Op: "set", Key: agent.KeyPassword, Value: pw})
	})
}

var rotatePasswordCmd = &cobra.Command{
	Use:   "rotate-password [password]",
	Short: "Rotate the Postgres password of the whole cluster",
	Long: `Rotate the password of the postgres and tembo_repl roles on every node.
Without an argument a password is generated and printed — the one time it is
shown; afterwards it lives in the cluster's store, which no GET returns.

The rotation commits through raft at once. The primary alters the roles on
its next reconcile tick, the standbys receive them by WAL replay, and every
agent starts connecting with the new password within a second or two —
expect a brief replication blip while they switch. A node with --standby-of
is not rotated: its password belongs to the foreign upstream.`,
	Args: cobra.MaximumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		pw := ""
		if len(args) > 0 {
			pw = args[0]
		} else {
			var err error
			if pw, err = generatePassword(24); err != nil {
				return err
			}
			fmt.Println("new password:", pw)
		}
		return call(cmd.Context(), http.MethodPut, "/password", []byte(pw))
	},
}

// passwordAlphabet holds exactly 64 characters, so a random byte maps onto
// it without bias.
const passwordAlphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_-"

func generatePassword(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	for i, v := range b {
		b[i] = passwordAlphabet[int(v)%len(passwordAlphabet)]
	}
	return string(b), nil
}

func init() {
	rootCmd.AddCommand(rotatePasswordCmd)
}
