package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"time"

	"github.com/ks-tool/tembo/internal/agent"
	"github.com/ks-tool/tembo/internal/exec"
	"github.com/ks-tool/tembo/internal/store"

	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
)

// dbNameRe is what ?db= of GET /dump accepts as a database name.
var dbNameRe = regexp.MustCompile(`^[a-zA-Z0-9_.$-]+$`)

// opsHandlers serve the actions on the local node (reinit, restart, reload,
// dump, backup, restore) and the cluster's history. The local actions are the
// node's own business: they do not go through raft.
func opsHandlers(mux *http.ServeMux, st *store.Store, pg *Instance, id string) {
	// health answers 200 while the local Postgres answers SQL — the check
	// for supervisors and load balancers. /status stays 200 regardless: it
	// is how the nodes see each other, and a node whose Postgres is down
	// must keep taking part in the cluster (its status says so).
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		state, _, _ := pg.Status(r.Context())
		if !state.Running {
			http.Error(w, "postgres is not running", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(state)
	})

	// reinit discards the standby's data directory, so its next start clones
	// the primary anew. The primary refuses: it would lose the cluster's data.
	mux.HandleFunc("POST /reinit", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), shutdownTimeout)
		defer cancel()
		primary, err := st.Get(agent.KeyPrimary)
		if err != nil && !errors.Is(err, store.ErrNotFound) {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		state, _, _ := pg.Status(ctx)
		if primary == id && state.Running && !state.InRecovery {
			http.Error(w, "the primary cannot be reinitialized", http.StatusConflict)
			return
		}
		if state.Running {
			if err := pg.Stop(ctx, "fast"); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
		}
		// supervise starts postgres again, and an empty directory is cloned.
		if err := pg.wipeData(); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		pg.setRewindFailures(0)
		log.Warn().Msg("reinit: the data directory is gone, cloning the primary")
		w.WriteHeader(http.StatusNoContent)
	})

	// restart stops postgres; supervise starts it again with the current
	// configuration.
	mux.HandleFunc("POST /restart", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), shutdownTimeout)
		defer cancel()
		state, _, _ := pg.Status(ctx)
		if !state.Running {
			http.Error(w, "postgres is not running; supervise starts it on its own", http.StatusConflict)
			return
		}
		if err := pg.Stop(ctx, "fast"); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		log.Info().Msg("restart: postgres stopped, supervise starts it again")
		w.WriteHeader(http.StatusNoContent)
	})

	mux.HandleFunc("POST /reload", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()
		if err := pg.reload(ctx); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})

	mux.HandleFunc("GET /history", func(w http.ResponseWriter, _ *http.Request) {
		raw, err := st.Get(agent.KeyHistory)
		if errors.Is(err, store.ErrNotFound) {
			raw, err = "{}", nil
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, raw)
	})

	// dump streams a logical dump of the local Postgres: pg_dumpall of the
	// whole cluster, or pg_dump of one database with ?db=. Restorable with
	// /restore, or with psql through the proxy.
	mux.HandleFunc("GET /dump", func(w http.ResponseWriter, r *http.Request) {
		dir, err := pg.socketDir()
		if err != nil {
			http.Error(w, "postgres is not running", http.StatusServiceUnavailable)
			return
		}
		name, args := "pg_dumpall", []string(nil)
		if db := r.URL.Query().Get("db"); db != "" {
			if !dbNameRe.MatchString(db) {
				http.Error(w, "bad database name", http.StatusBadRequest)
				return
			}
			name, args = "pg_dump", []string{"--dbname", db}
		}
		args = append(args, "-h", dir, "-p", strconv.Itoa(pg.Port))
		w.Header().Set("Content-Type", "application/sql")
		w.Header().Set("Content-Disposition", `attachment; filename="dump.sql"`)
		cmd := exec.New(exec.SetOutput(w), exec.SetErrOut(newErrLineLog(name))).Command(r.Context(), pg.bin(name), args...)
		if err := cmd.Run(); err != nil {
			log.Error().Err(err).Msg("dump") // the header is sent already
		}
	})

	// backup streams a physical backup of the local Postgres: a tar with the
	// data directory and the WAL, restorable with /restore-backup. The tar
	// goes through a temporary directory: pg_basebackup cannot stream the WAL
	// to stdout in tar mode, and plain format keeps it inside the directory.
	mux.HandleFunc("GET /backup", func(w http.ResponseWriter, r *http.Request) {
		dir, err := pg.socketDir()
		if err != nil {
			http.Error(w, "postgres is not running", http.StatusServiceUnavailable)
			return
		}
		tmp, err := os.MkdirTemp("", "tembo-backup-*")
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer func() { _ = os.RemoveAll(tmp) }()
		bb := exec.New(exec.SetOutput(newLineLog("pg_basebackup")), exec.SetErrOut(newErrLineLog("pg_basebackup"))).
			Command(r.Context(), pg.bin("pg_basebackup"),
				"-D", tmp, "-X", "stream",
				"-h", dir, "-p", strconv.Itoa(pg.Port), "-U", superuser, "--no-password")
		if err := bb.Run(); err != nil {
			http.Error(w, fmt.Errorf("pg_basebackup: %w", err).Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/x-tar")
		w.Header().Set("Content-Disposition", `attachment; filename="base.tar"`)
		cmd := exec.New(exec.SetOutput(w), exec.SetErrOut(newErrLineLog("tar"))).Command(r.Context(), "tar", "-C", tmp, "-c", ".")
		if err := cmd.Run(); err != nil {
			log.Error().Err(err).Msg("backup") // the header is sent already
		}
	})

	// restore feeds a dump made by /dump to psql of the local Postgres; its
	// output is the response.
	mux.HandleFunc("POST /restore", func(w http.ResponseWriter, r *http.Request) {
		if state, _, _ := pg.Status(r.Context()); !state.Running {
			http.Error(w, "postgres is not running", http.StatusServiceUnavailable)
			return
		}
		// The tail of the output: that is where an error shows itself, and
		// a huge restore gathers no more than it in memory.
		out := &boundedBuffer{}
		cmd := exec.New(exec.SetOutput(out)).Command(r.Context(), pg.bin("psql"),
			"-v", "ON_ERROR_STOP=1", "-p", strconv.Itoa(pg.Port), "-d", "postgres")
		cmd.Stdin = r.Body
		err := cmd.Run()
		w.Header().Set("Content-Type", "text/plain")
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
		}
		_, _ = w.Write(out.Bytes())
	})

	// restore-backup replaces the data directory with the tar of /backup;
	// supervise starts postgres with it afterwards. A standby ahead of the
	// backup heals itself: pg_rewind, or a reclone after its failures.
	mux.HandleFunc("POST /restore-backup", func(w http.ResponseWriter, r *http.Request) {
		if state, _, _ := pg.Status(r.Context()); state.Running {
			ctx, cancel := context.WithTimeout(r.Context(), shutdownTimeout)
			defer cancel()
			if err := pg.Stop(ctx, "fast"); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
		}
		if err := pg.wipeData(); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		cmd := exec.New(exec.SetOutput(io.Discard), exec.SetErrOut(newErrLineLog("tar"))).Command(r.Context(), "tar", "-x", "-C", pg.DataDir)
		cmd.Stdin = r.Body
		if err := cmd.Run(); err != nil {
			http.Error(w, fmt.Errorf("untar: %w", err).Error(), http.StatusInternalServerError)
			return
		}
		// tar applies the modes of the archive; postgres wants 0700 or 0750.
		if err := os.Chmod(pg.DataDir, 0o700); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		pg.setRewindFailures(0)
		log.Warn().Msg("restore-backup: the data directory is replaced, supervise starts postgres")
		w.WriteHeader(http.StatusNoContent)
	})
}

// restoreOutputLimit is how much of psql's output /restore keeps: the tail,
// where an error shows itself.
const restoreOutputLimit = 64 << 10

// boundedBuffer keeps the last restoreOutputLimit bytes written to it.
type boundedBuffer struct {
	buf []byte
}

func (b *boundedBuffer) Write(p []byte) (int, error) {
	b.buf = append(b.buf, p...)
	if excess := len(b.buf) - restoreOutputLimit; excess > 0 {
		b.buf = append(b.buf[:0], b.buf[excess:]...) // copy handles the overlap
	}
	return len(p), nil
}

func (b *boundedBuffer) Bytes() []byte { return b.buf }

var reinitCmd = &cobra.Command{
	Use:   "reinit",
	Short: "Discard the standby's data and clone the primary anew",
	Args:  cobra.NoArgs,
	RunE: func(cmd *cobra.Command, _ []string) error {
		return call(cmd.Context(), http.MethodPost, "/reinit", nil)
	},
}

var restartCmd = &cobra.Command{
	Use:   "restart",
	Short: "Restart Postgres of this node",
	Args:  cobra.NoArgs,
	RunE: func(cmd *cobra.Command, _ []string) error {
		return call(cmd.Context(), http.MethodPost, "/restart", nil)
	},
}

var reloadCmd = &cobra.Command{
	Use:   "reload",
	Short: "Reload the configuration files of Postgres",
	Args:  cobra.NoArgs,
	RunE: func(cmd *cobra.Command, _ []string) error {
		return call(cmd.Context(), http.MethodPost, "/reload", nil)
	},
}

// healthCmd is the check for supervisors and healthchecks: it exits non-zero
// unless the local Postgres of the node --addr points at answers SQL.
var healthCmd = &cobra.Command{
	Use:   "health",
	Short: "Exit non-zero unless the node's Postgres answers",
	Args:  cobra.NoArgs,
	RunE: func(cmd *cobra.Command, _ []string) error {
		return call(cmd.Context(), http.MethodGet, "/health", nil)
	},
}

var historyCmd = &cobra.Command{
	Use:   "history",
	Short: "Show the failover and switchover history",
	Args:  cobra.NoArgs,
	RunE: func(cmd *cobra.Command, _ []string) error {
		return call(cmd.Context(), http.MethodGet, "/history", nil)
	},
}

var dumpCmd = &cobra.Command{
	Use:   "dump [--db name]",
	Short: "Stream a logical dump of Postgres to stdout: tembo dump > dump.sql",
	Args:  cobra.NoArgs,
	RunE: func(cmd *cobra.Command, _ []string) error {
		ctx, cancel := context.WithTimeout(cmd.Context(), time.Hour)
		defer cancel()
		path := "/dump"
		if db, _ := cmd.Flags().GetString("db"); db != "" {
			path += "?db=" + url.QueryEscape(db)
		}
		return call(ctx, http.MethodGet, path, nil)
	},
}

var backupCmd = &cobra.Command{
	Use:   "backup",
	Short: "Stream a physical backup of Postgres to stdout: tembo backup > base.tar",
	Args:  cobra.NoArgs,
	RunE: func(cmd *cobra.Command, _ []string) error {
		ctx, cancel := context.WithTimeout(cmd.Context(), time.Hour)
		defer cancel()
		return call(ctx, http.MethodGet, "/backup", nil)
	},
}

var restoreCmd = &cobra.Command{
	Use:   "restore",
	Short: "Restore a dump made with dump, from stdin: tembo restore < dump.sql",
	Args:  cobra.NoArgs,
	RunE: func(cmd *cobra.Command, _ []string) error {
		ctx, cancel := context.WithTimeout(cmd.Context(), time.Hour)
		defer cancel()
		data, err := request(ctx, http.MethodPost, "/restore", os.Stdin)
		if err != nil {
			return err
		}
		_, err = os.Stdout.Write(data)
		return err
	},
}

var restoreBackupCmd = &cobra.Command{
	Use:   "restore-backup",
	Short: "Replace the data directory with a backup tar, from stdin: tembo restore-backup < base.tar",
	Args:  cobra.NoArgs,
	RunE: func(cmd *cobra.Command, _ []string) error {
		ctx, cancel := context.WithTimeout(cmd.Context(), time.Hour)
		defer cancel()
		data, err := request(ctx, http.MethodPost, "/restore-backup", os.Stdin)
		if err != nil {
			return err
		}
		_, err = os.Stdout.Write(data)
		return err
	},
}

func init() {
	dumpCmd.Flags().String("db", "", "dump only this database, default is the whole cluster")
	rootCmd.AddCommand(reinitCmd, restartCmd, reloadCmd, healthCmd, historyCmd, dumpCmd, backupCmd, restoreCmd, restoreBackupCmd)
}
