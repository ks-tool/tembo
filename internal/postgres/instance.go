package postgres

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/ks-tool/tembo/internal/agent"
	"github.com/ks-tool/tembo/internal/exec"

	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog/log"
)

// Defaults are postgresql.conf settings applied on top of defaultPGConfig.
var Defaults = map[string]string{
	"archive_timeout":            "5min",
	"dynamic_shared_memory_type": "posix",
	"full_page_writes":           "on",
	"hot_standby_feedback":       "on", // with sync_replication_slots a failover keeps failover-enabled logical slots
	"max_parallel_workers":       "32",
	"max_worker_processes":       "32",
	"max_replication_slots":      "32",
	"shared_memory_type":         "mmap",
	"sync_replication_slots":     "on", // a standby GUC; needs primary_slot_name, which tembo sets
	"wal_keep_size":              "512MB",
	"wal_level":                  "logical",
	"wal_log_hints":              "on",
	"wal_sender_timeout":         "5s",
	"wal_receiver_timeout":       "5s",
}

// superuser is the role initdb creates; the agent uses it over the local
// socket, and pg_rewind uses it against the source server.
const superuser = "postgres"

// replUser is the role standbys and slot sync connect to the primary with;
// the primary creates it, see ensureRoles.
const replUser = "tembo_repl"

// restartTimeout is how long Apply waits for postgres to come back after a restart.
const restartTimeout = 30 * time.Second

// rewindAttempts is how many times in a row pg_rewind may fail against a
// reachable primary before the data directory is discarded and cloned anew.
const rewindAttempts = 10

// statusSQL returns whether the server is a standby, the position of its WAL
// in bytes, the primary it follows, its replication slot there and the values
// of hotStandbyLimits.
var statusSQL = `select pg_is_in_recovery(),
	coalesce(case when pg_is_in_recovery()
		then greatest(pg_last_wal_receive_lsn(), pg_last_wal_replay_lsn())
		else pg_current_wal_lsn() end - '0/0', 0)::bigint,
	current_setting('primary_conninfo'),
	coalesce(current_setting('primary_slot_name', true), '')` + func() string {
	var b strings.Builder
	for _, name := range hotStandbyLimits {
		_, _ = fmt.Fprintf(&b, ",\n\tcurrent_setting('%s')::bigint", name)
	}
	return b.String()
}()

func defaultPGConfig() map[string]string {
	return map[string]string{
		"listen_addresses":           "*",
		"max_connections":            "100",
		"max_locks_per_transaction":  "64",
		"max_prepared_transactions":  "0",
		"max_wal_senders":            "10",
		"shared_buffers":             "128MB",
		"max_wal_size":               "1GB",
		"min_wal_size":               "80MB",
		"datestyle":                  "iso, mdy",
		"lc_messages":                "C",
		"lc_monetary":                "C",
		"lc_numeric":                 "C",
		"lc_time":                    "C",
		"default_text_search_config": "pg_catalog.english",
		"log_hostname":               "on",
	}
}

// postgresqlConf merges settings over the defaults. The parameters tembo
// sets itself are added by configure.
func postgresqlConf(settings map[string]string) map[string]string {
	conf := defaultPGConfig()
	maps.Copy(conf, Defaults)
	maps.Copy(conf, settings)
	return conf
}

// Instance is the local Postgres managed by tembo.
type Instance struct {
	ID      string // id of this node; part of its replication slot name
	BinDir  string // directory with Postgres binaries; empty means $PATH
	DataDir string
	Port    int // port of this instance and of the primary
	// Password is the superuser password from --pg-password, PasswordFile
	// the --pg-password-file variant (--pg-password wins when both are
	// given). The password actually used — for conninfo, pg_basebackup,
	// pg_rewind — is the startup one until the cluster rotates it, see
	// SeedPassword, SetPassword and ApplyPassword.
	Password     string
	PasswordFile string
	LogDir       string   // FIFOs the log goes to, see openLogs
	Hosts        []string // hosts of all nodes: standbys and pg_rewind reach Postgres directly, so pg_hba allows them, replication included
	ExtraHBA     []string // extra pg_hba.conf lines, from the hba setting; written verbatim after the nodes
	// InitdbArgs are extra arguments for initdb of a new cluster, e.g.
	// ["--data-checksums"]. CloneCmd, a shell command, replaces pg_basebackup:
	// it gets TEMBO_PGDATA, TEMBO_PRIMARY_HOST, TEMBO_PRIMARY_PORT and
	// PGPASSWORD, e.g. a restore by wal-g or pgBackRest.
	InitdbArgs []string
	CloneCmd   string
	// shell, when set, lets the shell log in as Kerberos users, see shell_krb5.go.
	shell *shellKerberos

	// LocalAuth is how the agent itself connects to Postgres: "peer" (the
	// Unix socket), "password" (TCP to 127.0.0.1) or "gss" (TCP + Kerberos).
	// HostAuth is the authentication method of the managed pg_hba lines of
	// the nodes — "scram-sha-256", "ldap" or "gss" — and HostAuthOpts its
	// verbatim options, e.g. "ldapbasedn=... ldapbinddn=...".
	LocalAuth    string
	HostAuth     string
	HostAuthOpts string
	// Krb5Keytab and Krb5Config are the agent's Kerberos credentials; the
	// Postgres processes that authenticate with Kerberos (walreceiver,
	// pg_basebackup, pg_rewind) get them as KRB5_KTNAME and KRB5_CONFIG.
	Krb5Keytab    string
	Krb5Config    string
	Krb5Principal string

	mu              sync.Mutex
	config          Config   // settings the configuration files are written with
	syncNames       string   // synchronous_standby_names of a primary, see SetSyncStandbys
	syncIDs         []string // the standbys in it
	hostsPending    bool     // SetHosts ran since the last RefreshHBA
	rewindFailures  int      // consecutive failures of pg_rewind, see rewindFailed
	activePassword  string   // the password in effect: the startup one, or the cluster's rotation
	appliedPassword string   // the last password a primary applied to the roles, see ApplyPassword
}

// Config returns the settings the configuration files are written with.
func (i *Instance) Config() Config {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.config
}

// SetConfig sets the settings for the next start; a running server is not
// affected, see Apply.
func (i *Instance) SetConfig(cfg Config) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.config = cfg
}

// SeedPassword sets the startup password and marks it applied: a cluster
// that never rotated must not have its roles altered on every start. It
// fails before any use of the file variant, at startup rather than at
// initdb.
func (i *Instance) SeedPassword(pw string) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.activePassword, i.appliedPassword = pw, pw
}

// SetPassword switches the password the agent connects with — to a rotation
// the cluster committed; a primary applies it to the roles on its next
// reconcile tick, see ApplyPassword.
func (i *Instance) SetPassword(pw string) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.activePassword = pw
}

// ActivePassword returns the password in effect.
func (i *Instance) ActivePassword() string {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.activePassword
}

// Run prepares DataDir and runs postgres until it exits. Cancelling ctx
// requests a fast shutdown.
//
// With an empty upstreamHost the instance is the primary, and an empty DataDir
// is created by initdb. Otherwise an empty DataDir is cloned from
// upstreamHost: by the clone command when set, else by pg_basebackup, into
// this node's replication slot when the upstream keeps it (the primary of the
// own cluster does; a cascaded or foreign upstream does not).
func (i *Instance) Run(ctx context.Context, upstreamHost string, cloneSlot bool) error {
	// A postmaster that outlived a killed agent holds the data directory, and
	// pg_rewind needs it stopped as well.
	if i.running(ctx) {
		log.Warn().Msg("another postmaster holds the data directory, stopping it")
		if err := i.Stop(ctx, "fast"); err != nil {
			return fmt.Errorf("stopping the postmaster left behind: %w", err)
		}
	}
	if err := i.createData(ctx, upstreamHost, cloneSlot); err != nil {
		return err
	}
	if upstreamHost != "" {
		// A standby always has standby.signal; promotion removes it. Without
		// it the directory belongs to a former primary.
		_, err := os.Stat(filepath.Join(i.DataDir, "standby.signal"))
		if errors.Is(err, fs.ErrNotExist) {
			if err = i.rewind(ctx, upstreamHost); err != nil {
				return i.rewindFailed(ctx, upstreamHost, err)
			}
			i.setRewindFailures(0)
		}
		if err != nil {
			return err
		}
	}
	if err := i.configure(upstreamHost, cloneSlot); err != nil {
		return fmt.Errorf("configure: %w", err)
	}
	// An agent that dies must not leave a postmaster that nobody fences: the
	// kernel makes it shut down at once, and it recovers on the next start.
	// Its stderr stays at info, unlike the utilities': routine startup LOG
	// lines land there too, and its real failures speak through the csv log
	// and the exit supervise logs.
	cmd := exec.New(
		exec.SetContext(ctx),
		exec.SetSignal(syscall.SIGINT),
		exec.SetCommand(i.bin("postgres"), "-D", i.DataDir),
		exec.SetOutput(newLineLog("postgres")),
		exec.SetErrOut(newLineLog("postgres")),
	)
	if i.LocalAuth == "gss" || i.HostAuth == "gss" {
		// walreceiver and the other libpq clients among its children find
		// the keytab and krb5.conf in the environment.
		i.krb5Env(cmd)
	}
	cmd.OnParentDeath(syscall.SIGQUIT)
	return cmd.Run()
}

// Status reports the state of the running Postgres, its primary_conninfo and
// its primary_slot_name. Running is false when the server does not answer.
func (i *Instance) Status(ctx context.Context) (agent.Postgres, string, string) {
	var inRecovery bool
	var lsn int64
	var conninfo, slot string
	limits := make([]int64, len(hotStandbyLimits))
	dest := []any{&inRecovery, &lsn, &conninfo, &slot}
	for n := range limits {
		dest = append(dest, &limits[n])
	}
	err := i.query(ctx, func(ctx context.Context, conn *pgx.Conn) error {
		return conn.QueryRow(ctx, statusSQL).Scan(dest...)
	})
	if err != nil {
		return agent.Postgres{}, "", ""
	}

	p := agent.Postgres{Running: true, InRecovery: inRecovery, LSN: uint64(lsn), Limits: make(map[string]int64, len(limits))}
	for n, name := range hotStandbyLimits {
		p.Limits[name] = limits[n]
	}
	return p, conninfo, slot
}

// ShutdownLSN returns the position of the shutdown checkpoint of a server that
// was stopped cleanly, or 0 when the server did not stop cleanly or cannot be
// asked. All WAL that carries data of a clean shutdown lies before it.
func (i *Instance) ShutdownLSN(ctx context.Context) uint64 {
	var out bytes.Buffer
	cmd := exec.New(exec.SetContext(ctx), exec.SetOutput(&out),
		exec.SetCommand(i.bin("pg_controldata"), "--pgdata", i.DataDir))
	cmd.SetEnv("LC_ALL", "C")
	if err := cmd.Run(); err != nil {
		return 0
	}
	return shutdownLSN(out.String())
}

// shutdownLSN picks the shutdown checkpoint out of the output of pg_controldata.
func shutdownLSN(controlData string) uint64 {
	var clean bool
	var lsn uint64
	for line := range strings.SplitSeq(controlData, "\n") {
		name, value, ok := strings.Cut(line, ":")
		value = strings.TrimSpace(value)
		switch {
		case !ok:
		case name == "Database cluster state":
			clean = value == "shut down"
		case name == "Latest checkpoint location":
			hi, lo, ok := strings.Cut(value, "/")
			h, herr := strconv.ParseUint(hi, 16, 32)
			l, lerr := strconv.ParseUint(lo, 16, 32)
			if ok && herr == nil && lerr == nil {
				lsn = h<<32 | l
			}
		}
	}
	if !clean {
		return 0
	}
	return lsn
}

// ReplayLSN returns the position up to which the standby has replayed WAL,
// in bytes.
func (i *Instance) ReplayLSN(ctx context.Context) (uint64, error) {
	var lsn int64
	err := i.query(ctx, func(ctx context.Context, conn *pgx.Conn) error {
		return conn.QueryRow(ctx, "select coalesce(pg_last_wal_replay_lsn() - '0/0', 0)::bigint").Scan(&lsn)
	})
	return uint64(lsn), err
}

// Promote turns the running standby into a primary and waits for it.
func (i *Instance) Promote(ctx context.Context) error {
	return i.command(ctx, "pg_ctl", "promote", "--pgdata", i.DataDir).Run()
}

// Stop shuts Postgres down in mode "fast", which writes a checkpoint and sends
// all WAL to the connected standbys, or "immediate", which fences a primary
// at once, without a checkpoint.
func (i *Instance) Stop(ctx context.Context, mode string) error {
	return i.command(ctx, "pg_ctl", "stop", "--pgdata", i.DataDir, "--mode", mode).Run()
}

// running reports whether a postmaster is running in DataDir.
func (i *Instance) running(ctx context.Context) bool {
	return i.probe(ctx, "pg_ctl", "status", "--pgdata", i.DataDir).Run() == nil
}

// RefreshHBA rewrites pg_hba.conf and has Postgres reload it when the
// addresses of the nodes no longer match it, e.g. after a membership change
// or after one was renumbered.
func (i *Instance) RefreshHBA(ctx context.Context) (bool, error) {
	i.mu.Lock()
	i.hostsPending = false
	i.mu.Unlock()

	path := filepath.Join(i.DataDir, "pg_hba.conf")
	want := i.hba()
	if have, err := os.ReadFile(path); err == nil && bytes.Equal(have, want) {
		return false, nil
	}
	if err := os.WriteFile(path, want, 0o600); err != nil {
		return false, err
	}
	return true, i.reload(ctx)
}

// SetHosts updates the hosts pg_hba.conf allows after a membership change;
// the next reconcile tick rewrites the file.
func (i *Instance) SetHosts(hosts []string) {
	i.mu.Lock()
	i.Hosts = hosts
	i.hostsPending = true
	i.mu.Unlock()
}

// SetExtraHBA updates the extra pg_hba.conf lines, from the hba setting;
// the next reconcile tick rewrites the file on a change.
func (i *Instance) SetExtraHBA(lines []string) {
	i.mu.Lock()
	if !slices.Equal(i.ExtraHBA, lines) {
		i.ExtraHBA = lines
		i.hostsPending = true
	}
	i.mu.Unlock()
}

// HostsPending reports whether SetHosts ran since the last RefreshHBA.
func (i *Instance) HostsPending() bool {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.hostsPending
}

// hba is the content of pg_hba.conf: the local socket, which the agent itself
// uses, the nodes of the cluster, and the lines of the hba setting verbatim.
// The nodes need replication access: standbys and pg_rewind connect directly,
// without the proxy.
func (i *Instance) hba() []byte {
	i.mu.Lock()
	hosts := slices.Clone(i.Hosts)
	extra := slices.Clone(i.ExtraHBA)
	i.mu.Unlock()

	addresses := make(map[string]bool)
	for _, host := range hosts {
		if ip := net.ParseIP(host); ip != nil {
			addresses[single(ip)] = true
			continue
		}
		addresses[host] = true
		// A name matches only where a reverse lookup works, so the addresses
		// it resolves to now are written too.
		ips, err := net.LookupHost(host)
		if err != nil {
			log.Warn().Err(err).Str("host", host).Msg("pg_hba: cannot resolve a node")
		}
		for _, a := range ips {
			if ip := net.ParseIP(a); ip != nil {
				addresses[single(ip)] = true
			}
		}
	}

	var b strings.Builder
	// The agent uses the local socket; a local pg_basebackup (GET /backup)
	// speaks the replication protocol, which "all" does not cover.
	b.WriteString("local all all peer\n")
	b.WriteString("local replication " + superuser + " peer\n")
	method := i.hostAuth()
	if i.LocalAuth != "" && i.LocalAuth != "peer" {
		// The agent connects over TCP then; its line follows the nodes' method.
		_, _ = fmt.Fprintf(&b, "host all %s 127.0.0.1/32 %s\n", superuser, method)
	}
	repl := i.replMethod()
	for _, a := range slices.Sorted(maps.Keys(addresses)) {
		_, _ = fmt.Fprintf(&b, "host all all %s %s\n", a, method)
		_, _ = fmt.Fprintf(&b, "host replication %s %s %s\n", replUser, a, repl)
	}
	for _, line := range extra {
		b.WriteString(line + "\n")
	}
	return []byte(b.String())
}

// hostAuth is the authentication method of the managed pg_hba lines, with
// its verbatim options; the zero value is scram-sha-256.
func (i *Instance) hostAuth() string {
	method := i.HostAuth
	if method == "" {
		method = "scram-sha-256"
	}
	if i.HostAuthOpts != "" {
		method += " " + i.HostAuthOpts
	}
	return method
}

// replMethod is the method of the replication lines of pg_hba.conf. Under gss
// the ordinary connections authenticate Kerberos principals, but replication
// runs on tembo_repl's password: a record of its own with scram-sha-256, which
// takes none of the gss options.
func (i *Instance) replMethod() string {
	if i.HostAuth == "gss" {
		return "scram-sha-256"
	}
	return i.hostAuth()
}

// single is how a lone address is written in pg_hba.conf: without a mask the
// next word is read as one, "address mask auth-method" being a valid line.
func single(ip net.IP) string {
	if ip.To4() != nil {
		return ip.String() + "/32"
	}
	return ip.String() + "/128"
}

// Follows reports whether conninfo, a standby's primary_conninfo, points at
// primaryHost. The server hands the setting back re-serialized — quoting and
// field order its own — so the strings are compared as the connections they
// parse to, not as text.
func (i *Instance) Follows(primaryHost, conninfo string) bool {
	want, err := i.replConninfo(primaryHost)
	if err != nil || conninfo == "" {
		return false
	}
	return sameConninfo(want, conninfo)
}

// sameConninfo reports whether two conninfo strings name the same host, port,
// user and password.
func sameConninfo(a, b string) bool {
	ca, err := pgx.ParseConfig(a)
	if err != nil {
		return false
	}
	cb, err := pgx.ParseConfig(b)
	if err != nil {
		return false
	}
	return ca.Host == cb.Host && ca.Port == cb.Port && ca.User == cb.User && ca.Password == cb.Password
}

// Follow points the running standby at primaryHost; slot tells whether the
// upstream keeps this node's replication slot, see configure.
func (i *Instance) Follow(ctx context.Context, primaryHost string, slot bool) error {
	if err := i.configure(primaryHost, slot); err != nil {
		return err
	}
	return i.reload(ctx)
}

// Apply makes the running server use cfg: it rewrites the configuration,
// checks it the way a start would, reloads and restarts if a changed setting
// needs it. On failure the previous configuration is written back; a server
// that did not come back after the restart is started with it by supervise.
func (i *Instance) Apply(ctx context.Context, cfg Config, primaryHost string, slot bool) error {
	old := i.Config()
	i.SetConfig(cfg)
	err := i.apply(ctx, primaryHost, slot)
	if err != nil {
		i.SetConfig(old)
		if rerr := i.configure(primaryHost, slot); rerr != nil {
			return errors.Join(err, rerr)
		}
		_ = i.reload(ctx) // undo settings a reload already took; fails if the server is down
	}
	return err
}

func (i *Instance) apply(ctx context.Context, primaryHost string, slot bool) error {
	if err := i.configure(primaryHost, slot); err != nil {
		return err
	}

	// postgres -C reads the files as a start does and fails on a bad setting.
	check := exec.New().Command(ctx, i.bin("postgres"), "-D", i.DataDir, "-C", "port")
	check.Stdout, check.Stderr = nil, nil
	if out, err := check.CombinedOutput(); err != nil {
		return fmt.Errorf("configuration rejected: %s", strings.TrimSpace(string(out)))
	}

	var loaded time.Time
	err := i.query(ctx, func(ctx context.Context, conn *pgx.Conn) error {
		return conn.QueryRow(ctx, "select pg_conf_load_time()").Scan(&loaded)
	})
	if err != nil {
		return err
	}
	if err = i.reload(ctx); err != nil {
		return err
	}
	restart, err := i.waitReload(ctx, loaded)
	if err != nil || !restart {
		return err
	}

	// supervise starts the server again with the new files.
	if err = i.Stop(ctx, "fast"); err != nil {
		return err
	}
	deadline := time.Now().Add(restartTimeout)
	for time.Now().Before(deadline) {
		if err = sleep(ctx, time.Second); err != nil {
			return err
		}
		err = i.query(ctx, func(ctx context.Context, conn *pgx.Conn) error { return conn.Ping(ctx) })
		if err == nil {
			return nil
		}
	}
	return fmt.Errorf("postgres did not start with the new configuration within %s", restartTimeout)
}

// waitReload waits until the server has reloaded its configuration, which
// was last loaded at loadedAt, and reports whether a setting needs a restart.
func (i *Instance) waitReload(ctx context.Context, loadedAt time.Time) (bool, error) {
	for range 50 {
		var reloaded, restart bool
		err := i.query(ctx, func(ctx context.Context, conn *pgx.Conn) error {
			return conn.QueryRow(ctx, "select pg_conf_load_time() > $1, bool_or(pending_restart) from pg_settings", loadedAt).
				Scan(&reloaded, &restart)
		})
		if err == nil && reloaded {
			return restart, nil
		}
		if err := sleep(ctx, 200*time.Millisecond); err != nil {
			return false, err
		}
	}
	return false, errors.New("postgres did not reload its configuration")
}

func (i *Instance) reload(ctx context.Context) error {
	return i.command(ctx, "pg_ctl", "reload", "--pgdata", i.DataDir).Run()
}

func (i *Instance) createData(ctx context.Context, upstreamHost string, cloneSlot bool) error {
	// A clone that did not finish leaves its marker, see cloneMarker. What the
	// directory holds is part of a copy, not data, so it goes before anything
	// else looks at it: PG_VERSION may already be there.
	if _, err := os.Stat(i.cloneMarker()); err == nil {
		log.Warn().Msg("discarding the data of an unfinished clone")
		if err = i.wipeData(); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("discarding an unfinished clone: %w", err)
		}
		if err = os.Remove(i.cloneMarker()); err != nil {
			return err
		}
	}

	_, err := os.Stat(filepath.Join(i.DataDir, "PG_VERSION"))
	if !errors.Is(err, fs.ErrNotExist) {
		return err // nil: the data directory already exists
	}

	if upstreamHost == "" {
		// Encoding and locale are fixed so the cluster does not depend on the
		// host's environment.
		pwfile, remove, err := i.pwfile()
		if err != nil {
			return err
		}
		defer remove()
		args := append([]string{"--pgdata", i.DataDir, "--encoding", "UTF8", "--locale", "C",
			"--username", superuser, "--pwfile", pwfile}, i.InitdbArgs...)
		err = i.command(ctx, "initdb", args...).Run()
		if err != nil {
			return fmt.Errorf("initdb: %w", err)
		}
		return nil
	}

	password, err := i.password()
	if err != nil {
		return err
	}
	if cloneSlot {
		// The slot comes before the copy whichever command makes it: from here
		// on the primary keeps the WAL the copy and the streaming need. The
		// primary keeps one slot per node, and ensureSlot covers a node it has
		// not seen yet. A cascaded or foreign upstream has no slot of ours.
		if err = i.ensureSlot(ctx, upstreamHost); err != nil {
			return fmt.Errorf("replication slot on %s: %w", upstreamHost, err)
		}
	}
	if err = i.markClone(); err != nil {
		return err
	}
	// Like pg_basebackup, a restore may keep the mode of an existing directory,
	// and postgres refuses to start unless it is 0700 or 0750.
	if err = os.Chmod(i.DataDir, 0o700); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	if i.CloneCmd != "" {
		// A shell command, not a Postgres binary: no BinDir lookup.
		cmd := exec.New(exec.SetContext(ctx), exec.SetSignal(syscall.SIGINT),
			exec.SetCommand("sh", "-c", i.CloneCmd), exec.SetOutput(newLineLog("clone")),
			exec.SetErrOut(newErrLineLog("clone")))
		cmd.SetEnv("TEMBO_PGDATA", i.DataDir)
		cmd.SetEnv("TEMBO_PRIMARY_HOST", upstreamHost)
		cmd.SetEnv("TEMBO_PRIMARY_PORT", strconv.Itoa(i.Port))
		cmd.SetEnv("PGPASSWORD", password)
		if err = cmd.Run(); err != nil {
			return fmt.Errorf("clone command: %w", err)
		}
	} else {
		args := []string{"--pgdata", i.DataDir,
			"--host", upstreamHost, "--port", strconv.Itoa(i.Port), "--username", replUser, "--no-password",
			"--wal-method", "stream", "--checkpoint", "fast"}
		if cloneSlot {
			// WAL streams into this node's slot from the start.
			args = append(args, "--slot", i.slotName())
		}
		cmd := i.command(ctx, "pg_basebackup", args...)
		// Replication runs on tembo_repl's password, under gss as well.
		cmd.SetEnv("PGPASSWORD", password)
		if i.HostAuth == "gss" {
			i.krb5Env(cmd)
		}
		if err = cmd.Run(); err != nil {
			return fmt.Errorf("pg_basebackup from %s: %w", upstreamHost, err)
		}
	}
	if err = os.WriteFile(filepath.Join(i.DataDir, "standby.signal"), nil, 0o600); err != nil {
		return err
	}
	// The marker goes last: a crash before this leaves it, and the next start
	// discards the copy instead of starting from it.
	return os.Remove(i.cloneMarker())
}

// cloneMarker is the file that says a clone is under way, or was interrupted.
// It lies beside the data directory, not in it: pg_basebackup writes only into
// an empty directory, and the marker must exist before the copy starts.
func (i *Instance) cloneMarker() string {
	return filepath.Clean(i.DataDir) + ".clone-inprogress"
}

// markClone creates the data directory if needed and the clone marker.
func (i *Instance) markClone() error {
	if err := os.MkdirAll(i.DataDir, 0o700); err != nil {
		return err
	}
	return os.WriteFile(i.cloneMarker(), nil, 0o600)
}

// rewind makes the data directory of a former primary a standby of
// primaryHost. pg_rewind discards what was written after the timelines forked,
// running crash recovery first if the server was not shut down cleanly, and
// copies what changed on the primary.
func (i *Instance) rewind(ctx context.Context, primaryHost string) error {
	source := fmt.Sprintf("host=%s port=%d user=%s dbname=postgres", primaryHost, i.Port, superuser)
	cmd := i.command(ctx, "pg_rewind", "--target-pgdata", i.DataDir, "--source-server", source)
	if i.HostAuth == "gss" {
		// libpq authenticates with Kerberos instead of a password.
		i.krb5Env(cmd)
	} else {
		password, err := i.password()
		if err != nil {
			return err
		}
		cmd.SetEnv("PGPASSWORD", password)
	}
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("pg_rewind from %s: %w", primaryHost, err)
	}
	return os.WriteFile(filepath.Join(i.DataDir, "standby.signal"), nil, 0o600)
}

func (i *Instance) setRewindFailures(n int) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.rewindFailures = n
}

// rewindFailed handles a failed pg_rewind. A primary that cannot be reached is
// no fault of the data directory, so only failures against a reachable one
// count. After rewindAttempts of them the directory is emptied, and the next
// start clones the primary, as it does for a new node.
func (i *Instance) rewindFailed(ctx context.Context, primaryHost string, rewindErr error) error {
	if ctx.Err() != nil {
		return rewindErr
	}
	conn, err := net.DialTimeout("tcp", net.JoinHostPort(primaryHost, strconv.Itoa(i.Port)), 2*time.Second)
	if err != nil {
		return rewindErr
	}
	_ = conn.Close()

	i.mu.Lock()
	i.rewindFailures++
	failures := i.rewindFailures
	i.mu.Unlock()
	if failures < rewindAttempts {
		return rewindErr
	}
	log.Error().Err(rewindErr).Int("attempts", failures).Msg("pg_rewind keeps failing, discarding the data directory to clone the primary")
	if err = i.wipeData(); err != nil {
		return errors.Join(rewindErr, err)
	}
	i.setRewindFailures(0)
	return rewindErr
}

// wipeData removes everything in DataDir but keeps the directory, which may be
// a mount point.
func (i *Instance) wipeData() error {
	entries, err := os.ReadDir(i.DataDir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if err = os.RemoveAll(filepath.Join(i.DataDir, e.Name())); err != nil {
			return err
		}
	}
	return nil
}

// confValue escapes a value for the single quotes of postgresql.conf, where
// a backslash would otherwise start an escape sequence.
var confValue = strings.NewReplacer(`\`, `\\`, `'`, `''`)

// configure rewrites postgresql.conf and pg_hba.conf, so their content is
// defined by tembo rather than by initdb or manual edits. A standby of
// primaryHost also gets primary_conninfo, and its replication slot when the
// upstream keeps it: the primary of the own cluster does, a cascaded or
// foreign upstream does not.
func (i *Instance) configure(primaryHost string, slot bool) error {
	conf := postgresqlConf(i.Config().Settings)
	maps.Copy(conf, logSettings(i.LogDir))
	conf["port"] = strconv.Itoa(i.Port)
	if primaryHost != "" {
		conninfo, err := i.replConninfo(primaryHost)
		if err != nil {
			return err
		}
		conf["primary_conninfo"] = conninfo
		if slot {
			conf["primary_slot_name"] = i.slotName()
		}
	} else if names := i.syncStandbyNames(); names != "" {
		// Only a primary carries the list: a standby promoted before its
		// first reconcile would otherwise wait for standbys that are gone.
		conf["synchronous_standby_names"] = names
	}

	var b strings.Builder
	for _, k := range slices.Sorted(maps.Keys(conf)) {
		_, _ = fmt.Fprintf(&b, "%s = '%s'\n", k, confValue.Replace(conf[k]))
	}
	if err := os.WriteFile(filepath.Join(i.DataDir, "postgresql.conf"), []byte(b.String()), 0o600); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(i.DataDir, "pg_hba.conf"), i.hba(), 0o600)
}

// conninfo is the primary_conninfo of a standby of primaryHost. The
// application_name identifies this node in the primary's pg_stat_replication,
// and the dbname is required by the logical slot sync worker.
func (i *Instance) conninfo(primaryHost string) (string, error) {
	if i.HostAuth == "gss" {
		// libpq (and pgx) authenticate with Kerberos; there is no password.
		// gssencmode is not set: the managed pg_hba lines are plain "host
		// ... gss" (auth only, no GSS encryption), and pgconn does not know
		// the parameter at all.
		return fmt.Sprintf("host=%s port=%d user=%s dbname=postgres application_name=%s",
			primaryHost, i.Port, replUser, i.ID), nil
	}
	return i.replConninfo(primaryHost)
}

// replConninfo is the connection of the replication role tembo_repl with its
// password: what primary_conninfo and pg_basebackup use, under gss as well.
func (i *Instance) replConninfo(primaryHost string) (string, error) {
	password, err := i.password()
	if err != nil {
		return "", err
	}
	quote := strings.NewReplacer(`\`, `\\`, `'`, `\'`)
	return fmt.Sprintf("host=%s port=%d user=%s password='%s' dbname=postgres application_name=%s",
		primaryHost, i.Port, replUser, quote.Replace(password), i.ID), nil
}

// slotName is the replication slot of this node on its primary.
func (i *Instance) slotName() string { return "tembo_" + i.ID }

// password is the superuser password in effect: the cluster's rotation, or
// the startup one — --pg-password, else the first line of the file the way
// initdb --pwfile reads it.
func (i *Instance) password() (string, error) {
	if pw := i.ActivePassword(); pw != "" {
		return pw, nil
	}
	if i.Password != "" {
		return i.Password, nil
	}
	b, err := os.ReadFile(i.PasswordFile)
	if err != nil {
		return "", err
	}
	line, _, _ := strings.Cut(string(b), "\n")
	return strings.TrimSuffix(line, "\r"), nil
}

// pwfile is how initdb gets the password: --pg-password-file, or a
// temporary file written from --pg-password and removed afterwards — the
// only time the env variant touches the filesystem.
func (i *Instance) pwfile() (path string, cleanup func(), err error) {
	if i.PasswordFile != "" {
		return i.PasswordFile, func() {}, nil
	}
	pw, err := i.password()
	if err != nil {
		return "", nil, err
	}
	f, err := os.CreateTemp("", "tembo-pw-*") // 0600
	if err != nil {
		return "", nil, err
	}
	if _, err = f.WriteString(pw + "\n"); err != nil {
		_ = f.Close()
		_ = os.Remove(f.Name())
		return "", nil, err
	}
	if err = f.Close(); err != nil {
		_ = os.Remove(f.Name())
		return "", nil, err
	}
	return f.Name(), func() { _ = os.Remove(f.Name()) }, nil
}

// ApplyPassword alters the local postgres and tembo_repl roles to the
// active password, if it changed since the last time it was applied. A
// primary only — roles are replicated, and a standby answers "cannot
// execute ALTER ROLE in a read-only transaction"; it gets the new password
// through WAL replay. tembo_repl takes one only where it authenticates with
// a password, mirroring ensureRoles.
func (i *Instance) ApplyPassword(ctx context.Context) error {
	i.mu.Lock()
	pw, applied := i.activePassword, i.appliedPassword
	i.mu.Unlock()
	if pw == "" || pw == applied {
		return nil
	}
	quoted := strings.ReplaceAll(pw, "'", "''")
	err := i.query(ctx, func(ctx context.Context, conn *pgx.Conn) error {
		roles := []string{superuser}
		if i.HostAuth != "ldap" {
			roles = append(roles, replUser)
		}
		for _, role := range roles {
			if _, err := conn.Exec(ctx, fmt.Sprintf("alter role %s with password '%s'", role, quoted)); err != nil {
				return fmt.Errorf("alter role %s: %w", role, err)
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	i.mu.Lock()
	i.appliedPassword = pw
	i.mu.Unlock()
	return nil
}

// Connect opens a connection to the running Postgres as the superuser: over
// the Unix socket with peer authentication, or over TCP with the password or
// Kerberos, as LocalAuth says.
func (i *Instance) Connect(ctx context.Context) (*pgx.Conn, error) {
	var conninfo string
	switch i.LocalAuth {
	case "password":
		password, err := i.password()
		if err != nil {
			return nil, err
		}
		quote := strings.NewReplacer(`\`, `\\`, `'`, `\'`)
		conninfo = fmt.Sprintf("host=127.0.0.1 port=%d user=%s dbname=postgres application_name=tembo password='%s'",
			i.Port, superuser, quote.Replace(password))
	case "gss":
		// The SPN is postgres/<hostname>: the loopback address would not
		// match the server's principal.
		host, err := os.Hostname()
		if err != nil {
			return nil, err
		}
		conninfo = fmt.Sprintf("host=%s port=%d user=%s dbname=postgres application_name=tembo",
			host, i.Port, superuser)
	default: // peer
		dir, err := i.socketDir()
		if err != nil {
			return nil, err
		}
		dir = strings.NewReplacer(`\`, `\\`, `'`, `\'`).Replace(dir)
		conninfo = fmt.Sprintf("host='%s' port=%d user=%s dbname=postgres application_name=tembo", dir, i.Port, superuser)
	}
	cfg, err := pgx.ParseConfig(conninfo)
	if err != nil {
		return nil, err
	}
	return pgx.ConnectConfig(ctx, cfg)
}

// socketDir returns the Unix socket directory the running postmaster
// announced in postmaster.pid.
func (i *Instance) socketDir() (string, error) {
	pid, err := os.ReadFile(filepath.Join(i.DataDir, "postmaster.pid"))
	if err != nil {
		return "", err
	}
	// Lines: PID, data directory, start time, port, socket directory, ...
	lines := strings.Split(string(pid), "\n")
	if len(lines) < 5 || lines[4] == "" {
		return "", errors.New("postmaster.pid names no socket directory")
	}
	return lines[4], nil
}

// query runs fn on a fresh connection, which it closes afterwards.
func (i *Instance) query(ctx context.Context, fn func(context.Context, *pgx.Conn) error) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	conn, err := i.Connect(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(context.Background()) }()
	return fn(ctx, conn)
}

// command prepares a Postgres binary whose output is logged line by line —
// stdout at info, stderr at error, where a failing utility's diagnostics
// belong. On cancel it gets SIGINT: a fast shutdown for postgres, a clean
// exit for initdb.
func (i *Instance) command(ctx context.Context, name string, args ...string) *exec.Command {
	return exec.New(
		exec.SetContext(ctx),
		exec.SetSignal(syscall.SIGINT),
		exec.SetCommand(i.bin(name), args...),
		exec.SetOutput(newLineLog(name)),
		exec.SetErrOut(newErrLineLog(name)),
	)
}

// probe prepares a command whose output is discarded: the boolean questions
// (pg_ctl status) ask it every tick, and a "no server running" answer is a
// normal state, not a log line.
func (i *Instance) probe(ctx context.Context, name string, args ...string) *exec.Command {
	return exec.New(
		exec.SetContext(ctx),
		exec.SetSignal(syscall.SIGINT),
		exec.SetCommand(i.bin(name), args...),
		exec.SetOutput(io.Discard),
	)
}

func (i *Instance) bin(name string) string {
	if i.BinDir == "" {
		return name
	}
	return filepath.Join(i.BinDir, name)
}

// krb5Env points a Postgres process at the agent's Kerberos credentials:
// libpq reads the keytab and krb5.conf of KRB5_KTNAME and KRB5_CONFIG.
func (i *Instance) krb5Env(cmd *exec.Command) {
	if i.Krb5Keytab != "" {
		cmd.SetEnv("KRB5_KTNAME", i.Krb5Keytab)
	}
	if i.Krb5Config != "" {
		cmd.SetEnv("KRB5_CONFIG", i.Krb5Config)
	}
}

func sleep(ctx context.Context, d time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(d):
		return nil
	}
}
