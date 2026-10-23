package postgres

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"maps"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	appconfig "github.com/ks-tool/app-config/go"
	"github.com/ks-tool/tembo/internal/addrlist"
	"github.com/ks-tool/tembo/internal/agent"
	"github.com/ks-tool/tembo/internal/audit"
	"github.com/ks-tool/tembo/internal/auth"
	"github.com/ks-tool/tembo/internal/metrics"
	"github.com/ks-tool/tembo/internal/proxy"
	"github.com/ks-tool/tembo/internal/store"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"
)

const (
	// restartDelay is the pause before starting Postgres again after it exited.
	restartDelay = 5 * time.Second

	// hungTimeout is how long a postmaster that is alive but answers no SQL is
	// tolerated before reconcile restarts it. A server in crash recovery never
	// answered since its start and is left alone.
	hungTimeout = 2 * time.Minute

	// shutdownTimeout bounds the clean stop of Postgres on shutdown; after it, and
	// on a second signal, Postgres is stopped without a checkpoint.
	shutdownTimeout = time.Minute
)

// nodeID matches an id that can be part of a replication slot name.
var nodeID = regexp.MustCompile(`^[a-z0-9_]+$`)

// runConfig is the startup configuration of run. appconfig loads it from the
// defaults below, an optional --config file, the Kubernetes objects of --k8s,
// the environment and the command line, the later winning over the earlier.
type runConfig struct {
	ID                       string   `env:"ID" flag:"id" help:"this node's id (must appear in --peers)"`
	Peers                    string   `env:"PEERS" flag:"peers" help:"all nodes as id=host:port or bare host:port (the host names the node), comma separated; without --peers and --consul the node runs standalone, a cluster of one without raft"`
	Join                     bool     `env:"JOIN" flag:"join" help:"join a running cluster: --peers names this node and at least one member"`
	DB                       string   `env:"DB" flag:"db" help:"BoltDB file (default <id>.db)"`
	Listen                   string   `env:"LISTEN" flag:"listen" help:"address for clients, forwarded to the primary"`
	ListenRO                 string   `env:"LISTEN_RO" flag:"listen-ro" help:"address for readers, forwarded to the standbys"`
	Retention                int64    `env:"RAFT_RETENTION_LOG_SIZE" flag:"raft-retention-log-size" help:"bytes of applied raft log to keep, older entries are compacted; 0 keeps all"`
	PGBin                    string   `env:"PG_BIN" flag:"pg-bin" help:"directory with initdb and postgres (default: $PATH)"`
	PGData                   string   `env:"PG_DATA" flag:"pg-data" help:"Postgres data directory"`
	PGPort                   int      `env:"PG_PORT" flag:"pg-port" help:"Postgres port, the same on every node"`
	PGPassword               string   `env:"PG_PASSWORD" flag:"pg-password" help:"Postgres superuser password, the same on every node; --pg-password-file is the file variant, --pg-password wins"`
	PGPasswordFile           string   `env:"PG_PASSWORD_FILE" flag:"pg-password-file" help:"file with the Postgres superuser password, the same on every node (or --pg-password)"`
	PGLocalAuth              string   `env:"PG_LOCAL_AUTH" flag:"pg-local-auth" help:"how the agent connects to Postgres: peer (the Unix socket), password or gss (Kerberos)"`
	PGHostAuth               string   `env:"PG_HOST_AUTH" flag:"pg-host-auth" help:"authentication of the nodes' pg_hba lines: scram-sha-256, ldap or gss; under gss, replication still runs on tembo_repl's password"`
	PGHostAuthOpts           string   `env:"PG_HOST_AUTH_OPTS" flag:"pg-host-auth-opts" help:"verbatim options of the --pg-host-auth method, e.g. \"ldapbasedn=... ldapbinddn=...\""`
	PGKrb5Keytab             string   `env:"PG_KRB5_KEYTAB" flag:"pg-krb5-keytab" help:"keytab the agent's Kerberos tickets come from (default: the credential cache)"`
	PGKrb5Config             string   `env:"PG_KRB5_CONFIG" flag:"pg-krb5-config" help:"krb5.conf for the agent and the Postgres processes (default: $KRB5_CONFIG or /etc/krb5.conf)"`
	PGKrb5Principal          string   `env:"PG_KRB5_PRINCIPAL" flag:"pg-krb5-principal" help:"the agent's Kerberos principal"`
	NoFailover               bool     `env:"NOFAILOVER" flag:"nofailover" help:"never make this node the primary"`
	NoSync                   bool     `env:"NOSYNC" flag:"nosync" help:"never commit synchronously with this node"`
	FailoverPriority         int      `env:"FAILOVER_PRIORITY" flag:"failover-priority" help:"higher is preferred at a failover"`
	InitdbArgs               string   `env:"INITDB_ARGS" flag:"initdb-args" help:"extra arguments for initdb of a new cluster, e.g. \"--data-checksums\""`
	CloneCmd                 string   `env:"CLONE_COMMAND" flag:"clone-command" help:"shell command cloning the primary instead of pg_basebackup; gets TEMBO_PGDATA, TEMBO_PRIMARY_HOST, TEMBO_PRIMARY_PORT, PGPASSWORD"`
	Follow                   string   `env:"FOLLOW" flag:"follow" help:"replicate from this member instead of the primary (cascading standby)"`
	StandbyOf                string   `env:"STANDBY_OF" flag:"standby-of" help:"replicate from this host (on --pg-port), e.g. a remote cluster; the node never becomes primary"`
	Config                   string   `env:"CONFIG" flag:"config" help:"config file to load first (.env or .json)"`
	K8s                      []string `env:"K8S" flag:"k8s" help:"Kubernetes URIs to configure from, cm://[ns/]name or secret://[ns/]name[/key]; keeps them applied and disables PATCH /config and /settings"`
	Consul                   string   `env:"CONSUL" flag:"consul" help:"Consul HTTP API address, e.g. http://127.0.0.1:8500: register this node and discover --peers by --consul-tags instead of listing them"`
	ConsulService            string   `env:"CONSUL_SERVICE" flag:"consul-service" help:"Consul service name for the registration and the discovery"`
	ConsulTags               string   `env:"CONSUL_TAGS" flag:"consul-tags" help:"comma-separated tags: nodes sharing the service and all the tags form one cluster"`
	ConsulExpect             int      `env:"CONSUL_EXPECT" flag:"consul-expect" help:"wait for at least this many discovered nodes before bootstrapping"`
	Advertise                string   `env:"ADVERTISE" flag:"advertise" help:"host[:port] the API is reached at (with --consul or standalone; default: the hostname, port 8001)"`
	BaoAddress               string   `env:"BAO_ADDRESS" flag:"bao-address" help:"OpenBao (Vault) API address, e.g. https://bao:8200; enables the password integration"`
	BaoToken                 string   `env:"BAO_TOKEN" flag:"bao-token" help:"OpenBao token; --bao-token-file is the file variant"`
	BaoTokenFile             string   `env:"BAO_TOKEN_FILE" flag:"bao-token-file" help:"file with the OpenBao token"`
	BaoRoleIDFile            string   `env:"BAO_ROLE_ID_FILE" flag:"bao-role-id-file" help:"AppRole login instead of a token, with --bao-secret-id-file"`
	BaoSecretIDFile          string   `env:"BAO_SECRET_ID_FILE" flag:"bao-secret-id-file" help:"AppRole secret id, with --bao-role-id-file; must allow more than one use"`
	BaoCAFile                string   `env:"BAO_CA_FILE" flag:"bao-ca-file" help:"CA bundle (PEM) to verify the OpenBao server with"`
	BaoKVMount               string   `env:"BAO_KV_MOUNT" flag:"bao-kv-mount" help:"KV v2 mount of the secrets (default: secret)"`
	BaoPasswordPath          string   `env:"BAO_PASSWORD_PATH" flag:"bao-password-path" help:"KV v2 path of the Postgres password secret (the key is \"password\"); OpenBao is the source of truth and PUT /password is off"`
	BaoLDAPPasswordPath      string   `env:"BAO_LDAP_PASSWORD_PATH" flag:"bao-ldap-password-path" help:"KV v2 path of the LDAP bind password secret (the key is \"password\"), read at startup"`
	TLSCertFile              string   `env:"TLS_CERT_FILE" flag:"tls-cert-file" help:"API TLS certificate (PEM): with --tls-key-file the API serves HTTPS"`
	TLSKeyFile               string   `env:"TLS_KEY_FILE" flag:"tls-key-file" help:"private key of --tls-cert-file (PEM)"`
	TLSCAFile                string   `env:"TLS_CA_FILE" flag:"tls-ca-file" help:"CA bundle (PEM): the nodes verify each other with it and clients may authenticate with a certificate"`
	AuthJWTKeyFile           string   `env:"AUTH_JWT_KEY_FILE" flag:"auth-jwt-key-file" help:"JWT verification key: a PEM public key (RS*/ES*) or an HMAC secret (HS*)"`
	AuthJWTJWKS              string   `env:"AUTH_JWT_JWKS" flag:"auth-jwt-jwks" help:"JWKS URL to verify JWTs with"`
	AuthJWTIssuer            string   `env:"AUTH_JWT_ISSUER" flag:"auth-jwt-issuer" help:"issuer a JWT must name"`
	AuthJWTAudience          string   `env:"AUTH_JWT_AUDIENCE" flag:"auth-jwt-audience" help:"audience a JWT must name"`
	AuthKrb5Keytab           string   `env:"AUTH_KRB5_KEYTAB" flag:"auth-krb5-keytab" help:"keytab of the API's Kerberos principal, enables SPNEGO"`
	ShellKrb5SPN             string   `env:"SHELL_KRB5_SPN" flag:"shell-krb5-spn" help:"Postgres service the shell impersonates Kerberos users to, e.g. postgres/db.example.com; needs --auth-krb5-keytab and --shell-krb5-principal"`
	ShellKrb5Principal       string   `env:"SHELL_KRB5_PRINCIPAL" flag:"shell-krb5-principal" help:"the agent's own principal user@REALM for S4U, whose key is in --auth-krb5-keytab"`
	AuthLDAPURL              string   `env:"AUTH_LDAP_URL" flag:"auth-ldap-url" help:"LDAP server (ldap:// or ldaps://) to authenticate Basic auth against"`
	AuthLDAPDNTemplate       string   `env:"AUTH_LDAP_DN_TEMPLATE" flag:"auth-ldap-dn-template" help:"direct bind: DN template with %s for the username"`
	AuthLDAPBindDN           string   `env:"AUTH_LDAP_BIND_DN" flag:"auth-ldap-bind-dn" help:"search-bind: DN to bind with"`
	AuthLDAPBindPasswordFile string   `env:"AUTH_LDAP_BIND_PASSWORD_FILE" flag:"auth-ldap-bind-password-file" help:"file with the password of --auth-ldap-bind-dn"`
	AuthLDAPBaseDN           string   `env:"AUTH_LDAP_BASE_DN" flag:"auth-ldap-base-dn" help:"search-bind: base DN to search the user under"`
	AuthLDAPFilter           string   `env:"AUTH_LDAP_FILTER" flag:"auth-ldap-filter" help:"search-bind: filter with %s for the username, e.g. \"(uid=%s)\""`
	AuthReadOnly             []string `env:"AUTH_READ_ONLY" flag:"auth-read-only" help:"identities limited to read-only API access, e.g. jwt:alice, comma separated"`
}

// rc is this node's startup configuration, the defaults included.
var rc = runConfig{Listen: ":5432", ListenRO: ":5434", Retention: 8 << 20, PGPort: 5433, ConsulService: "tembo",
	PGLocalAuth: "peer", PGHostAuth: "scram-sha-256", PGKrb5Principal: "postgres"}

var runCmd = &cobra.Command{
	Use:  "run",
	Args: cobra.NoArgs,
	RunE: runCmdRun,
}

func init() {
	appconfig.BindFlags(runCmd.Flags(), prefixOpts.Prefix, &rc)
	rootCmd.AddCommand(runCmd)
}

func runCmdRun(cmd *cobra.Command, _ []string) error {
	kube, sources, err := rcSources(cmd.Context(), cmd.Flags())
	if err != nil {
		return err
	}
	if err := appconfig.Load(&rc, sources...); err != nil {
		return err
	}
	// TLS of the API port, before the listeners below open: with --consul the
	// API listener is up from the registration on.
	tlsCfg, err := apiTLSConfig()
	if err != nil {
		return err
	}
	if err := validatePGAuth(); err != nil {
		return err
	}
	if rc.PGLocalAuth == "gss" || rc.PGHostAuth == "gss" {
		registerGSS()
	}

	if len(rc.ID) == 0 {
		host, err := os.Hostname()
		if err != nil {
			return err
		}
		rc.ID = nodeIDFromHost(host)
	}

	// With Consul the peers are discovered, not listed: this node registers
	// itself with a TCP check against the API port (open the listener first,
	// so the check can pass) and waits for the set with the same service and
	// tags to settle.
	var apiLn net.Listener
	if rc.Consul != "" {
		if rc.Peers != "" {
			return errConsulExcludesPeers
		}
		host, port, err := advertiseAddr(rc.Advertise)
		if err != nil {
			return err
		}
		apiLn, err = listenAPI(strconv.Itoa(port), tlsCfg)
		if err != nil {
			return err
		}
		consul := newConsulClient(rc.Consul, rc.ConsulService, strings.FieldsFunc(rc.ConsulTags, func(r rune) bool { return r == ',' || r == ' ' }), rc.ID)
		if err := consul.register(host, port); err != nil {
			return err
		}
		defer consul.deregister()
		log.Info().Str("id", rc.ID).Str("addr", net.JoinHostPort(host, strconv.Itoa(port))).Msg("consul: registered")
		rc.Peers, err = consul.waitPeers(cmd.Context(), rc.ConsulExpect)
		if err != nil {
			return err
		}
	}
	// OpenBao is built before the required check: its password counts as a
	// password source.
	var bao *baoClient
	if rc.BaoAddress != "" {
		bao, err = newBaoClient(rc.BaoAddress, rc.BaoToken, rc.BaoTokenFile,
			rc.BaoRoleIDFile, rc.BaoSecretIDFile, rc.BaoCAFile, rc.BaoKVMount)
		if err != nil {
			return err
		}
	}
	// Standalone — no --peers and no --consul — is a cluster of one without
	// raft: this node is the primary, and the configuration applies to its
	// store directly.
	standalone := rc.Peers == "" && rc.Consul == ""
	if rc.ID == "" || rc.PGData == "" ||
		(rc.PGPassword == "" && rc.PGPasswordFile == "" && rc.BaoPasswordPath == "") {
		return errors.New("required: --id, --pg-data, and --pg-password, --pg-password-file or --bao-password-path (or the TEMBO_ environment variables); a cluster also takes --peers or --consul")
	}
	if standalone && rc.Join {
		return errors.New("--join needs --peers naming this node and at least one member of the running cluster")
	}
	if standalone && rc.Follow != "" {
		return errors.New("--follow needs --peers: it names a member of the cluster")
	}
	var peers map[string]string
	if standalone {
		// The API address stands in for the membership: the allowlists and
		// pg_hba.conf are built from it.
		host, port, err := advertiseAddr(rc.Advertise)
		if err != nil {
			return err
		}
		peers = map[string]string{rc.ID: net.JoinHostPort(host, strconv.Itoa(port))}
	} else if peers, err = parsePeers(rc.Peers); err != nil {
		return err
	}

	addr, ok := peers[rc.ID]
	if !ok {
		return fmt.Errorf("--id %q must be one of the --peers", rc.ID)
	}
	if rc.Join && len(peers) < 2 {
		return errors.New("--join: --peers must name this node and at least one member of the running cluster")
	}
	if rc.Follow != "" {
		if rc.Follow == rc.ID {
			return errors.New("--follow must not name this node")
		}
		if _, ok := peers[rc.Follow]; !ok {
			return fmt.Errorf("--follow %q is not one of the --peers", rc.Follow)
		}
	}
	if rc.Follow != "" && rc.StandbyOf != "" {
		return errors.New("--follow and --standby-of exclude each other")
	}
	if rc.DB == "" {
		rc.DB = rc.ID + ".db"
	}

	st, err := store.Open(rc.DB)
	if err != nil {
		return err
	}
	defer func() { _ = st.Close() }()

	var srv *agent.Server
	if standalone {
		srv = agent.NewStandalone(rc.ID, addr, st)
	} else {
		srv, err = agent.NewServer(rc.ID, peers, st, rc.Retention, rc.Join)
		if err != nil {
			return err
		}
	}
	if tlsCfg != nil {
		// The nodes verify each other with the CA of --tls-ca-file, or with
		// the system pool; they carry no client certificates.
		roots, err := loadCertPool(rc.TLSCAFile)
		if err != nil {
			return err
		}
		srv.UseTLS(&tls.Config{RootCAs: roots})
	}

	logDir := filepath.Join(os.TempDir(), "tembo-"+rc.ID)
	plainLog, csvLog, err := openLogs(logDir)
	if err != nil {
		return err
	}
	// Closed on return, after Postgres has stopped.
	defer func() { _, _ = plainLog.Close(), csvLog.Close() }()

	// Listen before joining the cluster so peers can reach us at once. The API
	// for peers and the CLI listens on all interfaces, at the port of this
	// node's --peers address. With --consul the listener is already up: the
	// health check of the registration needed it.
	if apiLn == nil {
		_, port, err := net.SplitHostPort(addr)
		if err != nil {
			return err
		}
		apiLn, err = listenAPI(port, tlsCfg)
		if err != nil {
			return err
		}
	}
	defer func() { _ = apiLn.Close() }()

	proxyLn, err := net.Listen("tcp", rc.Listen)
	if err != nil {
		return err
	}
	defer func() { _ = proxyLn.Close() }()

	readerLn, err := net.Listen("tcp", rc.ListenRO)
	if err != nil {
		return err
	}
	defer func() { _ = readerLn.Close() }()

	hosts, err := hostsOf(peers)
	if err != nil {
		return err
	}

	pg := &Instance{
		ID:            rc.ID,
		BinDir:        rc.PGBin,
		DataDir:       rc.PGData,
		Port:          rc.PGPort,
		Password:      rc.PGPassword,
		PasswordFile:  rc.PGPasswordFile,
		LogDir:        logDir,
		Hosts:         hosts,
		InitdbArgs:    strings.Fields(rc.InitdbArgs),
		CloneCmd:      rc.CloneCmd,
		LocalAuth:     rc.PGLocalAuth,
		HostAuth:      rc.PGHostAuth,
		HostAuthOpts:  rc.PGHostAuthOpts,
		Krb5Keytab:    rc.PGKrb5Keytab,
		Krb5Config:    rc.PGKrb5Config,
		Krb5Principal: rc.PGKrb5Principal,
	}
	// The startup password: OpenBao when the integration is on — fail fast
	// here rather than at initdb if it is unreachable — else --pg-password
	// or --pg-password-file. The OpenBao password is set as active but not
	// applied: a primary alters the roles to whatever the secret says, so
	// moving a cluster under OpenBao converges it in place. A flag or file
	// password counts as applied — only a later change of the source makes
	// a primary alter the roles.
	if bao != nil && rc.BaoPasswordPath != "" {
		pw, err := bao.readPassword(cmd.Context(), rc.BaoPasswordPath)
		if err != nil {
			return err
		}
		pg.SetPassword(pw)
	} else {
		// Fails here on an unreadable --pg-password-file rather than at initdb.
		pw, err := pg.password()
		if err != nil {
			return err
		}
		pg.SeedPassword(pw)
	}
	// The membership decides which hosts pg_hba.conf allows; reconcile rewrites
	// the file on the next tick.
	srv.OnPeersChange = func(peers map[string]string) {
		hosts, err := hostsOf(peers)
		if err != nil {
			log.Error().Err(err).Msg("membership")
			return
		}
		pg.SetHosts(hosts)
	}

	// The LDAP bind password comes from OpenBao when configured; it is read
	// once, at startup — a change applies at the next restart of the agent.
	ldapPassword := ""
	if bao != nil && rc.BaoLDAPPasswordPath != "" {
		if ldapPassword, err = bao.readPassword(cmd.Context(), rc.BaoLDAPPasswordPath); err != nil {
			return err
		}
	}

	registry := prometheus.NewRegistry()
	registry.MustRegister(metrics.New(pg.Connect), srv.Metrics())
	if bao != nil {
		registry.MustRegister(bao) // tembo_bao_password_*
	}

	// The API and the proxies restrict themselves to the address list
	// pg_hba.conf is written from: the loopback (the CLI), the nodes (raft,
	// the proxies themselves) and the hba setting. watchAllowlist rebuilds
	// the list when the membership or the setting changes.
	var allowed atomic.Pointer[addrlist.List]
	allowed.Store(allowlist(peers, nil))

	// Authentication covers every client; the membership is exempt on the paths
	// the nodes use among themselves only, see nodeTraffic. peerAddrs is the
	// hosts of the current membership, rebuilt together with the allowlist.
	var peerAddrs atomic.Pointer[addrlist.List]
	peerAddrs.Store(peerlist(peers))
	authn, err := auth.New(auth.Config{
		JWTKeyFile:           rc.AuthJWTKeyFile,
		JWTJWKS:              rc.AuthJWTJWKS,
		JWTIssuer:            rc.AuthJWTIssuer,
		JWTAudience:          rc.AuthJWTAudience,
		Krb5Keytab:           rc.AuthKrb5Keytab,
		LDAPURL:              rc.AuthLDAPURL,
		LDAPDNTemplate:       rc.AuthLDAPDNTemplate,
		LDAPBindDN:           rc.AuthLDAPBindDN,
		LDAPBindPassword:     ldapPassword,
		LDAPBindPasswordFile: rc.AuthLDAPBindPasswordFile,
		LDAPBaseDN:           rc.AuthLDAPBaseDN,
		LDAPFilter:           rc.AuthLDAPFilter,
		MTLS:                 rc.TLSCAFile != "",
		ReadOnly:             rc.AuthReadOnly,
	})
	if err != nil {
		return err
	}

	if rc.ShellKrb5SPN != "" {
		if rc.AuthKrb5Keytab == "" {
			return errors.New("--shell-krb5-spn needs --auth-krb5-keytab")
		}
		k, err := newShellKerberos(rc.AuthKrb5Keytab, rc.ShellKrb5Principal, rc.ShellKrb5SPN)
		if err != nil {
			return fmt.Errorf("shell: %w", err)
		}
		pg.shell = k
	}
	mux := http.NewServeMux()
	mux.Handle("/", srv.Handler())
	mux.Handle("GET /metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{ErrorHandling: promhttp.ContinueOnError}))
	// In Kubernetes mode the configmap and the secret are the configuration:
	// PATCH /config and /settings, and with them the config and settings CLI,
	// are off.
	managed := len(rc.K8s) > 0
	configHandlers(mux, srv, st, pg.validateSettings, managed)
	settingsHandlers(mux, srv, st, managed)
	passwordHandlers(mux, srv)
	opsHandlers(mux, st, pg, rc.ID)

	httpSrv := &http.Server{Handler: allowAPI(mux, &allowed)}
	if authn != nil {
		httpSrv.Handler = allowAPI(probes(nodeTraffic(authn.Wrap(mux, nil), mux, &peerAddrs), mux), &allowed)
	}
	// Outside everything else, so that the refusals of the allowlist and of
	// the authentication are logged too, with the actor they name.
	httpSrv.Handler = audit.Middleware(httpSrv.Handler)
	serveErr := make(chan error, 1)
	go func() { serveErr <- httpSrv.Serve(apiLn) }()

	srv.Start()
	log.Info().Str("id", rc.ID).Bool("standalone", standalone).Stringer("api", apiLn.Addr()).Stringer("clients", proxyLn.Addr()).
		Stringer("readers", readerLn.Addr()).Str("db", rc.DB).Msg("node started")

	// The drains read the log FIFOs until the shutdown closes them; any
	// other way they stop fails the group, whose cancelled context runs the
	// shutdown below: nothing else drains the FIFOs, and a full one blocks
	// the logging collector — and every backend that logs — so the agent
	// takes Postgres down with itself and exits instead.
	drains, drainCtx := errgroup.WithContext(cmd.Context())
	ctx, stopPG := context.WithCancel(drainCtx)
	drains.Go(func() error { return tailFIFO(ctx, plainLog, newLineLog("postgres")) })
	drains.Go(func() error { return tailCSV(ctx, csvLog) })

	var wg sync.WaitGroup
	if managed {
		kw := newKubeWatcher(srv, st, pg)
		// Set before Watch runs: gaining leadership re-applies the objects. It
		// runs apart from the poll loop, which must not stall on the proposal.
		srv.OnLeaderChange = func() { go kw.apply(ctx) }
		wg.Go(func() { kw.watch(ctx, kube) })
	}
	wg.Go(func() { srv.Watch(ctx) })
	wg.Go(func() { reconcile(ctx, srv, st, pg, bao) })
	wg.Go(func() { leaveWhenRemoved(ctx, srv, stopPG) })
	px, readers := proxy.New(), proxy.New()
	wg.Go(func() { watchAllowlist(ctx, srv, st, &allowed, &peerAddrs, px, readers) })
	wg.Go(func() { route(ctx, srv, st, px, readers) })
	wg.Go(func() { px.Serve(proxyLn) })
	wg.Go(func() { readers.Serve(readerLn) })
	wg.Go(func() {
		supervise(ctx, func(ctx context.Context) error {
			if pg.Config().Hash == "" {
				cfg, err := loadConfig(st)
				if err != nil {
					return err
				}
				pg.SetConfig(cfg)
			}
			// A standby of a foreign cluster never becomes a primary: there is
			// no local primary for it, and its upstream is fixed.
			if rc.StandbyOf != "" {
				log.Info().Str("upstream", rc.StandbyOf).Msg("starting postgres as a standby of a foreign host")
				return pg.Run(ctx, rc.StandbyOf, false)
			}
			primary, err := srv.WaitPrimary(ctx)
			if err != nil {
				return err
			}
			if primary == rc.ID {
				if !srv.HasQuorum() {
					return errors.New("no quorum, not starting as primary")
				}
				if target := pendingSwitchover(st); target != "" {
					return fmt.Errorf("switchover to %s in progress, not starting as primary", target)
				}
				log.Info().Msg("starting postgres as primary")
				return pg.Run(ctx, "", false)
			}
			// A cascading standby replicates from another member; its slot is
			// on the primary, so cloning from it uses no slot.
			upstream, slot := primary, true
			if rc.Follow != "" && rc.Follow != primary {
				upstream, slot = rc.Follow, false
			}
			host, err := hostOf(srv.Peers(), upstream)
			if err != nil {
				return err
			}
			log.Info().Str("upstream", upstream).Msg("starting postgres as standby")
			return pg.Run(ctx, host, slot)
		})
	})

	select {
	case <-ctx.Done():
		log.Info().Msg("shutting down")
	case err := <-serveErr:
		// The tail of the shutdown returns the error of the log drains, so
		// the one that ended the api server is only logged here.
		log.Error().Err(err).Msg("the api server failed")
	}

	// The first signal stopped the loops, which shut Postgres down cleanly. A
	// second signal, or shutdownTimeout, gives up on that and kills it. The
	// hurry channel holds one signal; the stop it triggers runs once. Further
	// signals are not acted on here: the NotifyContext in main keeps capturing
	// them until this function returns, so they do not kill the agent either.
	hurry := make(chan os.Signal, 1)
	signal.Notify(hurry, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(hurry)

	stopPG()
	_, _ = proxyLn.Close(), readerLn.Close() // no new clients
	stopped := make(chan struct{})
	go func() { wg.Wait(); close(stopped) }()
	select {
	case <-stopped:
	case s := <-hurry:
		log.Warn().Str("signal", s.String()).Msg("shutdown hurried, stopping postgres immediately")
		stopNow(pg, stopped)
	case <-time.After(shutdownTimeout):
		log.Warn().Stringer("after", shutdownTimeout).Msg("clean stop takes too long, stopping postgres immediately")
		stopNow(pg, stopped)
	}
	px.SetTargets(nil) // close forwarded connections
	readers.SetTargets(nil)
	srv.Stop()
	// Inbound raft handlers write to the store, whose deferred Close runs
	// once this returns: waiting longer than any handler can plausibly take
	// keeps the two apart.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := httpSrv.Shutdown(shutdownCtx); err != nil {
		log.Warn().Err(err).Msg("stopping the api: a handler is still running against the closing store")
	}
	// Postgres is down: closing the log files ends the drains, and one that
	// stopped for another reason — that is what ran this shutdown — is the
	// error the agent exits with.
	_, _ = plainLog.Close(), csvLog.Close()
	return drains.Wait()
}

// parsePeers parses the --peers list. An item is id=host:port, or a bare
// host:port, which names the node after its host — dashes become
// underscores, the same rule the hostname default of --id follows — so
// tembo-a:8001 is node tembo_a. Hosts that do not sanitize into a node id
// (an IP, dots, uppercase) need the explicit id= form.
func parsePeers(s string) (map[string]string, error) {
	if s == "" {
		return nil, errors.New("--peers is required, e.g. a=127.0.0.1:8001,b=127.0.0.1:8002,c=127.0.0.1:8003")
	}
	peers := make(map[string]string)
	for item := range strings.SplitSeq(s, ",") {
		item = strings.TrimSpace(item)
		id, addr, ok := strings.Cut(item, "=")
		if !ok {
			// A bare host:port: the host names the node.
			host, _, err := net.SplitHostPort(item)
			if err != nil {
				return nil, fmt.Errorf("bad peer %q, want id=host:port or host:port", item)
			}
			id, addr = nodeIDFromHost(host), item
		} else if id == "" || addr == "" {
			return nil, fmt.Errorf("bad peer %q, want id=host:port or host:port", item)
		}
		if !nodeID.MatchString(id) {
			return nil, fmt.Errorf("bad peer id %q, want [a-z0-9_] only: it names a replication slot; write id=host:port", id)
		}
		if _, dup := peers[id]; dup {
			return nil, fmt.Errorf("duplicate peer %q", id)
		}
		peers[id] = addr
	}
	return peers, nil
}

// nodeIDFromHost turns a host into a node id the way the hostname default
// of --id does: tembo-a becomes tembo_a. What does not fit a slot name
// stays what it is and fails the nodeID check with a hint.
func nodeIDFromHost(host string) string {
	return strings.NewReplacer("-", "_").Replace(host)
}

// stopNow stops Postgres without a checkpoint and waits a little for the loops
// to notice, for a shutdown that cannot wait any longer.
func stopNow(pg *Instance, stopped <-chan struct{}) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := pg.Stop(ctx, "immediate"); err != nil {
		log.Error().Err(err).Msg("stopping postgres")
	}
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
	}
}

// hostOf returns the host of node id, taken from its membership address.
func hostOf(peers map[string]string, id string) (string, error) {
	host, _, err := net.SplitHostPort(peers[id])
	if err != nil {
		return "", fmt.Errorf("node %q: %w", id, err)
	}
	return host, nil
}

// hostsOf returns the hosts of every member, for pg_hba.conf.
func hostsOf(peers map[string]string) ([]string, error) {
	hosts := make([]string, 0, len(peers))
	for id := range peers {
		host, err := hostOf(peers, id)
		if err != nil {
			return nil, err
		}
		hosts = append(hosts, host)
	}
	return hosts, nil
}

// leaveWhenRemoved stops the agent once the cluster has dropped this node;
// cancelling ctx stops its Postgres with it, through the usual shutdown.
func leaveWhenRemoved(ctx context.Context, srv *agent.Server, stop context.CancelFunc) {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		if srv.Removed() {
			log.Warn().Msg("the cluster dropped this node, exiting")
			stop()
			return
		}
	}
}

// pendingSwitchover returns the switchover target, unless the switchover is
// scheduled for later: until its time comes the primary keeps serving.
func pendingSwitchover(st *store.Store) string {
	target, err := st.Get(agent.KeySwitchover)
	if err != nil {
		return ""
	}
	if at, err := st.Get(agent.KeySwitchoverAt); err == nil {
		if when, perr := time.Parse(time.RFC3339, at); perr == nil && time.Now().Before(when) {
			return ""
		}
	}
	return target
}

// route points the proxies at the cluster once a second: the clients of
// --listen go to the primary, the readers of --listen-ro are spread over the
// standbys, or go to the primary when the cluster has none. Both are refused
// while this node sees no quorum, so an isolated node does not send clients to
// a primary that is being fenced, while a switchover stops the primary, and
// while the primary is still in recovery after a failover. It runs apart from
// reconcile, which can be busy promoting or restarting Postgres.
func route(ctx context.Context, srv *agent.Server, st *store.Store, px, readers *proxy.Proxy) {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		var primaryTarget, standbyTargets []string
		primary, err := st.Get(agent.KeyPrimary)
		switchover := pendingSwitchover(st)
		nodes := srv.Nodes()
		peers := srv.Peers()
		if err == nil && switchover == "" && srv.HasQuorum() {
			if host, err := hostOf(peers, primary); err == nil && nodes[primary].Running && !nodes[primary].InRecovery {
				primaryTarget = []string{net.JoinHostPort(host, strconv.Itoa(rc.PGPort))}
			}
			for _, id := range slices.Sorted(maps.Keys(nodes)) {
				host, err := hostOf(peers, id)
				if err == nil && nodes[id].Running && nodes[id].InRecovery {
					standbyTargets = append(standbyTargets, net.JoinHostPort(host, strconv.Itoa(rc.PGPort)))
				}
			}
			if len(standbyTargets) == 0 {
				standbyTargets = primaryTarget // no standby: readers go to the primary
			}
		}
		if px.SetTargets(primaryTarget) {
			log.Info().Strs("targets", primaryTarget).Msg("proxy: clients go to")
		}
		if readers.SetTargets(standbyTargets) {
			log.Info().Strs("targets", standbyTargets).Msg("proxy: readers go to")
		}
	}
}

// allowlist builds the address list the API and the proxies restrict
// themselves to: the loopback (the CLI works locally), the nodes of the
// cluster (raft, the proxies dialing Postgres) and the addresses of the hba
// setting's host lines — the same list pg_hba.conf is written from.
func allowlist(peers map[string]string, hba []string) *addrlist.List {
	addrs := []string{"127.0.0.1", "::1"}
	for _, addr := range peers {
		if host, _, err := net.SplitHostPort(addr); err == nil {
			addrs = append(addrs, host)
		}
	}
	addrs = append(addrs, hbaAddresses(hba)...)
	// A name that resolves nowhere matches nothing; skip it instead of
	// dropping the whole list.
	valid := make([]string, 0, len(addrs))
	for _, a := range addrs {
		if _, err := addrlist.Parse([]string{a}); err != nil {
			log.Warn().Err(err).Str("address", a).Msg("allowlist: skipping an address")
			continue
		}
		valid = append(valid, a)
	}
	l, err := addrlist.Parse(valid)
	if err != nil { // a name stopped resolving between the checks
		l, _ = addrlist.Parse([]string{"127.0.0.1", "::1"})
	}
	return l
}

// hbaAddresses extracts the address fields of the host lines of the hba
// setting: "all" becomes the two wildcard networks, local lines and the
// samehost/samenet keywords match no address of another machine.
func hbaAddresses(lines []string) []string {
	var out []string
	for _, line := range lines {
		f := strings.Fields(line)
		if len(f) < 5 || f[0] == "local" {
			continue
		}
		switch f[3] {
		case "all":
			out = append(out, "0.0.0.0/0", "::/0")
		case "samehost", "samenet":
		default:
			out = append(out, f[3])
		}
	}
	return out
}

// watchAllowlist rebuilds the address list once a second when the membership
// or the hba setting changed, and hands it to the API and the proxies. nodes
// gets the hosts of the membership alone, for the authentication exemptions.
func watchAllowlist(ctx context.Context, srv *agent.Server, st *store.Store, cur, nodes *atomic.Pointer[addrlist.List], proxies ...*proxy.Proxy) {
	fingerprint := ""
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		peers := srv.Peers()
		hba := agent.LoadSettings(st).HBA
		key := fmt.Sprintf("%v|%v", peers, hba)
		if key == fingerprint {
			continue
		}
		fingerprint = key
		l := allowlist(peers, hba)
		cur.Store(l)
		nodes.Store(peerlist(peers))
		for _, p := range proxies {
			p.SetAllowed(l)
		}
	}
}

// peerlist builds the address list of the cluster nodes' hosts alone: the
// traffic between the nodes is trusted without API credentials.
func peerlist(peers map[string]string) *addrlist.List {
	valid := make([]string, 0, len(peers))
	for _, addr := range peers {
		host, _, err := net.SplitHostPort(addr)
		if err != nil {
			continue
		}
		if _, err := addrlist.Parse([]string{host}); err != nil {
			log.Warn().Err(err).Str("address", host).Msg("peerlist: skipping an address")
			continue
		}
		valid = append(valid, host)
	}
	l, err := addrlist.Parse(valid)
	if err != nil { // a name stopped resolving between the checks
		return nil
	}
	return l
}

// listenAPI opens the API listener on the port, TLS-wrapped when cfg is not
// nil.
func listenAPI(port string, cfg *tls.Config) (net.Listener, error) {
	ln, err := net.Listen("tcp", net.JoinHostPort("", port))
	if err != nil {
		return nil, err
	}
	if cfg != nil {
		ln = tls.NewListener(ln, cfg)
	}
	return ln, nil
}

// validatePGAuth checks the Postgres authentication modes and their Kerberos
// credentials at startup, instead of failing the first connection.
func validatePGAuth() error {
	switch rc.PGLocalAuth {
	case "peer", "password", "gss":
	default:
		return fmt.Errorf("bad --pg-local-auth %q, want peer, password or gss", rc.PGLocalAuth)
	}
	switch rc.PGHostAuth {
	case "scram-sha-256", "ldap", "gss":
	default:
		return fmt.Errorf("bad --pg-host-auth %q, want scram-sha-256, ldap or gss", rc.PGHostAuth)
	}
	if rc.PGHostAuth == "ldap" && rc.PGHostAuthOpts == "" {
		return errors.New(`--pg-host-auth ldap requires --pg-host-auth-opts, e.g. "ldapbasedn=dc=example,dc=com"`)
	}
	// The local TCP line of pg_hba gets the method of the nodes: a password
	// cannot satisfy gss, Kerberos cannot satisfy ldap or scram-sha-256.
	if rc.PGLocalAuth == "password" && rc.PGHostAuth == "gss" {
		return errors.New("--pg-local-auth password needs a password-based --pg-host-auth (scram-sha-256 or ldap)")
	}
	if rc.PGLocalAuth == "gss" && rc.PGHostAuth != "gss" {
		return errors.New("--pg-local-auth gss needs --pg-host-auth gss")
	}
	if rc.PGLocalAuth == "gss" || rc.PGHostAuth == "gss" {
		if rc.PGKrb5Keytab != "" {
			if _, err := os.Stat(rc.PGKrb5Keytab); err != nil {
				return fmt.Errorf("--pg-krb5-keytab: %w", err)
			}
		} else if _, err := os.Stat(ccachePath()); err != nil {
			return fmt.Errorf("gss needs --pg-krb5-keytab or a credential cache: %w", err)
		}
	}
	return nil
}

// apiTLSConfig builds the server TLS configuration of the API port; nil, nil
// keeps the API plain HTTP. With a CA file the listener verifies a client
// certificate it is given: the nodes connect without one, clients may
// authenticate with one.
func apiTLSConfig() (*tls.Config, error) {
	if (rc.TLSCertFile == "") != (rc.TLSKeyFile == "") {
		return nil, errors.New("--tls-cert-file and --tls-key-file go together")
	}
	if rc.TLSCertFile == "" {
		if rc.TLSCAFile != "" {
			return nil, errors.New("--tls-ca-file requires --tls-cert-file and --tls-key-file")
		}
		return nil, nil
	}
	cert, err := tls.LoadX509KeyPair(rc.TLSCertFile, rc.TLSKeyFile)
	if err != nil {
		return nil, err
	}
	cfg := &tls.Config{Certificates: []tls.Certificate{cert}}
	if rc.TLSCAFile != "" {
		pool, err := loadCertPool(rc.TLSCAFile)
		if err != nil {
			return nil, err
		}
		cfg.ClientCAs = pool
		cfg.ClientAuth = tls.VerifyClientCertIfGiven
	}
	return cfg, nil
}

// loadCertPool reads a PEM CA bundle; an empty path returns nil, meaning the
// system pool.
func loadCertPool(path string) (*x509.CertPool, error) {
	if path == "" {
		return nil, nil
	}
	pem, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("no certificates in %s", path)
	}
	return pool, nil
}

// allowAPI restricts the API to the address list pg_hba.conf is written
// from: the loopback (the CLI), the nodes (raft) and the hba setting.
// nodeTraffic lets the membership reach the two paths the nodes talk over
// without credentials: the raft RPCs and the status polls. Anything else from a
// member address authenticates like any client. The CLI on a node reaches the
// API by the node's own address, which is a member address, so exempting the
// whole API by address would let it skip authentication.
func nodeTraffic(authed, mux http.Handler, peers *atomic.Pointer[addrlist.List]) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		host, _, err := net.SplitHostPort(r.RemoteAddr)
		if err != nil {
			host = r.RemoteAddr
		}
		member := false
		if l := peers.Load(); l != nil {
			member = l.Allows(net.ParseIP(host))
		}
		internal := strings.HasPrefix(r.URL.Path, "/raft/") || (r.Method == http.MethodGet && r.URL.Path == "/status")
		if member && internal {
			mux.ServeHTTP(w, r)
			return
		}
		authed.ServeHTTP(w, r)
	})
}

// probes lets GET /health through without credentials. It tells only whether
// the node's Postgres answers SQL, and supervisors run `tembo health` with no
// credentials at all. Everything else goes through authed.
func probes(authed, mux http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Path == "/health" {
			mux.ServeHTTP(w, r)
			return
		}
		authed.ServeHTTP(w, r)
	})
}

func allowAPI(next http.Handler, cur *atomic.Pointer[addrlist.List]) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		host, _, err := net.SplitHostPort(r.RemoteAddr)
		if err != nil {
			host = r.RemoteAddr
		}
		if l := cur.Load(); l != nil && !l.Allows(net.ParseIP(host)) {
			http.Error(w, "the address is not on the hba list", http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// reconcile brings the running Postgres in line with the cluster state once a
// second: it promotes, fences or repoints the local server and rolls out the
// cluster's configuration. Starting a stopped server is left to supervise.
func reconcile(ctx context.Context, srv *agent.Server, st *store.Store, pg *Instance, bao *baoClient) {
	var failed, failure string // configuration that could not be applied here, and why
	var ticks int
	var responsive bool     // postgres answered SQL since its current start
	var hungSince time.Time // the postmaster is alive but answers no SQL since then
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		ticks++
		state, conninfo, slotName := pg.Status(ctx)
		if !state.Running {
			// A switchover waits for this: it promotes a standby that has the
			// WAL up to where this server stopped.
			state.ShutdownLSN = pg.ShutdownLSN(ctx)
		}
		switch {
		case state.Running:
			responsive, hungSince = true, time.Time{}
		case !pg.running(ctx):
			responsive, hungSince = false, time.Time{} // a new postmaster starts clean
		case responsive:
			// The postmaster lives but answers no SQL; a server in crash
			// recovery never answered since its start and is left alone.
			if hungSince.IsZero() {
				hungSince = time.Now()
			} else if since := time.Since(hungSince); since > hungTimeout {
				log.Error().Stringer("unresponsive", since).Msg("postgres accepts no queries, restarting it")
				if err := pg.Stop(ctx, "immediate"); err != nil {
					log.Error().Err(err).Msg("stopping the hung postgres")
				}
				responsive, hungSince = false, time.Time{}
			}
		}
		cfg, err := loadConfig(st)
		if err != nil {
			log.Error().Err(err).Msg("reconcile")
			continue
		}
		settings := agent.LoadSettings(st)
		// A rotation replaces the startup password on every node but a
		// standby of a foreign cluster: its upstream keeps its own password,
		// and the local one is none of this cluster's business. OpenBao is
		// the source of truth when the integration is on — polled every
		// fifth tick — else the cluster's store, written by PUT /password.
		if rc.StandbyOf == "" {
			switch {
			case rc.BaoPasswordPath != "" && ticks%5 == 0:
				if pw, err := bao.readPassword(ctx, rc.BaoPasswordPath); err != nil {
					log.Error().Err(err).Msg("password")
				} else if pw != "" && pw != pg.ActivePassword() {
					log.Warn().Msg("password: OpenBao has a new password, switching to it")
					pg.SetPassword(pw)
				}
			case rc.BaoPasswordPath == "":
				if pw, err := st.Get(agent.KeyPassword); err == nil && pw != "" && pw != pg.ActivePassword() {
					log.Warn().Msg("password: the cluster rotated its password, switching to it")
					pg.SetPassword(pw)
				}
			}
		}
		// The hba setting adds lines to pg_hba.conf; the file is rewritten
		// below on a change.
		pg.SetExtraHBA(settings.HBA)
		// The addresses of the nodes can change, and pg_hba.conf lists them;
		// a membership change rewrites it on the next tick already.
		if state.Running && (ticks%30 == 0 || pg.HostsPending()) {
			if changed, err := pg.RefreshHBA(ctx); err != nil {
				log.Error().Err(err).Msg("refreshing pg_hba.conf")
			} else if changed {
				log.Info().Msg("pg_hba.conf refreshed")
			}
		}

		primary, primaryErr := st.Get(agent.KeyPrimary)
		// A stopped standby keeps its hot standby limits: started with lower
		// ones, it could replay a record with the primary's former values and
		// pause. It lowers them while running, see caughtUp.
		if !state.Running && primaryErr == nil && cfg.Hash != failed && cfg.Hash != pg.Config().Hash &&
			(primary == rc.ID || !lowersLimit(cfg.Settings, limitsOf(pg.Config().Settings))) {
			pg.SetConfig(cfg) // a stopped server takes it when it starts
		}
		state.Config = pg.Config().Hash
		if cfg.Hash == failed {
			state.ConfigError = failure
		}
		// A primary publishes its synchronous standbys: at a failover the
		// leader prefers them over standbys that may miss committed data.
		state.SyncStandbys = pg.SyncStandbys()
		state.NoFailover = rc.NoFailover || rc.StandbyOf != ""
		state.NoSync = rc.NoSync
		state.FailoverPriority = rc.FailoverPriority
		srv.SetPostgres(state)

		if primaryErr != nil || !state.Running {
			continue
		}
		peers := srv.Peers()
		// A cascading standby follows its member instead of the primary.
		upstream := primary
		if rc.Follow != "" && rc.Follow != primary {
			upstream = rc.Follow
		}
		host, err := hostOf(peers, upstream)
		if err != nil {
			log.Error().Err(err).Msg("reconcile")
			continue
		}
		standbyOf := host
		switch {
		case primary == rc.ID:
			standbyOf = ""
		case rc.StandbyOf != "":
			standbyOf = rc.StandbyOf // the fixed upstream, for the config rollout
		}
		// The replication slot of this node lives on the primary of the own
		// cluster; a cascaded or foreign upstream has no slot of ours.
		slot := rc.StandbyOf == "" && upstream == primary
		wantSlot := ""
		if slot && standbyOf != "" {
			wantSlot = pg.slotName()
		}
		nodes := srv.Nodes()
		switchover := pendingSwitchover(st)

		// The primary keeps a replication slot per other node, so a node that
		// is down for a while still finds the WAL it needs when it returns.
		// Before the switch: the state is fresh, and a promotion this tick is
		// followed on the next one.
		if primary == rc.ID && !state.InRecovery {
			if err := pg.ensureRoles(ctx); err != nil {
				log.Error().Err(err).Msg("replication role")
			}
			// Roles are replicated: altering them here reaches the standbys
			// through WAL replay, see ApplyPassword.
			if err := pg.ApplyPassword(ctx); err != nil {
				log.Error().Err(err).Msg("password")
			}
			if err := pg.syncSlots(ctx, peerIDs(peers)); err != nil {
				log.Error().Err(err).Msg("replication slots")
			}
			// synchronous_standby_names follows the standbys that really
			// stream, identified by their application_name.
			streaming, err := pg.streamingStandbys(ctx)
			if err != nil {
				log.Error().Err(err).Msg("synchronous standbys")
			} else if pg.SetSyncStandbys(settings.SynchronousMode, settings.SynchronousNodeCount,
				syncCandidates(nodes, streaming, rc.ID)) {
				log.Info().Str("synchronous_standby_names", pg.syncStandbyNames()).Msg("synchronous standbys changed")
				if err := pg.Follow(ctx, "", false); err != nil { // rewrites and reloads the configuration
					log.Error().Err(err).Msg("synchronous standbys")
				}
			}
		}

		switch {
		case primary != rc.ID && !state.InRecovery:
			log.Warn().Str("primary", primary).Msg("another node is the primary, stopping postgres")
			err = pg.Stop(ctx, "immediate")
		case primary != rc.ID && rc.StandbyOf == "" && (!pg.Follows(host, conninfo) || slotName != wantSlot):
			// A standby-of node keeps its fixed upstream whatever the primary is.
			log.Info().Str("primary", primary).Msg("following the primary")
			err = pg.Follow(ctx, host, slot)
		case primary == rc.ID && !state.InRecovery && switchover != "":
			log.Warn().Str("target", switchover).Msg("switchover: stopping postgres")
			err = pg.Stop(ctx, "fast")
		case primary == rc.ID && !state.InRecovery && !srv.HasQuorum():
			log.Warn().Msg("no quorum, stopping postgres")
			err = pg.Stop(ctx, "immediate")
		case primary == rc.ID && state.InRecovery:
			if srv.HasQuorum() {
				log.Warn().Msg("promoting postgres")
				err = pg.Promote(ctx)
			}
		case switchover == "" && cfg.Hash != state.Config && cfg.Hash != failed && rolloutTurn(rc.ID, primary, cfg, nodes) &&
			(standbyOf == "" || !lowersLimit(cfg.Settings, state.Limits) || caughtUp(ctx, pg, nodes[primary])):
			log.Info().Str("config", cfg.Hash).Msg("applying configuration")
			if err = pg.Apply(ctx, cfg, standbyOf, slot); err != nil {
				failed, failure = cfg.Hash, err.Error()
				err = fmt.Errorf("configuration %s: %w", cfg.Hash, err)
			} else {
				log.Info().Str("config", cfg.Hash).Msg("configuration applied")
			}
		}
		if err != nil {
			log.Error().Err(err).Msg("reconcile")
		}
	}
}

// peerIDs returns the ids of the other nodes, sorted.
func peerIDs(peers map[string]string) []string {
	ids := make([]string, 0, len(peers))
	for id := range peers {
		if id != rc.ID {
			ids = append(ids, id)
		}
	}
	slices.Sort(ids)
	return ids
}

// caughtUp reports whether the standby has replayed the primary's WAL up to
// the position the primary last reported. A standby lowers a hot standby limit
// only then: a record with the primary's former, higher value still ahead in
// replay would pause its recovery.
func caughtUp(ctx context.Context, pg *Instance, primary agent.Postgres) bool {
	if !primary.Running || primary.InRecovery {
		return false
	}
	lsn, err := pg.ReplayLSN(ctx)
	if err != nil {
		log.Error().Err(err).Msg("reconcile")
		return false
	}
	return lsn >= primary.LSN
}

// supervise keeps Postgres running until ctx is done. The agent stays in the
// cluster while its Postgres is down, so a failed start is retried, not fatal.
func supervise(ctx context.Context, start func(context.Context) error) {
	for {
		err := start(ctx)
		if ctx.Err() != nil {
			return
		}
		log.Warn().Err(err).Stringer("restart_in", restartDelay).Msg("postgres exited")
		select {
		case <-ctx.Done():
			return
		case <-time.After(restartDelay):
		}
	}
}
