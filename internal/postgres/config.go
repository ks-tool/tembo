package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	appconfig "github.com/ks-tool/app-config/go"
	"github.com/ks-tool/tembo/internal/agent"
	"github.com/ks-tool/tembo/internal/store"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/spf13/cobra"
)

// Config is the user's part of postgresql.conf as stored in the cluster.
type Config struct {
	Hash     string            `json:"hash"`
	Settings map[string]string `json:"settings"`
}

// keyConfig holds the user's postgresql.conf settings as a JSON object.
const keyConfig = "config"

var (
	// prefixOpts scopes the environment to the variables of tembo.
	prefixOpts = appconfig.EnvOptions{Prefix: "TEMBO_"}

	// errValidationUnavailable marks a validation that could not run because the
	// local Postgres is not up; the client can retry later.
	errValidationUnavailable = errors.New("cannot validate: postgres is not running")

	// parameterName matches a name that can be written unquoted to postgresql.conf.
	parameterName = regexp.MustCompile(`^[a-z_][a-z0-9_]*(\.[a-z_][a-z0-9_]*)?$`)
)

var (
	// blockedParameters cannot be set through the cluster's configuration.
	// Some are set or relied upon by tembo; the rest make Postgres run a
	// command or load a library of its choice — through the shared
	// configuration that is code execution as the postgres user on every
	// node, a reload away (synchronous_standby_names is blocked for the
	// same reason of being tembo's own, see SetSyncStandbys).
	blockedParameters = map[string]struct{}{
		"archive_cleanup_command":                {},
		"archive_command":                        {},
		"archive_library":                        {},
		"config_file":                            {},
		"data_directory":                         {},
		"dynamic_library_path":                   {},
		"hba_file":                               {},
		"hot_standby":                            {},
		"ident_file":                             {},
		"include":                                {},
		"include_dir":                            {},
		"include_if_exists":                      {},
		"local_preload_libraries":                {},
		"log_destination":                        {},
		"log_directory":                          {},
		"log_filename":                           {},
		"log_rotation_age":                       {},
		"log_rotation_size":                      {},
		"log_truncate_on_rotation":               {},
		"logging_collector":                      {},
		"port":                                   {},
		"primary_conninfo":                       {},
		"primary_slot_name":                      {},
		"recovery_end_command":                   {},
		"restore_command":                        {},
		"session_preload_libraries":              {},
		"shared_preload_libraries":               {},
		"ssl_passphrase_command":                 {},
		"ssl_passphrase_command_supports_reload": {},
		"synchronous_standby_names":              {},
		"unix_socket_directories":                {},
	}

	// hotStandbyLimits must be at least as high on a standby as on its primary.
	hotStandbyLimits = []string{
		"max_connections",
		"max_locks_per_transaction",
		"max_prepared_transactions",
		"max_wal_senders",
		"max_worker_processes",
	}
)

func loadConfig(st *store.Store) (Config, error) {
	raw, err := st.Get(keyConfig)
	if errors.Is(err, store.ErrNotFound) {
		raw, err = "{}", nil
	}
	if err != nil {
		return Config{}, err
	}

	cfg := Config{Settings: make(map[string]string)}
	if err = json.Unmarshal([]byte(raw), &cfg.Settings); err != nil {
		return Config{}, fmt.Errorf("stored config: %w", err)
	}

	sum := sha256.Sum256([]byte(raw))
	cfg.Hash = hex.EncodeToString(sum[:6])
	return cfg, nil
}

// rolloutTurn reports whether node id may apply cfg now. Nodes apply a
// configuration one at a time: standbys in id order, then the primary. If cfg
// lowers a hot standby limit below the primary's value, the primary goes
// first, since a standby does not run with a lower one. A node waits until
// every reachable node before it runs cfg without error.
func rolloutTurn(id, primary string, cfg Config, nodes map[string]agent.Postgres) bool {
	order := slices.DeleteFunc(slices.Sorted(maps.Keys(nodes)), func(n string) bool { return n == primary })
	if p, ok := nodes[primary]; ok {
		if lowersLimit(cfg.Settings, p.Limits) {
			order = slices.Insert(order, 0, primary)
		} else {
			order = append(order, primary)
		}
	}
	for _, n := range order {
		if n == id {
			return true
		}
		if p := nodes[n]; p.Config != cfg.Hash || p.ConfigError != "" {
			return false
		}
	}
	return false
}

// lowersLimit reports whether settings put a hot standby limit below limits.
func lowersLimit(settings map[string]string, limits map[string]int64) bool {
	for name, want := range limitsOf(settings) {
		if have, ok := limits[name]; ok && want < have {
			return true
		}
	}
	return false
}

// limitsOf returns the hot standby limits a server gets from settings.
func limitsOf(settings map[string]string) map[string]int64 {
	conf := postgresqlConf(settings)
	limits := make(map[string]int64, len(hotStandbyLimits))
	for _, name := range hotStandbyLimits {
		if v, err := strconv.ParseInt(conf[name], 10, 64); err == nil {
			limits[name] = v
		}
	}
	return limits
}

// configHandlers serve the cluster configuration: GET shows it together with
// the state of every node, PATCH changes it. validate checks the proposed
// values before they are committed; rejects other than resets are refused
// while it is unavailable. In Kubernetes mode PATCH is off: the configmap
// and the secret of --k8s are the configuration.
func configHandlers(
	mux *http.ServeMux,
	srv *agent.Server,
	st *store.Store,
	validate func(context.Context, map[string]*string) error,
	managed bool,
) {
	mux.HandleFunc("GET /config", func(w http.ResponseWriter, _ *http.Request) {
		cfg, err := loadConfig(st)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(struct {
			Config
			Nodes map[string]agent.Postgres `json:"nodes"`
		}{cfg, srv.Nodes()})
	})

	mux.HandleFunc("PATCH /config", func(w http.ResponseWriter, r *http.Request) {
		if managed {
			http.Error(w, errManagedByKube.Error(), http.StatusForbidden)
			return
		}

		var patch map[string]*string // nil resets a parameter
		if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&patch); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		for name := range patch {
			_, exists := blockedParameters[name]
			if !parameterName.MatchString(name) || exists {
				http.Error(w, fmt.Sprintf("parameter %q cannot be set", name), http.StatusBadRequest)
				return
			}
		}

		if err := validate(r.Context(), patch); err != nil {
			if errors.Is(err, errValidationUnavailable) {
				http.Error(w, err.Error(), http.StatusServiceUnavailable)
			} else {
				http.Error(w, err.Error(), http.StatusBadRequest)
			}
			return
		}

		data, err := json.Marshal(patch)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		srv.Propose(w, r, store.Command{Op: "merge", Key: keyConfig, Value: string(data)})
	})
}

// validateSettings checks a proposed configuration against the local running
// Postgres before the change reaches the cluster: set_config parses the value
// and runs its check hooks, so an unknown name or an unparsable value is
// rejected instead of wedging every node into a restart loop. Parameters gated
// to a restart answer "cannot be changed now" only after the value has parsed;
// those pass. A reset (null) is always allowed, which keeps a way out when a
// bad value already reached the cluster.
func (i *Instance) validateSettings(ctx context.Context, patch map[string]*string) error {
	var names []string
	for name, value := range patch {
		if value != nil {
			names = append(names, name)
		}
	}
	if len(names) == 0 {
		return nil
	}
	slices.Sort(names)

	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	conn, err := i.Connect(ctx)
	if err != nil {
		return fmt.Errorf("%w: %v", errValidationUnavailable, err)
	}
	defer func() { _ = conn.Close(ctx) }()

	for _, name := range names {
		var ignored string
		err := conn.QueryRow(ctx, "select set_config($1, $2, false)", name, *patch[name]).Scan(&ignored)
		if err == nil {
			continue
		}

		var pe *pgconn.PgError
		if errors.As(err, &pe) && (pe.Code == "55P02" /* cannot be changed now */ ||
			pe.Code == "0A000" /* not settable via SET */) {
			continue // the name exists and the value parsed; it applies at restart
		}
		return fmt.Errorf("parameter %q: %s", name, pgError(err))
	}
	return nil
}

// pgError renders a Postgres error the way postgres reports it in the log.
func pgError(err error) string {
	if pe, ok := errors.AsType[*pgconn.PgError](err); ok {
		if pe.Detail != "" {
			return pe.Message + ": " + pe.Detail
		}
		return pe.Message
	}
	return err.Error()
}

var configCmd = &cobra.Command{
	Use:   "config",
	Short: "Show or change postgresql.conf settings of the cluster",
}

var configShowCmd = &cobra.Command{
	Use:  "show",
	Args: cobra.NoArgs,
	RunE: configShowCmdRun,
}

var configSetCmd = &cobra.Command{
	Use:  "set name=value...",
	Args: cobra.MinimumNArgs(1),
	RunE: configSetCmdRun,
}

var configResetCmd = &cobra.Command{
	Use:  "reset name...",
	Args: cobra.MinimumNArgs(1),
	RunE: configResetCmdRun,
}

func init() {
	configCmd.AddCommand(configShowCmd, configSetCmd, configResetCmd)
	rootCmd.AddCommand(configCmd)
}

func configShowCmdRun(cmd *cobra.Command, _ []string) error {
	return call(cmd.Context(), http.MethodGet, "/config", nil)
}

func configSetCmdRun(cmd *cobra.Command, args []string) error {
	patch := make(map[string]*string, len(args))
	for _, arg := range args {
		name, value, ok := strings.Cut(arg, "=")
		if !ok {
			return fmt.Errorf("bad setting %q, want name=value", arg)
		}
		patch[strings.ToLower(name)] = &value
	}
	return patchConfig(cmd.Context(), patch)
}

func configResetCmdRun(cmd *cobra.Command, args []string) error {
	patch := make(map[string]*string, len(args))
	for _, name := range args {
		patch[strings.ToLower(name)] = nil
	}
	return patchConfig(cmd.Context(), patch)
}

func patchConfig(ctx context.Context, patch map[string]*string) error {
	body, err := json.Marshal(patch)
	if err != nil {
		return err
	}
	return call(ctx, http.MethodPatch, "/config", body)
}
