package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/ks-tool/tembo/internal/agent"
	"github.com/ks-tool/tembo/internal/store"

	"github.com/spf13/cobra"
)

// knownSettings validates and normalizes the value of each behavior setting
// of the agents; an unknown name is rejected.
var knownSettings = map[string]func(string) (any, error){
	"max-lag-on-failover":    func(s string) (any, error) { return parseSize(s) },
	"synchronous_mode":       parseSyncMode,
	"synchronous_node_count": parseCount,
	"paused":                 parseBool,
	"hba":                    parseHBA,
}

// hostnameRe matches a pg_hba address that is neither an IP nor a CIDR and so
// must be a hostname.
var hostnameRe = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._-]*$`)

// hbaTypes are the connection types of a pg_hba.conf line.
var hbaTypes = map[string]bool{
	"local": true, "host": true, "hostssl": true, "hostnossl": true,
	"hostgssenc": true, "hostnogssenc": true,
}

// hbaAuthMethods are the authentication methods of a pg_hba.conf line.
var hbaAuthMethods = map[string]bool{
	"trust": true, "reject": true, "scram-sha-256": true, "md5": true,
	"password": true, "gss": true, "sspi": true, "ident": true, "peer": true,
	"cert": true, "pam": true, "ldap": true, "radius": true,
}

// parseHBA validates hba: pg_hba.conf lines, newline separated. Names in the
// address field are not resolved here: name resolution is each node's own
// business.
func parseHBA(s string) (any, error) {
	var out []string
	for line := range strings.Lines(s) {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if err := validHBALine(line); err != nil {
			return nil, err
		}
		out = append(out, line)
	}
	return out, nil
}

// validHBALine checks one pg_hba.conf line: a connection type, database,
// user, an address unless local, an auth method and its options.
func validHBALine(line string) error {
	f := strings.Fields(line)
	if len(f) < 4 {
		return fmt.Errorf("bad pg_hba line %q", line)
	}
	if !hbaTypes[f[0]] {
		return fmt.Errorf("bad connection type %q in %q", f[0], line)
	}
	auth := 3 // local database user auth [options]
	if f[0] != "local" {
		auth = 4 // host database user address auth [options]
		if len(f) <= auth {
			return fmt.Errorf("pg_hba line %q has no address or auth method", line)
		}
		switch addr := f[3]; addr {
		case "all", "samehost", "samenet": // keywords, not addresses
		default:
			if net.ParseIP(addr) == nil {
				if _, _, err := net.ParseCIDR(addr); err != nil && !hostnameRe.MatchString(addr) {
					return fmt.Errorf("bad address %q in %q, want an IP, a CIDR or a hostname", addr, line)
				}
			}
		}
	}
	if len(f) <= auth {
		return fmt.Errorf("pg_hba line %q has no auth method", line)
	}
	if !hbaAuthMethods[f[auth]] {
		return fmt.Errorf("bad auth method %q in %q", f[auth], line)
	}
	for _, opt := range f[auth+1:] {
		if !strings.Contains(opt, "=") {
			return fmt.Errorf("bad auth option %q in %q, want name=value", opt, line)
		}
	}
	return nil
}

// parseBool validates paused: a boolean.
func parseBool(s string) (any, error) {
	b, err := strconv.ParseBool(strings.TrimSpace(s))
	if err != nil {
		return false, fmt.Errorf("bad value %q, want true or false", s)
	}
	return b, nil
}

// sizeRe matches a byte size: a number with an optional kB/MB/GB/TB suffix,
// 1024-based as in postgresql.conf. A bare number is bytes.
var sizeRe = regexp.MustCompile(`^([0-9]+)\s*(b|kb|mb|gb|tb)?$`)

func parseSize(s string) (uint64, error) {
	m := sizeRe.FindStringSubmatch(strings.ToLower(strings.TrimSpace(s)))
	if m == nil {
		return 0, fmt.Errorf("bad size %q, want e.g. 1048576 or 16MB", s)
	}
	n, err := strconv.ParseUint(m[1], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("bad size %q: %v", s, err)
	}
	shift := map[string]uint{"": 0, "b": 0, "kb": 10, "mb": 20, "gb": 30, "tb": 40}[m[2]]
	if n > math.MaxUint64>>shift {
		return 0, fmt.Errorf("bad size %q: too big", s)
	}
	return n << shift, nil
}

// parseSyncMode validates synchronous_mode: off, quorum or strict.
func parseSyncMode(s string) (any, error) {
	switch s {
	case agent.SyncOff, agent.SyncQuorum, agent.SyncStrict:
		return s, nil
	}
	return nil, fmt.Errorf("bad mode %q, want off, quorum or strict", s)
}

// parseCount validates synchronous_node_count: a positive number.
func parseCount(s string) (any, error) {
	n, err := strconv.ParseUint(strings.TrimSpace(s), 10, 64)
	if err != nil || n == 0 {
		return 0, fmt.Errorf("bad count %q, want a positive number", s)
	}
	return n, nil
}

// settingsHandlers serve the behavior settings of the agents: GET shows them,
// PATCH changes them. Values are validated and normalized before they are
// committed; a null resets a setting to its default. In Kubernetes mode PATCH
// is off: the configmap and the secret of --k8s are the settings.
func settingsHandlers(mux *http.ServeMux, srv *agent.Server, st *store.Store, managed bool) {
	mux.HandleFunc("GET /settings", func(w http.ResponseWriter, _ *http.Request) {
		raw, err := st.Get(agent.KeySettings)
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
	mux.HandleFunc("PATCH /settings", func(w http.ResponseWriter, r *http.Request) {
		if managed {
			http.Error(w, errManagedByKube.Error(), http.StatusForbidden)
			return
		}
		var patch map[string]*string
		if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&patch); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		normalized := make(map[string]any, len(patch))
		for name, value := range patch {
			parse, ok := knownSettings[name]
			if !ok {
				http.Error(w, fmt.Sprintf("unknown setting %q", name), http.StatusBadRequest)
				return
			}
			if value == nil {
				normalized[name] = nil // a reset to the default
				continue
			}
			v, err := parse(*value)
			if err != nil {
				http.Error(w, fmt.Sprintf("setting %q: %v", name, err), http.StatusBadRequest)
				return
			}
			normalized[name] = v
		}
		data, err := json.Marshal(normalized)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		srv.Propose(w, r, store.Command{Op: "merge", Key: agent.KeySettings, Value: string(data)})
	})
}

var settingsCmd = &cobra.Command{
	Use:   "settings",
	Short: "Show or change the behavior settings of the agents",
}

var settingsShowCmd = &cobra.Command{
	Use:  "show",
	Args: cobra.NoArgs,
	RunE: settingsShowCmdRun,
}

var settingsSetCmd = &cobra.Command{
	Use:  "set name=value...",
	Args: cobra.MinimumNArgs(1),
	RunE: settingsSetCmdRun,
}

var settingsResetCmd = &cobra.Command{
	Use:  "reset name...",
	Args: cobra.MinimumNArgs(1),
	RunE: settingsResetCmdRun,
}

var setHBACmd = &cobra.Command{
	Use:   "set-hba local|host|hostssl|hostnossl|hostgssenc|hostnogssenc",
	Short: "Append a pg_hba.conf line on every node of the cluster",
	Args:  cobra.ExactArgs(1),
	RunE:  setHBACmdRun,
}

var (
	hbaDB       string
	hbaUser     string
	hbaAddr     string
	hbaAuth     string
	hbaAuthOpts []string
	hbaRemove   bool
)

func init() {
	setHBACmd.Flags().StringVar(&hbaDB, "db", "all", "databases the line matches, e.g. all or db1,db2")
	setHBACmd.Flags().StringVar(&hbaUser, "user", "all", "roles the line matches")
	setHBACmd.Flags().StringVar(&hbaAddr, "src-addr", "", "source address the line matches: an IP, a CIDR or a hostname (none for local)")
	setHBACmd.Flags().StringVar(&hbaAuth, "auth", "scram-sha-256", "authentication method")
	setHBACmd.Flags().StringArrayVar(&hbaAuthOpts, "auth-opts", nil, "authentication option name=value, repeatable")
	setHBACmd.Flags().BoolVar(&hbaRemove, "remove", false, "remove the matching line instead of adding it")
	settingsCmd.AddCommand(settingsShowCmd, settingsSetCmd, settingsResetCmd, setHBACmd)
	rootCmd.AddCommand(settingsCmd)
}

func settingsShowCmdRun(cmd *cobra.Command, _ []string) error {
	return call(cmd.Context(), http.MethodGet, "/settings", nil)
}

func settingsSetCmdRun(cmd *cobra.Command, args []string) error {
	patch := make(map[string]*string, len(args))
	for _, arg := range args {
		name, value, ok := strings.Cut(arg, "=")
		if !ok {
			return fmt.Errorf("bad setting %q, want name=value", arg)
		}
		patch[strings.ToLower(name)] = &value
	}
	return patchSettings(cmd.Context(), patch)
}

func settingsResetCmdRun(cmd *cobra.Command, args []string) error {
	patch := make(map[string]*string, len(args))
	for _, name := range args {
		patch[strings.ToLower(name)] = nil
	}
	return patchSettings(cmd.Context(), patch)
}

// setHBACmdRun builds a pg_hba.conf line from the flags and appends it to the
// hba setting. The setting is replaced as a whole: the lines are read, the
// new one is appended and the list is written back.
func setHBACmdRun(cmd *cobra.Command, args []string) error {
	typ := args[0]
	if !hbaTypes[typ] {
		return fmt.Errorf("bad connection type %q, want local, host, hostssl, hostnossl, hostgssenc or hostnogssenc", typ)
	}
	fields := []string{typ, hbaDB, hbaUser}
	if typ == "local" {
		if hbaAddr != "" {
			return errors.New("a local line matches no address, drop --src-addr")
		}
	} else {
		if hbaAddr == "" {
			return errors.New("--src-addr is required, e.g. 10.0.0.0/24")
		}
		fields = append(fields, hbaAddr)
	}
	fields = append(fields, hbaAuth)
	line := strings.Join(append(fields, hbaAuthOpts...), " ")
	if err := validHBALine(line); err != nil {
		return err
	}

	data, err := request(cmd.Context(), http.MethodGet, "/settings", nil)
	if err != nil {
		return err
	}
	var settings agent.Settings
	if err := json.Unmarshal(data, &settings); err != nil {
		return err
	}
	if hbaRemove {
		lines, err := removeHBALine(settings.HBA, line)
		if err != nil {
			return err
		}
		joined := strings.Join(lines, "\n")
		if err := patchSettings(cmd.Context(), map[string]*string{"hba": &joined}); err != nil {
			return err
		}
		fmt.Println("removed:", line)
		return nil
	}
	joined := strings.Join(append(settings.HBA, line), "\n")
	return patchSettings(cmd.Context(), map[string]*string{"hba": &joined})
}

// removeHBALine drops the first line equal to line; there is no error only
// when it was there.
func removeHBALine(lines []string, line string) ([]string, error) {
	for n, l := range lines {
		if l == line {
			return slices.Delete(slices.Clone(lines), n, n+1), nil
		}
	}
	return nil, fmt.Errorf("no such pg_hba line: %q", line)
}

func patchSettings(ctx context.Context, patch map[string]*string) error {
	body, err := json.Marshal(patch)
	if err != nil {
		return err
	}
	return call(ctx, http.MethodPatch, "/settings", body)
}
