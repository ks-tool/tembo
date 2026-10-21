package postgres

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// baoClient is a minimal OpenBao (Vault) HTTP API client for one job: reading
// the cluster's passwords from a KV v2 mount, so OpenBao is the source of
// truth and an update there reaches Postgres on its own. It authenticates
// with a token, or with AppRole, logging in again when the token expires.
type baoClient struct {
	addr  string // e.g. https://bao.internal:8200
	mount string // the KV v2 mount, "secret" by default
	hc    *http.Client

	// A token, or the files of an AppRole login; roleID and secretID are
	// empty when the token is the way in.
	token    string
	roleID   string
	secretID string

	mu       sync.Mutex // guards the token and the read outcome
	lastOK   bool
	lastOKAt time.Time
}

func newBaoClient(addr, token, tokenFile, roleIDFile, secretIDFile, caFile, mount string) (*baoClient, error) {
	if addr == "" {
		return nil, errors.New("--bao-address is required for the OpenBao integration")
	}
	c := &baoClient{addr: strings.TrimRight(addr, "/"), mount: mount, hc: &http.Client{Timeout: 5 * time.Second}}
	if c.mount == "" {
		c.mount = "secret"
	}
	switch {
	case tokenFile != "":
		b, err := os.ReadFile(tokenFile)
		if err != nil {
			return nil, fmt.Errorf("--bao-token-file: %w", err)
		}
		c.token = strings.TrimSpace(string(b))
	case token != "":
		c.token = token
	case roleIDFile != "" || secretIDFile != "":
		if roleIDFile == "" || secretIDFile == "" {
			return nil, errors.New("--bao-role-id-file and --bao-secret-id-file go together, or use --bao-token")
		}
		role, err := readTrimmed(roleIDFile)
		if err != nil {
			return nil, fmt.Errorf("--bao-role-id-file: %w", err)
		}
		secret, err := readTrimmed(secretIDFile)
		if err != nil {
			return nil, fmt.Errorf("--bao-secret-id-file: %w", err)
		}
		c.roleID, c.secretID = role, secret
	default:
		return nil, errors.New("openbao needs --bao-token, --bao-token-file or an AppRole: --bao-role-id-file and --bao-secret-id-file")
	}
	if caFile != "" {
		pem, err := os.ReadFile(caFile)
		if err != nil {
			return nil, fmt.Errorf("--bao-ca-file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("no certificates in %s", caFile)
		}
		c.hc.Transport = &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool}}
	}
	return c, nil
}

// readPassword returns the "password" key of the KV v2 secret at path,
// relative to the mount. Every outcome lands in tembo_bao_password_*.
func (c *baoClient) readPassword(ctx context.Context, path string) (string, error) {
	pw, err := c.read(ctx, path)
	c.mu.Lock()
	if err == nil {
		c.lastOK, c.lastOKAt = true, time.Now()
	} else {
		c.lastOK = false
	}
	c.mu.Unlock()
	return pw, err
}

func (c *baoClient) read(ctx context.Context, path string) (string, error) {
	status, body, err := c.get(ctx, "/v1/"+c.mount+"/data/"+strings.Trim(path, "/"))
	if err != nil {
		return "", err
	}
	if status != http.StatusOK {
		return "", fmt.Errorf("openbao: %s: %s", http.StatusText(status), bytes.TrimSpace(body))
	}
	var res struct {
		Data struct {
			Data map[string]string `json:"data"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &res); err != nil {
		return "", fmt.Errorf("openbao: %w", err)
	}
	pw := strings.TrimSpace(res.Data.Data["password"])
	if pw == "" {
		return "", fmt.Errorf("openbao: the secret at %s has no \"password\"", path)
	}
	return pw, nil
}

// The outcome of the password reads, for tembo_bao_password_*: an alert on
// success = 0 catches an OpenBao that went away, the timestamp one whose
// secret has quietly stopped changing.
var (
	baoSuccessDesc = prometheus.NewDesc("tembo_bao_password_success",
		"Whether the last read of a managed password secret succeeded: the Postgres password poll, or the LDAP password at startup.", nil, nil)
	baoLastSuccessDesc = prometheus.NewDesc("tembo_bao_password_last_success_seconds",
		"When a managed password secret was last read successfully, in Unix seconds; 0 is never.", nil, nil)
)

// Describe sends nothing: the collector is unchecked.
func (c *baoClient) Describe(chan<- *prometheus.Desc) {}

// Collect reports the outcome of the last password read.
func (c *baoClient) Collect(ch chan<- prometheus.Metric) {
	c.mu.Lock()
	ok, at := c.lastOK, c.lastOKAt
	c.mu.Unlock()
	success := 0.0
	if ok {
		success = 1
	}
	ch <- prometheus.MustNewConstMetric(baoSuccessDesc, prometheus.GaugeValue, success)
	ch <- prometheus.MustNewConstMetric(baoLastSuccessDesc, prometheus.GaugeValue, float64(at.Unix()))
}

// get runs an authenticated GET. An AppRole client logs in once and, when a
// request comes back unauthorized, logs in again and retries once — the
// token had expired.
func (c *baoClient) get(ctx context.Context, path string) (int, []byte, error) {
	status, body, err := c.attempt(ctx, path)
	if err != nil || status != http.StatusForbidden || c.roleID == "" {
		return status, body, err
	}
	if err := c.login(ctx); err != nil {
		return status, body, err
	}
	return c.attempt(ctx, path)
}

func (c *baoClient) attempt(ctx context.Context, path string) (int, []byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.addr+path, nil)
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("X-Vault-Token", c.currentToken())
	res, err := c.hc.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer func() { _ = res.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(res.Body, 1<<20))
	return res.StatusCode, body, err
}

// login runs the AppRole login and keeps the client token; the secret id
// must allow more than one use, for the re-logins.
func (c *baoClient) login(ctx context.Context) error {
	body, err := json.Marshal(map[string]string{"role_id": c.roleID, "secret_id": c.secretID})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.addr+"/v1/auth/approle/login", bytes.NewReader(body))
	if err != nil {
		return err
	}
	res, err := c.hc.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = res.Body.Close() }()
	data, err := io.ReadAll(io.LimitReader(res.Body, 1<<20))
	if err != nil {
		return err
	}
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("openbao: approle login: %s: %s", res.Status, bytes.TrimSpace(data))
	}
	var out struct {
		Auth struct {
			ClientToken string `json:"client_token"`
		} `json:"auth"`
	}
	if err := json.Unmarshal(data, &out); err != nil {
		return fmt.Errorf("openbao: approle login: %w", err)
	}
	if out.Auth.ClientToken == "" {
		return fmt.Errorf("openbao: approle login returned no token")
	}
	c.mu.Lock()
	c.token = out.Auth.ClientToken
	c.mu.Unlock()
	return nil
}

func (c *baoClient) currentToken() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.token
}

func readTrimmed(path string) (string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(b)), nil
}
