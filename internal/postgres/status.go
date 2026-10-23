package postgres

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/go-krb5/krb5/client"
	"github.com/go-krb5/krb5/config"
	"github.com/go-krb5/krb5/credentials"
	"github.com/go-krb5/krb5/keytab"
	"github.com/go-krb5/krb5/spnego"
	"github.com/spf13/cobra"
)

// agentAddr is the API address of the tembo the CLI talks to; the rest are
// the credentials it authenticates with, if the API asks for any.
var (
	agentAddr        string
	cliToken         string
	cliTokenFile     string
	cliTLSCert       string
	cliTLSKey        string
	cliTLSCA         string
	cliKrb5          bool
	cliKrb5Keytab    string
	cliKrb5Principal string
	cliKrb5Config    string
	cliUsername      string
	cliPassword      string
)

var statusCmd = &cobra.Command{
	Use:  "status",
	Args: cobra.NoArgs,
	RunE: statusCmdRun,
}

func init() {
	f := rootCmd.PersistentFlags()
	f.StringVar(&agentAddr, "addr", "127.0.0.1:8001", "API address of the tembo to talk to, with an optional http:// or https:// prefix")
	f.StringVar(&cliToken, "token", os.Getenv("TEMBO_TOKEN"), "bearer token for the API (TEMBO_TOKEN)")
	f.StringVar(&cliTokenFile, "token-file", "", "file with the bearer token for the API")
	f.StringVar(&cliTLSCert, "tls-cert", "", "client certificate (PEM) to authenticate with")
	f.StringVar(&cliTLSKey, "tls-key", "", "private key of --tls-cert (PEM)")
	f.StringVar(&cliTLSCA, "tls-ca", "", "CA bundle (PEM) to verify the API server with (default: the system pool)")
	f.BoolVar(&cliKrb5, "krb5", false, "authenticate with Kerberos (SPNEGO), the ticket from the credential cache")
	f.StringVar(&cliKrb5Keytab, "krb5-keytab", "", "keytab to get the Kerberos ticket from instead of the credential cache")
	f.StringVar(&cliKrb5Principal, "krb5-principal", "", "principal of --krb5-keytab, e.g. alice@TEMBO.TEST")
	f.StringVar(&cliKrb5Config, "krb5-config", "", "krb5.conf to use (default: $KRB5_CONFIG or /etc/krb5.conf)")
	f.StringVar(&cliUsername, "username", os.Getenv("TEMBO_USERNAME"), "username for Basic auth (TEMBO_USERNAME)")
	f.StringVar(&cliPassword, "password", os.Getenv("TEMBO_PASSWORD"), "password for Basic auth (TEMBO_PASSWORD)")
	rootCmd.AddCommand(statusCmd)
}

func statusCmdRun(cmd *cobra.Command, _ []string) error {
	return call(cmd.Context(), http.MethodGet, "/status", nil)
}

// call sends a request to the tembo at agentAddr and copies the response
// body to stdout. Without a deadline in ctx, it gives up after 10 seconds.
func call(ctx context.Context, method, path string, body []byte) error {
	data, err := request(ctx, method, path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	_, err = os.Stdout.Write(data)
	return err
}

// request sends a request to the tembo at agentAddr and returns the
// response body.
func request(ctx context.Context, method, path string, body io.Reader) ([]byte, error) {
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
	}

	res, err := send(ctx, method, path, body, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = res.Body.Close() }()

	data, err := io.ReadAll(res.Body)
	if res.StatusCode >= http.StatusMultipleChoices {
		return nil, fmt.Errorf("%s: %s", res.Status, strings.TrimSpace(string(data)))
	}
	return data, err
}

// send sends a request to the tembo at agentAddr with the credentials the
// flags name, and the headers hdr. The caller reads and closes the response body, which may stream.
func send(ctx context.Context, method, path string, body io.Reader, hdr http.Header) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, agentURL()+path, body)
	if err != nil {
		return nil, err
	}
	for k, v := range hdr {
		req.Header[k] = v
	}
	if !cliKrb5 && cliKrb5Keytab == "" {
		token, err := bearerToken()
		if err != nil {
			return nil, err
		}
		switch {
		case token != "":
			req.Header.Set("Authorization", "Bearer "+token)
		case cliUsername != "" || cliPassword != "":
			req.SetBasicAuth(cliUsername, cliPassword)
		}
	}

	c, err := agentHTTP()
	if err != nil {
		return nil, err
	}
	return c.Do(req)
}

// agentURL returns agentAddr with a scheme: the one it carries, https when
// any --tls-* flag is given, http otherwise.
func agentURL() string {
	if strings.HasPrefix(agentAddr, "https://") || strings.HasPrefix(agentAddr, "http://") {
		return agentAddr
	}
	if cliTLSCert != "" || cliTLSKey != "" || cliTLSCA != "" {
		return "https://" + agentAddr
	}
	return "http://" + agentAddr
}

// httpDoer is an *http.Client or the SPNEGO wrapper around one.
type httpDoer interface {
	Do(*http.Request) (*http.Response, error)
}

// agentHTTP builds the HTTP client of the CLI from the credential flags; it
// follows the redirects to the raft leader.
func agentHTTP() (httpDoer, error) {
	tlsCfg, err := cliTLSConfig()
	if err != nil {
		return nil, err
	}
	cl := &http.Client{}
	if tlsCfg != nil {
		cl.Transport = &http.Transport{TLSClientConfig: tlsCfg}
	}
	if cliKrb5 || cliKrb5Keytab != "" {
		return spnegoClient(cl)
	}
	return cl, nil
}

// cliTLSConfig builds the client TLS configuration: the certificate of
// --tls-cert/--tls-key to authenticate with and the CA of --tls-ca to verify
// the server with; nil, nil keeps the defaults.
func cliTLSConfig() (*tls.Config, error) {
	if (cliTLSCert == "") != (cliTLSKey == "") {
		return nil, errors.New("--tls-cert and --tls-key go together")
	}
	if cliTLSCert == "" && cliTLSCA == "" {
		return nil, nil
	}
	cfg := &tls.Config{}
	if cliTLSCert != "" {
		cert, err := tls.LoadX509KeyPair(cliTLSCert, cliTLSKey)
		if err != nil {
			return nil, err
		}
		cfg.Certificates = []tls.Certificate{cert}
	}
	if cliTLSCA != "" {
		pool, err := loadCertPool(cliTLSCA)
		if err != nil {
			return nil, err
		}
		cfg.RootCAs = pool
	}
	return cfg, nil
}

// bearerToken returns the token of --token or --token-file.
func bearerToken() (string, error) {
	if cliToken != "" {
		return cliToken, nil
	}
	if cliTokenFile == "" {
		return "", nil
	}
	data, err := os.ReadFile(cliTokenFile)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(data)), nil
}

// spnegoClient wraps httpCl with Kerberos authentication: the ticket comes
// from the credential cache (KRB5CCNAME or the default path), or is logged in
// fresh from --krb5-keytab and --krb5-principal.
func spnegoClient(httpCl *http.Client) (*spnego.Client, error) {
	cfg, err := krb5Config(cliKrb5Config)
	if err != nil {
		return nil, err
	}
	if cliKrb5Keytab == "" {
		ccache, err := credentials.LoadCCache(ccachePath())
		if err != nil {
			return nil, err
		}
		cl, err := client.NewFromCCache(ccache, cfg)
		if err != nil {
			return nil, err
		}
		return spnego.NewClient(cl, httpCl, ""), nil
	}
	if cliKrb5Principal == "" {
		return nil, errors.New("--krb5-keytab requires --krb5-principal, e.g. alice@TEMBO.TEST")
	}
	user, realm, ok := strings.Cut(cliKrb5Principal, "@")
	if !ok || user == "" || realm == "" {
		return nil, fmt.Errorf("bad --krb5-principal %q, want user@REALM", cliKrb5Principal)
	}
	kt, err := keytab.Load(cliKrb5Keytab)
	if err != nil {
		return nil, err
	}
	cl := client.NewWithKeytab(user, realm, kt, cfg)
	if err := cl.Login(); err != nil {
		return nil, err
	}
	return spnego.NewClient(cl, httpCl, ""), nil
}

// krb5Config loads krb5.conf from path, $KRB5_CONFIG or the default path.
func krb5Config(path string) (*config.Config, error) {
	if path == "" {
		path = os.Getenv("KRB5_CONFIG")
	}
	if path == "" {
		path = "/etc/krb5.conf"
	}
	return config.Load(path)
}

// ccachePath is the credential cache KRB5CCNAME names (a FILE: one) or the
// default path of the uid.
func ccachePath() string {
	if p := os.Getenv("KRB5CCNAME"); p != "" {
		if c, ok := strings.CutPrefix(p, "FILE:"); ok {
			return c
		}
		return p
	}
	return fmt.Sprintf("/tmp/krb5cc_%d", os.Getuid())
}
