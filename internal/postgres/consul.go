package postgres

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"net/http"
	"net/url"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
)

const (
	// consulWaitTimeout bounds peer discovery at startup.
	consulWaitTimeout = 5 * time.Minute

	// consulPollInterval is how often the catalog is asked again while the
	// discovered set has not settled yet.
	consulPollInterval = time.Second

	// consulStablePolls is how many identical answers in a row settle the
	// discovered set: the nodes start together, a set that stops changing is
	// taken as complete (unless --consul-expect asks for more).
	consulStablePolls = 3
)

// consulClient is a Consul agent HTTP API client for service discovery: it
// registers this node and finds the peers sharing the service and the tags.
type consulClient struct {
	api     string
	service string
	tags    []string
	id      string // this node's service id
	nodeID  string
	hc      *http.Client

	poll    time.Duration
	timeout time.Duration
}

func newConsulClient(api, service string, tags []string, nodeID string) *consulClient {
	return &consulClient{
		api:     strings.TrimRight(api, "/"),
		service: service,
		tags:    tags,
		id:      service + "-" + nodeID,
		nodeID:  nodeID,
		hc:      &http.Client{Timeout: 10 * time.Second},
		poll:    consulPollInterval,
		timeout: consulWaitTimeout,
	}
}

// register announces this node; a TCP check against the API port keeps dead
// registrations out of the discovery, and drops them after a minute.
func (c *consulClient) register(host string, port int) error {
	body := map[string]any{
		"ID":      c.id,
		"Name":    c.service,
		"Tags":    c.tags,
		"Address": host,
		"Port":    port,
		"Meta":    map[string]string{"tembo_id": c.nodeID},
		"Check": map[string]any{
			"TCP":                            net.JoinHostPort(host, strconv.Itoa(port)),
			"Interval":                       "10s",
			"DeregisterCriticalServiceAfter": "1m",
		},
	}
	return c.put("/v1/agent/service/register", body)
}

func (c *consulClient) deregister() {
	if err := c.put("/v1/agent/service/deregister/"+url.PathEscape(c.id), nil); err != nil {
		log.Warn().Err(err).Msg("consul: deregistration failed")
	}
}

func (c *consulClient) put(path string, body any) error {
	var r io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		r = bytes.NewReader(b)
	}
	req, err := http.NewRequest(http.MethodPut, c.api+path, r)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("consul: PUT %s: %s: %s", path, resp.Status, bytes.TrimSpace(b))
	}
	return nil
}

type consulHealthEntry struct {
	Node    struct{ Address string }
	Service struct {
		Address string
		Port    int
		Tags    []string
		Meta    map[string]string
	}
}

// peers returns the healthy services sharing the service name and all the
// tags, as id=host:port; the id travels in the tembo_id meta.
func (c *consulClient) peers() (map[string]string, error) {
	resp, err := c.hc.Get(c.api + "/v1/health/service/" + url.PathEscape(c.service) + "?passing=true")
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("consul: health service %s: %s", c.service, resp.Status)
	}
	var entries []consulHealthEntry
	if err := json.NewDecoder(resp.Body).Decode(&entries); err != nil {
		return nil, err
	}
	peers := make(map[string]string)
	for _, e := range entries {
		id := e.Service.Meta["tembo_id"]
		if id == "" || !hasTags(e.Service.Tags, c.tags) {
			continue
		}
		host := e.Service.Address
		if host == "" {
			host = e.Node.Address
		}
		peers[id] = net.JoinHostPort(host, strconv.Itoa(e.Service.Port))
	}
	return peers, nil
}

// waitPeers polls the catalog until the discovered set holds at least expect
// nodes, this one included, and stops changing; it renders the set as
// --peers would look.
func (c *consulClient) waitPeers(ctx context.Context, expect int) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	var last string
	stable := 0
	for {
		peers, err := c.peers()
		if err != nil {
			log.Warn().Err(err).Msg("consul: discovery failed, retrying")
		} else if _, ok := peers[c.nodeID]; ok && len(peers) >= expect {
			got := renderPeers(peers)
			if got == last {
				stable++
			} else {
				stable = 1
				last = got
				log.Info().Str("peers", got).Msg("consul: discovered")
			}
			if stable >= consulStablePolls {
				return got, nil
			}
		}
		select {
		case <-ctx.Done():
			if last != "" {
				return "", fmt.Errorf("consul: peers %s did not settle: %w", last, ctx.Err())
			}
			return "", fmt.Errorf("consul: no peers discovered: %w", ctx.Err())
		case <-time.After(c.poll):
		}
	}
}

// hasTags says whether got holds every tag of want.
func hasTags(got, want []string) bool {
	for _, t := range want {
		if !slices.Contains(got, t) {
			return false
		}
	}
	return true
}

// renderPeers renders a peer set as --peers would list it.
func renderPeers(peers map[string]string) string {
	ids := slices.Sorted(maps.Keys(peers))
	items := make([]string, 0, len(ids))
	for _, id := range ids {
		items = append(items, id+"="+peers[id])
	}
	return strings.Join(items, ",")
}

// advertiseAddr resolves the address other nodes reach this node at: the
// --advertise value, or the hostname, at the --advertise port or 8001.
func advertiseAddr(advertise string) (host string, port int, err error) {
	host, ps, err := net.SplitHostPort(advertise)
	if err != nil {
		if advertise == "" || !strings.Contains(advertise, ":") {
			host, ps = advertise, "8001"
		} else {
			return "", 0, fmt.Errorf("--advertise: %w", err)
		}
	}
	if host == "" {
		host, err = os.Hostname()
		if err != nil {
			return "", 0, err
		}
	}
	port, err = strconv.Atoi(ps)
	if err != nil || port <= 0 || port > 65535 {
		return "", 0, fmt.Errorf("--advertise: bad port %q", ps)
	}
	return host, port, nil
}

var errConsulExcludesPeers = errors.New("--peers and --consul exclude each other: either list the peers or discover them")
