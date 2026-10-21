package postgres

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// consulStub answers the agent API: it records the registrations and serves
// the health entries it is set to.
type consulStub struct {
	registered atomic.Value // map[string]any
	entries    atomic.Value // []consulHealthEntry
}

func (s *consulStub) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("PUT /v1/agent/service/register", func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		s.registered.Store(body)
	})
	mux.HandleFunc("PUT /v1/agent/service/deregister/{id}", func(w http.ResponseWriter, r *http.Request) {
		s.registered.Store(map[string]any{})
	})
	mux.HandleFunc("GET /v1/health/service/{name}", func(w http.ResponseWriter, r *http.Request) {
		entries, _ := s.entries.Load().([]consulHealthEntry)
		_ = json.NewEncoder(w).Encode(entries)
	})
	return mux
}

func healthEntry(id, serviceAddr, nodeAddr string, port int, tags ...string) consulHealthEntry {
	var e consulHealthEntry
	e.Node.Address = nodeAddr
	e.Service.Address = serviceAddr
	e.Service.Port = port
	e.Service.Tags = tags
	e.Service.Meta = map[string]string{"tembo_id": id}
	return e
}

func TestConsulRegisterAndDeregister(t *testing.T) {
	stub := &consulStub{}
	srv := httptest.NewServer(stub.handler())
	defer srv.Close()

	c := newConsulClient(srv.URL, "tembo", []string{"dc1"}, "a")
	if err := c.register("10.0.0.1", 8001); err != nil {
		t.Fatalf("register: %v", err)
	}
	body := stub.registered.Load().(map[string]any)
	if body["ID"] != "tembo-a" || body["Name"] != "tembo" || body["Address"] != "10.0.0.1" || body["Port"] != float64(8001) {
		t.Errorf("registration: %v", body)
	}
	if meta, _ := body["Meta"].(map[string]any); meta["tembo_id"] != "a" {
		t.Errorf("meta: %v", body["Meta"])
	}
	check, _ := body["Check"].(map[string]any)
	if check["TCP"] != "10.0.0.1:8001" {
		t.Errorf("check: %v", check)
	}

	c.deregister()
	if body, _ := stub.registered.Load().(map[string]any); len(body) != 0 {
		t.Error("deregister: the registration is still there")
	}
}

func TestConsulPeers(t *testing.T) {
	stub := &consulStub{}
	stub.entries.Store([]consulHealthEntry{
		healthEntry("a", "10.0.0.1", "", 8001, "dc1"),
		healthEntry("b", "10.0.0.2", "", 8001, "dc1"),
		healthEntry("c", "", "192.168.0.3", 8001, "dc1"), // no service address: the node's
		healthEntry("x", "10.0.0.9", "", 8001, "dc2"),    // other tags
		healthEntry("", "10.0.0.10", "", 8001, "dc1"),    // not a tembo service
	})
	srv := httptest.NewServer(stub.handler())
	defer srv.Close()

	c := newConsulClient(srv.URL, "tembo", []string{"dc1"}, "a")
	peers, err := c.peers()
	if err != nil {
		t.Fatalf("peers: %v", err)
	}
	want := map[string]string{"a": "10.0.0.1:8001", "b": "10.0.0.2:8001", "c": "192.168.0.3:8001"}
	if len(peers) != len(want) {
		t.Fatalf("peers: %v, want %v", peers, want)
	}
	for id, addr := range want {
		if peers[id] != addr {
			t.Errorf("peers[%s] = %q, want %q", id, peers[id], addr)
		}
	}
}

func TestConsulWaitPeers(t *testing.T) {
	stub := &consulStub{}
	stub.entries.Store([]consulHealthEntry{healthEntry("a", "10.0.0.1", "", 8001)})
	srv := httptest.NewServer(stub.handler())
	defer srv.Close()

	c := newConsulClient(srv.URL, "tembo", nil, "a")
	c.poll, c.timeout = time.Millisecond, 10*time.Second

	// The two others turn up while the first poll is running.
	go func() {
		time.Sleep(5 * time.Millisecond)
		stub.entries.Store([]consulHealthEntry{
			healthEntry("a", "10.0.0.1", "", 8001),
			healthEntry("b", "10.0.0.2", "", 8001),
			healthEntry("c", "10.0.0.3", "", 8001),
		})
	}()
	peers, err := c.waitPeers(context.Background(), 3)
	if err != nil {
		t.Fatalf("waitPeers: %v", err)
	}
	if want := "a=10.0.0.1:8001,b=10.0.0.2:8001,c=10.0.0.3:8001"; peers != want {
		t.Errorf("peers = %q, want %q", peers, want)
	}
}

func TestConsulWaitPeersTimesOut(t *testing.T) {
	stub := &consulStub{}
	stub.entries.Store([]consulHealthEntry{healthEntry("a", "10.0.0.1", "", 8001)})
	srv := httptest.NewServer(stub.handler())
	defer srv.Close()

	c := newConsulClient(srv.URL, "tembo", nil, "a")
	c.poll, c.timeout = time.Millisecond, 50*time.Millisecond
	// This node alone never reaches the expected three.
	if _, err := c.waitPeers(context.Background(), 3); err == nil {
		t.Error("waitPeers: want a timeout error")
	}
}

func TestAdvertiseAddr(t *testing.T) {
	for _, tc := range []struct {
		in       string
		wantHost string
		wantPort int
	}{
		{"10.0.0.5:8002", "10.0.0.5", 8002},
		{"node1", "node1", 8001},
		{":8003", "*hostname*", 8003},
		{"", "*hostname*", 8001},
	} {
		host, port, err := advertiseAddr(tc.in)
		if err != nil {
			t.Errorf("advertiseAddr(%q): %v", tc.in, err)
			continue
		}
		if tc.wantHost == "*hostname*" {
			if host == "" {
				t.Errorf("advertiseAddr(%q): empty host", tc.in)
			}
		} else if host != tc.wantHost {
			t.Errorf("advertiseAddr(%q) host = %q, want %q", tc.in, host, tc.wantHost)
		}
		if port != tc.wantPort {
			t.Errorf("advertiseAddr(%q) port = %d, want %d", tc.in, port, tc.wantPort)
		}
	}
	if _, _, err := advertiseAddr("host:bad"); err == nil {
		t.Error("advertiseAddr(host:bad): want an error")
	}
}
