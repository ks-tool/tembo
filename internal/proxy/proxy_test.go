package proxy_test

import (
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ks-tool/tembo/internal/addrlist"
	"github.com/ks-tool/tembo/internal/proxy"
)

// startEcho listens on an ephemeral address and echoes everything back,
// counting the connections it served.
func startEcho(t *testing.T) (string, *atomic.Int32) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	served := &atomic.Int32{}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			served.Add(1)
			go func() { _, _ = io.Copy(c, c); _ = c.Close() }()
		}
	}()
	t.Cleanup(func() { _ = ln.Close() })
	return ln.Addr().String(), served
}

// startProxy serves p on an ephemary address and returns it.
func startProxy(t *testing.T, p *proxy.Proxy) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go p.Serve(ln)
	t.Cleanup(func() { _ = ln.Close() })
	return ln.Addr().String()
}

func dial(t *testing.T, addr string) net.Conn {
	t.Helper()
	c, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// roundTrip writes a message and reads it back through the proxy.
func roundTrip(t *testing.T, c net.Conn) {
	t.Helper()
	_ = c.SetDeadline(time.Now().Add(2 * time.Second))
	if _, err := c.Write([]byte("ping")); err != nil {
		t.Fatalf("write: %v", err)
	}
	buf := make([]byte, 4)
	if _, err := io.ReadFull(c, buf); err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(buf) != "ping" {
		t.Fatalf("read %q, want %q", buf, "ping")
	}
}

func TestProxyRefusesWithoutTargets(t *testing.T) {
	p := proxy.New()
	addr := startProxy(t, p)

	c := dial(t, addr)
	_ = c.SetDeadline(time.Now().Add(2 * time.Second))
	if _, err := c.Read(make([]byte, 1)); err != io.EOF {
		t.Fatalf("read: got %v, want EOF", err)
	}
}

func TestProxyRefusesUnallowedClient(t *testing.T) {
	echo, served := startEcho(t)

	p := proxy.New()
	p.SetTargets([]string{echo})
	allowed, err := addrlist.Parse([]string{"10.9.9.9"}) // not this client
	if err != nil {
		t.Fatal(err)
	}
	p.SetAllowed(allowed)
	addr := startProxy(t, p)

	c := dial(t, addr)
	_ = c.SetDeadline(time.Now().Add(2 * time.Second))
	if _, err := c.Read(make([]byte, 1)); err != io.EOF {
		t.Fatalf("read: got %v, want EOF", err)
	}
	if served.Load() != 0 {
		t.Fatalf("the target served %d connections, want none", served.Load())
	}

	allowed, err = addrlist.Parse([]string{"127.0.0.1"})
	if err != nil {
		t.Fatal(err)
	}
	p.SetAllowed(allowed)
	roundTrip(t, dial(t, addr))
}

func TestProxySpreadsConnections(t *testing.T) {
	e1, served1 := startEcho(t)
	e2, served2 := startEcho(t)

	p := proxy.New()
	if !p.SetTargets([]string{e1, e2}) {
		t.Fatal("SetTargets reported no change")
	}
	addr := startProxy(t, p)

	// four connections, two per target
	for range 4 {
		roundTrip(t, dial(t, addr))
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && (served1.Load() != 2 || served2.Load() != 2) {
		time.Sleep(5 * time.Millisecond)
	}
	if served1.Load() != 2 || served2.Load() != 2 {
		t.Fatalf("served %d and %d, want 2 and 2", served1.Load(), served2.Load())
	}
}

func TestProxyClosesConnectionsOfRemovedTarget(t *testing.T) {
	e1, _ := startEcho(t)
	e2, _ := startEcho(t)

	p := proxy.New()
	p.SetTargets([]string{e1, e2})
	addr := startProxy(t, p)

	// both go through and echo before the targets change
	c1 := dial(t, addr)
	roundTrip(t, c1)
	c2 := dial(t, addr)
	roundTrip(t, c2)

	p.SetTargets([]string{e1})

	// exactly the connection to the removed target is closed, the other one
	// stays open
	eofs := 0
	for _, c := range []net.Conn{c1, c2} {
		_ = c.SetDeadline(time.Now().Add(500 * time.Millisecond))
		if _, err := c.Read(make([]byte, 1)); err == io.EOF {
			eofs++
		}
	}
	if eofs != 1 {
		t.Fatalf("%d connections closed, want 1", eofs)
	}
}
