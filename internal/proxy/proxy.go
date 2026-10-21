// Package proxy forwards client connections to the Postgres of the cluster.
package proxy

import (
	"errors"
	"io"
	"net"
	"slices"
	"sync"
	"time"

	"github.com/ks-tool/tembo/internal/addrlist"
)

// Proxy forwards every accepted connection to one of the current targets over
// TCP, so authentication and TLS stay between the client and Postgres.
type Proxy struct {
	mu      sync.Mutex
	targets []string            // host:port each; none refuses connections
	allowed *addrlist.List      // client addresses; nil allows them all
	next    int                 // the target of the next connection
	conns   map[net.Conn]string // client connection → its target
}

// New returns a proxy that refuses connections until SetTargets.
func New() *Proxy {
	return &Proxy{conns: make(map[net.Conn]string)}
}

// SetAllowed restricts new connections to the addresses of l; nil allows
// them all. The list pg_hba.conf is written from is the one to pass.
func (p *Proxy) SetAllowed(l *addrlist.List) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.allowed = l
}

// SetTargets spreads new connections over targets, or refuses them when there
// are none, and closes the connections to a target that is gone. It reports
// whether the targets changed.
func (p *Proxy) SetTargets(targets []string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if slices.Equal(p.targets, targets) {
		return false
	}
	p.targets = targets
	for c, target := range p.conns {
		if !slices.Contains(targets, target) {
			_ = c.Close()
			delete(p.conns, c)
		}
	}
	return true
}

// Serve accepts connections on ln until ln is closed.
func (p *Proxy) Serve(ln net.Listener) {
	for {
		c, err := ln.Accept()
		if errors.Is(err, net.ErrClosed) {
			return
		}
		if err != nil {
			time.Sleep(100 * time.Millisecond) // e.g. out of file descriptors
			continue
		}
		go p.forward(c)
	}
}

func (p *Proxy) forward(client net.Conn) {
	defer func() { _ = client.Close() }()

	p.mu.Lock()
	var target string
	if p.allowed != nil && !p.allowed.Allows(addrIP(client.RemoteAddr())) {
		p.mu.Unlock()
		return // not on the address list pg_hba.conf is written from
	}
	if len(p.targets) > 0 {
		target = p.targets[p.next%len(p.targets)]
		p.next++
		p.conns[client] = target
	}
	p.mu.Unlock()
	if target == "" {
		return
	}
	defer func() {
		p.mu.Lock()
		delete(p.conns, client)
		p.mu.Unlock()
	}()

	server, err := net.DialTimeout("tcp", target, 5*time.Second)
	if err != nil {
		return
	}
	defer func() { _ = server.Close() }()

	// When one direction stops, closing both connections stops the other.
	done := make(chan struct{}, 2)
	go func() { _, _ = io.Copy(server, client); done <- struct{}{} }()
	go func() { _, _ = io.Copy(client, server); done <- struct{}{} }()
	<-done
}

// addrIP extracts the IP of a remote address; nil when there is none.
func addrIP(a net.Addr) net.IP {
	if ta, ok := a.(*net.TCPAddr); ok {
		return ta.IP
	}
	return nil
}
