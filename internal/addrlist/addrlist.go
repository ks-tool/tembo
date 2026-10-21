// Package addrlist matches remote addresses against a list of allowed IPs,
// CIDRs and hostnames, the same list pg_hba.conf is written from.
package addrlist

import (
	"fmt"
	"net"
	"strings"
)

// List is a set of allowed addresses: literal IPs, networks and the
// addresses hostnames resolved to when the list was parsed.
type List struct {
	nets []*net.IPNet
	ips  map[string]bool
}

// Parse builds a List from IPs, CIDRs and hostnames. A hostname is resolved
// now; a name that does not resolve is an error, as it would match nothing.
func Parse(addrs []string) (*List, error) {
	l := &List{ips: make(map[string]bool)}
	for _, a := range addrs {
		a = strings.TrimSpace(a)
		if a == "" {
			continue
		}
		if ip := net.ParseIP(a); ip != nil {
			l.ips[ip.String()] = true
			continue
		}
		if _, n, err := net.ParseCIDR(a); err == nil {
			l.nets = append(l.nets, n)
			continue
		}
		ips, err := net.LookupHost(a)
		if err != nil {
			return nil, fmt.Errorf("address %q: %w", a, err)
		}
		for _, s := range ips {
			if ip := net.ParseIP(s); ip != nil {
				l.ips[ip.String()] = true
			}
		}
	}
	return l, nil
}

// Allows reports whether ip is on the list.
func (l *List) Allows(ip net.IP) bool {
	if ip == nil {
		return false
	}
	if l.ips[ip.String()] {
		return true
	}
	for _, n := range l.nets {
		if n.Contains(ip) {
			return true
		}
	}
	return false
}
