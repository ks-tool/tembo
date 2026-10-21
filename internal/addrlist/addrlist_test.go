package addrlist

import (
	"net"
	"testing"
)

func TestParseMatches(t *testing.T) {
	l, err := Parse([]string{"10.0.0.7", "192.168.0.0/24", "localhost", "  "})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	for ip, want := range map[string]bool{
		"10.0.0.7":     true,  // literal
		"10.0.0.8":     false, // next to it
		"192.168.0.55": true,  // inside the network
		"192.168.1.55": false, // outside it
		"127.0.0.1":    true,  // a resolved hostname
		"::1":          true,  // a resolved hostname, IPv6
		"2001:db8::1":  false,
		"8.8.8.8":      false,
	} {
		if got := l.Allows(net.ParseIP(ip)); got != want {
			t.Errorf("Allows(%s) = %v, want %v", ip, got, want)
		}
	}
	if l.Allows(nil) {
		t.Error("Allows(nil) = true, want false")
	}
}

func TestParseRejectsUnresolvable(t *testing.T) {
	if _, err := Parse([]string{"no-such-host.invalid"}); err == nil {
		t.Error("Parse of an unresolvable hostname: no error")
	}
	if _, err := Parse(nil); err != nil {
		t.Errorf("Parse(nil): %v, want an empty list", err)
	}
}
