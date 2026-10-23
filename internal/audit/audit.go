// Package audit logs who changed the agent's state through its API. Every
// request that is not a read, and every refused one, gets a line naming the
// actor: the identity the authentication gave, or the address of a request
// that carried none — a node, the loopback, or any request while
// authentication is off.
package audit

import (
	"context"
	"net"
	"net/http"
	"strings"

	"github.com/rs/zerolog/log"
)

type recordKey struct{}

// record is the actor of one request, filled in by the authentication.
type record struct{ actor string }

// Middleware logs the requests next serves that change state or are refused.
// It must wrap the authentication, so that SetActor reaches the record.
func Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec := &record{}
		sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(sw, r.WithContext(context.WithValue(r.Context(), recordKey{}, rec)))

		refused := sw.status == http.StatusUnauthorized || sw.status == http.StatusForbidden
		if !changes(r) && !refused {
			return
		}
		actor := rec.actor
		if actor == "" {
			actor = "addr:" + host(r.RemoteAddr)
		}
		log.Info().Bool("audit", true).Str("actor", actor).Str("method", r.Method).
			Str("path", r.URL.Path).Int("status", sw.status).Msg("api call")
	})
}

// SetActor names the client of the request. Without Middleware it does nothing.
func SetActor(r *http.Request, actor string) {
	if rec, ok := r.Context().Value(recordKey{}).(*record); ok {
		rec.actor = actor
	}
}

// changes reports whether a request can change state. The raft messages
// between the nodes are POSTs too, sent every heartbeat: they are not calls.
func changes(r *http.Request) bool {
	switch r.Method {
	case http.MethodGet, http.MethodHead, http.MethodOptions:
		return false
	}
	return !strings.HasPrefix(r.URL.Path, "/raft/")
}

func host(addr string) string {
	h, _, err := net.SplitHostPort(addr)
	if err != nil {
		return addr
	}
	return h
}

// statusWriter keeps the status the handler wrote.
type statusWriter struct {
	http.ResponseWriter
	status  int
	written bool
}

// Unwrap lets http.ResponseController reach the writer underneath: a shell
// session needs its full-duplex mode and flushes, and both go through it.
func (w *statusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func (w *statusWriter) WriteHeader(code int) {
	if !w.written {
		w.status, w.written = code, true
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Write(b []byte) (int, error) {
	w.written = true
	return w.ResponseWriter.Write(b)
}
