package postgres

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io/fs"
	"testing"

	"github.com/rs/zerolog"
)

// scripted reads its script entry by entry and then fails closed, the way a
// closed FIFO does.
type scripted struct {
	script []scriptEntry
	pos    int
}

type scriptEntry struct {
	data string
	err  error
}

func (s *scripted) Read(p []byte) (int, error) {
	if s.pos >= len(s.script) {
		return 0, fs.ErrClosed
	}
	e := s.script[s.pos]
	s.pos++
	if e.err != nil {
		return 0, e.err
	}
	return copy(p, e.data), nil
}

func TestTailFIFOStopsOnlyOutsideShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	// a read error fails the drain at once: nothing else drains the fifo
	r := &scripted{script: []scriptEntry{
		{err: errors.New("transient")},
		{data: "one line\n"},
	}}
	var out bytes.Buffer
	if err := tailFIFO(ctx, r, &out); err == nil {
		t.Fatal("an undrained fifo must fail the drain")
	}
	if r.pos != 1 || out.Len() != 0 {
		t.Fatalf("the drain must stop at the error: %d entries read, %q", r.pos, out.String())
	}

	// the shutdown closing the file does not
	cancel()
	if err := tailFIFO(ctx, &scripted{}, &out); err != nil {
		t.Fatalf("the shutdown close must not fail the drain: %v", err)
	}
}

func TestTailCSVStopsOnlyOutsideShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	// a malformed record is skipped, a read error fails the drain
	r := &scripted{script: []scriptEntry{
		{data: "a \"bare quote,b\n"},
		{err: errors.New("transient")},
		{data: "never read\n"},
	}}
	if err := tailCSV(ctx, r); err == nil {
		t.Fatal("an undrained fifo must fail the drain")
	}
	if r.pos != 2 {
		t.Fatalf("read %d entries, want the drain to stop at the error, the second", r.pos)
	}

	// the shutdown closing the file does not
	cancel()
	if err := tailCSV(ctx, &scripted{script: []scriptEntry{{err: fs.ErrClosed}}}); err != nil {
		t.Fatalf("the shutdown close must not fail the drain: %v", err)
	}
}

func TestLineLogLevels(t *testing.T) {
	var out bytes.Buffer
	logger := zerolog.New(&out)
	_, err := (&lineLog{logger: logger, level: zerolog.ErrorLevel}).Write([]byte("pg_ctl: directory does not exist\n"))
	if err != nil {
		t.Fatal(err)
	}
	var event map[string]any
	if err := json.Unmarshal(out.Bytes(), &event); err != nil {
		t.Fatal(err)
	}
	if event["level"] != "error" {
		t.Fatalf("level = %v, want error", event["level"])
	}

	out.Reset()
	_, err = (&lineLog{logger: logger, level: zerolog.InfoLevel}).Write([]byte("starting postgres\n"))
	if err != nil {
		t.Fatal(err)
	}
	event = nil
	if err := json.Unmarshal(out.Bytes(), &event); err != nil {
		t.Fatal(err)
	}
	if event["level"] != "info" {
		t.Fatalf("level = %v, want info", event["level"])
	}
}
