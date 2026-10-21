package postgres

import (
	"bytes"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// Postgres logs through its logging collector into two FIFOs that tembo
// reads and prints as JSON: postgres.csv carries the csvlog records, postgres
// the few lines written around elog. The FIFOs are kept outside the data
// directory, which pg_basebackup and pg_rewind copy.

// csvlogFields name the csvlog columns in order.
var csvlogFields = []string{
	"log_time", "user_name", "database_name", "process_id", "connection_from", "session_id",
	"session_line_num", "command_tag", "session_start_time", "virtual_transaction_id", "transaction_id",
	"error_severity", "sql_state_code", "message", "detail", "hint", "internal_query", "internal_query_pos",
	"context", "query", "query_pos", "location", "application_name", "backend_type", "leader_pid", "query_id",
}

// logSettings send the log of Postgres to the FIFOs in dir.
func logSettings(dir string) map[string]string {
	return map[string]string{
		"logging_collector":        "on",
		"log_destination":          "csvlog",
		"log_directory":            dir,
		"log_filename":             "postgres",
		"log_rotation_age":         "0",
		"log_rotation_size":        "0",
		"log_truncate_on_rotation": "off",
	}
}

// openLogs creates the FIFOs in dir and opens them for reading. They are
// opened for writing too, so opening does not block and reading never hits
// EOF: the same files serve every start of Postgres.
func openLogs(dir string) (plain, csvlog *os.File, err error) {
	if err = os.MkdirAll(dir, 0o700); err != nil {
		return nil, nil, err
	}
	if plain, err = openFIFO(filepath.Join(dir, "postgres")); err != nil {
		return nil, nil, err
	}
	if csvlog, err = openFIFO(filepath.Join(dir, "postgres.csv")); err != nil {
		_ = plain.Close()
		return nil, nil, err
	}
	return plain, csvlog, nil
}

func openFIFO(path string) (*os.File, error) {
	if fi, err := os.Lstat(path); err == nil && fi.Mode()&fs.ModeNamedPipe == 0 {
		if err = os.Remove(path); err != nil {
			return nil, err
		}
	}
	if err := syscall.Mkfifo(path, 0o600); err != nil && !errors.Is(err, fs.ErrExist) {
		return nil, err
	}
	return os.OpenFile(path, os.O_RDWR, 0)
}

// tailFIFO logs every line read from r, and tailCSV every csvlog record,
// until the shutdown closes the file: that is the one way a drain ends
// without an error. The two are the only readers of the FIFOs Postgres
// writes its log into — once one fills up the logging collector blocks in
// its write, and with it every backend that logs — so an error from them
// is how run stops Postgres and the agent at once, see drainStopped.
func tailFIFO(ctx context.Context, r io.Reader, w io.Writer) error {
	_, err := io.Copy(w, r)
	return drainStopped(ctx, err)
}

// drainStopped turns the end of a drain into what run waits for: nil when
// the shutdown is already closing the log files, an error otherwise. A nil
// err is an EOF, which an O_RDWR FIFO does not give.
func drainStopped(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return nil // the shutdown is closing the log files
	}
	if err == nil {
		err = io.EOF
	}
	log.Error().Err(err).Msg("the log fifo is not drained, stopping postgres and the agent")
	return fmt.Errorf("reading the log fifo: %w", err)
}

// tailCSV logs the csvlog records read from r until r is closed at
// shutdown; any other error fails the drain, see tailFIFO.
func tailCSV(ctx context.Context, r io.Reader) error {
	logger := sourceLog("postgres")
	c := csv.NewReader(r)
	c.FieldsPerRecord = -1
	for {
		record, err := c.Read()
		if err == nil {
			fields := make(map[string]any, len(record))
			for i, v := range record {
				if i < len(csvlogFields) && v != "" {
					fields[csvlogFields[i]] = v
				}
			}
			severity, _ := fields["error_severity"].(string)
			logger.WithLevel(severityLevel(severity)).Fields(fields).Send()
			continue
		}
		if _, ok := errors.AsType[*csv.ParseError](err); ok {
			logger.Warn().Err(err).Msg("unreadable csvlog record")
			continue
		}
		return drainStopped(ctx, err)
	}
}

func severityLevel(severity string) zerolog.Level {
	switch {
	case strings.HasPrefix(severity, "DEBUG"):
		return zerolog.DebugLevel
	case severity == "WARNING":
		return zerolog.WarnLevel
	case severity == "ERROR":
		return zerolog.ErrorLevel
	case severity == "FATAL":
		return zerolog.FatalLevel // WithLevel does not exit
	case severity == "PANIC":
		return zerolog.PanicLevel // nor panic
	}
	return zerolog.InfoLevel // LOG, INFO, NOTICE
}

// lineLog is an io.Writer that logs every line written to it.
type lineLog struct {
	logger zerolog.Logger
	level  zerolog.Level
	buf    []byte
}

func newLineLog(source string) *lineLog {
	return &lineLog{logger: sourceLog(source), level: zerolog.InfoLevel}
}

// newErrLineLog logs every line written to it at error level: stderr routed
// here is the diagnostics of a failing command, not chatter.
func newErrLineLog(source string) *lineLog {
	return &lineLog{logger: sourceLog(source), level: zerolog.ErrorLevel}
}

func (w *lineLog) Write(p []byte) (int, error) {
	w.buf = append(w.buf, p...)
	for {
		line, rest, ok := bytes.Cut(w.buf, []byte{'\n'})
		if !ok {
			break
		}
		if len(line) > 0 {
			w.logger.WithLevel(w.level).Msg(string(line))
		}
		w.buf = rest
	}
	return len(p), nil
}

// sourceLog returns a logger for output that does not come from tembo itself.
func sourceLog(source string) zerolog.Logger {
	return zerolog.New(os.Stdout).With().Timestamp().Str("source", source).Logger()
}
