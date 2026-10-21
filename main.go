package main

import (
	stdlog "log"
	"os"
	"strings"
	"time"

	"github.com/ks-tool/tembo/internal/postgres"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

const logTimeFormat = "2006-01-02T15:04:05.000MST"

func init() {
	if v := os.Getenv("TEMBO_LOG_DEBUG"); string2Bool(v) {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	} else {
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	}

	if v := os.Getenv("TEMBO_LOG_UTC"); string2Bool(v) {
		zerolog.TimestampFunc = func() time.Time { return time.Now().UTC() }
		postgres.Defaults["log_timezone"] = "UTC"
		postgres.Defaults["timezone"] = "UTC"
	}

	zerolog.TimestampFieldName = "timestamp"
	zerolog.TimeFieldFormat = logTimeFormat

	log.Logger = zerolog.New(os.Stdout).With().Timestamp().Str("source", "tembo").Logger()
	// Messages of the standard library, e.g. from net/http, come out as JSON too.
	stdlog.SetFlags(0)
	stdlog.SetOutput(log.Logger)
}

func main() {
	postgres.Run()
}

func string2Bool(s string) bool {
	if len(s) == 0 {
		return false
	}

	s = strings.ToLower(s)
	s = strings.TrimSpace(s)
	return s == "1" || s == "true" || s == "yes" || s == "on" || s[0] == 't' || s[0] == 'y'
}
