package metrics

// From postgres_exporter collector/pg_setting.go.

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog/log"
)

// Settings ignored due to invalid format:
//   - sync_commit_cancel_wait, specific to Azure Postgres
//   - google_dataplex.max_messages, specific to Google Cloud SQL
var pgSettingsQuery = "SELECT name, setting, COALESCE(unit, ''), COALESCE(short_desc, ''), vartype FROM pg_settings WHERE vartype IN ('bool', 'integer', 'real') AND name NOT IN ('sync_commit_cancel_wait', 'google_dataplex.max_messages');"

func updateSettings(ctx context.Context, conn *pgx.Conn, _ int, ch chan<- prometheus.Metric) error {
	rows, err := conn.Query(ctx, pgSettingsQuery)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		s := &pgSetting{}
		if err := rows.Scan(&s.name, &s.setting, &s.unit, &s.shortDesc, &s.vartype); err != nil {
			return err
		}
		metric, err := s.metric()
		if err != nil {
			log.Warn().Err(err).Str("setting", s.name).Str("value", s.setting).Str("unit", s.unit).
				Msg("metrics: error normalising unit for setting")
			continue
		}
		ch <- metric
	}
	return rows.Err()
}

// pgSetting is a runtime variable as returned by the pg_settings view.
type pgSetting struct {
	name, setting, unit, shortDesc, vartype string
}

func (s *pgSetting) metric() (prometheus.Metric, error) {
	var (
		err       error
		name      = strings.ReplaceAll(strings.ReplaceAll(s.name, ".", "_"), "-", "_")
		unit      string
		shortDesc = fmt.Sprintf("Server Parameter: %s", s.name)
		val       float64
	)

	switch s.vartype {
	case "bool":
		if s.setting == "on" {
			val = 1
		}
	case "integer", "real":
		if val, unit, err = s.normaliseUnit(); err != nil {
			return nil, err
		}
		if len(unit) > 0 {
			name = fmt.Sprintf("%s_%s", name, unit)
			shortDesc = fmt.Sprintf("%s [Units converted to %s.]", shortDesc, unit)
		}
	default:
		return nil, fmt.Errorf("pgsetting: unsupported vartype %q", s.vartype)
	}

	return gauge(newDesc("settings", name, shortDesc), val), nil
}

func (s *pgSetting) normaliseUnit() (val float64, unit string, err error) {
	val, err = strconv.ParseFloat(s.setting, 64)
	if err != nil {
		return val, unit, fmt.Errorf("error converting setting %q value %q to float: %s", s.name, s.setting, err)
	}

	// Units defined in: https://www.postgresql.org/docs/current/static/config-setting.html
	switch s.unit {
	case "":
		return val, unit, nil
	case "ms", "s", "min", "h", "d":
		unit = "seconds"
	case "B", "kB", "MB", "GB", "TB", "1kB", "2kB", "4kB", "8kB", "16kB", "32kB", "64kB", "16MB", "32MB", "64MB":
		unit = "bytes"
	default:
		return val, unit, fmt.Errorf("unknown unit for runtime variable: %q", s.unit)
	}

	// -1 is special, don't modify the value
	if val == -1 {
		return val, unit, nil
	}

	switch s.unit {
	case "ms":
		val /= 1000
	case "min":
		val *= 60
	case "h":
		val *= 60 * 60
	case "d":
		val *= 60 * 60 * 24
	case "kB":
		val *= math.Pow(2, 10)
	case "MB":
		val *= math.Pow(2, 20)
	case "GB":
		val *= math.Pow(2, 30)
	case "TB":
		val *= math.Pow(2, 40)
	case "1kB":
		val *= math.Pow(2, 10)
	case "2kB":
		val *= math.Pow(2, 11)
	case "4kB":
		val *= math.Pow(2, 12)
	case "8kB":
		val *= math.Pow(2, 13)
	case "16kB":
		val *= math.Pow(2, 14)
	case "32kB":
		val *= math.Pow(2, 15)
	case "64kB":
		val *= math.Pow(2, 16)
	case "16MB":
		val *= math.Pow(2, 24)
	case "32MB":
		val *= math.Pow(2, 25)
	case "64MB":
		val *= math.Pow(2, 26)
	}
	return val, unit, nil
}
