package metrics

// From postgres_exporter collector/pg_stat_activity_autovacuum.go.

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	statActivityAutovacuumAgeInSeconds = newDesc("stat_activity_autovacuum", "timestamp_seconds",
		"Start timestamp of the vacuum process in seconds", "relname")

	statActivityAutovacuumQuery = `
    SELECT
		SPLIT_PART(query, '.', 2) AS relname,
		EXTRACT(EPOCH FROM xact_start) AS timestamp_seconds
    FROM
    	pg_catalog.pg_stat_activity
    WHERE
		query LIKE 'autovacuum:%'
	`
)

func updateStatActivityAutovacuum(ctx context.Context, conn *pgx.Conn, _ int, ch chan<- prometheus.Metric) error {
	rows, err := conn.Query(ctx, statActivityAutovacuumQuery)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var relname string
		var ageInSeconds *float64
		if err := rows.Scan(&relname, &ageInSeconds); err != nil {
			return err
		}
		if ageInSeconds == nil {
			continue // no xact_start
		}
		ch <- gauge(statActivityAutovacuumAgeInSeconds, *ageInSeconds, relname)
	}
	return rows.Err()
}
