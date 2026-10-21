package metrics

// From postgres_exporter collector/pg_postmaster.go.

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	pgPostMasterStartTimeSeconds = newDesc("postmaster", "start_time_seconds", "Time at which postmaster started")

	pgPostmasterQuery = "SELECT extract(epoch from pg_postmaster_start_time) from pg_postmaster_start_time();"
)

func updatePostmaster(ctx context.Context, conn *pgx.Conn, _ int, ch chan<- prometheus.Metric) error {
	var startTimeSeconds *float64
	if err := conn.QueryRow(ctx, pgPostmasterQuery).Scan(&startTimeSeconds); err != nil {
		return err
	}
	ch <- gauge(pgPostMasterStartTimeSeconds, floatValue(startTimeSeconds))
	return nil
}
