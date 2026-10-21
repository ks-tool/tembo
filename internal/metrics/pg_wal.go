package metrics

// From postgres_exporter collector/pg_wal.go.

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	pgWALSegments = newDesc("wal", "segments", "Number of WAL segments")
	pgWALSize     = newDesc("wal", "size_bytes", "Total size of WAL segments")

	pgWALQuery = `
		SELECT
			COUNT(*) AS segments,
			SUM(size) AS size
		FROM pg_ls_waldir()
		WHERE name ~ '^[0-9A-F]{24}$'`
)

func updateWAL(ctx context.Context, conn *pgx.Conn, _ int, ch chan<- prometheus.Metric) error {
	var segments int64
	var size *int64
	if err := conn.QueryRow(ctx, pgWALQuery).Scan(&segments, &size); err != nil {
		return err
	}
	ch <- gauge(pgWALSegments, float64(segments))
	if size != nil {
		ch <- gauge(pgWALSize, float64(*size))
	}
	return nil
}
