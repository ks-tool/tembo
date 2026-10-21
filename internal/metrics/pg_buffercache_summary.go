package metrics

// From postgres_exporter collector/pg_buffercache_summary.go. Needs the
// pg_buffercache extension.

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	buffersUsedDesc   = newDesc("buffercache_summary", "buffers_used", "Number of used shared buffers")
	buffersUnusedDesc = newDesc("buffercache_summary", "buffers_unused", "Number of unused shared buffers")
	buffersDirtyDesc  = newDesc("buffercache_summary", "buffers_dirty", "Number of dirty shared buffers")
	buffersPinnedDesc = newDesc("buffercache_summary", "buffers_pinned", "Number of pinned shared buffers")
	usageCountAvgDesc = newDesc("buffercache_summary", "usagecount_avg", "Average usage count of used shared buffers")

	buffercacheQuery = `
		SELECT
		  buffers_used,
			buffers_unused,
			buffers_dirty,
			buffers_pinned,
			usagecount_avg
		FROM
		  pg_buffercache_summary()
		`
)

func updateBuffercacheSummary(ctx context.Context, conn *pgx.Conn, version int, ch chan<- prometheus.Metric) error {
	// pg_buffercache_summary is only in v16.
	if version < 160000 {
		return nil
	}
	rows, err := conn.Query(ctx, buffercacheQuery)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var used, unused, dirty, pinned *int64
		var usagecountAvg *float64
		if err := rows.Scan(&used, &unused, &dirty, &pinned, &usagecountAvg); err != nil {
			return err
		}
		ch <- gauge(usageCountAvgDesc, floatValue(usagecountAvg))
		ch <- gauge(buffersUsedDesc, intValue(used))
		ch <- gauge(buffersUnusedDesc, intValue(unused))
		ch <- gauge(buffersDirtyDesc, intValue(dirty))
		ch <- gauge(buffersPinnedDesc, intValue(pinned))
	}
	return rows.Err()
}
