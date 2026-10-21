package metrics

// From postgres_exporter collector/pg_statio_user_indexes.go.

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	statioUserIndexesIdxBlksRead = newDesc("statio_user_indexes", "idx_blks_read_total",
		"Number of disk blocks read from this index", "schemaname", "relname", "indexrelname")
	statioUserIndexesIdxBlksHit = newDesc("statio_user_indexes", "idx_blks_hit_total",
		"Number of buffer hits in this index", "schemaname", "relname", "indexrelname")

	statioUserIndexesQuery = `
	SELECT
		schemaname,
		relname,
		indexrelname,
		idx_blks_read,
		idx_blks_hit
	FROM pg_statio_user_indexes
	`
)

func updateStatioUserIndexes(ctx context.Context, conn *pgx.Conn, _ int, ch chan<- prometheus.Metric) error {
	rows, err := conn.Query(ctx, statioUserIndexesQuery)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var schemaname, relname, indexrelname *string
		var idxBlksRead, idxBlksHit *int64
		if err := rows.Scan(&schemaname, &relname, &indexrelname, &idxBlksRead, &idxBlksHit); err != nil {
			return err
		}
		labels := []string{stringValue(schemaname, "unknown"), stringValue(relname, "unknown"), stringValue(indexrelname, "unknown")}
		ch <- counter(statioUserIndexesIdxBlksRead, wrapped(idxBlksRead), labels...)
		ch <- counter(statioUserIndexesIdxBlksHit, wrapped(idxBlksHit), labels...)
	}
	return rows.Err()
}
