package metrics

// From postgres_exporter collector/pg_statio_user_tables.go.

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	statioUserTablesHeapBlksRead = newDesc("statio_user_tables", "heap_blocks_read",
		"Number of disk blocks read from this table", userTableLabels...)
	statioUserTablesHeapBlksHit = newDesc("statio_user_tables", "heap_blocks_hit",
		"Number of buffer hits in this table", userTableLabels...)
	statioUserTablesIdxBlksRead = newDesc("statio_user_tables", "idx_blocks_read",
		"Number of disk blocks read from all indexes on this table", userTableLabels...)
	statioUserTablesIdxBlksHit = newDesc("statio_user_tables", "idx_blocks_hit",
		"Number of buffer hits in all indexes on this table", userTableLabels...)
	statioUserTablesToastBlksRead = newDesc("statio_user_tables", "toast_blocks_read",
		"Number of disk blocks read from this table's TOAST table (if any)", userTableLabels...)
	statioUserTablesToastBlksHit = newDesc("statio_user_tables", "toast_blocks_hit",
		"Number of buffer hits in this table's TOAST table (if any)", userTableLabels...)
	statioUserTablesTidxBlksRead = newDesc("statio_user_tables", "tidx_blocks_read",
		"Number of disk blocks read from this table's TOAST table indexes (if any)", userTableLabels...)
	statioUserTablesTidxBlksHit = newDesc("statio_user_tables", "tidx_blocks_hit",
		"Number of buffer hits in this table's TOAST table indexes (if any)", userTableLabels...)

	statioUserTablesQuery = `SELECT
		current_database() datname,
		schemaname,
		relname,
		heap_blks_read,
		heap_blks_hit,
		idx_blks_read,
		idx_blks_hit,
		toast_blks_read,
		toast_blks_hit,
		tidx_blks_read,
		tidx_blks_hit
	FROM pg_statio_user_tables`
)

func updateStatioUserTables(ctx context.Context, conn *pgx.Conn, _ int, ch chan<- prometheus.Metric) error {
	rows, err := conn.Query(ctx, statioUserTablesQuery)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var datname, schemaname, relname *string
		var heapBlksRead, heapBlksHit, idxBlksRead, idxBlksHit, toastBlksRead, toastBlksHit, tidxBlksRead, tidxBlksHit *int64
		if err := rows.Scan(&datname, &schemaname, &relname, &heapBlksRead, &heapBlksHit, &idxBlksRead, &idxBlksHit,
			&toastBlksRead, &toastBlksHit, &tidxBlksRead, &tidxBlksHit); err != nil {
			return err
		}
		labels := []string{stringValue(datname, "unknown"), stringValue(schemaname, "unknown"), stringValue(relname, "unknown")}
		ch <- counter(statioUserTablesHeapBlksRead, wrapped(heapBlksRead), labels...)
		ch <- counter(statioUserTablesHeapBlksHit, wrapped(heapBlksHit), labels...)
		ch <- counter(statioUserTablesIdxBlksRead, wrapped(idxBlksRead), labels...)
		ch <- counter(statioUserTablesIdxBlksHit, wrapped(idxBlksHit), labels...)
		ch <- counter(statioUserTablesToastBlksRead, wrapped(toastBlksRead), labels...)
		ch <- counter(statioUserTablesToastBlksHit, wrapped(toastBlksHit), labels...)
		ch <- counter(statioUserTablesTidxBlksRead, wrapped(tidxBlksRead), labels...)
		ch <- counter(statioUserTablesTidxBlksHit, wrapped(tidxBlksHit), labels...)
	}
	return rows.Err()
}
