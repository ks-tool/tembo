package metrics

// From postgres_exporter collector/pg_stat_archiver.go.

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	statArchiverArchivedCountDesc = newDesc("stat_archiver", "archived_count",
		"Number of WAL files that have been successfully archived")
	statArchiverFailedCountDesc = newDesc("stat_archiver", "failed_count",
		"Number of failed attempts for archiving WAL files")
	statArchiverLastArchiveAgeDesc = newDesc("stat_archiver", "last_archive_age",
		"Time in seconds since last WAL segment was successfully archived")

	statArchiverQuery = `SELECT
		archived_count,
		failed_count,
		extract(epoch from now() - last_archived_time) AS last_archive_age
	FROM pg_stat_archiver;`
)

func updateStatArchiver(ctx context.Context, conn *pgx.Conn, _ int, ch chan<- prometheus.Metric) error {
	var archivedCount, failedCount *int64
	var lastArchiveAge *float64
	if err := conn.QueryRow(ctx, statArchiverQuery).Scan(&archivedCount, &failedCount, &lastArchiveAge); err != nil {
		return err
	}
	if archivedCount != nil {
		ch <- counter(statArchiverArchivedCountDesc, wrapped(archivedCount))
	}
	if failedCount != nil {
		ch <- counter(statArchiverFailedCountDesc, wrapped(failedCount))
	}
	if lastArchiveAge != nil {
		ch <- gauge(statArchiverLastArchiveAgeDesc, *lastArchiveAge)
	}
	return nil
}
