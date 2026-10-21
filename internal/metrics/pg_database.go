package metrics

// From postgres_exporter collector/pg_database.go.

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	pgDatabaseSizeDesc             = newDesc("database", "size_bytes", "Disk space used by the database", "datname")
	pgDatabaseConnectionLimitsDesc = newDesc("database", "connection_limit", "Connection limit set for the database", "datname")

	pgDatabaseQuery     = "SELECT pg_database.datname, pg_database.datconnlimit FROM pg_database;"
	pgDatabaseSizeQuery = "SELECT pg_database_size($1)"
)

func updateDatabase(ctx context.Context, conn *pgx.Conn, _ int, ch chan<- prometheus.Metric) error {
	rows, err := conn.Query(ctx, pgDatabaseQuery)
	if err != nil {
		return err
	}
	var databases []string
	for rows.Next() {
		var datname *string
		var connLimit *int64
		if err := rows.Scan(&datname, &connLimit); err != nil {
			rows.Close()
			return err
		}
		if datname == nil {
			continue
		}
		databases = append(databases, *datname)
		ch <- gauge(pgDatabaseConnectionLimitsDesc, intValue(connLimit), *datname)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}

	// The connection runs one query at a time, so sizes follow the list.
	for _, datname := range databases {
		var size *int64
		if err := conn.QueryRow(ctx, pgDatabaseSizeQuery, datname).Scan(&size); err != nil {
			return err
		}
		ch <- gauge(pgDatabaseSizeDesc, intValue(size), datname)
	}
	return nil
}
