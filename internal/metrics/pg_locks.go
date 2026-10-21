package metrics

// From postgres_exporter collector/pg_locks.go.

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	pgLocksDesc = newDesc("locks", "count", "Number of locks", "datname", "mode")

	pgLocksQuery = `
		SELECT
		  pg_database.datname as datname,
		  tmp.mode as mode,
		  COALESCE(count, 0) as count
		FROM
		  (
		    VALUES
		      ('accesssharelock'),
		      ('rowsharelock'),
		      ('rowexclusivelock'),
		      ('shareupdateexclusivelock'),
		      ('sharelock'),
		      ('sharerowexclusivelock'),
		      ('exclusivelock'),
		      ('accessexclusivelock'),
		      ('sireadlock')
		  ) AS tmp(mode)
		  CROSS JOIN pg_database
		  LEFT JOIN (
		    SELECT
		      database,
		      lower(mode) AS mode,
		      count(*) AS count
		    FROM
		      pg_locks
		    WHERE
		      database IS NOT NULL
		    GROUP BY
		      database,
		      lower(mode)
		  ) AS tmp2 ON tmp.mode = tmp2.mode
		  and pg_database.oid = tmp2.database
		ORDER BY
		  1
	`
)

func updateLocks(ctx context.Context, conn *pgx.Conn, _ int, ch chan<- prometheus.Metric) error {
	rows, err := conn.Query(ctx, pgLocksQuery)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var datname, mode *string
		var count *int64
		if err := rows.Scan(&datname, &mode, &count); err != nil {
			return err
		}
		if datname == nil || mode == nil {
			continue
		}
		ch <- gauge(pgLocksDesc, intValue(count), *datname, *mode)
	}
	return rows.Err()
}
