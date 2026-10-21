package metrics

// From postgres_exporter collector/pg_process_idle.go. As there, only the
// first row of the result is reported.

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	pgProcessIdleSeconds = newDesc("process_idle", "seconds", "Idle time of server processes", "state", "application_name")

	pgProcessIdleQuery = `WITH
			metrics AS (
				SELECT
				state,
				application_name,
				SUM(EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - state_change))::bigint)::float AS process_idle_seconds_sum,
				COUNT(*) AS process_idle_seconds_count
				FROM pg_stat_activity
				WHERE state ~ '^idle'
				AND pid <> pg_backend_pid()
				GROUP BY state, application_name
			),
			buckets AS (
				SELECT
				state,
				application_name,
				le,
				SUM(
					CASE WHEN EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - state_change)) <= le
					THEN 1
					ELSE 0
					END
				)::bigint AS bucket
				FROM
				pg_stat_activity,
				UNNEST(ARRAY[1, 2, 5, 15, 30, 60, 90, 120, 300]) AS le
				WHERE pid <> pg_backend_pid()
				GROUP BY state, application_name, le
				ORDER BY state, application_name, le
			)
			SELECT
			state,
			application_name,
			process_idle_seconds_sum as seconds_sum,
			process_idle_seconds_count as seconds_count,
			ARRAY_AGG(le) AS seconds,
			ARRAY_AGG(bucket) AS seconds_bucket
			FROM metrics JOIN buckets USING (state, application_name)
			GROUP BY 1, 2, 3, 4;`
)

func updateProcessIdle(ctx context.Context, conn *pgx.Conn, _ int, ch chan<- prometheus.Metric) error {
	var state, applicationName *string
	var secondsSum *float64
	var secondsCount *int64
	var seconds, secondsBucket []int64
	err := conn.QueryRow(ctx, pgProcessIdleQuery).Scan(&state, &applicationName, &secondsSum, &secondsCount, &seconds, &secondsBucket)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil // no idle processes
	}
	if err != nil {
		return err
	}

	buckets := make(map[float64]uint64, len(seconds))
	for i, second := range seconds {
		if i >= len(secondsBucket) {
			break
		}
		buckets[float64(second)] = uint64(secondsBucket[i])
	}
	var count uint64
	if secondsCount != nil {
		count = uint64(*secondsCount)
	}
	ch <- prometheus.MustNewConstHistogram(pgProcessIdleSeconds, count, floatValue(secondsSum), buckets,
		stringValue(state, "unknown"), stringValue(applicationName, "unknown"))
	return nil
}
