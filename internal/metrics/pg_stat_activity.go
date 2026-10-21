package metrics

// From postgres_exporter collector/pg_stat_activity.go.

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	statActivityLabels = []string{
		"datname",
		"state",
		"usename",
		"application_name",
		"backend_type",
		"wait_event_type",
		"wait_event",
	}
	statActivityCountDesc = newDesc("stat_activity", "count",
		"number of connections in this state", statActivityLabels...)
	statActivityMaxTxDurationDesc = newDesc("stat_activity", "max_tx_duration",
		"max duration in seconds any active transaction has been running", statActivityLabels...)

	statActivityQuery = `
			SELECT
				pg_database.datname,
				tmp.state,
				tmp2.usename,
				tmp2.application_name,
				tmp2.backend_type,
				tmp2.wait_event_type,
				tmp2.wait_event,
				COALESCE(count,0) as count,
				COALESCE(max_tx_duration,0) as max_tx_duration
			FROM
				(
				  VALUES ('active'),
				  		 ('idle'),
				  		 ('idle in transaction'),
				  		 ('idle in transaction (aborted)'),
				  		 ('fastpath function call'),
				  		 ('disabled')
				) AS tmp(state) CROSS JOIN pg_database
			LEFT JOIN
			(
				SELECT
					datname,
					state,
					usename,
					application_name,
					backend_type,
					wait_event_type,
					wait_event,
					count(*) AS count,
					MAX(EXTRACT(EPOCH FROM now() - xact_start))::float AS max_tx_duration
				FROM pg_stat_activity
				WHERE pid <> pg_backend_pid()
				GROUP BY datname,state,usename,application_name,backend_type,wait_event_type,wait_event) AS tmp2
				ON tmp.state = tmp2.state AND pg_database.datname = tmp2.datname
			`
)

func updateStatActivity(ctx context.Context, conn *pgx.Conn, _ int, ch chan<- prometheus.Metric) error {
	rows, err := conn.Query(ctx, statActivityQuery)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var datname, state, usename, applicationName, backendType, waitEventType, waitEvent *string
		var count *int64
		var maxTxDuration *float64
		if err := rows.Scan(&datname, &state, &usename, &applicationName, &backendType, &waitEventType, &waitEvent,
			&count, &maxTxDuration); err != nil {
			return err
		}
		labels := []string{
			stringValue(datname, ""),
			stringValue(state, ""),
			stringValue(usename, ""),
			stringValue(applicationName, ""),
			stringValue(backendType, ""),
			stringValue(waitEventType, ""),
			stringValue(waitEvent, ""),
		}
		if count != nil {
			ch <- gauge(statActivityCountDesc, float64(*count), labels...)
		}
		if maxTxDuration != nil {
			ch <- gauge(statActivityMaxTxDurationDesc, *maxTxDuration, labels...)
		}
	}
	return rows.Err()
}
