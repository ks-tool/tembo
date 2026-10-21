package metrics

// From postgres_exporter collector/pg_long_running_transactions.go, with its
// default threshold of one minute.

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"
)

const longRunningTransactionsThreshold = time.Minute

var (
	longRunningTransactionsCount = prometheus.NewDesc("pg_long_running_transactions",
		"Current number of long running transactions", nil, nil)
	longRunningTransactionsAgeInSeconds = newDesc("long_running_transactions", "oldest_timestamp_seconds",
		"The current maximum transaction age in seconds")

	longRunningTransactionsQuery = `
	SELECT
    COUNT(*) as transactions,
    MAX(EXTRACT(EPOCH FROM clock_timestamp() - pg_stat_activity.xact_start)) AS oldest_timestamp_seconds
FROM pg_catalog.pg_stat_activity
WHERE state IS DISTINCT FROM 'idle'
AND (now() - pg_stat_activity.xact_start) > make_interval(secs => $1)
AND query NOT LIKE 'autovacuum:%'
AND pg_stat_activity.xact_start IS NOT NULL
AND pid <> pg_backend_pid();
	`
)

func updateLongRunningTransactions(ctx context.Context, conn *pgx.Conn, _ int, ch chan<- prometheus.Metric) error {
	rows, err := conn.Query(ctx, longRunningTransactionsQuery, longRunningTransactionsThreshold.Seconds())
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var transactions int64
		var ageInSeconds *float64 // NULL without long running transactions
		if err := rows.Scan(&transactions, &ageInSeconds); err != nil {
			return err
		}
		ch <- gauge(longRunningTransactionsCount, float64(transactions))
		ch <- gauge(longRunningTransactionsAgeInSeconds, floatValue(ageInSeconds))
	}
	return rows.Err()
}
