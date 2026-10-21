package metrics

// From postgres_exporter collector/pg_stat_statements.go, with its defaults:
// no query text, no excluded databases or users, a limit of 100 statements.
// Needs the pg_stat_statements extension.

import (
	"context"
	"strconv"

	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog/log"
)

var (
	statStatementsCallsTotal = newDesc("stat_statements", "calls_total",
		"Number of times executed", "user", "datname", "queryid")
	statStatementsSecondsTotal = newDesc("stat_statements", "seconds_total",
		"Total time spent in the statement, in seconds", "user", "datname", "queryid")
	statStatementsRowsTotal = newDesc("stat_statements", "rows_total",
		"Total number of rows retrieved or affected by the statement", "user", "datname", "queryid")
	statStatementsBlockReadSecondsTotal = newDesc("stat_statements", "block_read_seconds_total",
		"Total time the statement spent reading blocks, in seconds", "user", "datname", "queryid")
	statStatementsBlockWriteSecondsTotal = newDesc("stat_statements", "block_write_seconds_total",
		"Total time the statement spent writing blocks, in seconds", "user", "datname", "queryid")

	pgStatStatementsQuery_PG13 = `SELECT
		pg_get_userbyid(userid) as user,
		pg_database.datname,
		pg_stat_statements.queryid,
		pg_stat_statements.calls as calls_total,
		pg_stat_statements.total_exec_time / 1000.0 as seconds_total,
		pg_stat_statements.rows as rows_total,
		pg_stat_statements.blk_read_time / 1000.0 as block_read_seconds_total,
		pg_stat_statements.blk_write_time / 1000.0 as block_write_seconds_total
		FROM pg_stat_statements(false)
	JOIN pg_database
		ON pg_database.oid = pg_stat_statements.dbid
	WHERE
		total_exec_time > (
		SELECT percentile_cont(0.1)
			WITHIN GROUP (ORDER BY total_exec_time)
			FROM pg_stat_statements(false)
		)
	ORDER BY seconds_total DESC
	LIMIT 100;`

	pgStatStatementsQuery_PG17 = `SELECT
		pg_get_userbyid(userid) as user,
		pg_database.datname,
		pg_stat_statements.queryid,
		pg_stat_statements.calls as calls_total,
		pg_stat_statements.total_exec_time / 1000.0 as seconds_total,
		pg_stat_statements.rows as rows_total,
		pg_stat_statements.shared_blk_read_time / 1000.0 as block_read_seconds_total,
		pg_stat_statements.shared_blk_write_time / 1000.0 as block_write_seconds_total
		FROM pg_stat_statements(false)
	JOIN pg_database
		ON pg_database.oid = pg_stat_statements.dbid
	WHERE
		total_exec_time > (
		SELECT percentile_cont(0.1)
			WITHIN GROUP (ORDER BY total_exec_time)
			FROM pg_stat_statements(false)
		)
	ORDER BY seconds_total DESC
	LIMIT 100;`
)

func updateStatStatements(ctx context.Context, conn *pgx.Conn, version int, ch chan<- prometheus.Metric) error {
	query := pgStatStatementsQuery_PG13
	if version >= 170000 {
		query = pgStatStatementsQuery_PG17
	}
	rows, err := conn.Query(ctx, query)
	if err != nil {
		return err
	}
	defer rows.Close()

	seen := make(map[[3]string]bool) // duplicates by (user, datname, queryid)
	for rows.Next() {
		var user, datname *string
		var queryid, callsTotal, rowsTotal *int64
		var secondsTotal, blockReadSecondsTotal, blockWriteSecondsTotal *float64
		if err := rows.Scan(&user, &datname, &queryid, &callsTotal, &secondsTotal, &rowsTotal,
			&blockReadSecondsTotal, &blockWriteSecondsTotal); err != nil {
			return err
		}

		labels := [3]string{stringValue(user, "unknown"), stringValue(datname, "unknown"), "unknown"}
		if queryid != nil {
			labels[2] = strconv.FormatInt(*queryid, 10)
		}
		if seen[labels] {
			log.Warn().Str("user", labels[0]).Str("datname", labels[1]).Str("queryid", labels[2]).
				Msg("metrics: duplicate pg_stat_statements entry")
			continue
		}
		seen[labels] = true

		ch <- counter(statStatementsCallsTotal, wrapped(callsTotal), labels[:]...)
		ch <- counter(statStatementsSecondsTotal, floatValue(secondsTotal), labels[:]...)
		ch <- counter(statStatementsRowsTotal, wrapped(rowsTotal), labels[:]...)
		ch <- counter(statStatementsBlockReadSecondsTotal, floatValue(blockReadSecondsTotal), labels[:]...)
		ch <- counter(statStatementsBlockWriteSecondsTotal, floatValue(blockWriteSecondsTotal), labels[:]...)
	}
	return rows.Err()
}
