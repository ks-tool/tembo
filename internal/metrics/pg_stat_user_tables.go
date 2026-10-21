package metrics

// From postgres_exporter collector/pg_stat_user_tables.go.

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	userTableLabels = []string{"datname", "schemaname", "relname"}

	statUserTablesSeqScan = newDesc("stat_user_tables", "seq_scan",
		"Number of sequential scans initiated on this table", userTableLabels...)
	statUserTablesSeqTupRead = newDesc("stat_user_tables", "seq_tup_read",
		"Number of live rows fetched by sequential scans", userTableLabels...)
	statUserTablesIdxScan = newDesc("stat_user_tables", "idx_scan",
		"Number of index scans initiated on this table", userTableLabels...)
	statUserTablesIdxTupFetch = newDesc("stat_user_tables", "idx_tup_fetch",
		"Number of live rows fetched by index scans", userTableLabels...)
	statUserTablesNTupIns = newDesc("stat_user_tables", "n_tup_ins",
		"Number of rows inserted", userTableLabels...)
	statUserTablesNTupUpd = newDesc("stat_user_tables", "n_tup_upd",
		"Number of rows updated", userTableLabels...)
	statUserTablesNTupDel = newDesc("stat_user_tables", "n_tup_del",
		"Number of rows deleted", userTableLabels...)
	statUserTablesNTupHotUpd = newDesc("stat_user_tables", "n_tup_hot_upd",
		"Number of rows HOT updated (i.e., with no separate index update required)", userTableLabels...)
	statUserTablesNLiveTup = newDesc("stat_user_tables", "n_live_tup",
		"Estimated number of live rows", userTableLabels...)
	statUserTablesNDeadTup = newDesc("stat_user_tables", "n_dead_tup",
		"Estimated number of dead rows", userTableLabels...)
	statUserTablesNModSinceAnalyze = newDesc("stat_user_tables", "n_mod_since_analyze",
		"Estimated number of rows changed since last analyze", userTableLabels...)
	statUserTablesLastVacuum = newDesc("stat_user_tables", "last_vacuum",
		"Last time at which this table was manually vacuumed (not counting VACUUM FULL)", userTableLabels...)
	statUserTablesLastAutovacuum = newDesc("stat_user_tables", "last_autovacuum",
		"Last time at which this table was vacuumed by the autovacuum daemon", userTableLabels...)
	statUserTablesLastAnalyze = newDesc("stat_user_tables", "last_analyze",
		"Last time at which this table was manually analyzed", userTableLabels...)
	statUserTablesLastAutoanalyze = newDesc("stat_user_tables", "last_autoanalyze",
		"Last time at which this table was analyzed by the autovacuum daemon", userTableLabels...)
	statUserTablesVacuumCount = newDesc("stat_user_tables", "vacuum_count",
		"Number of times this table has been manually vacuumed (not counting VACUUM FULL)", userTableLabels...)
	statUserTablesAutovacuumCount = newDesc("stat_user_tables", "autovacuum_count",
		"Number of times this table has been vacuumed by the autovacuum daemon", userTableLabels...)
	statUserTablesAnalyzeCount = newDesc("stat_user_tables", "analyze_count",
		"Number of times this table has been manually analyzed", userTableLabels...)
	statUserTablesAutoanalyzeCount = newDesc("stat_user_tables", "autoanalyze_count",
		"Number of times this table has been analyzed by the autovacuum daemon", userTableLabels...)
	statUserIndexSize = newDesc("stat_user_tables", "index_size_bytes",
		"Total disk space used by this index, in bytes", userTableLabels...)
	statUserTableSize = newDesc("stat_user_tables", "table_size_bytes",
		"Total disk space used by this table, in bytes", userTableLabels...)

	statUserTablesQuery = `SELECT
		current_database() datname,
		schemaname,
		relname,
		seq_scan,
		seq_tup_read,
		idx_scan,
		idx_tup_fetch,
		n_tup_ins,
		n_tup_upd,
		n_tup_del,
		n_tup_hot_upd,
		n_live_tup,
		n_dead_tup,
		n_mod_since_analyze,
		COALESCE(last_vacuum, '1970-01-01Z') as last_vacuum,
		COALESCE(last_autovacuum, '1970-01-01Z') as last_autovacuum,
		COALESCE(last_analyze, '1970-01-01Z') as last_analyze,
		COALESCE(last_autoanalyze, '1970-01-01Z') as last_autoanalyze,
		vacuum_count,
		autovacuum_count,
		analyze_count,
		autoanalyze_count,
		pg_indexes_size(relid) as indexes_size,
		pg_table_size(relid) as table_size
	FROM
		pg_stat_user_tables`
)

func updateStatUserTables(ctx context.Context, conn *pgx.Conn, _ int, ch chan<- prometheus.Metric) error {
	rows, err := conn.Query(ctx, statUserTablesQuery)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var datname, schemaname, relname *string
		var seqScan, seqTupRead, idxScan, idxTupFetch, nTupIns, nTupUpd, nTupDel, nTupHotUpd, nLiveTup, nDeadTup,
			nModSinceAnalyze, vacuumCount, autovacuumCount, analyzeCount, autoanalyzeCount, indexSize, tableSize *int64
		var lastVacuum, lastAutovacuum, lastAnalyze, lastAutoanalyze *time.Time
		if err := rows.Scan(&datname, &schemaname, &relname, &seqScan, &seqTupRead, &idxScan, &idxTupFetch, &nTupIns,
			&nTupUpd, &nTupDel, &nTupHotUpd, &nLiveTup, &nDeadTup, &nModSinceAnalyze, &lastVacuum, &lastAutovacuum,
			&lastAnalyze, &lastAutoanalyze, &vacuumCount, &autovacuumCount, &analyzeCount, &autoanalyzeCount,
			&indexSize, &tableSize); err != nil {
			return err
		}

		labels := []string{stringValue(datname, "unknown"), stringValue(schemaname, "unknown"), stringValue(relname, "unknown")}
		ch <- counter(statUserTablesSeqScan, wrapped(seqScan), labels...)
		ch <- counter(statUserTablesSeqTupRead, wrapped(seqTupRead), labels...)
		ch <- counter(statUserTablesIdxScan, wrapped(idxScan), labels...)
		ch <- counter(statUserTablesIdxTupFetch, wrapped(idxTupFetch), labels...)
		ch <- counter(statUserTablesNTupIns, wrapped(nTupIns), labels...)
		ch <- counter(statUserTablesNTupUpd, wrapped(nTupUpd), labels...)
		ch <- counter(statUserTablesNTupDel, wrapped(nTupDel), labels...)
		ch <- counter(statUserTablesNTupHotUpd, wrapped(nTupHotUpd), labels...)
		ch <- gauge(statUserTablesNLiveTup, intValue(nLiveTup), labels...)
		ch <- gauge(statUserTablesNDeadTup, intValue(nDeadTup), labels...)
		ch <- gauge(statUserTablesNModSinceAnalyze, intValue(nModSinceAnalyze), labels...)
		ch <- gauge(statUserTablesLastVacuum, timeValue(lastVacuum), labels...)
		ch <- gauge(statUserTablesLastAutovacuum, timeValue(lastAutovacuum), labels...)
		ch <- gauge(statUserTablesLastAnalyze, timeValue(lastAnalyze), labels...)
		ch <- gauge(statUserTablesLastAutoanalyze, timeValue(lastAutoanalyze), labels...)
		ch <- counter(statUserTablesVacuumCount, wrapped(vacuumCount), labels...)
		ch <- counter(statUserTablesAutovacuumCount, wrapped(autovacuumCount), labels...)
		ch <- counter(statUserTablesAnalyzeCount, wrapped(analyzeCount), labels...)
		ch <- counter(statUserTablesAutoanalyzeCount, wrapped(autoanalyzeCount), labels...)
		ch <- gauge(statUserIndexSize, intValue(indexSize), labels...)
		ch <- gauge(statUserTableSize, intValue(tableSize), labels...)
	}
	return rows.Err()
}
