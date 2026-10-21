package metrics

// From postgres_exporter collector/pg_stat_database.go.

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	statDatabaseNumbackends = newDesc("stat_database", "numbackends",
		"Number of backends currently connected to this database. This is the only column in this view that returns a value reflecting current state; all other columns return the accumulated values since the last reset.",
		"datid", "datname")
	statDatabaseXactCommit = newDesc("stat_database", "xact_commit",
		"Number of transactions in this database that have been committed", "datid", "datname")
	statDatabaseXactRollback = newDesc("stat_database", "xact_rollback",
		"Number of transactions in this database that have been rolled back", "datid", "datname")
	statDatabaseBlksRead = newDesc("stat_database", "blks_read",
		"Number of disk blocks read in this database", "datid", "datname")
	statDatabaseBlksHit = newDesc("stat_database", "blks_hit",
		"Number of times disk blocks were found already in the buffer cache, so that a read was not necessary (this only includes hits in the PostgreSQL buffer cache, not the operating system's file system cache)",
		"datid", "datname")
	statDatabaseTupReturned = newDesc("stat_database", "tup_returned",
		"Number of rows returned by queries in this database", "datid", "datname")
	statDatabaseTupFetched = newDesc("stat_database", "tup_fetched",
		"Number of rows fetched by queries in this database", "datid", "datname")
	statDatabaseTupInserted = newDesc("stat_database", "tup_inserted",
		"Number of rows inserted by queries in this database", "datid", "datname")
	statDatabaseTupUpdated = newDesc("stat_database", "tup_updated",
		"Number of rows updated by queries in this database", "datid", "datname")
	statDatabaseTupDeleted = newDesc("stat_database", "tup_deleted",
		"Number of rows deleted by queries in this database", "datid", "datname")
	statDatabaseConflicts = newDesc("stat_database", "conflicts",
		"Number of queries canceled due to conflicts with recovery in this database. (Conflicts occur only on standby servers; see pg_stat_database_conflicts for details.)",
		"datid", "datname")
	statDatabaseTempFiles = newDesc("stat_database", "temp_files",
		"Number of temporary files created by queries in this database. All temporary files are counted, regardless of why the temporary file was created (e.g., sorting or hashing), and regardless of the log_temp_files setting.",
		"datid", "datname")
	statDatabaseTempBytes = newDesc("stat_database", "temp_bytes",
		"Total amount of data written to temporary files by queries in this database. All temporary files are counted, regardless of why the temporary file was created, and regardless of the log_temp_files setting.",
		"datid", "datname")
	statDatabaseDeadlocks = newDesc("stat_database", "deadlocks",
		"Number of deadlocks detected in this database", "datid", "datname")
	statDatabaseBlkReadTime = newDesc("stat_database", "blk_read_time",
		"Time spent reading data file blocks by backends in this database, in milliseconds", "datid", "datname")
	statDatabaseBlkWriteTime = newDesc("stat_database", "blk_write_time",
		"Time spent writing data file blocks by backends in this database, in milliseconds", "datid", "datname")
	statDatabaseStatsReset = newDesc("stat_database", "stats_reset",
		"Time at which these statistics were last reset", "datid", "datname")
	statDatabaseActiveTime = newDesc("stat_database", "active_time_seconds_total",
		"Time spent executing SQL statements in this database, in seconds", "datid", "datname")
)

func statDatabaseQuery(columns []string) string {
	return fmt.Sprintf("SELECT %s FROM pg_stat_database;", strings.Join(columns, ","))
}

func updateStatDatabase(ctx context.Context, conn *pgx.Conn, version int, ch chan<- prometheus.Metric) error {
	columns := []string{
		"datid",
		"datname",
		"numbackends",
		"xact_commit",
		"xact_rollback",
		"blks_read",
		"blks_hit",
		"tup_returned",
		"tup_fetched",
		"tup_inserted",
		"tup_updated",
		"tup_deleted",
		"conflicts",
		"temp_files",
		"temp_bytes",
		"deadlocks",
		"blk_read_time",
		"blk_write_time",
		"stats_reset",
	}
	activeTimeAvail := version >= 140000
	if activeTimeAvail {
		columns = append(columns, "active_time")
	}

	rows, err := conn.Query(ctx, statDatabaseQuery(columns))
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var datid *uint32
		var datname *string
		var numBackends, xactCommit, xactRollback, blksRead, blksHit, tupReturned, tupFetched, tupInserted, tupUpdated, tupDeleted, conflicts, tempFiles, tempBytes, deadlocks *int64
		var blkReadTime, blkWriteTime, activeTime *float64
		var statsReset *time.Time
		r := []any{
			&datid,
			&datname,
			&numBackends,
			&xactCommit,
			&xactRollback,
			&blksRead,
			&blksHit,
			&tupReturned,
			&tupFetched,
			&tupInserted,
			&tupUpdated,
			&tupDeleted,
			&conflicts,
			&tempFiles,
			&tempBytes,
			&deadlocks,
			&blkReadTime,
			&blkWriteTime,
			&statsReset,
		}
		if activeTimeAvail {
			r = append(r, &activeTime)
		}
		if err := rows.Scan(r...); err != nil {
			return err
		}

		// A row with a NULL is skipped; a NULL stats_reset counts as 0.
		if datid == nil || datname == nil || numBackends == nil || xactCommit == nil || xactRollback == nil ||
			blksRead == nil || blksHit == nil || tupReturned == nil || tupFetched == nil || tupInserted == nil ||
			tupUpdated == nil || tupDeleted == nil || conflicts == nil || tempFiles == nil || tempBytes == nil ||
			deadlocks == nil || blkReadTime == nil || blkWriteTime == nil || activeTimeAvail && activeTime == nil {
			continue
		}

		labels := []string{strconv.FormatUint(uint64(*datid), 10), *datname}
		ch <- gauge(statDatabaseNumbackends, float64(*numBackends), labels...)
		ch <- counter(statDatabaseXactCommit, wrapped(xactCommit), labels...)
		ch <- counter(statDatabaseXactRollback, wrapped(xactRollback), labels...)
		ch <- counter(statDatabaseBlksRead, wrapped(blksRead), labels...)
		ch <- counter(statDatabaseBlksHit, wrapped(blksHit), labels...)
		ch <- counter(statDatabaseTupReturned, wrapped(tupReturned), labels...)
		ch <- counter(statDatabaseTupFetched, wrapped(tupFetched), labels...)
		ch <- counter(statDatabaseTupInserted, wrapped(tupInserted), labels...)
		ch <- counter(statDatabaseTupUpdated, wrapped(tupUpdated), labels...)
		ch <- counter(statDatabaseTupDeleted, wrapped(tupDeleted), labels...)
		ch <- counter(statDatabaseConflicts, wrapped(conflicts), labels...)
		ch <- counter(statDatabaseTempFiles, wrapped(tempFiles), labels...)
		ch <- counter(statDatabaseTempBytes, wrapped(tempBytes), labels...)
		ch <- counter(statDatabaseDeadlocks, wrapped(deadlocks), labels...)
		ch <- counter(statDatabaseBlkReadTime, *blkReadTime, labels...)
		ch <- counter(statDatabaseBlkWriteTime, *blkWriteTime, labels...)
		ch <- counter(statDatabaseStatsReset, timeValue(statsReset), labels...)
		if activeTimeAvail {
			ch <- counter(statDatabaseActiveTime, *activeTime/1000.0, labels...)
		}
	}
	return rows.Err()
}
