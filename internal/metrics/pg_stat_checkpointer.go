package metrics

// From postgres_exporter collector/pg_stat_checkpointer.go.

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	statCheckpointerNumTimedDesc = newDesc("stat_checkpointer", "num_timed_total",
		"Number of scheduled checkpoints due to timeout")
	statCheckpointerNumRequestedDesc = newDesc("stat_checkpointer", "num_requested_total",
		"Number of requested checkpoints that have been performed")
	statCheckpointerRestartpointsTimedDesc = newDesc("stat_checkpointer", "restartpoints_timed_total",
		"Number of scheduled restartpoints due to timeout or after a failed attempt to perform it")
	statCheckpointerRestartpointsReqDesc = newDesc("stat_checkpointer", "restartpoints_req_total",
		"Number of requested restartpoints")
	statCheckpointerRestartpointsDoneDesc = newDesc("stat_checkpointer", "restartpoints_done_total",
		"Number of restartpoints that have been performed")
	statCheckpointerWriteTimeDesc = newDesc("stat_checkpointer", "write_time_total",
		"Total amount of time that has been spent in the portion of processing checkpoints and restartpoints where files are written to disk, in milliseconds")
	statCheckpointerSyncTimeDesc = newDesc("stat_checkpointer", "sync_time_total",
		"Total amount of time that has been spent in the portion of processing checkpoints and restartpoints where files are synchronized to disk, in milliseconds")
	statCheckpointerBuffersWrittenDesc = newDesc("stat_checkpointer", "buffers_written_total",
		"Number of buffers written during checkpoints and restartpoints")
	statCheckpointerStatsResetDesc = newDesc("stat_checkpointer", "stats_reset_total",
		"Time at which these statistics were last reset")

	statCheckpointerQuery = `SELECT
		num_timed
		,num_requested
		,restartpoints_timed
		,restartpoints_req
		,restartpoints_done
		,write_time
		,sync_time
		,buffers_written
		,stats_reset
	FROM pg_stat_checkpointer;`
)

func updateStatCheckpointer(ctx context.Context, conn *pgx.Conn, version int, ch chan<- prometheus.Metric) error {
	// pg_stat_checkpointer appeared in PostgreSQL 17.
	if version < 170000 {
		return nil
	}

	var nt, nr, rpt, rpr, rpd, bw *int64
	var wt, st *float64
	var sr *time.Time
	if err := conn.QueryRow(ctx, statCheckpointerQuery).Scan(&nt, &nr, &rpt, &rpr, &rpd, &wt, &st, &bw, &sr); err != nil {
		return err
	}
	ch <- counter(statCheckpointerNumTimedDesc, wrapped(nt))
	ch <- counter(statCheckpointerNumRequestedDesc, wrapped(nr))
	ch <- counter(statCheckpointerRestartpointsTimedDesc, wrapped(rpt))
	ch <- counter(statCheckpointerRestartpointsReqDesc, wrapped(rpr))
	ch <- counter(statCheckpointerRestartpointsDoneDesc, wrapped(rpd))
	ch <- counter(statCheckpointerWriteTimeDesc, floatValue(wt))
	ch <- counter(statCheckpointerSyncTimeDesc, floatValue(st))
	ch <- counter(statCheckpointerBuffersWrittenDesc, wrapped(bw))
	ch <- counter(statCheckpointerStatsResetDesc, timeValue(sr))
	return nil
}
