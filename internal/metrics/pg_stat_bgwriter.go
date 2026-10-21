package metrics

// From postgres_exporter collector/pg_stat_bgwriter.go.

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	statBGWriterCheckpointsTimedDesc = newDesc("stat_bgwriter", "checkpoints_timed_total",
		"Number of scheduled checkpoints that have been performed")
	statBGWriterCheckpointsReqDesc = newDesc("stat_bgwriter", "checkpoints_req_total",
		"Number of requested checkpoints that have been performed")
	statBGWriterCheckpointsReqTimeDesc = newDesc("stat_bgwriter", "checkpoint_write_time_total",
		"Total amount of time that has been spent in the portion of checkpoint processing where files are written to disk, in milliseconds")
	statBGWriterCheckpointsSyncTimeDesc = newDesc("stat_bgwriter", "checkpoint_sync_time_total",
		"Total amount of time that has been spent in the portion of checkpoint processing where files are synchronized to disk, in milliseconds")
	statBGWriterBuffersCheckpointDesc = newDesc("stat_bgwriter", "buffers_checkpoint_total",
		"Number of buffers written during checkpoints")
	statBGWriterBuffersCleanDesc = newDesc("stat_bgwriter", "buffers_clean_total",
		"Number of buffers written by the background writer")
	statBGWriterMaxwrittenCleanDesc = newDesc("stat_bgwriter", "maxwritten_clean_total",
		"Number of times the background writer stopped a cleaning scan because it had written too many buffers")
	statBGWriterBuffersBackendDesc = newDesc("stat_bgwriter", "buffers_backend_total",
		"Number of buffers written directly by a backend")
	statBGWriterBuffersBackendFsyncDesc = newDesc("stat_bgwriter", "buffers_backend_fsync_total",
		"Number of times a backend had to execute its own fsync call (normally the background writer handles those even when the backend does its own write)")
	statBGWriterBuffersAllocDesc = newDesc("stat_bgwriter", "buffers_alloc_total",
		"Number of buffers allocated")
	statBGWriterStatsResetDesc = newDesc("stat_bgwriter", "stats_reset_total",
		"Time at which these statistics were last reset")

	statBGWriterQueryBefore17 = `SELECT
		checkpoints_timed
		,checkpoints_req
		,checkpoint_write_time
		,checkpoint_sync_time
		,buffers_checkpoint
		,buffers_clean
		,maxwritten_clean
		,buffers_backend
		,buffers_backend_fsync
		,buffers_alloc
		,stats_reset
	FROM pg_stat_bgwriter;`

	statBGWriterQueryAfter17 = `SELECT
		buffers_clean
		,maxwritten_clean
		,buffers_alloc
		,stats_reset
	FROM pg_stat_bgwriter;`
)

func updateStatBGWriter(ctx context.Context, conn *pgx.Conn, version int, ch chan<- prometheus.Metric) error {
	if version >= 170000 {
		var bc, mwc, ba *int64
		var sr *time.Time
		if err := conn.QueryRow(ctx, statBGWriterQueryAfter17).Scan(&bc, &mwc, &ba, &sr); err != nil {
			return err
		}
		ch <- counter(statBGWriterBuffersCleanDesc, wrapped(bc))
		ch <- counter(statBGWriterMaxwrittenCleanDesc, wrapped(mwc))
		ch <- counter(statBGWriterBuffersAllocDesc, wrapped(ba))
		ch <- counter(statBGWriterStatsResetDesc, timeValue(sr))
		return nil
	}

	var cpt, cpr, bcp, bc, mwc, bb, bbf, ba *int64
	var cpwt, cpst *float64
	var sr *time.Time
	if err := conn.QueryRow(ctx, statBGWriterQueryBefore17).Scan(&cpt, &cpr, &cpwt, &cpst, &bcp, &bc, &mwc, &bb, &bbf, &ba, &sr); err != nil {
		return err
	}
	ch <- counter(statBGWriterCheckpointsTimedDesc, wrapped(cpt))
	ch <- counter(statBGWriterCheckpointsReqDesc, wrapped(cpr))
	ch <- counter(statBGWriterCheckpointsReqTimeDesc, floatValue(cpwt))
	ch <- counter(statBGWriterCheckpointsSyncTimeDesc, floatValue(cpst))
	ch <- counter(statBGWriterBuffersCheckpointDesc, wrapped(bcp))
	ch <- counter(statBGWriterBuffersCleanDesc, wrapped(bc))
	ch <- counter(statBGWriterMaxwrittenCleanDesc, wrapped(mwc))
	ch <- counter(statBGWriterBuffersBackendDesc, wrapped(bb))
	ch <- counter(statBGWriterBuffersBackendFsyncDesc, wrapped(bbf))
	ch <- counter(statBGWriterBuffersAllocDesc, wrapped(ba))
	ch <- counter(statBGWriterStatsResetDesc, timeValue(sr))
	return nil
}
