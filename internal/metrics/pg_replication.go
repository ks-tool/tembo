package metrics

// From postgres_exporter collector/pg_replication.go.

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	pgReplicationLag        = newDesc("replication", "lag_seconds", "Replication lag behind master in seconds")
	pgReplicationIsReplica  = newDesc("replication", "is_replica", "Indicates if the server is a replica")
	pgReplicationLastReplay = newDesc("replication", "last_replay_seconds", "Age of last replay in seconds")

	pgReplicationQuery = `SELECT
	CASE
		WHEN NOT pg_is_in_recovery() THEN 0
                WHEN pg_last_wal_receive_lsn () = pg_last_wal_replay_lsn () THEN 0
		ELSE GREATEST (0, EXTRACT(EPOCH FROM (now() - pg_last_xact_replay_timestamp())))
	END AS lag,
	CASE
		WHEN pg_is_in_recovery() THEN 1
		ELSE 0
	END as is_replica,
	GREATEST (0, EXTRACT(EPOCH FROM (now() - pg_last_xact_replay_timestamp()))) as last_replay`
)

func updateReplication(ctx context.Context, conn *pgx.Conn, _ int, ch chan<- prometheus.Metric) error {
	var lag, replayAge float64
	var isReplica int64
	if err := conn.QueryRow(ctx, pgReplicationQuery).Scan(&lag, &isReplica, &replayAge); err != nil {
		return err
	}
	ch <- gauge(pgReplicationLag, lag)
	ch <- gauge(pgReplicationIsReplica, float64(isReplica))
	ch <- gauge(pgReplicationLastReplay, replayAge)
	return nil
}
