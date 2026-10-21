package metrics

// From postgres_exporter collector/pg_stat_replication.go.

import (
	"context"
	"strconv"

	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	statReplicationLabels = []string{"application_name", "client_addr", "state", "slot_name", "pid"}

	statReplicationCurrentWalLSNBytesDesc = newDesc("stat_replication", "pg_current_wal_lsn_bytes",
		"WAL position in bytes", statReplicationLabels...)
	statReplicationWalLSNDiffDesc = newDesc("stat_replication", "pg_wal_lsn_diff",
		"Lag in bytes between master and slave", statReplicationLabels...)

	// slot_name is not a column of pg_stat_replication; it lives on
	// pg_replication_slots and is linked via the standby's WAL sender PID.
	statReplicationQuery = `
			SELECT
				application_name,
				client_addr::text,
				state,
				s.slot_name,
				pg_stat_replication.pid,
				(case pg_is_in_recovery() when 't' then pg_wal_lsn_diff(pg_last_wal_receive_lsn(), pg_lsn('0/0'))::float else pg_wal_lsn_diff(pg_current_wal_lsn(), pg_lsn('0/0'))::float end) AS pg_current_wal_lsn_bytes,
				(case pg_is_in_recovery() when 't' then pg_wal_lsn_diff(pg_last_wal_receive_lsn(), replay_lsn)::float else pg_wal_lsn_diff(pg_current_wal_lsn(), replay_lsn)::float end) AS pg_wal_lsn_diff
			FROM pg_stat_replication
			LEFT JOIN pg_replication_slots s ON s.active_pid = pg_stat_replication.pid
			`
)

func updateStatReplication(ctx context.Context, conn *pgx.Conn, _ int, ch chan<- prometheus.Metric) error {
	rows, err := conn.Query(ctx, statReplicationQuery)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var applicationName, clientAddr, state, slotName *string
		var pid *int64
		var currentWalLSNBytes, walLSNDiff *float64
		if err := rows.Scan(&applicationName, &clientAddr, &state, &slotName, &pid, &currentWalLSNBytes, &walLSNDiff); err != nil {
			return err
		}
		labels := []string{
			stringValue(applicationName, ""),
			stringValue(clientAddr, ""),
			stringValue(state, ""),
			stringValue(slotName, ""),
			"",
		}
		if pid != nil {
			labels[4] = strconv.FormatInt(*pid, 10)
		}
		if currentWalLSNBytes != nil {
			ch <- gauge(statReplicationCurrentWalLSNBytesDesc, *currentWalLSNBytes, labels...)
		}
		if walLSNDiff != nil {
			ch <- gauge(statReplicationWalLSNDiffDesc, *walLSNDiff, labels...)
		}
	}
	return rows.Err()
}
