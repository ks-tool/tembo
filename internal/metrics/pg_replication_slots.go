package metrics

// From postgres_exporter collector/pg_replication_slots.go.

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	replicationSlotsLabels     = []string{"slot_name", "database"}
	replicationSlotsSlotLabels = []string{"slot_name", "slot_type"}

	replicationSlotsActiveDesc = newDesc("replication_slots", "active",
		"Flag indicating if the slot is active", replicationSlotsLabels...)
	replicationSlotsWalLSNDiffDesc = newDesc("replication_slots", "pg_wal_lsn_diff",
		"Replication lag in bytes", replicationSlotsLabels...)
	replicationSlotsCurrentWalDesc = newDesc("replication_slots", "slot_current_wal_lsn",
		"current wal lsn value", replicationSlotsSlotLabels...)
	replicationSlotsCurrentFlushDesc = newDesc("replication_slots", "slot_confirmed_flush_lsn",
		"last lsn confirmed flushed to the replication slot", replicationSlotsSlotLabels...)
	replicationSlotsIsActiveDesc = newDesc("replication_slots", "slot_is_active",
		"whether the replication slot is active or not", replicationSlotsSlotLabels...)
	replicationSlotsSafeWal = newDesc("replication_slots", "safe_wal_size_bytes",
		"number of bytes that can be written to WAL such that this slot is not in danger of getting in state lost", replicationSlotsSlotLabels...)
	replicationSlotsWalStatus = newDesc("replication_slots", "wal_status",
		"availability of WAL files claimed by this slot", "slot_name", "slot_type", "wal_status")

	replicationSlotsQuery = `
			SELECT slot_name, database, active,
				(case pg_is_in_recovery() when 't' then pg_wal_lsn_diff(pg_last_wal_receive_lsn(), restart_lsn) else pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) end) as pg_wal_lsn_diff
			FROM pg_replication_slots
			`
	replicationSlotsSlotNewQuery = `SELECT
		slot_name,
		slot_type,
		CASE WHEN pg_is_in_recovery() THEN
		    pg_last_wal_receive_lsn() - '0/0'
		ELSE
		    pg_current_wal_lsn() - '0/0'
		END AS current_wal_lsn,
		COALESCE(confirmed_flush_lsn, '0/0') - '0/0' AS confirmed_flush_lsn,
		active,
		safe_wal_size,
		wal_status
	FROM pg_replication_slots;`
)

func updateReplicationSlots(ctx context.Context, conn *pgx.Conn, _ int, ch chan<- prometheus.Metric) error {
	rows, err := conn.Query(ctx, replicationSlotsQuery)
	if err != nil {
		return err
	}
	for rows.Next() {
		var slotName, database *string
		var active *bool
		var walLSNDiff *float64
		if err := rows.Scan(&slotName, &database, &active, &walLSNDiff); err != nil {
			rows.Close()
			return err
		}
		labels := []string{stringValue(slotName, ""), stringValue(database, "")}
		if active != nil {
			ch <- gauge(replicationSlotsActiveDesc, boolValue(*active), labels...)
		}
		if walLSNDiff != nil {
			ch <- gauge(replicationSlotsWalLSNDiffDesc, *walLSNDiff, labels...)
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}

	rows, err = conn.Query(ctx, replicationSlotsSlotNewQuery)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var slotName, slotType, walStatus *string
		var walLSN, flushLSN *float64
		var isActive *bool
		var safeWalSize *int64
		if err := rows.Scan(&slotName, &slotType, &walLSN, &flushLSN, &isActive, &safeWalSize, &walStatus); err != nil {
			return err
		}
		labels := []string{stringValue(slotName, "unknown"), stringValue(slotType, "unknown")}
		if walLSN != nil {
			ch <- gauge(replicationSlotsCurrentWalDesc, *walLSN, labels...)
		}
		if isActive != nil && *isActive && flushLSN != nil {
			ch <- gauge(replicationSlotsCurrentFlushDesc, *flushLSN, labels...)
		}
		if isActive != nil {
			ch <- gauge(replicationSlotsIsActiveDesc, boolValue(*isActive), labels...)
		}
		if safeWalSize != nil {
			ch <- gauge(replicationSlotsSafeWal, float64(*safeWalSize), labels...)
		}
		if walStatus != nil {
			ch <- gauge(replicationSlotsWalStatus, 1, labels[0], labels[1], *walStatus)
		}
	}
	return rows.Err()
}

func boolValue(b bool) float64 {
	if b {
		return 1
	}
	return 0
}
