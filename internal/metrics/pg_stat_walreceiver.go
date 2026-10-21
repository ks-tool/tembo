package metrics

// From postgres_exporter collector/pg_stat_walreceiver.go. Since PostgreSQL 13
// pg_stat_wal_receiver always has flushed_lsn, so the query that checks for
// the column is left out.

import (
	"context"
	"strconv"

	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	labelCats = []string{"upstream_host", "slot_name", "status"}

	statWalReceiverReceiveStartLsn = newDesc("stat_wal_receiver", "receive_start_lsn",
		"First write-ahead log location used when WAL receiver is started represented as a decimal", labelCats...)
	statWalReceiverReceiveStartTli = newDesc("stat_wal_receiver", "receive_start_tli",
		"First timeline number used when WAL receiver is started", labelCats...)
	statWalReceiverFlushedLSN = newDesc("stat_wal_receiver", "flushed_lsn",
		"Last write-ahead log location already received and flushed to disk, the initial value of this field being the first log location used when WAL receiver is started represented as a decimal", labelCats...)
	statWalReceiverReceivedTli = newDesc("stat_wal_receiver", "received_tli",
		"Timeline number of last write-ahead log location received and flushed to disk", labelCats...)
	statWalReceiverLastMsgSendTime = newDesc("stat_wal_receiver", "last_msg_send_time",
		"Send time of last message received from origin WAL sender", labelCats...)
	statWalReceiverLastMsgReceiptTime = newDesc("stat_wal_receiver", "last_msg_receipt_time",
		"Send time of last message received from origin WAL sender", labelCats...)
	statWalReceiverLatestEndLsn = newDesc("stat_wal_receiver", "latest_end_lsn",
		"Last write-ahead log location reported to origin WAL sender as integer", labelCats...)
	statWalReceiverLatestEndTime = newDesc("stat_wal_receiver", "latest_end_time",
		"Time of last write-ahead log location reported to origin WAL sender", labelCats...)
	statWalReceiverUpstreamNode = newDesc("stat_wal_receiver", "upstream_node",
		"Node ID of the upstream node", labelCats...)

	pgStatWalReceiverQuery = `
	SELECT
		trim(both '''' from substring(conninfo from 'host=([^ ]*)')) as upstream_host,
		slot_name,
		status,
		(receive_start_lsn- '0/0') % (2^52)::bigint as receive_start_lsn,
		(flushed_lsn - '0/0') % (2^52)::bigint as flushed_lsn,
		receive_start_tli,
		received_tli,
		extract(epoch from last_msg_send_time) as last_msg_send_time,
		extract(epoch from last_msg_receipt_time) as last_msg_receipt_time,
		(latest_end_lsn - '0/0') % (2^52)::bigint as latest_end_lsn,
		extract(epoch from latest_end_time) as latest_end_time,
		substring(slot_name from 'repmgr_slot_([0-9]*)') as upstream_node
	FROM pg_catalog.pg_stat_wal_receiver
	`
)

func updateStatWalReceiver(ctx context.Context, conn *pgx.Conn, _ int, ch chan<- prometheus.Metric) error {
	rows, err := conn.Query(ctx, pgStatWalReceiverQuery)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var upstreamHost, slotName, status, upstreamNode *string
		var receiveStartLsn, flushedLsn, receiveStartTli, receivedTli, latestEndLsn *int64
		var lastMsgSendTime, lastMsgReceiptTime, latestEndTime *float64
		if err := rows.Scan(&upstreamHost, &slotName, &status, &receiveStartLsn, &flushedLsn, &receiveStartTli,
			&receivedTli, &lastMsgSendTime, &lastMsgReceiptTime, &latestEndLsn, &latestEndTime, &upstreamNode); err != nil {
			return err
		}
		// Like postgres_exporter, skip a receiver with any NULL but upstream_node.
		if upstreamHost == nil || slotName == nil || status == nil || receiveStartLsn == nil || receiveStartTli == nil ||
			flushedLsn == nil || receivedTli == nil || lastMsgSendTime == nil || lastMsgReceiptTime == nil ||
			latestEndLsn == nil || latestEndTime == nil {
			continue
		}

		labels := []string{*upstreamHost, *slotName, *status}
		ch <- counter(statWalReceiverReceiveStartLsn, wrapped(receiveStartLsn), labels...)
		ch <- counter(statWalReceiverFlushedLSN, wrapped(flushedLsn), labels...)
		ch <- gauge(statWalReceiverReceiveStartTli, float64(*receiveStartTli), labels...)
		ch <- gauge(statWalReceiverReceivedTli, float64(*receivedTli), labels...)
		ch <- counter(statWalReceiverLastMsgSendTime, *lastMsgSendTime, labels...)
		ch <- counter(statWalReceiverLastMsgReceiptTime, *lastMsgReceiptTime, labels...)
		ch <- counter(statWalReceiverLatestEndLsn, wrapped(latestEndLsn), labels...)
		ch <- counter(statWalReceiverLatestEndTime, *latestEndTime, labels...)
		if upstreamNode != nil {
			if node, err := strconv.ParseInt(*upstreamNode, 10, 64); err == nil {
				ch <- gauge(statWalReceiverUpstreamNode, float64(node), labels...)
			}
		}
	}
	return rows.Err()
}
