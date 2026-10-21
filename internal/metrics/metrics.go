// Package metrics exposes the metrics of the local Postgres the way the
// collectors of postgres_exporter do
// (https://github.com/prometheus-community/postgres_exporter, Apache License
// 2.0): their queries, metric names, labels and conversions are copied, and the
// queries run over pgx. Query variants for servers older than PostgreSQL 13,
// which tembo does not support, are left out.
package metrics

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog/log"
)

const namespace = "pg"

// collectTimeout bounds a scrape, as the collection timeout of postgres_exporter does.
const collectTimeout = time.Minute

type update func(ctx context.Context, conn *pgx.Conn, version int, ch chan<- prometheus.Metric) error

var collectors = []struct {
	name   string
	update update
}{
	{"buffercache_summary", updateBuffercacheSummary},
	{"database", updateDatabase},
	{"database_wraparound", updateDatabaseWraparound},
	{"locks", updateLocks},
	{"long_running_transactions", updateLongRunningTransactions},
	{"postmaster", updatePostmaster},
	{"process_idle", updateProcessIdle},
	{"replication", updateReplication},
	{"replication_slots", updateReplicationSlots},
	{"roles", updateRoles},
	{"settings", updateSettings},
	{"stat_activity", updateStatActivity},
	{"stat_activity_autovacuum", updateStatActivityAutovacuum},
	{"stat_archiver", updateStatArchiver},
	{"stat_bgwriter", updateStatBGWriter},
	{"stat_checkpointer", updateStatCheckpointer},
	{"stat_database", updateStatDatabase},
	{"stat_progress_vacuum", updateStatProgressVacuum},
	{"stat_replication", updateStatReplication},
	{"stat_statements", updateStatStatements},
	{"stat_user_tables", updateStatUserTables},
	{"stat_wal_receiver", updateStatWalReceiver},
	{"statio_user_indexes", updateStatioUserIndexes},
	{"statio_user_tables", updateStatioUserTables},
	{"wal", updateWAL},
}

var (
	scrapeDurationDesc = newDesc("scrape", "collector_duration_seconds", "postgres_exporter: Duration of a collector scrape.", "collector")
	scrapeSuccessDesc  = newDesc("scrape", "collector_success", "postgres_exporter: Whether a collector succeeded.", "collector")
)

// Collector is a prometheus.Collector of the Postgres that connect reaches.
type Collector struct {
	connect func(context.Context) (*pgx.Conn, error)
}

// New returns a Collector that opens a connection with connect for every scrape.
func New(connect func(context.Context) (*pgx.Conn, error)) *Collector {
	return &Collector{connect: connect}
}

// Describe sends nothing: which metrics exist depends on the server, so the
// collector is unchecked.
func (c *Collector) Describe(chan<- *prometheus.Desc) {}

// Collect runs every collector over one connection. A collector that fails,
// e.g. for a missing extension, is reported by pg_scrape_collector_success.
func (c *Collector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), collectTimeout)
	defer cancel()
	conn, err := c.connect(ctx)
	if err != nil {
		log.Debug().Err(err).Msg("metrics: no connection to postgres")
		return
	}
	defer func() { _ = conn.Close(context.Background()) }()

	version := serverVersion(conn)
	for _, col := range collectors {
		begin := time.Now()
		success := 1.0
		if err := col.update(ctx, conn, version, ch); err != nil {
			success = 0
			log.Debug().Err(err).Str("collector", col.name).Msg("metrics: collector failed")
		}
		ch <- gauge(scrapeDurationDesc, time.Since(begin).Seconds(), col.name)
		ch <- gauge(scrapeSuccessDesc, success, col.name)
	}
}

// serverVersion returns the version of the server as server_version_num does,
// e.g. 170011 for 17.11.
func serverVersion(conn *pgx.Conn) int {
	major, minor := 0, 0
	v := conn.PgConn().ParameterStatus("server_version") // e.g. "17.11 (Debian 17.11-1.pgdg13+2)"
	n := &major
	for _, r := range v {
		switch {
		case r >= '0' && r <= '9':
			*n = *n*10 + int(r-'0')
		case r == '.' && n == &major:
			n = &minor
		default:
			return major*10000 + minor
		}
	}
	return major*10000 + minor
}

func newDesc(subsystem, name, help string, labels ...string) *prometheus.Desc {
	return prometheus.NewDesc(prometheus.BuildFQName(namespace, subsystem, name), help, labels, nil)
}

func gauge(desc *prometheus.Desc, value float64, labels ...string) prometheus.Metric {
	return prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, value, labels...)
}

func counter(desc *prometheus.Desc, value float64, labels ...string) prometheus.Metric {
	return prometheus.MustNewConstMetric(desc, prometheus.CounterValue, value, labels...)
}

// wrapped converts a Postgres counter, NULL being 0. It wraps at 2^53, beyond
// which float64 does not hold every integer.
func wrapped(v *int64) float64 {
	switch {
	case v == nil:
		return 0
	case *v >= 0:
		return float64(uint64(*v) % (1 << 53))
	}
	return float64(*v)
}

func intValue(v *int64) float64 {
	if v == nil {
		return 0
	}
	return float64(*v)
}

func floatValue(v *float64) float64 {
	if v == nil {
		return 0
	}
	return *v
}

// timeValue returns v in Unix seconds, NULL being 0.
func timeValue(v *time.Time) float64 {
	if v == nil {
		return 0
	}
	return float64(v.Unix())
}

func stringValue(v *string, null string) string {
	if v == nil {
		return null
	}
	return *v
}
