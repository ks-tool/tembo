package metrics

// From postgres_exporter collector/pg_database_wraparound.go.

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	databaseWraparoundAgeDatfrozenxid = newDesc("database_wraparound", "age_datfrozenxid_seconds",
		"Age of the oldest transaction ID that has not been frozen.", "datname")
	databaseWraparoundAgeDatminmxid = newDesc("database_wraparound", "age_datminmxid_seconds",
		"Age of the oldest multi-transaction ID that has been replaced with a transaction ID.", "datname")

	databaseWraparoundQuery = `
	SELECT
		datname,
		age(d.datfrozenxid) as age_datfrozenxid,
		mxid_age(d.datminmxid) as age_datminmxid
	FROM
		pg_catalog.pg_database d
	WHERE
		d.datallowconn
	`
)

func updateDatabaseWraparound(ctx context.Context, conn *pgx.Conn, _ int, ch chan<- prometheus.Metric) error {
	rows, err := conn.Query(ctx, databaseWraparoundQuery)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var datname *string
		var ageDatfrozenxid, ageDatminmxid *int64
		if err := rows.Scan(&datname, &ageDatfrozenxid, &ageDatminmxid); err != nil {
			return err
		}
		if datname == nil || ageDatfrozenxid == nil || ageDatminmxid == nil {
			continue
		}
		ch <- gauge(databaseWraparoundAgeDatfrozenxid, float64(*ageDatfrozenxid), *datname)
		ch <- gauge(databaseWraparoundAgeDatminmxid, float64(*ageDatminmxid), *datname)
	}
	return rows.Err()
}
