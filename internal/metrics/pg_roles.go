package metrics

// From postgres_exporter collector/pg_roles.go.

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	pgRolesConnectionLimitsDesc = newDesc("roles", "connection_limit", "Connection limit set for the role", "rolname")

	pgRolesConnectionLimitsQuery = "SELECT pg_roles.rolname, pg_roles.rolconnlimit FROM pg_roles"
)

func updateRoles(ctx context.Context, conn *pgx.Conn, _ int, ch chan<- prometheus.Metric) error {
	rows, err := conn.Query(ctx, pgRolesConnectionLimitsQuery)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var rolname *string
		var connLimit *int64
		if err := rows.Scan(&rolname, &connLimit); err != nil {
			return err
		}
		if rolname == nil || connLimit == nil {
			continue
		}
		ch <- gauge(pgRolesConnectionLimitsDesc, float64(*connLimit), *rolname)
	}
	return rows.Err()
}
