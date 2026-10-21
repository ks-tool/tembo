package metrics

// From postgres_exporter collector/pg_stat_progress_vacuum.go.

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"
)

var vacuumPhases = []string{
	"initializing",
	"scanning heap",
	"vacuuming indexes",
	"vacuuming heap",
	"cleaning up indexes",
	"truncating heap",
	"performing final cleanup",
}

var (
	statProgressVacuumPhase = newDesc("stat_progress_vacuum", "phase",
		"Current vacuum phase (1 = active, 0 = inactive). Label 'phase' is human-readable.", "datname", "relname", "phase")
	statProgressVacuumHeapBlksTotal = newDesc("stat_progress_vacuum", "heap_blks",
		"Total number of heap blocks in the table being vacuumed.", "datname", "relname")
	statProgressVacuumHeapBlksScanned = newDesc("stat_progress_vacuum", "heap_blks_scanned",
		"Number of heap blocks scanned so far.", "datname", "relname")
	statProgressVacuumHeapBlksVacuumed = newDesc("stat_progress_vacuum", "heap_blks_vacuumed",
		"Number of heap blocks vacuumed so far.", "datname", "relname")
	statProgressVacuumIndexVacuumCount = newDesc("stat_progress_vacuum", "index_vacuums",
		"Number of completed index vacuum cycles.", "datname", "relname")
	statProgressVacuumMaxDeadTuples = newDesc("stat_progress_vacuum", "max_dead_tuples",
		"Maximum number of dead tuples that can be stored before cleanup is performed.", "datname", "relname")
	statProgressVacuumNumDeadTuples = newDesc("stat_progress_vacuum", "num_dead_tuples",
		"Current number of dead tuples found so far.", "datname", "relname")

	// Based on the view definition of pg_stat_progress_vacuum, without the
	// conversion of "phase" to a human-readable string. relname is resolved
	// via ::regclass::text when the vacuum runs in the connected database;
	// otherwise the raw OID is used, pg_class being per-database.
	statProgressVacuumQuery = `SELECT
		d.datname,
		COALESCE(
			CASE WHEN d.datname = current_database() THEN s.relid::regclass::text END,
			s.relid::text
		) AS relname,
		s.param1 AS phase,
		s.param2 AS heap_blks_total,
		s.param3 AS heap_blks_scanned,
		s.param4 AS heap_blks_vacuumed,
		s.param5 AS index_vacuum_count,
		s.param6 AS max_dead_tuples,
		s.param7 AS num_dead_tuples
	FROM
		pg_stat_get_progress_info('VACUUM'::text)
		s(pid, datid, relid, param1, param2, param3, param4, param5, param6, param7, param8, param9, param10, param11, param12, param13, param14, param15, param16, param17, param18, param19, param20)
	LEFT JOIN
		pg_database d ON s.datid = d.oid`
)

func updateStatProgressVacuum(ctx context.Context, conn *pgx.Conn, _ int, ch chan<- prometheus.Metric) error {
	rows, err := conn.Query(ctx, statProgressVacuumQuery)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var datname, relname *string
		var phase, heapBlksTotal, heapBlksScanned, heapBlksVacuumed, indexVacuumCount, maxDeadTuples, numDeadTuples *int64
		if err := rows.Scan(&datname, &relname, &phase, &heapBlksTotal, &heapBlksScanned, &heapBlksVacuumed,
			&indexVacuumCount, &maxDeadTuples, &numDeadTuples); err != nil {
			return err
		}

		datnameLabel, relnameLabel := stringValue(datname, "unknown"), stringValue(relname, "unknown")
		for i, label := range vacuumPhases {
			// Only the current phase is 1.
			v := 0.0
			if phase != nil && int64(i) == *phase {
				v = 1.0
			}
			ch <- gauge(statProgressVacuumPhase, v, datnameLabel, relnameLabel, label)
		}
		ch <- gauge(statProgressVacuumHeapBlksTotal, intValue(heapBlksTotal), datnameLabel, relnameLabel)
		ch <- gauge(statProgressVacuumHeapBlksScanned, intValue(heapBlksScanned), datnameLabel, relnameLabel)
		ch <- gauge(statProgressVacuumHeapBlksVacuumed, intValue(heapBlksVacuumed), datnameLabel, relnameLabel)
		ch <- gauge(statProgressVacuumIndexVacuumCount, intValue(indexVacuumCount), datnameLabel, relnameLabel)
		ch <- gauge(statProgressVacuumMaxDeadTuples, intValue(maxDeadTuples), datnameLabel, relnameLabel)
		ch <- gauge(statProgressVacuumNumDeadTuples, intValue(numDeadTuples), datnameLabel, relnameLabel)
	}
	return rows.Err()
}
