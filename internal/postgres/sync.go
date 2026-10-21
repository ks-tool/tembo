package postgres

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/ks-tool/tembo/internal/agent"

	"github.com/jackc/pgx/v5"
)

// SetSyncStandbys records the standbys this primary commits synchronously
// with, identified by their application_name; configure writes them as
// synchronous_standby_names and the caller reloads. An empty list turns
// synchronous commit off, except in strict mode, where the previous list is
// kept: without it every commit would block, which is the durability strict
// mode is for. It returns whether the rendered value changed.
func (i *Instance) SetSyncStandbys(mode string, count uint64, ids []string) bool {
	i.mu.Lock()
	if len(ids) == 0 && mode == agent.SyncStrict && len(i.syncIDs) > 0 {
		i.mu.Unlock()
		return false
	}
	names := formatSyncStandbys(mode, count, ids)
	changed := names != i.syncNames
	if changed {
		i.syncNames, i.syncIDs = names, slices.Clone(ids)
	}
	i.mu.Unlock()
	return changed
}

// SyncStandbys returns the standbys this primary commits synchronously with;
// empty on a standby and with synchronous_mode off.
func (i *Instance) SyncStandbys() []string {
	i.mu.Lock()
	defer i.mu.Unlock()
	return slices.Clone(i.syncIDs)
}

// syncStandbyNames is the synchronous_standby_names value configure writes
// for a primary; "" turns synchronous commit off.
func (i *Instance) syncStandbyNames() string {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.syncNames
}

// formatSyncStandbys renders synchronous_standby_names: count standbys of ids
// picked in quorum (ANY) or strict (FIRST) order. "off" and an empty list
// render "", synchronous commit off.
func formatSyncStandbys(mode string, count uint64, ids []string) string {
	if len(ids) == 0 || mode == agent.SyncOff {
		return ""
	}
	if count < 1 {
		count = 1
	}
	if uint64(len(ids)) < count {
		count = uint64(len(ids))
	}
	quoted := make([]string, len(ids))
	for n, id := range ids {
		quoted[n] = strconv.Quote(id)
	}
	list := fmt.Sprintf("%d (%s)", count, strings.Join(quoted, ", "))
	if mode == agent.SyncStrict {
		return "FIRST " + list
	}
	return "ANY " + list
}

// syncCandidates returns the sorted ids of the standbys that both report
// themselves running and stream from the primary right now. A node tagged
// nosync never takes part.
func syncCandidates(nodes map[string]agent.Postgres, streaming []string, self string) []string {
	live := make(map[string]bool, len(streaming))
	for _, name := range streaming {
		live[name] = true
	}
	var ids []string
	for id, p := range nodes {
		if id != self && p.Running && p.InRecovery && !p.NoSync && live[id] {
			ids = append(ids, id)
		}
	}
	slices.Sort(ids)
	return ids
}

// streamingStandbys returns the application_name of every standby streaming
// from this primary: the ids of the nodes, which conninfo sets.
func (i *Instance) streamingStandbys(ctx context.Context) ([]string, error) {
	var names []string
	err := i.query(ctx, func(ctx context.Context, conn *pgx.Conn) error {
		rows, err := conn.Query(ctx, "select application_name from pg_stat_replication where state = 'streaming'")
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				return err
			}
			names = append(names, name)
		}
		return rows.Err()
	})
	return names, err
}
