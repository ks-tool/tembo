package postgres

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/rs/zerolog/log"
)

// slotPrefix starts the name of every replication slot tembo manages.
const slotPrefix = "tembo_"

// ensureSlot creates this node's replication slot on the primary unless it is
// there, so that pg_basebackup can stream WAL into it from the start.
func (i *Instance) ensureSlot(ctx context.Context, primaryHost string) error {
	conninfo, err := i.conninfo(primaryHost)
	if err != nil {
		return err
	}
	cfg, err := pgx.ParseConfig(conninfo + " dbname=postgres")
	if err != nil {
		return err
	}
	if i.HostAuth == "gss" {
		// The pgx GSS provider authenticates as the agent's Kerberos
		// principal, which maps to the superuser.
		cfg.User = superuser
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(context.Background()) }()

	var exists bool
	name := i.slotName()
	err = conn.QueryRow(ctx, "select count(*) > 0 from pg_replication_slots where slot_name = $1", name).Scan(&exists)
	if err != nil || exists {
		return err
	}
	return createSlot(ctx, conn, name)
}

// createSlot creates a physical replication slot that reserves WAL at once.
// A slot created meanwhile is fine.
func createSlot(ctx context.Context, conn *pgx.Conn, name string) error {
	_, err := conn.Exec(ctx, "select pg_create_physical_replication_slot($1, true)", name)
	var pe *pgconn.PgError
	if errors.As(err, &pe) && pe.Code == "42710" { // duplicate_object
		return nil
	}
	return err
}

// ensureRoles creates the replication role on the primary: standbys, slot
// sync and pg_basebackup connect with it, so the superuser stays local.
// A clone carries the role over; only a fresh initdb cluster needs this.
func (i *Instance) ensureRoles(ctx context.Context) error {
	return i.query(ctx, func(ctx context.Context, conn *pgx.Conn) error {
		var exists bool
		err := conn.QueryRow(ctx, "select count(*) > 0 from pg_roles where rolname = $1", replUser).Scan(&exists)
		if err != nil || exists {
			return err
		}
		var ddl string
		if i.HostAuth == "ldap" {
			// LDAP authenticates the role; it has no password.
			ddl = fmt.Sprintf("create role %s with login replication", replUser)
		} else {
			password, err := i.password()
			if err != nil {
				return err
			}
			ddl = fmt.Sprintf("create role %s with login replication password '%s'",
				replUser, strings.ReplaceAll(password, "'", "''"))
		}
		if _, err := conn.Exec(ctx, ddl); err != nil {
			return err
		}
		log.Info().Str("role", replUser).Msg("replication role created")
		return nil
	})
}

// syncSlots keeps on the primary one physical replication slot per other node
// of the cluster and drops the slots of nodes that are gone. A slot makes the
// primary keep the WAL its node needs however long it is down; a node that is
// down thus holds WAL until it returns, which fixed membership makes
// acceptable.
func (i *Instance) syncSlots(ctx context.Context, ids []string) error {
	return i.query(ctx, func(ctx context.Context, conn *pgx.Conn) error {
		rows, err := conn.Query(ctx, "select slot_name from pg_replication_slots where slot_type = 'physical'")
		if err != nil {
			return err
		}
		var have []string
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				rows.Close()
				return err
			}
			have = append(have, name)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}

		want := make(map[string]bool, len(ids))
		for _, id := range ids {
			want[slotPrefix+id] = true
		}
		for _, name := range have {
			if strings.HasPrefix(name, slotPrefix) && !want[name] {
				if _, err := conn.Exec(ctx, "select pg_drop_replication_slot($1)", name); err != nil {
					return fmt.Errorf("drop slot %s: %w", name, err)
				}
				log.Info().Str("slot", name).Msg("replication slot dropped")
			}
		}
		for _, id := range ids {
			name := slotPrefix + id
			if slices.Contains(have, name) {
				continue
			}
			if err := createSlot(ctx, conn, name); err != nil {
				return fmt.Errorf("create slot %s: %w", name, err)
			}
			log.Info().Str("slot", name).Msg("replication slot created")
		}
		return nil
	})
}
