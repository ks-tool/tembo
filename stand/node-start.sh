#!/bin/sh
# Entrypoint of a stand node. It starts tembo, and in the background prepares what the Kerberos
# stand needs once the local Postgres answers: the keytab for the Postgres acceptor, and the role
# of the Kerberos user alice. Both run on every node; on a standby the role creation fails, which
# is expected, since roles replicate from the primary.
set -eu

psql_local() { psql -X -h /var/run/postgresql -p 5433 -U postgres -v ON_ERROR_STOP=1 "$@"; }

(
	until psql_local -tAc 'select 1' >/dev/null 2>&1; do sleep 2; done
	psql_local -c "alter system set krb_server_keyfile = '/krb/node.keytab'" -c 'select pg_reload_conf()' >/dev/null
	psql_local -c 'create role alice login' >/dev/null 2>&1 || true
	# Kerberos principals to roles, for the gss rules of pg_hba.conf (map=tembo): the agent's
	# principal postgres is the superuser and the replication role, and alice is herself.
	cat > "$TEMBO_PG_DATA/pg_ident.conf" <<'MAP'
# MAPNAME  SYSTEM-USERNAME  PG-USERNAME
tembo      postgres         postgres
tembo      postgres         tembo_repl
tembo      alice            alice
MAP
	psql_local -c 'select pg_reload_conf()' >/dev/null
) &

exec tembo "$@"
