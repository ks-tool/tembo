# Postgres management

The agent owns the Postgres instance end to end: the data directory, the
configuration files, the process. Nothing else should write them — manual
edits to `postgresql.conf` or `pg_hba.conf` are overwritten on the next
rewrite.

## Bootstrap and cloning

`Instance.Run` prepares the data directory before starting the postmaster:

- **A primary's empty directory** is created by `initdb` with fixed encoding
  and locale (`--encoding UTF8 --locale C`), the superuser `postgres` and the
  password file; extra arguments come from `--initdb-args`
  (e.g. `--data-checksums`).
- **A standby's empty directory** is cloned from the upstream:
    - when the upstream is the cluster's primary, the node's slot
      `tembo_<id>` is created on it first, so the WAL is kept from that
      moment on whichever command makes the copy;
    - with `--clone-command`, a shell command of yours, run with
      `TEMBO_PGDATA`, `TEMBO_PRIMARY_HOST`, `TEMBO_PRIMARY_PORT` and
      `PGPASSWORD` in the environment — the wal-g/pgBackRest restore entry
      point. After it succeeds the agent writes `standby.signal` itself;
    - otherwise with `pg_basebackup --wal-method=stream --checkpoint=fast`,
      connecting as `tembo_repl`, `--slot tembo_<id>` on the primary, so the
      WAL streams into the slot from the first byte and there is no gap at
      the cutover.
    - A clone leaves `<PGDATA>.clone-inprogress` beside the directory while
      it runs. A failed or interrupted clone keeps the marker, and the next
      start discards the directory and clones again; the marker goes only
      after `standby.signal` is written.
- **A former primary's directory** (no `standby.signal`, e.g. after it was
  fenced and the cluster moved on) is rewound, see below.
- **A postmaster that outlived a killed agent** is detected via
  `pg_ctl status` and stopped before anything else.

The first start of a new cluster: the raft leader, once it sees no `primary`
key, proposes itself (`add` — set only if absent) and starts Postgres as the
primary, refusing to do so without a quorum or while a switchover is pending.
The other nodes wait for the key and clone whoever it names.

## Role changes and pg_rewind

`supervise` re-derives the role from the store on every start: primary (with
quorum), standby of the primary, standby of `--follow`, or DR standby of
`--standby-of`. Reconcile converges a *running* server the same way every
second: a standby whose `primary_conninfo` no longer matches (compared as
parsed conninfo, not text) is repointed with a reload; a primary that the
store no longer names primary is stopped immediately; the node the store
names primary but that is still in recovery is promoted — provided the node
has a quorum.

A former primary rejoining after a failover rewinds: `pg_rewind --source-server`
against the new primary discards everything written after the timelines
forked (running crash recovery first if it was not shut down cleanly) and
copies what changed. Rewind failures count only against a *reachable*
primary; after 10 consecutive failures the data directory is discarded and
the node reclones itself as if it were new. `reinit` and `restore-backup`
reset that counter.

## The configuration files

`configure` rewrites both files from scratch on every apply:

### `postgresql.conf`

Base settings, tembo's replication defaults, the cluster configuration, then
the agent's own parameters (`port`, the logging collector block,
`primary_conninfo`/`primary_slot_name` on a standby,
`synchronous_standby_names` on a primary), sorted by name, values
single-quoted with backslash and quote escaped.

### `pg_hba.conf`

Generated, in this order:

```
local all all peer                                  # the agent, over the socket
local replication postgres peer                     # local pg_basebackup (/backup)
host all postgres 127.0.0.1/32 <method>             # only with --pg-local-auth password|gss
host all all <node-address-1> <method>              # one pair per cluster node,
host replication tembo_repl <node-address-1> <method>   # resolved names included; scram-sha-256 under gss
...
<hba setting lines, verbatim>                       # yours, via settings set-hba
```

`<method>` is `--pg-host-auth` with its `--pg-host-auth-opts`. Node
addresses are written as `/32`–`/128` CIDRs; a hostname is written both as a
name and as the IPs it currently resolves to. While Postgres is running, the
file is rewritten whenever the membership or the `hba` setting changes
(checked each second after a change, and every 30 ticks otherwise) and
Postgres reloads it.

To grant application access, add `host` lines with `settings set-hba`
([cli.md](cli.md#set-hba--add-or-remove-a-pg_hbaconf-line-on-every-node));
replication always goes direct between nodes, not through the proxies.

## Configuration rollout

A `PATCH /config` change commits through raft (a blocked parameter is
refused with 400 first — the list is in
[security.md](security.md#blocked-parameters)), then nodes apply it one at a
time:

- order: standbys in node-id order, the primary last;
- a configuration that **lowers a hot-standby limit** (`max_connections`,
  `max_locks_per_transaction`, `max_prepared_transactions`,
  `max_wal_senders`, `max_worker_processes`) applies to the **primary
  first** — a standby must not run with lower limits than its primary;
- a standby applies a limit-lowering change only after it has replayed the
  primary's WAL up to the position the primary last reported (a record with
  the old, higher value still ahead in replay would pause its recovery);
- a node waits until every reachable node before it in the order runs the
  new hash without error; a node whose apply failed reports `config_error`
  (shown by `GET /config`) and **blocks the nodes after it** — it does not
  retry the same hash. The rollout resumes with a new configuration (a new
  hash) or once the failed node drops out of the status poll;
- applying = rewrite the files, check them the way a start would (`postgres -C`), reload, and — if anything shows
  `pending_restart` — stop
  fast and let `supervise` restart (up to 30 s to come back). On failure the
  previous configuration is written back and the error is recorded.

Values are validated *before* they commit, against the local running
Postgres (`set_config` parses the value and runs its check hooks), so a typo
answers 400 instead of wedging every node in a restart loop.

## Authentication to Postgres

Two independent knobs:

- **The agent itself** (`--pg-local-auth`): `peer` over the Unix socket (the default — run the agent as the postgres OS
  user), `password` over TCP
  to 127.0.0.1, or `gss` over TCP with Kerberos.
- **The nodes between themselves** (`--pg-host-auth`): the method of the
  managed `pg_hba.conf` lines — `scram-sha-256` (default), `ldap` (with
  `--pg-host-auth-opts`, e.g. `"ldapbasedn=..."`), or `gss`.

Standbys, slot sync and `pg_basebackup` connect as the role **`tembo_repl`**
(`login replication`), which the primary creates itself — with the shared
password in the default setup, and without one under LDAP host
authentication. Under `gss` it has a password too: the replication lines of
`pg_hba.conf` are `scram-sha-256` records of their own, and carry none of the
gss options. The superuser stays local: the Unix socket and `pg_rewind`,
which needs it. The `tembo_repl` password is the superuser's —
`TEMBO_PG_PASSWORD` (or `--pg-password-file`) at bootstrap, and afterwards
whatever the cluster last rotated to with
[`tembo rotate-password`](cli.md#rotate-password--rotate-the-cluster-password):
the rotation commits through raft, every agent starts using it within a
second or two, the primary alters the roles that hold one — `postgres`
always, `tembo_repl` wherever it has a password (not under LDAP) —
with `ALTER ROLE ... PASSWORD`, and the standbys receive the change by WAL
replay. The startup password counts
as applied, so a cluster that never rotated has its roles altered by no one.

With the OpenBao integration (`--bao-password-path`) the store is out of
this loop: every node polls the secret every fifth reconcile tick (~5 s), a
new value rotates the roles the same way, and `PUT /password` answers 403 —
see [configuration.md](configuration.md#openbao).
`--bao-ldap-password-path` likewise supplies, at startup, the LDAP bind
password of the API authentication.

With `gss` anywhere, the agent obtains its tickets from
`--pg-krb5-keytab` (or the credential cache), uses
`--pg-krb5-principal` (default `postgres`) and the `krb5.conf` of
`--pg-krb5-config`, and exports `KRB5_KTNAME`/`KRB5_CONFIG` to the Postgres
processes so libpq finds the same keys. Ordinary connections of the nodes —
and the superuser connections of `pg_rewind` and slot creation — authenticate
with those tickets. Replication does not: `pg_basebackup` and the standbys'
`primary_conninfo` use `tembo_repl`'s password. Constraint pairs:
`password` local auth needs a password-based host auth; `gss` local auth
needs `gss` host auth.

## Roles and slots on the primary

Every reconcile tick on the primary ensures:

- the `tembo_repl` role exists (a fresh `initdb` cluster needs this; clones
  carry it over);
- one physical slot `tembo_<id>` per other node, dropping `tembo_*` slots of
  nodes that are no longer members;
- `synchronous_standby_names` follows the standbys actually streaming (see
  [high-availability.md](high-availability.md#synchronous-replication)).

## Logging

The agent does not let Postgres write log files. `logging_collector` is
forced on, `log_destination = csvlog`, and the collector writes into two
FIFOs the agent created and holds open in `$TMPDIR/tembo-<id>/` (`/tmp`
unless `TMPDIR` points elsewhere):

- `postgres.csv` — the structured csvlog stream, parsed by the agent into
  JSON events with the csvlog columns as fields and the severity mapped to
  the agent's levels (LOG/INFO/NOTICE → info, WARNING → warn, ERROR →
  error, FATAL → fatal, PANIC → panic, DEBUG* → debug);
- `postgres` — the few plain lines the collector writes around `elog`,
  logged verbatim.

The FIFOs live outside the data directory on purpose: `pg_basebackup`,
`pg_rewind` and backups must not copy them. Output of every Postgres
utility the agent runs (`initdb`, `pg_basebackup`, `pg_ctl`, …) is captured
line by line into the agent's JSON log with a `source` field naming the
program.

The drains are load-bearing: if a reader stops for any reason other than
shutdown, the 64 KB FIFO fills, the logging collector blocks in `write`, and
with it every backend that logs — so the agent treats it as fatal, stops
Postgres cleanly and exits non-zero rather than leave a wedged server
behind.

## Local maintenance

`reinit` (wipe + reclone), `restart`, `reload`, `dump`/`restore`,
`backup`/`restore-backup` — all described in [cli.md](cli.md); they are
local HTTP actions on the node concerned, not raft commands.

One caveat: `restore-backup` replaces the data directory wholesale; on a
node whose raft bbolt file is intact that is safe (raft state and KV live
outside the data directory), and a restored standby that is ahead of the
backup heals itself via `pg_rewind` or a reclone.
