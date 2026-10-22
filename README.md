# tembo

A high-availability agent for PostgreSQL — a Patroni replacement without an
external DCS. Consensus is a built-in minimal Raft over HTTP, persisted in a
single bbolt file; the cluster state (the primary, the configuration, the
event history) is a replicated key/value space in that same file.

Full documentation is in [docs/](docs/README.md) — architecture, consensus,
configuration reference, CLI, HTTP API, HA rules, security, metrics and
day-2 operations.

## What it does

- **Failover and switchover.** Automatic failover ~15 s after the primary
  dies: among the standbys under the `max-lag-on-failover` limit (1 MB by
  default) the candidate is picked by `--failover-priority`, then by having
  been a synchronous standby of the primary, then by the most WAL;
  zero-loss switchover by
  shutdown LSN; scheduled switchover; manual failover, including while
  paused. Fencing: a primary without a quorum stops Postgres within ~5 s,
  and the postmaster dies with its agent (Pdeathsig SIGQUIT).
- **Replication.** Physical `tembo_<id>` slots on the primary, logical slot
  failover (PG17: `sync_replication_slots`), synchronous replication
  off/quorum/strict, cascading standbys (`--follow`) and DR replicas of a
  foreign cluster (`--standby-of`).
- **Membership.** Dynamic, via joint consensus: `members add/remove` with no
  restarts.
- **Configuration.** A cluster-wide `postgresql.conf` validated against a
  real Postgres and rolled out one node at a time (standbys, then the
  primary — a change that lowers a hot-standby limit goes to the primary
  first); `synchronous_standby_names` is managed by the agent. Optionally
  sourced from Kubernetes ConfigMaps/Secrets (`--k8s`) or discovered through
  Consul instead of `--peers`.
- **Access.** TCP proxies: `--listen` forwards to the primary, `--listen-ro`
  spreads over the standbys. pg_hba.conf is written by the agent; your own
  lines go through `settings set-hba`. API authentication: JWT (incl. JWKS),
  Kerberos/SPNEGO, LDAP, mTLS, with read-only identities.
- **Operations.** `status`, `config`, `settings`, `members`, `switchover`,
  `failover`, `rotate-password`, `reinit`, `restart`, `reload`, `history`,
  `dump`/`restore`, `backup`/`restore-backup`; Prometheus metrics of the
  agent, Raft and a postgres_exporter-compatible set.

## Quick start

Three nodes, `postgres:17` binaries in PATH, the same superuser password
everywhere (`TEMBO_PG_PASSWORD` or `--pg-password-file`):

```sh
TEMBO_PG_PASSWORD=... tembo run --id a \
    --peers a=10.0.0.1:8001,b=10.0.0.2:8001,c=10.0.0.3:8001 \
    --pg-data /var/lib/postgresql/a
# the same with --id b and --id c; the first node runs initdb, the rest clone it
```

Clients connect to `a:5432` (writes) and `a:5434` (reads); the CLI is
`tembo --addr 10.0.0.1:8001 status`. Adding a node to a live cluster:
start it with `--join` and `--peers d=10.0.0.4:8001,a=10.0.0.1:8001`, then
`tembo members add d=10.0.0.4:8001`.

A single node without high availability runs standalone — drop `--peers` and
`--consul`, and there is no raft: the node is the primary from the start and
configuration writes apply locally:

```sh
TEMBO_PG_PASSWORD=... tembo run --id solo --pg-data /var/lib/postgresql/solo
```

## Limitations

- Raft has no pre-vote: expect brief election storms after a partition.
- One Postgres password serves the `postgres` and `tembo_repl` roles
  cluster-wide; it rotates with `tembo rotate-password`, and between
  rotations the value sits in plain text in each node's store file.
- Linux is the target platform (Pdeathsig); on macOS fencing relies on the
  agent restart.
- Health endpoints are only `tembo health` (200 while the local Postgres
  answers); no role-based `/primary`/`/replica` for external load balancers
  and no lifecycle hooks — see
  [docs/README.md](docs/README.md#known-limitations) for the full list.

A local three-node cluster in Docker: `docker compose up --build` — see
[docker-compose.yaml](docker-compose.yaml).
