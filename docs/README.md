# tembo documentation

tembo (binary `tembo`) is a high-availability agent for PostgreSQL — a
Patroni replacement without an external DCS. Consensus is provided by a
built-in minimal Raft implementation over HTTP, persisted in a single bbolt
file. The cluster state — which node is the primary, the cluster's
`postgresql.conf`, the agent behavior settings, the failover history — is a
replicated key/value space in that same file.

Each node runs one agent. The agent runs and supervises the local Postgres,
elects the primary through Raft, fails over when the primary dies, keeps
replication slots and `synchronous_standby_names` in order, rolls out
configuration changes one node at a time, forwards clients to the primary (read/write) and spreads readers over the
standbys, and exposes Prometheus
metrics.

## Document index

| Document                                         | Contents                                                                                                                              |
|--------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------|
| [getting-started.md](getting-started.md)         | Requirements, a three-node walkthrough, adding/removing nodes, where clients connect                                                  |
| [architecture.md](architecture.md)               | The components of a node and the control loops that tie them together                                                                 |
| [consensus.md](consensus.md)                     | The Raft package: elections, joint-consensus membership, log compaction, snapshots, the bbolt layout                                  |
| [configuration.md](configuration.md)             | Every `run` flag and environment variable, config files, Kubernetes and Consul sources, agent settings, the cluster `postgresql.conf` |
| [cli.md](cli.md)                                 | The `tembo` command line reference                                                                                                    |
| [http-api.md](http-api.md)                       | The HTTP API every agent serves                                                                                                       |
| [high-availability.md](high-availability.md)     | Failover, switchover, fencing, synchronous replication, slots, cascading and DR standbys, node tags                                   |
| [postgres-management.md](postgres-management.md) | Bootstrap and cloning, pg_rewind recovery, configuration rollout, pg_hba.conf, logging, reinit/restart                                |
| [security.md](security.md)                       | API authentication (JWT, Kerberos, LDAP, mTLS), TLS, the address allowlist, blocked `postgresql.conf` parameters                      |
| [metrics.md](metrics.md)                         | Prometheus metrics of the agent, Raft and Postgres                                                                                    |
| [operations.md](operations.md)                   | Day-2 procedures: backups, dumps, restores, membership changes, Kubernetes mode, Consul discovery, shutdown behavior                  |

## The one-minute overview

A cluster is an odd number of nodes (three or five), each with:

- an **API port** (default suggestion `8001`) serving raft RPCs, the CLI and
  `/metrics`;
- two **proxy ports**: `--listen` (default `:5432`) forwarding to the primary
  and `--listen-ro` (default `:5434`) spreading over the standbys;
- a **Postgres** on `--pg-port` (default `5433`) managed end to end by the
  agent;
- a **bbolt file** (default `<id>.db`) holding the raft log, term and vote,
  and the replicated key/value state machine.

The raft leader decides failovers; every node reconciles its Postgres against
the replicated state once a second. A primary that loses its quorum fences
itself (stops Postgres) within ~5 seconds; a dead primary is replaced after
~15 seconds by the standby with the most WAL that is not `nofailover` and not
lagging more than `max-lag-on-failover`.

## Known limitations

- **No pre-vote in Raft.** A partitioned node can raise its term and cause a
  short election storm when the partition heals. A node joining with `--join`
  starts with elections frozen, which covers the common case.
- **Shared cluster password.** One password serves the `postgres` and
  `tembo_repl` roles on every node, from `TEMBO_PG_PASSWORD` (or
  `--pg-password-file`). It rotates cluster-wide with
  `tembo rotate-password`; between rotations the value lives in each node's
  store file, in plain text.
- **No role-based health endpoints for external load balancers** (`/primary`,
  `/replica`); there is `GET /health` (`tembo health`), which is 200 only
  while the local Postgres answers SQL, and routing is done by the built-in
  proxies.
- **No lifecycle hooks** (`on_role_change` and friends).
- **Linux is the target platform.** The postmaster is tied to the agent with
  `Pdeathsig SIGQUIT`; on macOS fencing relies on the agent restart.
- **Blocked parameters trade-off.** `shared_preload_libraries` and the other
  command/library parameters cannot be set through the cluster configuration
  at all (see [security.md](security.md#blocked-parameters)); extensions that
  need it cannot be enabled via `PATCH /config`.
- The log FIFOs live under `/tmp/tembo-<id>`, a predictable path on a
  multi-user host.

A local three-node cluster in Docker: `docker compose up --build`, see
[docker-compose.yaml](../docker-compose.yaml).
