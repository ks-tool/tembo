# Architecture

One `tembo run` process per node. Everything a node is made of:

```
                ┌────────────────────────────────────────────────┐
                │                  tembo                      │
                │                                                │
  API port ────►│  http.Server ── allowlist ── authn ── mux     │
  (8001)        │   ├─ /raft/*          → raft.Node (inbound)    │
                │   ├─ /status /switchover /failover /members   │
                │   ├─ /config /settings /history /reinit …     │
                │   └─ /metrics          → Prometheus registry   │
                │                                                │
                │  agent.Server                                  │
                │   ├─ raft.Node ── HTTPTransport (outbound) ───┼──► peers
                │   ├─ store.Store (bbolt: raft log + KV)       │
                │   └─ Watch: polls every node's /status        │
                │                                                │
  :5432 ───────►│  proxy.Proxy  → primary's Postgres            │
  :5434 ───────►│  proxy.Proxy  → standbys (round-robin)        │
                │                                                │
                │  reconcile ──► Instance ──► postgres -D …     │
                │  supervise ──┘        │  (Pdeathsig SIGQUIT)  │
                │  route ────────────────┘                      │
                │  drains ◄── log FIFOs (/tmp/tembo-<id>)    │
                └────────────────────────────────────────────────┘
```

Without `--peers` and `--consul` the same process runs standalone: no
`raft.Node`, no `/raft/*`, `/switchover`, `/failover` and `/members`
endpoints — the node is always the leader and the primary, and writes that
would go through the log apply to the store directly.

## Packages

| Package             | Responsibility                                                                                                                                                                                                                                                                                                                                                                           |
|---------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `internal/raft`     | Minimal Raft: elections, log replication, joint-consensus membership changes, compaction, snapshot install. Transport-agnostic.                                                                                                                                                                                                                                                          |
| `internal/store`    | The bbolt file: the replicated key/value state machine (`Apply`), the raft log/term/vote storage, snapshots of the KV as JSON.                                                                                                                                                                                                                                                           |
| `internal/agent`    | `Server` — glues raft + store + HTTP together; the `Watch` poll loop that detects failures and carries out switchover/failover; the handlers of `/status`, `/switchover`, `/failover`, `/members`, `/raft/*`; agent metrics.                                                                                                                                                             |
| `internal/postgres` | `Instance` — everything about the local Postgres: initdb/clone/rewind, configuration files, promote/stop, slots, sync-standby names, status SQL. `run.go` — the `run` command and the node's control loops. Also the CLI-facing HTTP handlers (`/config`, `/settings`, `/password`, `/health`, the ops), the rest of the CLI, the Kubernetes and Consul integrations, log FIFO handling. |
| `internal/proxy`    | Plain TCP forwarding with swappable target lists and an address allowlist.                                                                                                                                                                                                                                                                                                               |
| `internal/auth`     | API authentication: JWT, SPNEGO (Kerberos), LDAP Basic, mTLS; read-only identities.                                                                                                                                                                                                                                                                                                      |
| `internal/metrics`  | The postgres_exporter collector set over pgx (namespace `pg`).                                                                                                                                                                                                                                                                                                                           |
| `internal/addrlist` | IP/CIDR/hostname address lists — the same list `pg_hba.conf` is written from gates the API and the proxies.                                                                                                                                                                                                                                                                              |
| `internal/exec`     | `os/exec` wrappers: line-by-line output capture, per-command environment, cancellation signals, `Pdeathsig` on Linux.                                                                                                                                                                                                                                                                    |

## The replicated state machine

The store is a key/value space raft replicates. The keys tembo uses:

| Key             | Meaning                                                                | Written by                                                             |
|-----------------|------------------------------------------------------------------------|------------------------------------------------------------------------|
| `primary`       | The node id whose Postgres is the primary                              | Leader (election bootstraps it with `add`; failover/switchover set it) |
| `config`        | The user's `postgresql.conf` settings, a JSON object of name → value   | `PATCH /config`, Kubernetes `config.json`                              |
| `settings`      | Agent behavior settings (`max-lag-on-failover`, `synchronous_mode`, …) | `PATCH /settings`, Kubernetes `settings.json`                          |
| `switchover`    | The target node of a pending switchover                                | `PUT /switchover/{id}`                                                 |
| `switchover_at` | When a scheduled switchover runs (RFC3339)                             | `PUT /switchover/{id}` with a body                                     |
| `history`       | Failover/switchover history, a JSON object of timestamp → event        | Leader, on every event                                                 |
| `password`      | The cluster's Postgres password (never returned by any GET)            | `PUT /password`                                                        |

Commands (`set`, `add`, `del`, `merge` — see [consensus.md](consensus.md))
travel through the raft log as JSON and are applied on every node. A
follower's copy may briefly lag the leader's.

## Control loops

All loops tick once a second (`route`, `reconcile`, `Watch`, the allowlist
watcher, `leaveWhenRemoved` — which stops the agent, and with it Postgres,
once the cluster has dropped the node) or at raft's heartbeat interval (50 ms); with `--k8s`, `kubeWatch`
applies the objects as the informers report them and re-applies the latest
set when the node becomes the raft leader.
They communicate only
through the replicated store, `Server`'s polls and the `Instance` — never
through direct RPCs to each other, so a busy loop never blocks another.

### Watch (`agent.Server.Watch`, the leader's failure detector)

Every second it fetches `GET /status` from every member concurrently and
records each node's reported Postgres state (`postgres` from the JSON) plus
the membership the node reported. From that it keeps:

- `HasQuorum` — a raft leader was known and a majority of nodes answered
  within the last 5 seconds (`fenceTimeout`);
- `nodes` — the state of every node that answered the last poll (what
  `GET /config` and `pickStandby` see);
- `reported` — the membership each node reported, which is how a removed
  node learns it left even without its raft copy.

On the leader only: it carries out a pending switchover and, when the
primary's Postgres has looked dead for 15 seconds (`failoverTimeout`),
promotes a replacement (see [high-availability.md](high-availability.md)).

### reconcile (`run.go`, the actuator)

Brings the local Postgres in line with the cluster state once a second:

1. Asks Postgres for its state (`pg_is_in_recovery()`, WAL position,
   `primary_conninfo`, `primary_slot_name`, the hot-standby limit settings).
   For a stopped server it reads the shutdown checkpoint LSN from
   `pg_controldata`.
2. Detects a hung postmaster: alive per `pg_ctl status` but answering no SQL
   for 2 minutes → stop immediate; a server in crash recovery (never answered
   since its start) is left alone. `supervise` restarts it.
3. Publishes the state (running, LSN, sync standbys, node tags, config hash,
   config errors) to `Server.SetPostgres` — this is what `Watch` polls.
4. Rewrites `pg_hba.conf` when the membership or the `hba` setting changed 
   (checked every tick after a change, every 30 ticks otherwise).
5. On the primary: ensures the `tembo_repl` role, one physical replication
   slot per other node, and `synchronous_standby_names` following the
   standbys that actually stream.
6. Acts on divergences, in this order of precedence: stop if another node is
   the primary; repoint `primary_conninfo` if it follows the wrong upstream;
   stop cleanly for a pending switchover; stop immediately if the primary has
   no quorum (fencing); promote if this node is the primary but still in
   recovery; roll out a new cluster configuration when it is this node's
   turn.

Starting a stopped Postgres is `supervise`'s job, not reconcile's — reconcile
never starts, it only stops/repromotes/repairs.

### supervise

Runs `Instance.Run` in a loop: when Postgres exits for any reason other than
shutdown, it is started again after 5 seconds. The start decision (primary
with quorum, standby of the elected primary, cascading standby, DR standby)
is re-derived from the store on every start, so a restart is also how role
changes take effect. A primary that would start without a quorum, or while a
switchover to another node is pending, refuses and retries.

### route

Points the proxies at the cluster once a second. The write proxy goes to the
node the store names primary — but only while it is running, not in recovery,
the cluster has a quorum, and no switchover is pending; otherwise the proxies
refuse connections, so clients never reach a primary being fenced or
demoted. The read proxy spreads over all running standbys in node-id order,
falling back to the primary when there are none.

### watchAllowlist

Rebuilds the address list — loopback, the current membership's hosts, and the
addresses of the `hba` setting's `host` lines — whenever the membership or
the setting changes, and hands it to the API middleware and both proxies.
This is the same list `pg_hba.conf` is written from; see
[security.md](security.md#the-address-allowlist).

### The log drains

Postgres logs through its `logging_collector` into two FIFOs 
(`/tmp/tembo-<id>/postgres` and `postgres.csv`) that the agent created and
holds open. Two readers turn them into the agent's JSON log: plain lines
verbatim, CSV records parsed into fields with their severity mapped. The
FIFOs are the only pipe; if a reader dies the 64 KB FIFO fills, the logging
collector blocks in `write`, and every backend that logs hangs — so a drain
that stops for any reason other than shutdown fails the node's errgroup,
which runs the shutdown path and takes the agent down (non-zero exit) with
Postgres. See [postgres-management.md](postgres-management.md#logging).

## Failure detection and fencing, in one page

- Each node's Postgres state is re-published every second; the leader polls
  everyone every second.
- The **primary fences itself** when it has not seen a quorum for 5 s 
  (`fenceTimeout`): reconcile stops Postgres immediately. This is what makes
  failover safe — by the time the leader replaces the primary (after 15 s,
  `failoverTimeout`), an isolated primary has already stopped accepting
  writes.
- The **postmaster dies with the agent** (`Pdeathsig SIGQUIT`), so an agent
  crash never leaves an unfenced Postgres behind.
- The **proxies refuse clients** while the node sees no quorum or a switchover
  is in progress, so traffic never reaches a fenced primary.

Timings are constants in `internal/agent/watch.go` and `internal/postgres/run.go`.
