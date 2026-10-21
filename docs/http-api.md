# HTTP API

Every agent serves its API on the port of its own `--peers` address, on all
interfaces, plain HTTP or HTTPS per the `--tls-*` flags. The same port
carries raft RPCs, the CLI endpoints and `/metrics`.

Requests from addresses outside the allowlist (loopback, cluster nodes, the
`hba` setting's addresses) are refused with 403 before anything else. When
authentication is configured, requests from outside the cluster's own
addresses must carry credentials; cluster nodes and the loopback are exempt.
See [security.md](security.md).

Writes that go through raft answer from the leader: a follower replies with
307 (Temporary Redirect) to the leader's URL, keeping the method and body, so
clients that follow redirects just work. While there is no leader the answer
is 503. A write whose outcome is unknown (leadership lost mid-flight, or the
5-second propose deadline passed) is a 503 too — the client decides whether
to retry.

## Cluster state

### `GET /status`

One node's view, as the CLI's `status` prints:

```json
{
  "id": "a",
  "state": "leader",
  // follower | candidate | leader
  "term": 3,
  "leader": "a",
  "applied": 12,
  // raft index applied to the local store
  "primary": "a",
  // the cluster's primary
  "postgres": {
    // the local Postgres, as its agent last saw it
    "running": true,
    "in_recovery": false,
    // standby
    "lsn": 67553424,
    // WAL position in bytes
    "shutdown_lsn": 0,
    // set on a cleanly stopped server
    "sync_standbys": [
      "b"
    ],
    // set on a primary
    "nofailover": false,
    "nosync": false,
    "failover_priority": 0,
    "limits": {
      "max_connections": 100,
      "max_locks_per_transaction": 64,
      "max_prepared_transactions": 0,
      "max_wal_senders": 10,
      "max_worker_processes": 32
    },
    "config": "d41d8c9af012",
    // hash of the configuration it runs
    "config_error": "…"
    // omitted when the last rollout succeeded
  },
  "members": [
    "a",
    "b",
    "c"
  ]
  // committed membership, both sets while joint
}
```

### `GET /config`

The cluster's `postgresql.conf` settings plus every node's state:

```json
{
  "hash": "d41d8c9af012",
  "settings": {
    "shared_buffers": "256MB"
  },
  "nodes": {
    "a": {
      "running": true,
      "in_recovery": false,
      "lsn": 67553424,
      "config": "d41d8c9af012"
    },
    "b": {
      "running": true,
      "in_recovery": true,
      "lsn": 67550016,
      "config": "d41d8c9af012"
    }
  }
}
```

The `nodes` map is what the node's `Watch` poll last saw cluster-wide — it is
how you follow a configuration rollout's progress.

### `PATCH /config`

Body: a JSON object of parameter → value, or parameter → `null` to reset.

```json
{
  "shared_buffers": "256MB",
  "max_connections": 200,
  "work_mem": null
}
```

Blocked parameters (see [security.md](security.md#blocked-parameters)) are
refused with 400 before anything else. Surviving names are validated against
the local Postgres: an unknown name or an unparsable value answers 400 with
the Postgres error; if the local Postgres is down, 503 — retry later. The
change then goes through raft; when it commits, nodes apply it one at a time.

In Kubernetes mode answers 403 — the ConfigMap/Secret is the configuration.

### `GET /settings` / `PATCH /settings`

The agent behavior settings, and their changes. `GET` prints the stored JSON
object (an absent key means the default). `PATCH` takes the same shape as
`/config` — values are strings that are parsed and normalized per setting, a
`null` resets. Unknown settings answer 400. In Kubernetes mode `PATCH`
answers 403.

```sh
curl -X PATCH .../settings -d '{"max-lag-on-failover":"16MB","paused":true}'
```

### `GET /history`

The failover/switchover history: a JSON object of RFC3339 timestamp → event
string, e.g. `"2026-10-21T09:15:02.1Z": "failover: a → b"`. Kept by the raft
leader under the `history` key.

### `PUT /password`

Rotate the Postgres password of the whole cluster. The request body is the
new password (a trailing newline is ignored; ≤1024 bytes; empty is 400). The
value commits through raft; every agent starts connecting with it on its
next reconcile tick, the primary alters the `postgres` and `tembo_repl`
roles, and the standbys receive them by WAL replay. The stored value is
never handed back by any GET — the CLI's `rotate-password` prints a
generated one exactly once. Answers 204; a follower redirects to the leader.
With the OpenBao integration (`--bao-password-path`) answers 403: the
secret is the source of truth.

## Failover and switchover

### `PUT /switchover/{id}`

Ask to move the primary role to node `{id}`; the handler (leader only) waits
for the outcome and answers `{"primary": "<id>"}`. With an RFC3339 timestamp
as the request body the switchover is scheduled instead and the handler
answers 204 at once.

Refusals (409): a switchover is already pending; the target is the primary
already, is `nofailover`, is not a running standby, or the primary is
unreachable. 503: no leader, the new leader is still catching up, no quorum.
After ~70 s without an outcome: 504 "still in progress" — the leader may
still finish it or abandon it on its 60-second timeout.

### `POST /failover/{id}`

Promote node `{id}` without waiting for the old primary — the manual,
primary-is-dead tool; works while `paused`. Guards as for `switchover`, plus
409 when the primary looks alive ("use switchover"). Answers 204 at once; the
promotion itself happens on the target's next reconcile tick.

## Membership

### `GET /members`

The membership in effect on this node (committed or not):

```json
{
  "voters": {
    "a": "10.0.0.1:8001",
    "b": "10.0.0.2:8001",
    "c": "10.0.0.3:8001"
  },
  "joint": {
    "d": "10.0.0.4:8001"
  }
}
```

`joint` is present only while a change is settling.

### `PUT /members/{id}`

Add node `{id}`, or update its address. The body is the node's `host:port`.
Answers 204 once the joint entry has committed and applied locally; 409 while
another membership change is in flight.

### `DELETE /members/{id}`

Remove node `{id}` from the voting set. The removed node learns this on its
own and exits; if it is down, the leader keeps trying to deliver the change
to it for a grace period (~10 election timeouts), then lets it go.

## Local node operations

These are the node's own business — they do not go through raft; run them
against the node they concern.

| Endpoint               | Effect                                                                                                                                                                                                                                                           | Refused (409/503)                                    |
|------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------|
| `GET /health`          | 200 with the Postgres state while the local Postgres answers SQL, 503 otherwise — the check for supervisors and load balancers (`tembo health` exits non-zero accordingly). `GET /status` stays 200 even with Postgres down: it is how the nodes see each other. | Without a running Postgres (503)                     |
| `POST /reinit`         | Stop fast, wipe the data directory, reclone via `supervise`. Resets the pg_rewind failure counter.                                                                                                                                                               | On the primary (409); while a clean stop fails (500) |
| `POST /restart`        | Stop fast; `supervise` starts Postgres again.                                                                                                                                                                                                                    | If not running (409)                                 |
| `POST /reload`         | `pg_ctl reload` of the configuration files.                                                                                                                                                                                                                      | On failure (500)                                     |
| `GET /dump[?db=name]`  | Stream `pg_dumpall` (or `pg_dump` of one database) as `application/sql`.                                                                                                                                                                                         | If Postgres is not running (503)                     |
| `GET /backup`          | Stream a tar of a fresh `pg_basebackup -X stream` of the local server as `application/x-tar`.                                                                                                                                                                    | If not running (503)                                 |
| `POST /restore`        | Feed the request body to `psql` (`ON_ERROR_STOP=1`); respond with its output's tail (≤64 KB). 500 on a psql failure.                                                                                                                                             | If not running (503)                                 |
| `POST /restore-backup` | Stop fast, wipe the data directory, untar the request body into it, `chmod 0700`; `supervise` starts Postgres.                                                                                                                                                   | On failure (500)                                     |

`?db=` accepts `[a-zA-Z0-9_.$-]+` only.

## Raft RPCs (nodes only)

| Endpoint              | Purpose                                                         |
|-----------------------|-----------------------------------------------------------------|
| `POST /raft/vote`     | `RequestVote` — elections                                       |
| `POST /raft/append`   | `AppendEntries` — replication and heartbeats                    |
| `POST /raft/snapshot` | `InstallSnapshot` — the state machine for a node compacted past |

JSON bodies and responses as defined in `pkg/raft`. Not useful to humans,
and outside the allowlist there is nothing to reach them with.

## Metrics

`GET /metrics` — Prometheus text format: the agent/raft metrics (`tembo_*`)
and the postgres_exporter set (`pg_*`) for the local Postgres. See
[metrics.md](metrics.md).
