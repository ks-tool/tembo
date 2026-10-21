# Operations

## Membership changes on a live cluster

Adding a node (details in [getting-started.md](getting-started.md)):

1. Start the new agent with `--join`, `--peers` naming itself and at least
   one member, its `--pg-data` and `--pg-password-file`. It serves its API
   immediately, does not vote yet (elections frozen), and waits.
2. `tembo members add d=10.0.0.4:8001` — the joint-consensus change
   commits; the new node unfreezes, clones the primary and becomes a
   standby.

Keep the voter count odd. Removing a node: `members remove d` — the node's
agent notices (its raft copy or a quorum of the polls tells it), stops its
Postgres and exits. The leader keeps trying to deliver the removal to it for
a grace period (≈6 s, 10 election timeouts), and a node that was down still
learns it later — from its raft log once it catches up, or from what the
other nodes report.

Re-addressing a member is the same `members add id=newaddr` (it updates the
voter's address). A quorum must survive any change — with a 3-node cluster
that has already lost one node, fix that node or remove it before touching
membership again.

## Pause and maintenance

```sh
tembo settings set paused=true    # suspend automatic failover
tembo settings set paused=false
```

While paused, the leader still detects a dead primary but never replaces it;
switchover is still refused while the primary is unreachable, but **manual
failover works** (`tembo failover <node>`). Use pause for work that
fools the failure detector — Postgres upgrades of standbys, storage
maintenance — and unpause after.

`--nofailover` on a node is the targeted variant: the cluster keeps failing
over, just never to that node.

## Password rotation

```sh
tembo rotate-password            # generates a password, prints it once, rotates
tembo rotate-password 's3cret'   # rotates to a value of your choice
```

The value commits through raft; the primary alters the `postgres` and
`tembo_repl` roles on its next reconcile tick, the standbys receive them by
WAL replay, and every agent switches to it within a second or two — a brief
replication blip while the standby walreceivers reconnect. The stored value
is never returned by any GET; it lives in plain text in each node's bbolt
file, so guard that file as you would a pgpass. `TEMBO_PG_PASSWORD`/
`--pg-password-file` remain the bootstrap password only — a node restarting
with a stale environment value picks the rotated one up from the store. A
`--standby-of` node is not rotated (its upstream is foreign); rotate that
cluster's password on its own terms.

**OpenBao-managed clusters** (`--bao-password-path`) rotate outside the CLI:
update the secret's `password` key and every node picks it up within ~5
seconds — the primary alters the roles, the standbys follow by WAL replay,
the same blip, no raft involved, and the value never lands in the nodes'
store files. `rotate-password` answers 403 there. Moving a running cluster
under OpenBao adopts in place: set the secret to the current password (or
run with `--pg-local-auth peer`, where the agent needs no password itself)
and let the primary converge the roles; see
[configuration.md](configuration.md#openbao).

## Scheduled switchover

```sh
tembo switchover b --at 2026-10-22T02:00:00Z
```

Stored in the cluster; the leader runs it when its time comes, and the
request survives leader changes. Pause does not hold it back (a pending
switchover is handled before the pause check), and there is no cancel API:
once its time comes it proceeds like an immediate switchover, and is
abandoned only by its own guards — the target no longer a running standby,
or the 60-second timeout when the primary's agent cannot stop it.

## Kubernetes mode

Run with `--k8s cm://[ns/]name` and/or `--k8s secret://[ns/]name[/key]`
(repeatable). The node id defaults to the pod's hostname (dashes become
underscores). Inside a pod the in-cluster service account is used; outside,
`$KUBECONFIG`. The objects' keys:

- `config.json` — the cluster's `postgresql.conf` settings (same shape and
  blocklist as `PATCH /config`; a `null` resets);
- `settings.json` — the agent behavior settings (normalized like
  `PATCH /settings`).

While `--k8s` is on, `PATCH /config` and `PATCH /settings` answer 403: the
objects are the desired state. Only the raft leader writes; every node
watches the objects (and re-applies every 15 s), so a new leader picks the
state up. Values are validated on the leader before they are proposed. A
key the object drops releases that part of the configuration back to the
defaults.

A StatefulSet with `podManagementPolicy: Parallel` and the headless service
per pod is the natural fit: peers as `a-0.a:8001,...`, one PVC per pod, the
ConfigMap/Secret for the configuration. Remember the id rule — the hostname's
dashes become underscores in the slot names.

## Consul discovery

Instead of `--peers`, run each node with `--consul http://<agent>:8500` (and
optionally `--consul-service`, default `tembo`, `--consul-tags`,
`--consul-expect`, `--advertise`).

A local demonstration lives in [docker-compose.ext.yaml](../docker-compose.ext.yaml):
layered over the base compose file (`docker compose -f docker-compose.yaml
-f docker-compose.ext.yaml up -d`) it adds a Consul agent and swaps the
nodes' `command` from `--peers` to `--consul http://consul:8500
--consul-expect 3`.

With discovery, each node:

1. registers itself in Consul with a TCP health check against its API port (10 s interval; a critical service
   deregisters after a minute) and its
   node id in the service metadata;
2. opens its API listener first, so the check can pass immediately;
3. polls the catalog until the set of healthy services with the same name
   and all the same tags holds at least `--consul-expect` nodes (this node
   included) and has stopped changing for three consecutive polls — that
   set becomes the peers.

The API address other nodes see is `--advertise` (default: the hostname,
port 8001). Bootstrapping gives up after 5 minutes. The membership is still
raft's own: nodes added later go through `members add`, and Consul's
deregistration of a dead node does not remove it from the voters.

## Backups and disaster recovery

Three layers, all streaming over the API:

- **Logical, whole cluster**: `tembo dump > dump.sql` (`pg_dumpall`,
  roles and tablespaces included) — restorable anywhere with
  `tembo restore < dump.sql` or plain `psql`. `dump --db name` for one
  database.
- **Physical, one node**: `tembo backup > base.tar` —
  `pg_basebackup -X stream` (WAL included) tarred. Restore on a node with
  `tembo restore-backup < base.tar`; a standby ahead of the backup heals
  with `pg_rewind`.
- **External clones**: point `--clone-command` at wal-g/pgBackRest and a
  reinit/rejoin restores through your existing tooling.

A node's identity — the raft log and the key/value state — lives in the
bbolt file (`--db`, default `<id>.db`) next to the data directory. Losing it
means rejoining as a new member (`--join` + `members add`); back it up with
the data directory if you want the node back as-is.

Rebuilding a node from scratch is the standard drill: wipe `--pg-data` and
`--db`, start with `--join` pointing at a surviving member, `members add`.
Or on the same membership: `reinit` reclones just the data directory.

## Shutdown and signals

`SIGINT`/`SIGTERM` (the first one) begin a clean shutdown:

1. The control loops stop; Postgres is asked to stop **fast** (checkpoint,
   WAL to the standbys) — a primary stops as a clean former primary, ready
   to rewind or promote on return.
2. A **second** signal, or 60 seconds of waiting, stops Postgres
   immediately (no checkpoint), with 5 more seconds for the loops to notice.
3. The proxies close their forwarded connections, the raft node stops, the
   API drains in-flight handlers for up to 30 s, the log FIFOs are closed,
   and the agent exits with the drains' status (non-zero if a drain had
   failed — the case that also *triggers* a shutdown by itself, see
   [postgres-management.md](postgres-management.md#logging)).

A third signal falls through to the default action. `SIGHUP` is ignored
entirely. The postmaster always carries `Pdeathsig SIGQUIT`: whatever kills
the agent — crash, `kill -9`, a panic — takes Postgres down with it, and it
recovers on the next start. systemd unit writers: `KillMode=control-group`
is fine; do not use `KillMode=process`.

## Reading the log

Everything is JSON on stdout (`zerolog`), one event per line, with a
`source` field: `tembo` for the agent itself, `postgres` for the csvlog
stream, and the program name (`pg_basebackup`, `initdb`, …) for captured
tool output. Debug level with `TEMBO_LOG_DEBUG=1`; UTC timestamps with
`TEMBO_LOG_UTC=1`.

Events worth knowing on sight:

- `no quorum, stopping postgres` — fencing; the proxies stop forwarding at
  the same time.
- `failover: primary is down` / `failover: a → b (requested)` — the leader
  replaced the primary (automatic / manual).
- `switchover: primary stopped, promoting target` — the zero-loss handover
  moment.
- `switchover: … abandoned: timed out` / `abandoned: target is not a running
  standby` — the request was dropped; check the target.
- `pg_rewind keeps failing, discarding the data directory to clone the
  primary` — after 10 failures the node reclones itself.
- `the cluster dropped this node, exiting` — removal reaching the node.
- `postgres accepts no queries, restarting it` — the hung-postmaster
  detector fired.
- `the log fifo is not drained, stopping postgres and the agent` — the
  fatal-drain path; the agent exits right after.

## Troubleshooting quick reference

| Symptom                          | Where to look                                                                                                                 |
|----------------------------------|-------------------------------------------------------------------------------------------------------------------------------|
| Clients refused on `:5432`       | `status`: no quorum? pending switchover? primary in recovery? All three blank the proxy targets on purpose.                   |
| `switchover` answers 504         | The leader's log: the primary never stopped, or the target never caught up (its LSN vs the primary's `shutdown_lsn`).         |
| A standby will not rejoin        | `pg_rewind` errors in its log; after 10 failures it reclones on its own, or force it with `reinit`.                           |
| Configuration stuck              | `GET /config`: `nodes` shows who has not applied the hash and their `config_error`.                                           |
| Election storm after a partition | Expected without pre-vote; it settles. A node stuck campaigning while removed should not happen — check its `members` output. |
| API answers 403                  | The client's address is not on the allowlist — loopback, cluster nodes, or the `hba` setting's addresses.                     |
