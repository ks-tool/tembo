# High availability

The HA rules are deliberately few and time-based. All numbers are constants
in `internal/agent/watch.go` (`switchoverTimeout` in
`internal/agent/switchover.go`):

| Constant            | Value | Meaning                                                                       |
|---------------------|-------|-------------------------------------------------------------------------------|
| `pollInterval`      | 1 s   | How often the leader polls every node and every node reconciles.              |
| `fenceTimeout`      | 5 s   | How fresh "I have a quorum" must be.                                          |
| `failoverTimeout`   | 15 s  | How long the primary's Postgres must look dead before the leader replaces it. |
| `switchoverTimeout` | 60 s  | How long a switchover may take before it is abandoned.                        |

## Fencing

A primary that cannot reach a quorum must stop taking writes — this is the
safety property everything else builds on, and it does not depend on the
leader at all:

- Each node tracks whether it knew a raft leader and reached a majority of
  nodes within the last `fenceTimeout` (`HasQuorum`).
- The moment a primary loses that (partition, raft leadership lost, majority
  of agents down), its next reconcile tick — at most a second later — stops
  Postgres with `--mode immediate`: no checkpoint and no promised WAL
  shipment, so the next start runs crash recovery. (Contrast with the clean
  `fast` stop of a switchover below.)
- The kernel backstop: the postmaster runs with `Pdeathsig SIGQUIT`, so if
  the agent itself dies, Postgres dies with it. Nothing runs unfenced.
- While a node has no quorum, both of its proxies refuse connections, and
  `supervise` will not start Postgres as a primary.

Because `failoverTimeout` (15 s) is much larger than `fenceTimeout` (5 s), an
isolated primary has always already fenced itself by the time the leader
promotes a replacement — there is no window with two writable primaries.

## Automatic failover

The raft leader's `Watch` loop, every second:

1. It is the leader, has a quorum, and has applied everything committed
   before its election (`Ready`) — otherwise it does nothing.
2. No switchover is pending: while the `switchover` key sits in the store,
   the tick is spent carrying it out and the failover path is never reached.
3. The cluster is not `paused`.
4. The primary's Postgres has not answered a poll as running for
   `failoverTimeout`, and this leader has been leader that long too (a new
   leader must watch for a full timeout; what the previous leader saw is
   lost).
5. A candidate is chosen:

    - running, in recovery (a standby), and not tagged `nofailover`;
    - not missing more WAL than `max-lag-on-failover` (1 MB by default, `0`
      lifts the limit) behind the primary's last known position — a standby
      that has run ahead of the primary's last report counts as not lagging;
    - among the survivors, the best one wins by, in order: higher
      `--failover-priority`, having been one of the primary's synchronous
      standbys (it misses no committed data), then more WAL.

   With no candidate at all, the leader logs the reason and keeps waiting.

6. `primary` is set to the winner through raft. The winner's next reconcile
   tick sees itself primary-but-in-recovery and runs `pg_ctl promote`. The
   old primary, when it comes back, finds `standby.signal` missing, runs
   `pg_rewind` against the new primary and rejoins as a standby.

The promotion itself takes another second or two (the reconcile tick plus
`pg_ctl promote`), so the observed failover time is ~15–20 s from the primary
becoming unreachable. If the primary's *agent* was merely cut off but its
Postgres alive, it fences itself within ~5–6 s (the 5-second quorum freshness
window plus up to one reconcile tick) and reappears as a standby. A node that
finds itself dropped from the membership (`members remove`) likewise stops
its Postgres and exits on its own — see `leaveWhenRemoved`.

Synchronous replication changes what a failover may lose: with
`synchronous_mode` on and the promoted standby in the sync set, committed
transactions are not lost at all (see below).

## Switchover

A switchover moves the primary role with no transaction loss. Requested with
`PUT /switchover/{id}` (the CLI's `switchover`), guarded by the leader as
described in [http-api.md](http-api.md#put-switchoverid), then carried out by
`Watch`:

1. The request (the target's id) is committed to the store.
2. The proxies stop forwarding (a pending switchover blanks the targets) —
   clients are refused, not sent to a primary about to stop.
3. The primary's reconcile sees the pending switchover and stops Postgres
   with `--mode fast`: a checkpoint is written and all WAL is sent to the
   connected standbys. Its agent then reports the shutdown checkpoint's LSN (from `pg_controldata`, "Database cluster
   state: shut down") as
   `shutdown_lsn` — every WAL position before it carries data of a committed
   transaction.
4. The leader promotes the target once it has the primary's `shutdown_lsn`
   (`target.LSN >= shutdown_lsn`).
5. The request is dropped from the store, the event goes to `history`, the
   proxies move.

If the target stops being a running standby, the request is abandoned with
an event. If the primary does not stop, or the target does not catch up,
within `switchoverTimeout` — abandoned. An unreachable primary (its agent
down) is waited for the full timeout, then abandoned: use `failover`.

A **scheduled switchover** (`switchover b --at <RFC3339>`) stores the time
under `switchover_at`; the leader picks the request up only once its time has
come, and a primary starting up while a scheduled switchover is still in the
future keeps serving.

## Manual failover and pause

`POST /failover/{id}` promotes a standby at once, without the primary
cooperating — the tool for a primary that is down for good. It is also the
way to fail over while the cluster is `paused` (`settings set paused=true`),
since pause suspends only the *automatic* failover. Its guard is the mirror
of the switchover's: a primary that looks alive is a 409.

## Synchronous replication

Driven by the settings `synchronous_mode` and `synchronous_node_count`
(`settings set synchronous_mode=quorum synchronous_node_count=1`):

- `off` — asynchronous; a failover may lose the WAL the dead primary had not
  shipped (bounded by nothing but the network).
- `quorum` — `synchronous_standby_names` is rendered as
  `ANY <count> (...)`; a commit waits for any `count` standbys.
- `strict` — `FIRST <count> (...)`; a commit waits for the first `count`
  standbys in the list.

The primary's reconcile tick recomputes the list once a second: the
candidates are nodes that report themselves running standbys, are not tagged
`nosync`, and are actually streaming from this primary right now (matched by
`application_name`, which the agent sets to the node id) — a standby that
connects but does not stream is never waited for. The rendered value is
written to the primary's `postgresql.conf` and reloaded; a standby promoted
before its first reconcile writes no list at all, so it does not wait for
standbys that may be gone.

Edge case inherited from Patroni's semantics: in `strict` mode, when the
list would come out empty the previous list is kept — without it every
commit would block, which defeats the durability strict mode exists for. In
`quorum` mode an empty list turns synchronous commit off: availability wins.

The primary reports its current synchronous standbys in `sync_standbys`, and
the failover candidate ordering prefers them: a standby the primary was
committing with misses no committed data.

## Replication slots

- Every node has a physical slot `tembo_<id>` on the primary. The primary's
  reconcile tick creates missing slots (with immediate WAL reservation) and
  drops `tembo_*` slots of nodes that left the membership, so a down node's
  WAL is kept for as long as it is a member — and released when it is not.
- A standby sets `primary_slot_name = tembo_<id>` and, when cloning with
  `pg_basebackup`, streams the initial backup into the same slot (`--slot`),
  so there is no WAL gap between the clone and the streaming that follows.
- Cascading standbys (`--follow`) and DR standbys (`--standby-of`) have no
  slot of their own: their upstream is not the primary that keeps slots.
- **Logical slot failover** (PostgreSQL 17): tembo's defaults set
  `sync_replication_slots = on` (with `primary_slot_name`, which tembo
  manages) and `hot_standby_feedback = on`. User-created logical slots with
  `failover = true` are then synchronized to the standbys by Postgres itself
  and survive a failover — CDC and subscriptions keep working.

A slot on a down node holds WAL on the primary. With fixed raft membership
that is a deliberate trade: the slot lives exactly as long as the node is a
member.

## Cascading and DR standbys

- `--follow <member-id>`: the node replicates from that member instead of
  the primary (a replica of a replica — e.g. to keep cross-DC traffic off
  the primary). It stays a full cluster member: it votes, it can be promoted (unless also `nofailover`). When the
  followed member is or becomes the
  primary, it follows the primary like anyone else.
- `--standby-of <host>`: a DR copy of a *foreign* cluster, replicating from
  a fixed host on `--pg-port`. The node never becomes a primary (it publishes
  `nofailover`), the agent never repoints it, and the config rollout uses
  the fixed upstream. Give the foreign host's clients access with
  `settings set-hba`.

## Node tags

| Flag                    | Effect                                                                                                |
|-------------------------|-------------------------------------------------------------------------------------------------------|
| `--nofailover`          | The node is never picked for automatic failover and refuses to be a switchover/failover target.       |
| `--nosync`              | The node is never named in `synchronous_standby_names`.                                               |
| `--failover-priority N` | Higher N wins the failover candidate tie-break (checked before sync-standby status and WAL position). |

The tags are local flags republished through each node's status every
second — change them by restarting the agent with different flags.

## The hung primary

A postmaster that is alive but answers no SQL (storage stuck, D-state) is
detected by reconcile: once it has been unresponsive for 2 minutes after
having answered at least once since its start, it is stopped immediately and
restarted by `supervise`. A server in crash recovery — one that has not
answered since it started — is left alone to finish recovering. Automatic
failover does not wait for this: it keys on the poll status, which a hung
server stops answering within `failoverTimeout`.
