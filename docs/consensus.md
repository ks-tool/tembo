# Consensus: the raft package and the store

`internal/raft` is a small, self-contained implementation of the Raft
consensus algorithm — leader election, log replication and commit, and
membership
changes through joint consensus. It knows nothing about HTTP or Postgres:
it talks to the outside world through a `Transport` interface (outbound) and
three `Handle*` methods (inbound), and persists through a `Storage`
interface. tembo wires it up as JSON over HTTP with a bbolt-backed storage;
see `internal/agent/transport.go` and `internal/store/raftlog.go`.

What it deliberately leaves out:

- **Pre-vote.** A partitioned node campaigns freely and raises its term; when
  the partition heals there can be a short election storm. A node joining a
  running cluster with `--join` starts with its election timer frozen (`Config.Frozen`) until a configuration from a
  leader names it, which
  covers the common disruption.
- **Leader lease / check-quorum.** Leadership is what the term says; a leader
  cut off from the majority learns it from a response with a higher term.
  tembo's safety does not depend on this — fencing is done at the Postgres
  level, see [high-availability.md](high-availability.md).
- **Batching, pipelining, flow control.** Heartbeats every 50 ms carry
  everything the follower needs; the log is a few settings keys, not a data
  plane.

## Timeouts

| Parameter           | Default | Meaning                                                                                                    |
|---------------------|---------|------------------------------------------------------------------------------------------------------------|
| `ElectionTimeout`   | 600 ms  | Base follower timeout; the actual timeout is randomized in `[1×, 2×)`.                                     |
| `HeartbeatInterval` | 50 ms   | Leader-to-follower contact interval; also the node's tick resolution. Must be less than `ElectionTimeout`. |

A RPC (vote, append, snapshot install) is given `ElectionTimeout` to answer (10× for a snapshot); any error counts as
"no answer" and is retried by the
next tick.

## The log and its entries

`Entry{Index, Term, Data}`. Indexes start at 1. Two entry kinds share the
log, told apart by their data: application entries (JSON `store.Command`
objects) and configuration entries (a zero byte followed by the membership
JSON — application data can never start with a zero byte, since it is JSON).

Entries of the current term commit only when replicated on a quorum (Raft
§5.4.2); entries of earlier terms commit indirectly through the no-op entry
a new leader appends on election. That no-op doubles as the readiness marker:
`Node.Ready()` reports whether the leader has applied everything committed
before its election — until then, its state machine may lack what the
previous leader committed, and tembo's leader refuses switchover/failover
requests and defers automatic failover.

`Propose` blocks until the entry is committed **and applied locally**, or the
context expires. A follower answers `ErrNotLeader`; the HTTP layer turns
that into a 307 redirect to the leader.

## Membership: joint consensus

`Membership{Voters, Joint}` — `Voters` is the current voting set (node id →
address, opaque to raft), `Joint` the incoming set while a change is being
committed. A change is:

1. The leader appends an entry naming both sets (`ProposeMembership`).
   It takes effect the moment it enters the log: both sets must reach quorum
   — a majority of `Voters` **and** of `Joint`.
2. Once the joint entry commits, the leader appends a plain entry naming the
   new set alone. `ProposeMembership` returns when the joint entry has
   committed; only one change is in flight at a time (`ErrMembershipBusy`).

Nodes a committed configuration dropped are kept as replication targets for
a grace period (10 × election timeout), so they receive the entry and its
commit and learn on their own that they left; a removed node never campaigns
again (`Node.Removed()`).

A configuration from the compacted prefix survives compaction: the storage
remembers the membership in effect at the base entry, and a snapshot carries
the membership at its last included index.

## Compaction and snapshots

With `RetentionLogSize` (tembo: `--raft-retention-log-size`, default 8 MiB)
the applied prefix of the log is compacted once it exceeds retention by a
quarter, down to retention. The storage then holds:

- the **base entry** (index/term of the last compacted entry),
- the **membership in effect at the base**,
- the entries after the base.

A follower that needs entries compacted away is sent the state machine as a
whole (`InstallSnapshot`) together with that membership. In tembo the state
machine is the key/value bucket serialized as a JSON object, so a snapshot is
small. Installing a snapshot replaces the KV, moves the applied index, and —
in the same bbolt transaction — trims the log to the snapshot's base,
keeping the suffix when the follower's log still contains the base entry.

A node running without a `Storage` (not tembo's case) refuses a snapshot with
`Success: false`, and the leader does not count phantom delivery.

## The store (`internal/store`)

One bbolt file (default `<id>.db`) with four buckets:

| Bucket      | Contents                                                                                     |
|-------------|----------------------------------------------------------------------------------------------|
| `kv`        | The replicated key/value state machine                                                       |
| `meta`      | `applied` — the index of the last applied raft entry                                         |
| `raftlog`   | The raft log: entry index (big-endian key) → term (8 bytes) + data                           |
| `raftstate` | `term`, `voted_for`, the base entry (`base_index`, `base_term`) and the `config` at the base |

Every write is committed to disk before the node acts on it (durability
before use, as Raft requires). Two invariants are kept transactionally:

- **Exactly-once apply.** The applied index and the entry's effect are
  written in one transaction, so a node that restarts — and a follower that
  replays the log after an election — applies each entry once. Committed
  entries never change, so skipping by index is safe.
- **Compaction/snapshot atomicity.** `Compact` and `InstallSnapshot` cut the
  log and move the base (or replace the state machine) in the same
  transaction as their metadata, and compaction is refused past the applied
  index.

`OnStorageError` in tembo is `log.Fatal`: a node that lost what it promised
must not take part in the cluster any longer.

### Commands

```go
type Command struct {
    Op    string `json:"op"`    // "set", "add", "del" or "merge"
    Key   string `json:"key"`
    Value string `json:"value,omitempty"`
}
```

- `set` — put the value;
- `add` — put only if the key is absent (used once, to bootstrap `primary`);
- `del` — delete;
- `merge` — the value is a JSON object merged into the object at the key; a
  `null` field removes it. Objects are re-marshaled with sorted keys, so
  every node stores the same bytes.

A malformed command is logged and skipped — the log must never wedge.

### Snapshot contents

`Snapshot()` reads the `kv` bucket and the applied index in one transaction
and returns them as one JSON object — the two belong together, which is why
the state machine lives in the same file as the log.

## The HTTP transport

Raft RPCs are JSON over HTTP POST: `/raft/vote`, `/raft/append`,
`/raft/snapshot`. The outbound transport resolves node ids to addresses
through the current membership (membership changes take effect immediately),
uses plain HTTP, or HTTPS with the CA of `--tls-ca-file` when the API serves
TLS. The inbound handlers are mounted on the same port as the CLI API, behind
the same allowlist (cluster nodes, the loopback, and the addresses of the
`hba` setting — see [security.md](security.md#the-address-allowlist)).
