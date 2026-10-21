# Getting started

## Requirements

- PostgreSQL binaries in `$PATH` (or pointed to by `--pg-bin`):
  `postgres`, `initdb`, `pg_ctl`, `pg_controldata`, `pg_basebackup`,
  `pg_rewind`, `pg_dump`/`pg_dumpall`, `psql`. PostgreSQL 13+; the logical
  slot failover defaults need 17.
- Linux for production fencing (`Pdeathsig`); the agent also builds and runs
  on macOS.
- A private network between the nodes, or TLS plus authentication on the API (see [security.md](security.md)).
- The Postgres superuser password, the same on every node: set
  `TEMBO_PG_PASSWORD` in the environment (or pass `--pg-password-file`).
  It seeds the cluster at bootstrap; later, rotate it with
  `tembo rotate-password` ([cli.md](cli.md#rotate-password--rotate-the-cluster-password)).

## A three-node cluster

Build the agent:

```sh
go build -o tembo .
```

Run three agents on three hosts (`a`, `b`, `c` at `10.0.0.1`–`10.0.0.3`).
Every node lists all nodes in `--peers`, itself included:

```sh
# on 10.0.0.1
tembo run --id a \
    --peers a=10.0.0.1:8001,b=10.0.0.2:8001,c=10.0.0.3:8001 \
    --pg-data /var/lib/postgresql/a \
    --pg-password-file /etc/tembo/pgpass

# the same with --id b and --id c on the other hosts
```

When the hostname is a usable node id, both the id and the `id=` prefixes
can go: with `hostname: tembo-a` (or any dash-free name) a bare
`--peers tembo-a:8001,tembo-b:8001,tembo-c:8001` names every node after its
host — that is the shape of [docker-compose.yaml](../docker-compose.yaml).
IP addresses cannot: dots do not name a replication slot.

What happens next:

1. The three raft nodes elect a leader. The leader records itself as the
   cluster's primary under the `primary` key.
2. On the primary, an empty data directory is created by `initdb`
   (UTF8, locale `C`).
3. The other nodes clone the primary with `pg_basebackup` into their
   replication slots (`tembo_b`, `tembo_c`) and start as standbys.
4. Both proxies come up: writes on `:5432`, reads on `:5434`.

Clients connect to `10.0.0.1:5432` (or any node — every node runs both
proxies) for read/write, and to `10.0.0.1:5434` for read-only traffic spread
over the standbys. Authentication is Postgres's own: the proxy forwards TCP
and stays out of it.

Check the state:

```sh
tembo --addr 10.0.0.1:8001 status
```

```json
{
  "id": "a",
  "state": "leader",
  "term": 3,
  "leader": "a",
  "applied": 12,
  "primary": "a",
  "postgres": {"running": true, "in_recovery": false, "lsn": 67553424, "config": "d41d8c"},
  "members": ["a", "b", "c"]
}
```

## Exercising it

Move the primary to node `b` without losing transactions (the primary stops
cleanly; the standby is promoted once it has the primary's shutdown LSN):

```sh
tembo --addr 10.0.0.1:8001 switchover b
```

Kill the primary's host outright. Within about 15 seconds the leader promotes
the most advanced standby. When the old primary returns, `pg_rewind` turns
its data directory into a standby's; if rewind keeps failing, after 10
attempts the directory is discarded and the node reclones itself.

Change a setting on the whole cluster — validated against a real Postgres
before it commits, rolled out standbys first:

```sh
tembo --addr 10.0.0.1:8001 config set shared_buffers=256MB max_connections=200
```

Give your applications access. The agent writes `pg_hba.conf` itself; your
lines go into the cluster-wide `hba` setting:

```sh
tembo --addr 10.0.0.1:8001 settings set-hba host \
    --db app --user app --src-addr 10.1.0.0/24 --auth scram-sha-256
```

## Adding a fourth node to a running cluster

Start the node naming itself and one existing member, and tell it that it is
joining (its elections stay frozen until the cluster's configuration reaches
it):

```sh
# on 10.0.0.4
tembo run --id d \
    --peers d=10.0.0.4:8001,a=10.0.0.1:8001 \
    --pg-data /var/lib/postgresql/d --pg-password-file /etc/tembo/pgpass \
    --join
```

Then add it to the membership, from any node (the request redirects to the
leader):

```sh
tembo --addr 10.0.0.1:8001 members add d=10.0.0.4:8001
```

The change goes through Raft as a joint consensus: the old and the new voter
sets both vote until the change settles, so the cluster stays available
throughout. The new node clones the primary and becomes a standby.

Removing a node:

```sh
tembo --addr 10.0.0.1:8001 members remove d
```

A removed node learns this on its own — its agent stops its Postgres and
exits.

## Where to go next

- [configuration.md](configuration.md) for every flag, the environment, files,
  Kubernetes and Consul sources.
- [high-availability.md](high-availability.md) for the failover and fencing
  rules and timings.
- [operations.md](operations.md) for backups, dumps and restores.
- [security.md](security.md) before exposing the API beyond the cluster
  network.
