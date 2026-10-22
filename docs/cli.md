# CLI reference

The `tembo` binary is both the daemon (`run`) and the client. Every other
command is an HTTP client to one agent's API and prints the response to
stdout. Commands that change cluster state redirect to the raft leader
automatically (HTTP 307), so any node's address works.

## Global flags

| Flag                                | Default                               | Description                                                                                     |
|-------------------------------------|---------------------------------------|-------------------------------------------------------------------------------------------------|
| `--addr`                            | `127.0.0.1:8001`                      | API address to talk to; an optional `http://`/`https://` prefix overrides the scheme guess.     |
| `--token` / `--token-file`          | `$TEMBO_TOKEN` / —                    | Bearer token for the API.                                                                       |
| `--username` / `--password`         | `$TEMBO_USERNAME` / `$TEMBO_PASSWORD` | Basic auth credentials (for LDAP-backed API authentication).                                    |
| `--tls-cert`, `--tls-key`           | —                                     | Client certificate to authenticate with.                                                        |
| `--tls-ca`                          | system pool                           | CA bundle to verify the API server with.                                                        |
| `--krb5`                            | off                                   | Authenticate with Kerberos (SPNEGO); the ticket comes from the credential cache (`KRB5CCNAME`). |
| `--krb5-keytab`, `--krb5-principal` | —                                     | Log in fresh from a keytab instead (principal as `user@REALM`).                                 |
| `--krb5-config`                     | `$KRB5_CONFIG` or `/etc/krb5.conf`    | `krb5.conf` to use.                                                                             |

Requests without a deadline get one of 10 seconds; long-running ones (`switchover` 90 s, `dump`/`backup`/`restore*` 1 h)
set their own.

## `run`

Starts the node: opens the store, joins or forms the raft cluster, serves the
API, runs both proxies, supervises the local Postgres. Without `--peers` and
`--consul` the node runs standalone — no raft, always the primary; the
cluster commands below (`members`, `switchover`, `failover`) then answer 404.
See
[configuration.md](configuration.md) for the full flag reference. Runs until
`SIGINT`/`SIGTERM` (see [operations.md](operations.md#shutdown-and-signals)
for the shutdown sequence). `SIGHUP` is ignored — there is nothing to reload.

## `status`

Prints `GET /status` of one node: its raft state, term, leader, applied
index, the cluster's primary, the local Postgres state and the committed
membership. See [http-api.md](http-api.md#get-status) for the shape.

## `config show|set|reset` — cluster `postgresql.conf`

```sh
tembo config show
tembo config set shared_buffers=256MB max_connections=200
tembo config reset shared_buffers
```

`set` takes `name=value` pairs; values are validated against a real Postgres
before committing (a refused parameter names its error). `reset` returns
parameters to the agent's defaults. Changes roll out node by node — standbys
in id order, then the primary; a configuration that lowers a hot-standby
limit goes to the primary first instead (see
[postgres-management.md](postgres-management.md#configuration-rollout)).
Disabled in Kubernetes mode.

## `settings show|set|reset|set-hba` — agent behavior

```sh
tembo settings show
tembo settings set max-lag-on-failover=16MB synchronous_mode=quorum
tembo settings set paused=true
tembo settings reset paused
```

See [configuration.md](configuration.md#agent-behavior-settings) for the
settings themselves. Like `config`, `set`/`reset`/`set-hba` are disabled in
Kubernetes mode (`PATCH /settings` answers 403).

### `set-hba` — add or remove a `pg_hba.conf` line on every node

```sh
tembo settings set-hba host \
    --db app --user app --src-addr 10.1.0.0/24 \
    --auth scram-sha-256 [--auth-opts name=value ...]
tembo settings set-hba host --db app --user app --src-addr 10.1.0.0/24 \
    --auth scram-sha-256 --remove
```

The first argument is the connection type: `local`, `host`, `hostssl`,
`hostnossl`, `hostgssenc` or `hostnogssenc`. Flags: `--db` (default `all`),
`--user` (default `all`), `--src-addr` (IP, CIDR or hostname; required for
non-`local`), `--auth` (default `scram-sha-256`), `--auth-opts` (repeatable
`name=value`), `--remove` (drop the matching line instead of appending).
`settings reset hba` clears all lines. The addresses of `host` lines double
as the agent's own allowlist — see
[security.md](security.md#the-address-allowlist).

## `members list|add|remove` — cluster membership

```sh
tembo members list
tembo members add d=10.0.0.4:8001
tembo members remove d
```

`add` updates an existing member's address; ids are `[a-z0-9_]+`. Changes go
through joint consensus — the cluster stays available while they settle. A
removed node stops its Postgres and exits on its own. Remember to start the
new node first with `--join` (see [getting-started.md](getting-started.md)).

## `switchover node` — planned primary move

```sh
tembo switchover b
tembo switchover b --at 2026-10-22T02:00:00Z   # scheduled; answers at once
```

Moves the primary role to a running standby without losing transactions: the
primary stops cleanly (a fast shutdown writes a checkpoint and flushes WAL to
the standbys), and the target is promoted once it has the primary's shutdown
checkpoint LSN. Clients are refused by the proxies while it runs. The command
waits for the outcome (up to ~90 s); `--at` takes an RFC3339 timestamp and
returns immediately, the switchover runs when its time comes.

Refused with 409 if: a switchover is already pending, the target is the
primary, is tagged `nofailover`, is not a running standby, or the primary is
unreachable (use `failover`). An unknown node id is a 404.

## `failover node` — promote while the primary is down

```sh
tembo failover b
```

Promotes a running standby without waiting for the old primary — the tool for
when the primary is down for good, and the way to fail over manually while
the cluster is `paused`. Refused with 409 if the primary looks alive ("use
switchover") or a switchover is pending; an unknown node id is a 404.

## `rotate-password` — rotate the cluster password

```sh
tembo rotate-password            # generates one and prints it — the only time it is shown
tembo rotate-password 's3cret'   # or sets the one you give
```

Rotates the password of the `postgres` and `tembo_repl` roles on the whole
cluster: the value commits through raft (`PUT /password`), every agent
starts connecting with it within a second or two, the primary alters the
roles on its next reconcile tick, and the standbys receive them by WAL
replay — expect a brief replication blip while the standby walreceivers
reconnect. The stored value is never returned by any GET. A node with
`--standby-of` keeps the password of its foreign upstream and is not
rotated. The `TEMBO_PG_PASSWORD`/`--pg-password-file` value is only the
bootstrap password: change it with this command, not by editing the
environment. With the OpenBao integration (`--bao-password-path`) this
command answers 403 — change the secret in OpenBao instead, see
[configuration.md](configuration.md#openbao).

## `reinit` — discard a standby's data and reclone

```sh
tembo --addr 10.0.0.2:8001 reinit
```

Stops the node's Postgres, deletes the data directory's contents and lets
`supervise` clone the primary anew (or run `--clone-command`). The primary
refuses (409). Run it against the node being rebuilt — it is a local action,
not a raft command.

## `restart` / `reload` — local Postgres

```sh
tembo --addr 10.0.0.2:8001 restart   # stop fast; supervise starts it again
tembo --addr 10.0.0.2:8001 reload    # pg_ctl reload (configuration files)
```

`restart` refuses (409) when Postgres is not running — `supervise` is already
starting it. `reload` re-reads `postgresql.conf`/`pg_hba.conf`; the agent
reloads on its own after every configuration change, so this is for manual
file edits (which the agent overwrites on its next rewrite).

## `health` — the healthcheck

```sh
tembo health    # exit 0 while this node's Postgres answers SQL, 1 otherwise
```

Serves supervisors, orchestrators and load balancers: it is `GET /health`,
which answers 200 only while the local Postgres of the node `--addr` points
at answers SQL, and 503 otherwise — so a container/systemd check goes
unhealthy exactly when the node cannot serve queries. Contrast with
`status`, which stays 200 regardless: it is the cluster's own view, and a
node whose Postgres is down must keep taking part in it.

## `history`

Prints the cluster's failover and switchover history — a JSON object of
RFC3339 timestamp → event, kept under the `history` key and written by
whichever node was the raft leader at the time.

## `dump` / `restore` — logical backups

```sh
tembo dump > dump.sql                  # pg_dumpall of the whole cluster
tembo dump --db app > app.sql          # pg_dump of one database
tembo restore < dump.sql               # feeds psql (ON_ERROR_STOP=1)
```

Runs against the node you point `--addr` at, over its local socket. `restore`
prints the tail of psql's output (up to 64 KB) — that is where an error shows
itself.

## `backup` / `restore-backup` — physical backups

```sh
tembo backup > base.tar                # pg_basebackup -X stream, tar of the data dir + WAL
tembo --addr 10.0.0.3:8001 restore-backup < base.tar
```

`restore-backup` stops the node's Postgres, replaces the data directory with
the archive and lets `supervise` start it. A standby that is ahead of the
backup heals itself with `pg_rewind` (or a reclone after repeated rewind
failures).
