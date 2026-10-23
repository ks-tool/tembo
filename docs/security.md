# Security

The API port carries the full control plane: raft writes, switchover,
configuration. tembo's model is layered, and the first layer is the network —
in the simplest deployment the cluster network is closed, and that is the
entire defense. The layers beyond it, in the order a request meets them:

```
TLS (optional) → address allowlist → authentication (optional) → handler
```

## The address allowlist

Every request's source address is matched against the same list
`pg_hba.conf` is written from:

- the loopback (the CLI reaches the API on its node; with authentication on it still
  has to authenticate, see below),
- the hosts of the current raft membership (raft RPCs, status polls,
  proxy connections to Postgres),
- the address fields of the `host` lines of the `hba` setting (`all` widens to `0.0.0.0/0` and `::/0`; `samehost`/
  `samenet` and `local`
  lines contribute nothing).

Anything else is refused with 403 (`the address is not on the hba list`)
before any handler runs. The list is rebuilt within a second of a membership
or `hba` change; a name that stops resolving is skipped rather than dropping
the whole list. Both proxies enforce the same list on their client side.

Consequence: **granting a network access to Postgres with
`settings set-hba` grants it access to the API and the proxies too** — keep
application CIDRs coarse and put authentication on the API when the network
is not closed.

## TLS

`--tls-cert-file` + `--tls-key-file` switch the API port to HTTPS. With
`--tls-ca-file` additionally:

- the nodes verify each other's servers (raft RPCs, status polls, the
  redirect to the leader — all move to `https` with that CA);
- clients may authenticate with a certificate: the listener asks for one (`VerifyClientCertIfGiven`) and a verified
  chain identifies the client (mTLS, see below).

The CLI side: `--tls-ca` to verify the server, `--tls-cert`/`--tls-key` to
present a client certificate. An `--addr` without a scheme is assumed
`https` when any `--tls-*` CLI flag is given.

## API authentication

Enabled by any of the `--auth-*` flags — or by `--tls-ca-file` alone, which
turns mTLS on. Cluster nodes are exempt on the raft and status paths only
(`/raft/*`, `GET /status`); every other request, the
loopback included, must authenticate. `GET /health` is the one exception: it
answers only whether the local Postgres answers SQL, and supervisors run
`tembo health` without credentials. Supported mechanisms, checked by the credential the request
carries:

| Mechanism | Request carries                     | Identity             | Enabled by                                                                                                                                                                                                        |
|-----------|-------------------------------------|----------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| JWT       | `Authorization: Bearer <token>`     | `jwt:<subject>`      | `--auth-jwt-key-file` (PEM public key RS*/ES*/Ed25519 or HMAC secret HS*) or `--auth-jwt-jwks` (a JWKS URL; refetched when a token names an unknown `kid`); `--auth-jwt-issuer`/`--auth-jwt-audience` optionally required |
| Kerberos  | `Authorization: Negotiate <spnego>` | `krb5:<principal>`   | `--auth-krb5-keytab` (the API's own principal)                                                                                                                                                                    |
| LDAP      | `Authorization: Basic ...`          | `ldap:<bound DN>`    | `--auth-ldap-url`; bind either directly (`--auth-ldap-dn-template "cn=%s,ou=users,dc=example,dc=com"`) or through a search (`--auth-ldap-bind-dn`/`-bind-password-file`/`-base-dn`/`-filter "(uid=%s)"`)          |
| mTLS      | the TLS handshake                   | `cert:<CN>`          | `--tls-ca-file` (a verified client certificate chain; the identity is the certificate's CN, or its first DNS SAN when the CN is empty)                                                                            |

A request with no recognized credential is answered 401 with a
`WWW-Authenticate` header naming the enabled mechanisms — with mTLS alone
the list is empty and no header is sent.

**Read-only identities.** `--auth-read-only` (repeatable/comma-separated,
e.g. `jwt:alice,ldap:cn=bob,ou=users,dc=example,dc=com`) restricts an
identity to `GET`/`HEAD`; every
other method is 403. Everything not listed has full access.

## Blocked parameters

`PATCH /config` refuses — with a 400, before validation — any parameter that
tembo relies on or that would let the
configuration execute code as the `postgres` user on every node at a reload's
distance (on the Kubernetes `config.json` path a blocked parameter
invalidates the whole object instead, and the error is only logged):

- **agent-owned**: `config_file`, `data_directory`, `hba_file`,
  `ident_file`, `port`, `primary_conninfo`, `primary_slot_name`,
  `synchronous_standby_names`, `hot_standby`, `unix_socket_directories`,
  `log_destination`, `log_directory`, `log_filename`, `log_rotation_age`,
  `log_rotation_size`, `log_truncate_on_rotation`, `logging_collector`;
- **code-execution vectors** (cross-checked against CloudNativePG's fixed
  set): `archive_command`, `archive_cleanup_command`, `archive_library`,
  `restore_command`, `recovery_end_command`, `ssl_passphrase_command`,
  `ssl_passphrase_command_supports_reload`, `shared_preload_libraries`,
  `session_preload_libraries`, `local_preload_libraries`,
  `dynamic_library_path`;
- **file inclusion**: `include`, `include_dir`, `include_if_exists`.

Parameter names must also match `^[a-z_][a-z0-9_]*(\.[a-z_][a-z0-9_]*)?$`,
which keeps quoting games out of the file. Values are escaped for
single-quoted `postgresql.conf` syntax (backslash and quote doubled).

The trade-off: `shared_preload_libraries` cannot be set through the cluster
configuration at all, so extensions that need it (pg_stat_statements as a
preload, pgcrypto is fine as `CREATE EXTENSION`) cannot be enabled this way.
Not yet blocked (reviewed, deliberately open for now): the
`recovery_target_*` family, `promote_trigger_file`, `listen_addresses`,
`log_file_mode`, and minimum enforcement for `wal_level`/`wal_log_hints`/
slot-related parameters — a user could still set `wal_level = replica`
cluster-wide and quietly lose logical slot failover.

## What runs as whom

- The agent runs as your OS user (typically `postgres`, for `peer` local
  auth) and spawns `postgres`, `initdb`, `pg_ctl`, `pg_basebackup`,
  `pg_rewind`, `pg_dump`, `psql`, `tar` — with output captured, cancellation
  mapped to SIGINT, and (on Linux) `Pdeathsig SIGQUIT` on the postmaster.
- `--clone-command` runs through `sh -c` as the agent's user — it is an
  operator-provided flag, not reachable through the API.
- The API's file writes belong to the local ops: `/restore-backup` untars
  the request body into the (wiped) data directory, and `/backup` stages
  `pg_basebackup` in a temporary directory. Shelling out likewise happens in
  the local-ops handlers — `pg_dumpall`/`pg_dump`/`psql` for
  `dump`/`restore`, `pg_basebackup`/`tar` for `backup`/`restore-backup`,
  `pg_ctl` for `reinit`/`restart`/`reload` and the `/health`/`/status`
  probes — with fixed argument vectors; the only client-controlled string
  reaching a command line is a
  `?db=` value guarded by `[a-zA-Z0-9_.$-]+`.

## Passwords

- `TEMBO_PG_PASSWORD`/`--pg-password` never touches the filesystem: the only
  writer is `initdb` of a new cluster, which gets a temporary pwfile (0600, `/tmp`, removed when `initdb` exits).
- `PUT /password` (`rotate-password`) reads at most 1024 bytes of the body —
  a longer one is silently truncated, an empty one is a 400 — and stores the
  password under the `password` key of the replicated store — readable by
  every node, returned by no GET. It sits in plain text in each node's
  bbolt file: guard that file as you would a pgpass. Raft log entries carry
  it too, until compaction.
- **With the OpenBao integration** (`--bao-password-path`) the store is out
  of the loop: the password lives in the KV v2 secret and in Postgres, the
  nodes poll it every ~5 s (authenticated with a token or AppRole —
  [configuration.md](configuration.md#openbao)), and `PUT /password` answers
    403. Of the three placements this is the only one where the value never
         enters the raft log or the bbolt files.
- The rotation reaches the roles as `ALTER ROLE ... PASSWORD` over the
  agent's local connection — the Unix socket, 127.0.0.1 or the host's own
  name, per `--pg-local-auth peer|password|gss` — never over the network in
  a conninfo. The standbys' `primary_conninfo` passwords do not travel
  through WAL either: every standby's agent rewrites its own
  `postgresql.conf` with the new value (reconcile → `Follow`).
- The LDAP bind password of the API authentication may also come from
  OpenBao (`--bao-ldap-password-path`); in the store mode it does not
  participate — it is a file on each node (`--auth-ldap-bind-password-file`;
  there is no value flag).

## Audit of API calls

Every API request that changes state (anything but `GET` and `HEAD`), and
every refused one (401, 403), is logged as one JSON line:

```json
{"level":"info","audit":true,"actor":"jwt:alice","method":"PUT","path":"/switchover/node2","status":200,"message":"api call"}
```

- `actor` is the identity the authentication gave: `jwt:<subject>`,
  `krb5:<user@domain>`, `ldap:<DN of the user>` or `cert:<certificate CN>`.
- A request with no identity is named by its address: `addr:<host>`. That is
  a node, or any request while authentication is off. With authentication on,
  the CLI on a node is named by its credentials like any other client.
- The body is not logged: it can hold passwords (`PUT /password`,
  `PATCH /config` with secrets). Query strings are not logged either.
- The raft messages between the nodes (`/raft/*`) are not logged: they are
  sent every heartbeat.
- `history` keeps the cluster's failover and switchover events; the audit
  line is the record of who asked for them and everything else.
- A `shell` session is one `POST /shell`: its API line names who opened it.
  The agent logs the session's Postgres role and database when it opens and
  when it ends. The SQL typed in it is not logged by tembo; only Postgres's own
  logging (`log_statement`) records it, and tembo does not manage that setting.
- The session's Postgres password travels in the `X-Tembo-Pg-Password` header,
  so the client refuses to send it over plain HTTP to any host but the loopback.
  The header is not logged.

## Network exposure summary

| Port                 | Exposed to                                                     | Protected by                                                                    |
|----------------------|----------------------------------------------------------------|---------------------------------------------------------------------------------|
| API (`8001`)         | CLI users, peer nodes                                          | allowlist; TLS + authentication when configured                                 |
| write proxy (`5432`) | applications                                                   | allowlist (same list as `pg_hba.conf`); Postgres's own authentication beyond it |
| read proxy (`5434`)  | applications                                                   | same                                                                            |
| Postgres (`5433`)    | nodes (replication, rewind), whatever the `hba` setting allows | `pg_hba.conf` as written by the agent                                           |

The proxy forwards raw TCP — authentication and TLS are between the client
and Postgres, untouched.
