# Configuration

Configuration reaches the agent from several sources, applied in growing
priority (later wins):

1. built-in defaults,
2. the `--config` file (`.env` or `.json`),
3. the Kubernetes objects named by `--k8s` (their keys are the same
   `TEMBO_*` names as the environment's, plus the special `config.json`/
   `settings.json` — see [operations.md](operations.md#kubernetes-mode)),
4. the environment (`TEMBO_*`),
5. the command line.

The same names run through all sources: the flag `--pg-port` is the
environment variable `TEMBO_PG_PORT`, and that is also the key in the
`--config` file and the `--k8s` objects — the `TEMBO_` prefix stays, in
upper- or lower-case (`TEMBO_PG_PORT = 5433` in `.env`; a `.json` object may
also nest, `{"tembo_pg": {"port": 5433}}` flattening to `TEMBO_PG_PORT`).
Values are loaded by `app-config` before startup validation.

The environment also carries two logging switches read before anything else:
`TEMBO_LOG_DEBUG` (any `1/true/yes/on/t*/y*` value enables debug logging) and
`TEMBO_LOG_UTC` (timestamps in UTC; also sets Postgres `log_timezone`/`timezone` to UTC).

## `run` reference

### Identity and cluster topology

| Flag           | Env                | Default       | Description                                                                                                                                                                                                                                                                                                 |
|----------------|--------------------|---------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `--id`         | `TEMBO_ID`         | the hostname  | This node's id. Must appear in `--peers`. Lowercase letters, digits and underscores only — it names a replication slot; when unset it comes from the hostname with dashes turned into underscores (in Kubernetes, the pod's name).                                                                          |
| `--peers`      | `TEMBO_PEERS`      | —             | All nodes, comma separated, this node included. An item is `id=host:port`, or a bare `host:port`, which names the node after its host — `tembo-a:8001` is node `tembo_a`, the same rule as the `--id` default. Hosts that do not sanitize into an id (an IP, dots, uppercase) need the explicit `id=` form. Without `--peers` and `--consul` the node runs standalone. |
| `--join`       | `TEMBO_JOIN`       | off           | Join a running cluster: `--peers` names this node and at least one member. Elections stay frozen until the cluster's configuration arrives.                                                                                                                                                                 |
| `--follow`     | `TEMBO_FOLLOW`     | —             | Replicate from this member instead of the primary (cascading standby).                                                                                                                                                                                                                                      |
| `--standby-of` | `TEMBO_STANDBY_OF` | —             | Replicate from a foreign host (on `--pg-port`), e.g. a DR copy of another cluster. The node never becomes a primary. Excludes `--follow`.                                                                                                                                                                   |
| `--advertise`  | `TEMBO_ADVERTISE`  | hostname:8001 | With `--consul` or standalone: the address the API is reached at.                                                                                                                                                                                                                                           |

`--id`, `--pg-data` and a password (`--pg-password`, `--pg-password-file` or
`--bao-password-path`) are required; a cluster also takes `--peers` (or
`--consul`).

### Standalone mode

Without `--peers` and `--consul` the node runs standalone: a cluster of one
without raft — no elections, no replicated log, no replication slots for
peers. The node is always the leader and the primary, so Postgres starts at
once, and `PATCH /config`, `PATCH /settings` and `PUT /password` apply to the
local store directly instead of going through consensus. `GET /status`
reports `state: standalone`; the cluster endpoints (`/switchover`,
`/failover`, `/members`) are absent and answer 404. The API listens on the
port of `--advertise` (8001 by default). `--join` and `--follow` need
`--peers`; `--standby-of` combines with standalone for a lone replica of a
foreign host.

### Passwords

The password the agent connects with is, in order of precedence:

1. **OpenBao**, when `--bao-password-path` is set — polled every ~5 s, and
   an update of the secret reaches Postgres on its own (see below);
2. **the cluster's rotated password** — the `password` key of the store, see
   `rotate-password` in [cli.md](cli.md#rotate-password--rotate-the-cluster-password);
3. `--pg-password` / `TEMBO_PG_PASSWORD`;
4. the first line of `--pg-password-file`.

A rotation commits through raft and takes effect on every node within a
second or two; the environment or file value is only the bootstrap one: on a
fresh cluster `initdb --pwfile` sets the superuser's password from a
temporary file that exists only for the `initdb` run, and the primary then
creates the `tembo_repl` role with that same password (`ensureRoles`).
Changing the environment or file on a running cluster does nothing — rotate
with the command instead. A node with `--standby-of` is never rotated: its
password belongs to the foreign upstream.

### OpenBao

Point the node at an [OpenBao](https://openbao.org) (Vault-compatible)
server to make it the source of truth for the cluster's password:

| Flag                                         | Env                                                  | Default     | Description                                                                                                                 |
|----------------------------------------------|------------------------------------------------------|-------------|-----------------------------------------------------------------------------------------------------------------------------|
| `--bao-address`                              | `TEMBO_BAO_ADDRESS`                                  | —           | OpenBao API address, e.g. `https://bao:8200`; enables the integration.                                                      |
| `--bao-token` / `--bao-token-file`           | `TEMBO_BAO_TOKEN` / `TEMBO_BAO_TOKEN_FILE`           | —           | Token authentication.                                                                                                       |
| `--bao-role-id-file`, `--bao-secret-id-file` | `TEMBO_BAO_ROLE_ID_FILE`, `TEMBO_BAO_SECRET_ID_FILE` | —           | AppRole login instead of a token; the secret id must allow more than one use (the agent re-logs in when the token expires). |
| `--bao-ca-file`                              | `TEMBO_BAO_CA_FILE`                                  | system pool | CA bundle to verify the OpenBao server with.                                                                                |
| `--bao-kv-mount`                             | `TEMBO_BAO_KV_MOUNT`                                 | `secret`    | The KV v2 mount.                                                                                                            |
| `--bao-password-path`                        | `TEMBO_BAO_PASSWORD_PATH`                            | —           | KV v2 path of the Postgres password secret, e.g. `tembo/pg`; the secret's `password` key is the cluster password.           |
| `--bao-ldap-password-path`                   | `TEMBO_BAO_LDAP_PASSWORD_PATH`                       | —           | KV v2 path of the LDAP bind password secret (`password` key), read at startup; replaces `--auth-ldap-bind-password-file`.   |

With `--bao-password-path`, every node reads the secret at startup and polls
it every ~5 seconds afterwards. A changed `password` value has the effect of
a cluster-wide rotation: the agents switch within seconds, the primary
alters the `postgres` role (and `tembo_repl` where `--pg-host-auth` is
password-based — under `ldap`/`gss` that role has no password), and the
standbys receive them by WAL replay — [the rotation flow](#passwords)
without the CLI. Consequences:

- `PUT /password` and `rotate-password` answer 403: change the secret, not
  the cluster. The store's `password` key is ignored, so the value lives in
  OpenBao and in Postgres — not in every node's bbolt file.
- Moving a running cluster under OpenBao converges it in place: the primary
  alters the roles to whatever the secret says (with `--pg-local-auth
  password` the secret must hold the current password, since the agent can
  no longer connect otherwise).
- OpenBao must be reachable at startup; later outages are logged and the
  node keeps its last known password. The outcome of every read is visible
  in `tembo_bao_password_success` and
  `tembo_bao_password_last_success_seconds`
  ([metrics.md](metrics.md#agent-and-raft-metrics)).
- With `--bao-ldap-password-path`, the LDAP bind password is read from the
  secret at startup — a change applies at the next restart of the agent.

### Listeners and storage

| Flag                        | Env                             | Default           | Description                                                                                                      |
|-----------------------------|---------------------------------|-------------------|------------------------------------------------------------------------------------------------------------------|
| `--listen`                  | `TEMBO_LISTEN`                  | `:5432`           | Client proxy: forwarded to the primary.                                                                          |
| `--listen-ro`               | `TEMBO_LISTEN_RO`               | `:5434`           | Reader proxy: spread over the standbys.                                                                          |
| `--db`                      | `TEMBO_DB`                      | `<id>.db`         | The bbolt file of this node.                                                                                     |
| `--raft-retention-log-size` | `TEMBO_RAFT_RETENTION_LOG_SIZE` | `8388608` (8 MiB) | Bytes of applied raft log to keep; older entries are compacted into the store snapshot. `0` keeps the whole log. |

The API port is not a flag: the agent listens on the port of this node's own
`--peers` address (8001 in the examples throughout this documentation).

### Postgres

| Flag                 | Env                      | Default | Description                                                                                                                                                                                      |
|----------------------|--------------------------|---------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `--pg-bin`           | `TEMBO_PG_BIN`           | `$PATH` | Directory with `initdb`, `postgres`, etc.                                                                                                                                                        |
| `--pg-data`          | `TEMBO_PG_DATA`          | —       | Postgres data directory.                                                                                                                                                                         |
| `--pg-port`          | `TEMBO_PG_PORT`          | `5433`  | Postgres port, the same on every node.                                                                                                                                                           |
| `--pg-password`      | `TEMBO_PG_PASSWORD`      | —       | The Postgres superuser password, the same on every node. It never touches disk as a file: `initdb` alone gets a temporary one. Wins over `--pg-password-file`.                                   |
| `--pg-password-file` | `TEMBO_PG_PASSWORD_FILE` | —       | File whose first line is the superuser password — the file variant of `--pg-password`; one of the two is required.                                                                               |
| `--initdb-args`      | `TEMBO_INITDB_ARGS`      | —       | Extra arguments for `initdb` of a new cluster, e.g. `"--data-checksums"`.                                                                                                                        |
| `--clone-command`    | `TEMBO_CLONE_COMMAND`    | —       | Shell command that clones the primary instead of `pg_basebackup`. Gets `TEMBO_PGDATA`, `TEMBO_PRIMARY_HOST`, `TEMBO_PRIMARY_PORT`, `PGPASSWORD` — the entry point for wal-g/pgBackRest restores. |

### Postgres authentication

| Flag                  | Env                       | Default                            | Description                                                                                                                  |
|-----------------------|---------------------------|------------------------------------|------------------------------------------------------------------------------------------------------------------------------|
| `--pg-local-auth`     | `TEMBO_PG_LOCAL_AUTH`     | `peer`                             | How the agent connects to the local Postgres: `peer` (Unix socket), `password` (TCP to 127.0.0.1) or `gss` (TCP + Kerberos). |
| `--pg-host-auth`      | `TEMBO_PG_HOST_AUTH`      | `scram-sha-256`                    | Authentication method of the nodes' `pg_hba.conf` lines: `scram-sha-256`, `ldap` or `gss`.                                   |
| `--pg-host-auth-opts` | `TEMBO_PG_HOST_AUTH_OPTS` | —                                  | Verbatim options of the method, e.g. `"ldapbasedn=dc=example,dc=com ldapbinddn=..."`. Required with `ldap`.                  |
| `--pg-krb5-keytab`    | `TEMBO_PG_KRB5_KEYTAB`    | credential cache                   | Keytab the agent's Kerberos tickets come from.                                                                               |
| `--pg-krb5-config`    | `TEMBO_PG_KRB5_CONFIG`    | `$KRB5_CONFIG` or `/etc/krb5.conf` | `krb5.conf` for the agent and the Postgres processes.                                                                        |
| `--pg-krb5-principal` | `TEMBO_PG_KRB5_PRINCIPAL` | `postgres`                         | The agent's Kerberos principal.                                                                                              |

`--pg-local-auth password` requires a password-based `--pg-host-auth`;
`--pg-local-auth gss` requires `--pg-host-auth gss`. Details in
[postgres-management.md](postgres-management.md#authentication-to-postgres).

### Node tags

| Flag                  | Env                       | Default | Description                                                                           |
|-----------------------|---------------------------|---------|---------------------------------------------------------------------------------------|
| `--nofailover`        | `TEMBO_NOFAILOVER`        | off     | Never make this node the primary; excluded from failover and switchover targets.      |
| `--nosync`            | `TEMBO_NOSYNC`            | off     | Never commit synchronously with this node; excluded from `synchronous_standby_names`. |
| `--failover-priority` | `TEMBO_FAILOVER_PRIORITY` | `0`     | Higher is preferred when picking a failover candidate.                                |

### API TLS and authentication

| Flag                                                                                                 | Env                                                | Description                                                                                                                |
|------------------------------------------------------------------------------------------------------|----------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------|
| `--tls-cert-file`, `--tls-key-file`                                                                  | `TEMBO_TLS_CERT_FILE`, `TEMBO_TLS_KEY_FILE`        | Serve the API over HTTPS. The pair goes together.                                                                          |
| `--tls-ca-file`                                                                                      | `TEMBO_TLS_CA_FILE`                                | CA bundle (PEM): the nodes verify each other, and clients may authenticate with a certificate. Requires the cert/key pair. |
| `--auth-jwt-key-file`                                                                                | `TEMBO_AUTH_JWT_KEY_FILE`                          | JWT verification key: PEM public key (RS*/ES*) or an HMAC secret (HS*).                                                    |
| `--auth-jwt-jwks`                                                                                    | `TEMBO_AUTH_JWT_JWKS`                              | JWKS URL to verify JWTs with (refetched when a token names an unknown `kid`).                                              |
| `--auth-jwt-issuer`, `--auth-jwt-audience`                                                           | `TEMBO_AUTH_JWT_ISSUER`, `TEMBO_AUTH_JWT_AUDIENCE` | What the token must name.                                                                                                  |
| `--auth-krb5-keytab`                                                                                 | `TEMBO_AUTH_KRB5_KEYTAB`                           | Keytab of the API's Kerberos principal; enables SPNEGO.                                                                    |
| `--shell-krb5-spn`   | `TEMBO_SHELL_KRB5_SPN`   | —  | Postgres service the shell impersonates Kerberos users to, e.g. `postgres/db.example.com`. Needs `--auth-krb5-keytab`. |
| `--shell-krb5-principal` | `TEMBO_SHELL_KRB5_PRINCIPAL` | — | The agent's own principal `user@REALM` for S4U, whose key is in `--auth-krb5-keytab`. |
| `--auth-ldap-url`                                                                                    | `TEMBO_AUTH_LDAP_URL`                              | LDAP server (`ldap://`/`ldaps://`) to authenticate Basic auth against.                                                     |
| `--auth-ldap-dn-template`                                                                            | `TEMBO_AUTH_LDAP_DN_TEMPLATE`                      | Direct bind: DN template with `%s` for the username.                                                                       |
| `--auth-ldap-bind-dn`, `--auth-ldap-bind-password-file`, `--auth-ldap-base-dn`, `--auth-ldap-filter` | `TEMBO_AUTH_LDAP_*`                                | Search-bind alternative: bind with this account, search `base-dn` with the filter (`%s` = username).                       |
| `--auth-read-only`                                                                                   | `TEMBO_AUTH_READ_ONLY`                             | Comma-separated identities limited to read-only API access, e.g. `jwt:alice`.                                              |

Any of these enabled turns authentication on for everyone outside the
cluster's own addresses. See [security.md](security.md).

### Configuration sources

| Flag                 | Env                    | Description                                                                                                                                                                                                                                                              |
|----------------------|------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `--config`           | `TEMBO_CONFIG`         | A `.env` or `.json` file loaded before the other sources.                                                                                                                                                                                                                |
| `--k8s` (repeatable) | `TEMBO_K8S`            | Kubernetes URIs: `cm://[ns/]name` or `secret://[ns/]name[/key]`. Their `config.json` and `settings.json` keys become the cluster configuration and the agent settings; PATCH `/config` and `/settings` are disabled. See [operations.md](operations.md#kubernetes-mode). |
| `--consul`           | `TEMBO_CONSUL`         | Consul HTTP API address; enables discovery instead of `--peers`.                                                                                                                                                                                                         |
| `--consul-service`   | `TEMBO_CONSUL_SERVICE` | Service name for registration and discovery (default `tembo`).                                                                                                                                                                                                           |
| `--consul-tags`      | `TEMBO_CONSUL_TAGS`    | Comma-separated tags: nodes sharing the service and all the tags form one cluster.                                                                                                                                                                                       |
| `--consul-expect`    | `TEMBO_CONSUL_EXPECT`  | Wait for at least this many discovered nodes before bootstrapping.                                                                                                                                                                                                       |

`--peers` and `--consul` exclude each other. Consul details in
[operations.md](operations.md#consul-discovery).

## Agent behavior settings

Replicated under the `settings` key, shown and changed with
`settings show|set|reset` (that is, `GET`/`PATCH /settings`). A `null` in a
PATCH resets a setting to its default.

| Setting                  | Default          | Meaning                                                                                                                                                                                   |
|--------------------------|------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `max-lag-on-failover`    | `1048576` (1 MB) | The most WAL a standby may be missing and still be promoted. `0` lifts the limit. Accepts sizes with `kB/MB/GB/TB` suffixes (1024-based, as in `postgresql.conf`).                        |
| `synchronous_mode`       | `off`            | `off`, `quorum` (`ANY n (...)`) or `strict` (`FIRST n (...)`) — see [high-availability.md](high-availability.md#synchronous-replication).                                                 |
| `synchronous_node_count` | `1`              | How many standbys a commit waits for.                                                                                                                                                     |
| `paused`                 | `false`          | Suspend automatic failover for maintenance. Explicit switchover and manual failover still work.                                                                                           |
| `hba`                    | empty            | Extra `pg_hba.conf` lines, written verbatim after the agent-managed ones on every node. Managed with `settings set-hba`; see [postgres-management.md](postgres-management.md#pg_hbaconf). |

## The cluster's `postgresql.conf`

Replicated under the `config` key; changed with `config set|reset`
(`PATCH /config`). Every node's `postgresql.conf` is rewritten wholesale by
the agent from, in rising precedence:

1. a fixed base (`listen_addresses = *`, `max_connections = 100`,
   `max_locks_per_transaction = 64`, `max_prepared_transactions = 0`,
   `max_wal_senders = 10`, `shared_buffers = 128MB`, `max_wal_size = 1GB`,
   `min_wal_size = 80MB`, locale/datestyle defaults, `log_hostname = on`);
2. tembo's replication defaults: `archive_timeout = 5min`,
   `dynamic_shared_memory_type = posix`, `full_page_writes = on`,
   `hot_standby_feedback = on`, `max_parallel_workers = 32`,
   `max_worker_processes = 32`, `max_replication_slots = 32`,
   `shared_memory_type = mmap`, `sync_replication_slots = on` (PG17 logical
   slot failover), `wal_keep_size = 512MB`, `wal_level = logical`,
   `wal_log_hints = on`, `wal_sender_timeout = 5s`,
   `wal_receiver_timeout = 5s`;
3. the cluster configuration (the `config` key);
4. the agent's own parameters: `port`, the logging collector settings, and on
   a standby `primary_conninfo` and `primary_slot_name`, or on a primary
   `synchronous_standby_names`.

The agent's own parameters and a blocklist of dangerous or agent-managed
names cannot be set through the cluster configuration — `PATCH /config`
refuses them (see [security.md](security.md#blocked-parameters)). Proposed
values are validated against a running Postgres (`set_config` runs the parse
and check hooks) before the change commits; parameters that only apply at
restart pass validation and take effect at the node's restart during rollout.

### Hot-standby limits

`max_connections`, `max_locks_per_transaction`,
`max_prepared_transactions`, `max_wal_senders` and `max_worker_processes`
must be at least as high on a standby as on its primary, or recovery pauses.
The rollout handles this: a configuration that lowers a limit applies to the
primary first, and a standby applies such a change only after it has replayed
the primary's WAL to the last reported position. A stopped standby takes a
limit-raising configuration at its next start.
