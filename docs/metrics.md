# Metrics

`GET /metrics` on the API port serves the Prometheus text format from a
private registry: the agent and raft metrics (`tembo_*` prefix) plus a
postgres_exporter-compatible collector set (`pg_*` prefix) for the local
Postgres. A scrape takes one Postgres connection and is bounded by a
1-minute timeout.

## Agent and raft metrics

The `tembo_raft_*` series exist only with a raft cluster; a standalone node
(no `--peers`, no `--consul`) reports the `tembo_agent_*` ones alone.

| Metric                                    | Type  | Labels                              | Meaning                                                                                                                                                                                                                                                                   |
|-------------------------------------------|-------|-------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `tembo_raft_state`                        | gauge | `state` = follower/candidate/leader | 1 for the role this node plays now, 0 for the others.                                                                                                                                                                                                                     |
| `tembo_raft_term`                         | gauge |                                     | Current raft term.                                                                                                                                                                                                                                                        |
| `tembo_raft_leader`                       | gauge | `id`                                | Present (1) with the believed leader's id; absent while unknown.                                                                                                                                                                                                          |
| `tembo_raft_commit_index`                 | gauge |                                     | The raft commit index.                                                                                                                                                                                                                                                    |
| `tembo_raft_applied_index`                | gauge |                                     | The last raft index applied to the local store.                                                                                                                                                                                                                           |
| `tembo_raft_last_log_index`               | gauge |                                     | The last index in the local raft log.                                                                                                                                                                                                                                     |
| `tembo_raft_members`                      | gauge | `set` = voters/joint                | Size of the committed membership; the `joint` series exists only while a change is settling.                                                                                                                                                                              |
| `tembo_agent_quorum`                      | gauge |                                     | 1 while this node recently (≤5 s) knew a leader and reached a majority.                                                                                                                                                                                                   |
| `tembo_agent_nodes_running`               | gauge |                                     | Nodes whose Postgres answered the last poll, this one included.                                                                                                                                                                                                           |
| `tembo_bao_password_success`              | gauge |                                     | 1 while the last read of a managed password secret succeeded — with the OpenBao integration ([configuration.md](configuration.md#openbao)) only; the metrics are absent otherwise. 0 means OpenBao is unreachable or the secret is broken; the agent log names the error. |
| `tembo_bao_password_last_success_seconds` | gauge |                                     | Unix time of the last successful read of a managed password secret; 0 is never. Catches a secret that has quietly stopped being polled.                                                                                                                                   |

Useful starting points:

- a split brain or a fenced node shows as `tembo_agent_quorum 0`;
- replication health across the cluster: compare `tembo_raft_applied_index`
  between nodes (a lagging follower) and watch `tembo_agent_nodes_running`
  on the leader;
- election churn: `tembo_raft_term` climbing with `tembo_raft_state` flapping;
- with OpenBao: `tembo_bao_password_success 0` is an outage of OpenBao or a
  broken secret, `tembo_bao_password_last_success_seconds` going stale means
  the polls stopped.

## Postgres metrics

The `pg_*` collectors mirror
[postgres_exporter](https://github.com/prometheus-community/postgres_exporter)
(Apache 2.0): the same queries, metric names, labels and conversions, run
over pgx and scoped to the local server. Collectors:

```
buffercache_summary        database            database_wraparound
locks                      long_running_transactions
postmaster                 process_idle        replication
replication_slots          roles               settings
stat_activity              stat_activity_autovacuum
stat_archiver              stat_bgwriter       stat_checkpointer
stat_database              stat_progress_vacuum
stat_replication           stat_statements     stat_user_tables
stat_wal_receiver          statio_user_indexes statio_user_tables
wal
```

So the familiar names are all there: `pg_postmaster_start_time_seconds`,
`pg_replication_lag_seconds`, `pg_replication_is_replica`,
`pg_stat_replication_pg_wal_lsn_diff`, `pg_replication_slots_*`,
`pg_database_size_bytes`, `pg_locks_count`, `pg_settings_*`,
`pg_stat_database_*`, `pg_stat_user_tables_*`, `pg_stat_statements_*`
(needs the extension), `pg_stat_wal_receiver_*`, `pg_wal_size_bytes`,
`pg_wal_segments`, `pg_stat_bgwriter_*`, `pg_stat_checkpointer_*`,
wraparound freezes (`pg_database_wraparound_*`), and so on.

Each collector also reports:

- `pg_scrape_collector_duration_seconds{collector=...}`
- `pg_scrape_collector_success{collector=...}` — 0 when a collector failed (e.g. `stat_statements` without the
  extension, `buffercache_summary` on PG16+ without `pg_buffercache`); a failed collector never fails the scrape.

Pre-PG13 query variants are not included; tembo does not support those
servers. Extension-dependent collectors fail soft with
`pg_scrape_collector_success 0` — except `buffercache_summary` on PG13–15,
where `pg_buffercache_summary()` does not exist: there it reports success 1
and simply yields no metrics.

## Scraping

Scrape every node's API port — each reports its own raft state and its own
Postgres. A node whose Postgres is down still serves `/metrics` (the `pg_*`
part yields nothing but a fresh connection failure; the `tembo_*` part keeps
working). `/metrics` goes through the same authentication as the rest of the
API: when it is configured, a scraper outside the cluster's own addresses
needs credentials — only the nodes are exempt ([security.md](security.md#api-authentication)).
