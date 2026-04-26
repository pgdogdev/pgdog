# PgDog Result Cache (Redis)

PgDog supports an **experimental** Redis/RESP-backed **query result cache** inspired by pgpool-II's memqcache, but designed for Redis-compatible servers such as **Redis**, **Valkey**, and **Dragonfly**.

## What it does (MVP)

- Caches **SELECT results** and returns the cached Postgres-protocol response on cache hit.
- Stores a binary payload containing the response messages (e.g. `RowDescription` + `DataRow*` + `CommandComplete` + `ReadyForQuery`).

## Conservative invalidation (MVP)

To avoid stale results while still being precise, PgDog uses **table-based invalidation**:

- On `DML/DDL` (outside an explicit transaction), PgDog extracts touched tables from the parsed AST.
- Each cached entry is tagged into Redis sets keyed by table name.
- When a table changes, PgDog deletes only the keys referenced by that table tag set.

## Configuration

Add this section to `pgdog.toml`:

```toml
[result_cache]
enabled = true
redis_url = "redis://127.0.0.1:6379"
expire_seconds = 30
max_entry_bytes = 524288
key_prefix = "pgdog:result_cache"

# Optional allow/deny lists (regex). Unsafe lists take precedence.
cache_safe_schema_list = []
cache_unsafe_schema_list = []
cache_safe_table_list = []
cache_unsafe_table_list = []
```

## Current limitations

- Only caches **simple query protocol** (`Query` message) requests.
- Skips caching inside explicit transactions.
- Session signature is conservative and may lead to fewer cache hits (by design).

