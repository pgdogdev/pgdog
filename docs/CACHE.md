# Redis Cache for pgdog — State of Implementation

## Architecture

Cache SELECT queries in Redis, bypass PostgreSQL on cache hit, populate cache on cache miss. Three-tier policy resolution: SQL comment → per-database config → auto-decision engine.

---

## Initial Implementation (Before Debugging Session)

### Files Added

#### 1. `pgdog/src/frontend/client/query_engine/cache/` (new module)

**`mod.rs`** — Module exports:
```rust
pub mod client;
pub mod integration;
pub mod policy;
pub mod stats;

pub use client::CacheClient;
pub use integration::{cache_check, cache_response, send_cached_response, CacheCheckResult};
pub use policy::{
    CacheDecision, CachePolicyDispatcher, CachePolicyExtractor, CachePolicyResolver,
    CommentCacheExtractor, ParameterCacheExtractor,
};
pub use stats::QueryStatsTracker;
```

**`client.rs`** — Redis client wrapper using `fred` v9:
- `CacheClient::new(config)` — builds client from `Option<&DatabaseCache>`, returns disabled stub if no config/URL
- `ensure_connected()` — lazy one-time `client.init().await` followed by `client.ping()` verification; sets `redis_connected` flag
- `get(&self, key)` — returns `Result<Option<Vec<u8>>>`; fetches cached wire-protocol bytes
- `set(&self, key, value, ttl)` — stores bytes with EX expiration; respects `max_result_size`
- `spawn_reconnect()` — background task that retries `init()` every 500ms, verifies with `ping()`, sets `redis_connected = true`
- `mark_disconnected()` — sets `redis_connected = false`, spawns reconnect if not already running (CAS-guarded)
- `is_connected()` — reads our atomic flag (not fred's potentially stale `ClientState`)
- Keys are prefixed with `"pgdog:"`
- Error types: `RedisError(String)`, `ConnectionFailed(String)`
- `redis_connected: Arc<AtomicBool>` — authoritative connection gate, only true after PING succeeds
- `reconnecting: Arc<AtomicBool>` — prevents multiple concurrent reconnect tasks
- All Redis operations wrapped in `tokio::time::timeout(REDIS_OPERATION_TIMEOUT)` (2s) as safety net

**`policy.rs`** — 3-tier policy resolution with trait-based extraction:
- `CacheDirective` enum: `None`, `Cache { ttl_seconds }`, `NoCache` (moved here from `route.rs`)
- `trait CachePolicyExtractor`: abstract interface with `fn extract(query, params) -> CacheDirective`
- `struct CommentCacheExtractor`: scans SQL query string with standalone regex — **works even when parser is bypassed**
- `struct ParameterCacheExtractor`: reads `pgdog.cache` connection startup parameter
- `struct CachePolicyDispatcher`: chains extractors in priority order, returns first non-`None` result
- Tier 1: Extractor result (`CacheDirective::Cache { ttl }` or `CacheDirective::NoCache` from comments/params)
- Tier 2: Database config `CachePolicy` (`NoCache` / `Cache` / `Auto`)
- Tier 3: `auto_decision()` — caches when `hit_count > miss_count` AND `avg_result_size < 1MB`

**`stats.rs`** — Per-fingerprint query statistics tracker:
- `QueryStatsTracker` with `record_hit(fingerprint, size)` / `record_miss(fingerprint)` / `get(fingerprint)`
- Internally: `Arc<Mutex<HashMap<String, QueryStats>>>` using `parking_lot`

**`integration.rs`** — Integration logic (as currently exists after debugging fixes):
- `cache_check()` — main entry point, creates `CachePolicyDispatcher` with `CommentCacheExtractor` + `ParameterCacheExtractor`, calls `dispatcher.extract(query, params)` to get `CacheDirective`, then runs `CachePolicyResolver::resolve()`
- `is_likely_read()` — fallback heuristic for when parser is disabled: checks SQL starts with SELECT/SHOW/EXPLAIN/WITH
- `send_cached_response()` — deserializes wire-format bytes and sends to client
- `cache_response()` — serializes `Vec<Message>` into wire bytes and stores in Redis
- `get_db_cache_config()` — looks up `DatabaseCache` from global config by database name
- `compute_cache_key()` — DefaultHasher-based hash of `{database, query}`; database name is hashed first to namespace keys and prevent collisions when multiple databases share one Redis

### Files Modified

#### 2. `pgdog-config/src/database.rs`

Added before the `Database` struct:

- `CachePolicy` enum: `NoCache` (default), `Cache`, `Auto`
  - Implements `FromStr`, `Display`, `Serialize`, `Deserialize`, `Copy`
- `DatabaseCache` struct:
  - `enabled: Option<bool>` — is caching on?
  - `policy: Option<CachePolicy>` — which policy?
  - `ttl: Option<u64>` — default TTL seconds (default 300)
  - `redis_url: Option<String>` — Redis connection URL
  - `max_result_size: Option<usize>` — max cached result bytes
  - Helper methods: `is_enabled()`, `policy()`, `ttl()`, `max_result_size()`
- Added `cache: Option<DatabaseCache>` field to `Database` struct

#### 3. `pgdog-config/src/lib.rs`

Added `CachePolicy` and `DatabaseCache` to the public `pub use database::` export.

#### 4. `pgdog/src/frontend/router/parser/route.rs`

`CacheDirective` enum was **moved to** `cache/policy.rs` — `route.rs` now re-exports it via `pub use crate::frontend::client::query_engine::cache::policy::CacheDirective`. Route still has `cache_directive` field and methods available for manual override, but the type is imported from the cache module.

#### 5. `pgdog/src/frontend/router/parser/comment.rs`

All cache-related regex and parsing was **removed** from this file. Cache extraction is now independent and lives in `cache/policy.rs` with its own standalone regex. The `comment()` function returns a 2-tuple `(Option<Shard>, Option<Role>)` again.

#### 6. `pgdog/src/frontend/router/parser/cache/ast.rs`

`comment_cache_directive` field was **removed** from `AstInner` struct and `new()` method. Cache parsing is no longer done at the AST level.

#### 7. `pgdog/src/frontend/router/parser/query/mod.rs`

All cache directive handling was **removed**: `cache_directive` field removed from `QueryParser` struct, cache directive propagation removed. Cache policy extraction now happens independently in `integration.rs`.

#### 8. `pgdog/src/frontend/router/parser/mod.rs`

- Updated export: `pub use route::{CacheDirective, Route, Shard, ShardWithPriority, ShardsWithPriority};`

#### 9. `pgdog/Cargo.toml`

- Added `fred = { version = "9", features = ["enable-rustls"] }` to dependencies

#### 10. `pgdog/src/frontend/client/query_engine/mod.rs`

- Added `pub mod cache;` module declaration
- Added `cache_client: CacheClient`, `cache_stats: QueryStatsTracker`, `database: String`, `cache_miss: Option<(String, Option<u64>)>`, `cache_response_buffer: Vec<Message>` fields to `QueryEngine`
- `new()` looks up cache config from global config by database name and creates `CacheClient`
- `handle()` flow: after `route_query()` and before `before_execution()`, calls `cache_check()`. On HIT: sends cached response and returns. On MISS: stores `(cache_key, ttl)` and starts capture. On Passthrough: clears miss state.
- After `match command`, calls `self.finalize_cache().await` to store the captured response in Redis.
- Added helper methods: `start_cache_capture()`, `capture_response()`, `is_caching()`, `finalize_cache()`

#### 11. `pgdog/src/frontend/client/query_engine/query.rs`

- `process_server_message()` added cache capture at the top: if `self.is_caching()`, clones and stores the message via `self.capture_response()`.

---

## Key Design Decisions

| Decision | Choice |
|----------|--------|
| Interception point | Between `parse_and_rewrite()` and `route_query()` in `handle()` |
| Cache config scope | Per-database (`Database.cache` field) |
| Redis client | `fred` crate v9 (async-native, tokio integration) |
| Cacheable queries | Only reads (`Route::is_read()` + `is_likely_read()` fallback) |
| Cache policy resolution | 3-tier: SQL comment → per-database config → auto-decision |
| Cache HIT flow | Deserialize wire bytes → parse messages → send to client → `return Ok(())` |
| Cache MISS flow | Normal execute → capture response bytes → store in Redis → respond |
| Auto-decision engine | `hit_count > miss_count` AND `avg_result_size < 1MB` |
| Cache key | `DefaultHasher` of `{database}:{query}` — database name is hashed first to namespace keys, preventing collisions when multiple databases share one Redis |
| Wire format | Full PostgreSQL wire messages stored as raw bytes (one concatenated buffer) |

---

## Bugs Found & Fixed

1. **Redis client never connects** - Problem: CacheClient::new() built the client but never called init(). Fred requires explicit connection initialization. Fix: Added lazy `ensure_connected()` using `client.init().await`, guarded by `AtomicBool` so it runs exactly once on first get()/set(). Changed CacheClient from `#[derive(Debug)]` to manual Debug impl (contains `Arc<AtomicBool>`).

2. **Redis GET fails on NULL / cache miss** - Problem: `client.get::<bytes::Bytes>()` throws `Parse Error: Cannot parse into bytes` when the key doesn't exist. Fix: Use `client.get::<RedisValue, _>()` and check `val.is_null()` before extracting bytes.

3. **Wire format deserialization wrong in send_cached_response** - Problem: PostgreSQL wire message structure is `[1B code][4B length]` where length includes the 4B itself. I calculated `offset + 5 + msg_len` (treating length as payload-only), causing incorrect byte slicing. Fix: Corrected to `offset + 1 + msg_len`.

4. **Route incorrectly reports read-only as write when parser is disabled** - Problem: `query_parser_bypass()` conservatively returns `Route::write()` for all SQL when the query parser is disabled. Since pgdog doesn't enable the parser by default for simple queries, `route.is_read()` was false for `SELECT 1`. Fix: Added `is_likely_read()` heuristic function in `cache_check` that checks uppercase SQL prefix (SELECT/SHOW/EXPLAIN/WITH) as a fallback when parser is disabled.

5. **DB cache config defaults** - Observation: `DatabaseCache.policy` defaults to `CachePolicy::NoCache`. Even with `enabled = true`, caching is skipped unless policy is explicitly set. User action taken: Added `policy = "cache"` to pgdog.toml.

---

## Refactoring: Decoupled Cache Policy Extraction

The original implementation entangled cache directive parsing with pgdog's general comment parser (`comment.rs`), which only activates when the full query parser runs. This meant `/* pgdog_cache: ... */` annotations were silently ignored for simple queries and when `query_parser_bypass()` triggered.

**What was done:**

- `CacheDirective` enum moved from `route.rs` to `cache/policy.rs`
- Cache parsing **removed** from `comment.rs`, `ast.rs`, `query/mod.rs` — they no longer handle `CacheDirective`
- `route.rs` now re-exports `CacheDirective` from the cache module
- New **trait-based extraction system** in `cache/policy.rs`:
  - `CachePolicyExtractor` trait with `fn extract(query, params) -> CacheDirective`
  - `CommentCacheExtractor`: standalone regex scan on raw query string — works independent of AST parser
  - `ParameterCacheExtractor`: reads `pgdog.cache` connection startup parameter
  - `CachePolicyDispatcher`: chains extractors, returns first non-`None` result
- `integration.rs` now creates the dispatcher inline in `cache_check()` and passes `context.params` for parameter extraction

This ensures cache annotations work regardless of whether the query parser is enabled or bypassed.

## How to Control Cache

### SQL Comments

Add a C-style comment before your query. The first matching directive wins:

```sql
-- Force bypass cache for this query
/* pgdog_cache: no-cache */
SELECT * FROM users WHERE id = 1;

-- Cache with database default TTL
/* pgdog_cache: cache */
SELECT * FROM products WHERE category = 'electronics';

-- Cache with custom TTL in seconds
/* pgdog_cache: cache ttl=300 */
SELECT * FROM orders;
```

### Connection Parameter

Set `pgdog.cache` at connection time (via DSN options) or with `SET` after connecting:

```sql
-- Session-wide: all queries in this connection bypass cache
SET pgdog.cache = 'no-cache';

-- Session-wide: cache all queries with default TTL
SET pgdog.cache = 'cache';

-- Session-wide: cache all queries with 5-minute TTL
SET pgdog.cache = 'cache ttl=300';
```

### Priority Order

Extractors are checked in order — first non-`None` result wins, then falls through to database config:

```
SQL comment  →  pgdog.cache parameter  →  DB policy config  →  Auto-decision
(highest)                                                            (lowest)
```

---

# What's Left To Do

1. **Redo is_likely_read** — **DONE.** Instead of heuristic-based detection, caching now requires the query parser. If `query_parser = "auto"` and any database has `cache.enabled = true`, it's auto-upgraded to `"on"` globally. If `query_parser = "off"` or `"session_control"` and cache is enabled, a startup warning is emitted and caching won't work for that database. `ClusterConfig::new()` also forces `On` per-cluster if cache is enabled and global parser is `Off`/`SessionControl`/`Auto`. This means `route.is_read()` from the AST parser is always accurate — it correctly detects CTE writes (`WITH ... INSERT`), `FOR UPDATE/SHARE`, and volatile functions (`nextval()`, `pg_advisory_lock()`). The old `is_likely_read()` string-prefix heuristic has been removed entirely.

2. **pgdog_cache: comment annotation** — **DONE.** Cache directive extraction now uses its own standalone regex in `cache/policy.rs`, working independently of the AST parser. It scans the raw query string, so it functions correctly even when `query_parser_bypass()` is triggered. The `/* pgdog_cache: ... */` comment format is supported with optional `ttl=` parameter.

3. **Auto policy** — Implemented but untested. Relies on stats tracker to decide based on hit/miss ratio and avg result size after enough observations.

4. **Multi-step execution caching** — InsertSplit and ShardingKeyUpdate rewrite paths use process_server_message() which captures responses, but the finalize_cache() call happens after match command block. Need to verify caching works correctly for multi-step rewrites.

5. **Response capture for prepared statements** — Extended protocol (Parse/Bind/Execute) response capture works through process_server_message() but hasn't been tested with PREPARE/EXECUTE. (Note: Actually, pgdog implements prepared statements caching. But i don't know what kind of caching is this: just query cache or result cache. And if we'll implement our cache, will this break this prepared statement cache?)

6. **Error handling / Reconnection** — DONE. Automatic reconnection with background task, CAS-guarded single reconnect, 2s operation timeout on all Redis calls, PING-based connection verification.

7. **max_result_size config** — Implemented but not exposed in the initial pgdog.toml. Worth documenting in the config.

8. **Cache key collision across databases sharing one Redis** — Problem: `compute_cache_key()` only hashed the raw query string. When two databases point to the same Redis and both run `SELECT * FROM users WHERE id = 1`, they produce identical keys and can serve wrong data on cache hits. Fix: Changed `compute_cache_key(query: &str)` to `compute_cache_key(query: &str, database: &str)` — database name is now hashed first, then the query, guaranteeing unique keys per database even on a shared Redis instance (`integration.rs:99`).

9. **Redis disconnect/reconnect blocks all queries** — Problem: When Redis becomes unavailable after initial connection, `client.get()`/`client.set()` block for the full timeout duration (2s) because fred's `default_command_timeout` is `Duration::from_millis(0)` (no timeout). After the first request fails, subsequent requests still hit the timeout. After Redis restarts, caching never recovers. Root cause analysis: (a) fred's `ClientState` can report `Connected` even when TCP isn't ready, so relying on `client.state()` for the fast-path check leads to unnecessary blocking. (b) `force_reconnection()` hangs indefinitely when Redis is down — fred's router task can't respond without a connection, so the reconnect loop deadlocks. (c) Even after Redis restarts, if the initial `init()` failed, fred's routing tasks never started, so `ping()` and all operations fail silently. Fix: (1) Replaced `connect_initiated` + state-check logic with a single `redis_connected: AtomicBool` — the authoritative gate for all Redis operations. Returns error immediately if false, no Redis call attempted. (2) `ensure_connected()` calls `init()` (only one-shot on fresh start), then verifies with `ping()`. Sets `redis_connected = true` only after PING succeeds. (3) `mark_disconnected()` sets `redis_connected = false` and spawns exactly one background reconnect task (CAS-guarded via `reconnecting: AtomicBool`). (4) Reconnect task retries `client.init()` every 500ms (fred allows re-init after disconnect). On success, verifies with PING, then sets `redis_connected = true`. (5) All Redis calls (init, get, set, ping) wrapped in `tokio::time::timeout(2s)` as safety net.

---

## Testing

### Framework

System tests live in `integration/rust/tests/integration/` alongside the existing integration suite. They use:
- `sqlx` and `tokio-postgres` for PG queries through pgdog on port 6432
- `#[tokio::test]` + `#[serial]` from `serial_test` for test isolation
- `reqwest` to read metrics from `http://127.0.0.1:9090/metrics`

Run with: `cd integration/rust && cargo nextest run --no-fail-fast --test-threads=1`

### External Dependencies

Redis must be running locally on port 6379 before cache tests execute. Unlike Postgres, Redis is **not** currently provisioned by `integration/setup.sh` or any CI workflow. To add cache tests to CI:
- **GitHub Actions:** add `sudo apt-get install -y redis-server && sudo service redis-server start` in `.github/workflows/ci.yml`
- **RWX:** add a `*redis-bg-process` alias in `.rwx/integration.yml` (same pattern as `*postgres-bg-process`)
- **Local/dev:** start Redis manually (expected on `127.0.0.1:6379`)

### Planned Tests

1. **Database key namespace collision** — Two databases (`db_a`, `db_b`) sharing one Redis, both running `SELECT 1 AS val` but with different underlying PG data. Verify each database gets its own correct data and no cross-database cache hit occurs.
2. **Basic cache hit/miss** — Run a SELECT once (expect miss), run again (expect hit), verify metrics.
3. **TTL expiration** — Cache a query with short TTL, wait for expiry, verify miss on third call.
4. **Write bypasses cache** — Execute INSERT/UPDATE/DELETE, verify these operations do not populate or consume the cache.
5. **Redis unavailable** — Stop Redis mid-flight, verify queries pass through to PG without blocking or crashing.
6. **Redis reconnection** — Restart Redis after disconnect, verify cache recovers automatically.
