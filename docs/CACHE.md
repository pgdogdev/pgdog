# Redis Cache for pgdog — State of Implementation

## Architecture

Cache SELECT queries in Redis, bypass PostgreSQL on cache hit, populate cache on cache miss. Three-tier policy resolution: SQL comment → per-database config → auto-decision engine.

---

## Implementation

### Files Added

#### 1. `pgdog-config/src/cache.rs` (new file)

**CachePolicy enum:** `NoCache` (default), `Cache`, `Auto`. Implements `FromStr`, `Display`, `Serialize`, `Deserialize`, `Copy`, `JsonSchema`.

**Cache struct:**
- `enabled: Option<bool>` — is caching on?
- `policy: Option<CachePolicy>` — which policy?
- `ttl: Option<u64>` — default TTL seconds (default 300)
- `redis_url: Option<String>` — Redis connection URL
- `max_result_size: Option<usize>` — max cached result bytes
- Helper methods: `is_enabled()`, `policy()`, `ttl()`, `max_result_size()`

#### 2. `pgdog-config/src/general.rs`

Added `cache: Cache` field to `General` struct — **cache config is global**, not per-database.

#### 3. `pgdog-config/src/lib.rs`

Added `pub mod cache;` and `pub use cache::{CachePolicy, Cache};` to public exports.

#### 4. `pgdog/src/frontend/client/query_engine/cache/` (new module)

**`mod.rs`** — Module exports and main `Cache` struct:
```rust
pub mod client;
pub mod context;
pub mod integration;
pub mod policy;
pub mod stats;

pub use client::CacheClient;
pub use integration::CacheCheckResult;
pub use policy::{
    CacheDecision, CachePolicyDispatcher, CachePolicyExtractor, CachePolicyResolver,
    CommentCacheExtractor, ParameterCacheExtractor,
};
pub use stats::QueryStatsTracker;
```

`Cache` struct wraps: `CacheClient`, `QueryStatsTracker`, `CacheConfig`, `database`, `policy_dispatcher`.

Key methods:
- `new(cache_config, database)` — creates client, stats, dispatcher
- `try_read_cache(context)` — calls `cache_check()`, handles HIT/MISS/PASS-through
- `save_response_in_cache(context)` — finalizes by storing the captured response

**`client.rs`** — Redis client wrapper using `fred` v9:
- `CacheClient::new(config)` — builds client from `&CacheConfig`, returns disabled stub if no config/URL
- `ensure_connected()` — lazy one-time `client.init().await` followed by `client.ping()` verification; sets `redis_connected` flag
- `get(&self, key)` — returns `Result<Option<Vec<u8>>>`; fetches cached wire-protocol bytes
- `set(&self, key, value, ttl)` — stores bytes with EX expiration; respects `max_result_size`
- `spawn_reconnect()` — background task that retries `init()` every 500ms, verifies with `ping()`, sets `redis_connected = true`
- `mark_disconnected()` — sets `redis_connected = false`, spawns reconnect if not already running (CAS-guarded)
- `is_connected()` — reads our atomic flag (not fred's potentially stale `ClientState`)
- `is_enabled()` — returns true if both client exists and config enabled
- Keys are prefixed with `"pgdog:"`
- Error types: `RedisError(String)`, `ConnectionFailed(String)`
- `redis_connected: Arc<AtomicBool>` — authoritative connection gate, only true after PING succeeds
- `reconnecting: Arc<AtomicBool>` — prevents multiple concurrent reconnect tasks
- All Redis operations wrapped in `tokio::time::timeout(REDIS_OPERATION_TIMEOUT)` (2s) as safety net

**`policy.rs`** — 3-tier policy resolution with trait-based extraction:
- `CacheDirective` enum: `None`, `Cache { ttl_seconds }`, `NoCache`
- `trait CachePolicyExtractor`: abstract interface with `fn extract(query, params) -> CacheDirective`
- `struct CommentCacheExtractor`: scans SQL query string with standalone regex — **works even when parser is bypassed**
- `struct ParameterCacheExtractor`: reads `pgdog.cache` connection startup parameter
- `struct CachePolicyDispatcher`: chains extractors in priority order, returns first non-`None` result
- Tier 1: Extractor result (`CacheDirective::Cache { ttl }` or `CacheDirective::NoCache` from comments/params)
- Tier 2: Database config `CachePolicy` (`NoCache` / `Cache` / `Auto`)
- Tier 3: `auto_decision()` — caches when `hit_count > miss_count` AND `avg_result_size < 1MB`

**`stats.rs`** — Per-fingerprint query statistics tracker:
- `QueryStats` struct: `hit_count`, `miss_count`, `total_result_size`, `avg_result_size()`
- `QueryStatsTracker` with `record_hit(fingerprint, size)` / `record_miss(fingerprint)` / `get(fingerprint)`
- Internally: `Arc<scc::HashMap<u64, QueryStats>>`

**`context.rs`** — Cache context held in `QueryEngineContext`:
- `CacheContext` with `cache_miss: Option<(u64, Option<u64>)>` and `response_buffer: Vec<Message>`
- `capture_response(message)` — stores message in buffer when cache miss is tracked

**`integration.rs`** — Integration methods on `impl Cache`:
- `cache_check()` — main entry point, checks route, extracts directive, resolves policy, checks Redis
- `send_cached_response()` — deserializes wire-format bytes and sends to client
- `cache_response()` — serializes `Vec<Message>` into wire bytes and stores in Redis
- Cache key: XXH3 hash of `database_name + raw_query_string`

### Files Modified

#### 5. `pgdog/Cargo.toml`

- Added `fred = { version = "9", features = ["enable-rustls"] }` to dependencies

#### 6. `pgdog/src/frontend/client/query_engine/mod.rs`

- Added `pub mod cache;` module declaration
- Added `cache: Cache` field to `QueryEngine`
- `new()` loads `cache_config` from `config().config.general.cache` and creates `Cache::new(cache_config, database)`
- `handle()` flow: after `route_query()` and before `before_execution()`, calls `self.cache.try_read_cache(context)`. If HIT: sends cached response and returns. On MISS: stores state in `context.cache_context`.
- After `match command`, calls `self.cache.save_response_in_cache(context)` to store the captured response in Redis.

#### 7. `pgdog/src/frontend/client/query_engine/query.rs`

- `process_server_message()` added cache capture: `context.cache_context.capture_response(message.clone())`.

#### 8. `pgdog/src/frontend/client/query_engine/context.rs`

- Added `cache_context: CacheContext` field to `QueryEngineContext`.

#### 9. `pgdog/src/backend/pool/cluster.rs`

- Added `cache_enabled: bool` field to `ClusterConfig` and `Cluster`
- `cluster.rs` adds `|| self.cache_enabled()` in query parser requirement check — when caching is on, the query parser is forced on alongside `dry_run`, `prepared_statements`, `pub_sub`, and `regex_parser`

#### 10. `pgdog-config/src/core.rs`

- Added startup warning: `cache requires enabled query parser but it's disabled or session controlled` when `cache.is_enabled()` and parser is `Off` or `SessionControl`

---

## Key Design Decisions

| Decision | Choice |
|----------|--------|
| Interception point | Between `route_query()` and `before_execution()` in `handle()` |
| Cache config scope | **Global** (`config.general.cache`) |
| Redis client | `fred` crate v9 (async-native, tokio integration) |
| Cacheable queries | Only reads (`route.is_read()`) |
| Cache policy resolution | 3-tier: SQL comment → pgdog.cache param → DB policy → auto-decision |
| Cache HIT flow | Deserialize wire bytes → parse messages → send to client → return `Ok(true)` |
| Cache MISS flow | Normal execute → capture response via `CacheContext` → store in Redis → respond |
| Auto-decision engine | `hit_count > miss_count` AND `avg_result_size < 1MB` |
| Cache key | `pg_query::fingerprint(query).value.wrapping_add(db_hash)` where `db_hash = DefaultHasher of database name` |
| Wire format | Full PostgreSQL wire messages stored as raw bytes (one concatenated buffer) |

---

## Bugs Found & Fixed

1. **Redis client never connects** - Problem: CacheClient::new() built the client but never called init(). Fred requires explicit connection initialization. Fix: Added lazy `ensure_connected()` using `client.init().await`, guarded by `AtomicBool` so it runs exactly once on first get()/set(). Changed CacheClient from `#[derive(Debug)]` to manual Debug impl (contains `Arc<AtomicBool>`).

2. **Redis GET fails on NULL / cache miss** - Problem: `client.get::<bytes::Bytes>()` throws `Parse Error: Cannot parse into bytes` when the key doesn't exist. Fix: Use `client.get::<RedisValue, _>()` and check `val.is_null()` before extracting bytes.

3. **Wire format deserialization wrong in send_cached_response** - Problem: PostgreSQL wire message structure is `[1B code][4B length]` where length includes the 4B itself. I calculated `offset + 5 + msg_len` (treating length as payload-only), causing incorrect byte slicing. Fix: Corrected to `offset + 1 + msg_len`.

4. **Route incorrectly reports read-only as write when parser is disabled** - Problem: `query_parser_bypass()` conservatively returns `Route::write()` for all SQL when the query parser is disabled. Since pgdog doesn't enable the parser by default for simple queries, `route.is_read()` was false for `SELECT 1`. Fix: When any database has `cache.enabled = true`, the query parser level is auto-upgraded to `On` in the cluster config. The `|| self.cache_enabled()` check in `cluster.rs:475` forces the parser on. Cache also emits a startup warning if parser is `Off` or `SessionControl`. The old `is_likely_read()` string-prefix heuristic has been removed entirely.

5. **DB cache config defaults** - Observation: `Cache.policy` defaults to `CachePolicy::NoCache`. Even with `enabled = true`, caching is skipped unless policy is explicitly set. User action taken: Add `policy = "cache"` to pgdog.toml.

---

## Refactoring: Decoupled Cache Policy Extraction

The original implementation entangled cache directive parsing with pgdog's general comment parser, which only activates when the full query parser runs. This meant `/* pgdog_cache: ... */` annotations were silently ignored for simple queries and when `query_parser_bypass()` triggered.

**What was done:**

- New **`cache/`** module created under `query_engine/`
- `CachePolicyExtractor` trait with `fn extract(query, params) -> CacheDirective`
- `CommentCacheExtractor`: standalone regex scan on raw query string — works independent of AST parser
- `ParameterCacheExtractor`: reads `pgdog.cache` connection startup parameter
- `CachePolicyDispatcher`: chains extractors, returns first non-`None` result
- `Cache` struct as abstraction layer over client, stats, config, and dispatcher
- `CacheContext` struct holds `cache_miss` and `response_buffer` per-query
- Cache integration happens via `try_read_cache()` and `save_response_in_cache()` methods on `Cache`

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

## Completed

1. **Query parser auto-upgrade for caching** — When caching is enabled and parser is `Auto`/`Off`/`SessionControl`, the parser is forced to `On` via `|| self.cache_enabled()` check in `cluster.rs`. A startup warning is emitted in `core.rs` if parser remains incompatible.

2. **Decoupled cache policy extraction** — Cache directives extracted via standalone regex in `cache/policy.rs`, works regardless of parser state. Supports `/* pgdog_cache: ... */` format with optional `ttl=` parameter.

3. **Error handling / Reconnection** — Automatic reconnection with background task, CAS-guarded single reconnect, 2s operation timeout on all Redis calls, PING-based connection verification.

4. **Cache key collision across databases sharing one Redis** — Database name and raw query string are combined via a single XXH3 hash call, producing deterministic, collision-resistant per-database keys even on shared Redis. Different literal values in queries produce different cache keys.

5. **Wire format serialization/deserialization** — PostgreSQL wire messages stored as raw bytes. Correct byte slice calculation: `offset + 1 + msg_len`.

---

## What's Left To Do

1. **Auto policy** — Implemented but untested. Relies on stats tracker to decide based on hit/miss ratio and avg result size after enough observations.

2. **Multi-step execution caching** — InsertSplit and ShardingKeyUpdate rewrite paths use process_server_message() which captures responses, but the finalize_cache() call happens after match command block. Need to verify caching works correctly for multi-step rewrites.

3. **Response capture for prepared statements** — Extended protocol (Parse/Bind/Execute) response capture works through process_server_message() but hasn't been tested with PREPARE/EXECUTE. (Note: pgdog implements prepared statements caching. But unknown what kind of caching this is: just query cache or result cache. And if we implement our cache, will this break this prepared statement cache?)

4. **max_result_size config** — Implemented but not exposed in the initial pgdog.toml. Worth documenting in the config.

5. **Redis disconnect/reconnect under heavy load** — The reconnection logic works, but the fast-path check (`ensure_connected`) and the reconnect task can have timing edge cases under rapid disconnect/reconnect cycles. Need to stress-test.

6. **Integration tests** — Tests live in `integration/rust/tests/integration/`. Redis must be running on 127.0.0.1:6379 before tests. Run with: `cd integration/rust && cargo nextest run --no-fail-fast --test-threads=1`

### Planned Tests

1. **Database key namespace collision** — Two databases sharing one Redis, both running same query but with different underlying PG data. Verify correct isolation.
2. **Basic cache hit/miss** — Run a SELECT once (expect miss), run again (expect hit), verify metrics.
3. **TTL expiration** — Cache a query with short TTL, wait for expiry, verify miss on third call.
4. **Write bypasses cache** — Execute INSERT/UPDATE/DELETE, verify these do not populate or consume the cache.
5. **Redis unavailable** — Stop Redis mid-flight, verify queries pass through to PG without blocking.
6. **Redis reconnection** — Restart Redis after disconnect, verify cache recovers automatically.
