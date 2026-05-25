# Cache for pgdog — State of Implementation

## Architecture

Cache SELECT queries in Redis, bypass PostgreSQL on cache hit, populate cache on cache miss. Two-tier policy resolution: SQL comment/connection parameter → pgdog's config.

---

## Implementation

### Configuration (`pgdog-config`)

**`cache.rs`** — Cache configuration types:

**CachePolicy enum:** `NoCache` (default), `Cache`. Implements `FromStr`, `Display`, `Serialize`, `Deserialize`, `Copy`, `JsonSchema`.

**CacheBackend enum:** `Redis` (default). Discriminator for selecting the storage backend and for hotswap detection when the backend type changes in config.

**RedisConfig struct** (`[general.cache.redis]`):
- `url: String` — Redis connection URL (default `redis://localhost:6379`)
- `cache_key_prefix: String` — prefix prepended to every Redis key (default `pgdog:`)

**Cache struct** (`[general.cache]`):
- `enabled: bool` — is caching on? (default `false`)
- `policy: CachePolicy` — which policy? (default `no_cache`)
- `ttl: u64` — default TTL seconds (default `300`)
- `backend: CacheBackend` — which storage backend (default `redis`)
- `redis: RedisConfig` — Redis-specific settings
- `max_result_size: usize` — max cached result bytes (default `0` = unlimited)

Example TOML:
```toml
[general.cache]
enabled = true
policy  = "cache"
ttl     = 300

[general.cache.redis]
url              = "redis://localhost:6379"
cache_key_prefix = "pgdog:"
```

**`general.rs`** — `General` struct holds `cache: Cache` field. **Cache config is global.**

**`lib.rs`** — Exports `pub use cache::{CacheBackend, CachePolicy, Cache, RedisConfig as CacheRedisConfig};`.

### Cache Module (`pgdog/src/frontend/cache/`)

**`mod.rs`** — Module exports, global singleton, and main `Cache` struct:
```rust
pub mod context;
pub mod integration;
pub mod policy;
pub mod storage;

pub use context::CacheContext;
pub use integration::CacheCheckResult;
pub use policy::CacheDecision;
pub use storage::{CacheStorage, RedisCacheStorage};
```

`Cache` struct wraps `RwLock<Option<Box<dyn CacheStorage>>>` (tokio `RwLock`).

**Global singleton:** Cache is global-scoped, not connection-scoped. Accessed via `cache()` function which returns `Arc<Cache>` from a `Lazy<Arc<Cache>>` static. `Cache::new()` reads config internally — no parameters needed.

**Config hotswap:** `hotswap_if_needed()` is called at the top of `try_read_cache` and `save_response_in_cache`. It fast-paths with a read-lock; acquires write-lock only if the URL or backend type has changed, then rebuilds the storage.

Key methods:
- `new()` — creates storage from current config (or `None` if disabled)
- `hotswap_if_needed()` — compares live config against the active storage's one with `has_config_changed()`; swaps if `true` returned
- `try_read_cache(cache_context, in_transaction, client_request, params)` — hotswaps, calls `cache_check()`, returns `Ok(Some(Vec<Message>))` on HIT (caller replays through pipeline), `Ok(None)` on MISS/PASSTHROUGH
- `save_response_in_cache(cache_context)` — hotswaps, finalizes by storing the captured response

**`storage/mod.rs`** — Abstract storage trait and error type:
- `CacheStorage` trait: `get`, `set`, `is_enabled`, `has_config_changed` — implemented by all cache backends
- `Error` enum shared across all backends: `RedisError`, `ConnectionFailed`, `CacheMiss`

**`storage/redis.rs`** — Redis storage backend (`RedisCacheStorage`) implementing `CacheStorage`:
- `RedisCacheStorage::new(config)` — builds client from given URL; immediately spawns a background connection task; returns `None` if URL is invalid
- Background connect task: retries `init()` in a loop (5ms to 5s exponential backoff); sets `reconnecting = false` on success; CAS-guarded so only one task runs at a time
- `get(&self, key)` — returns `Result<Vec<u8>, Error>`; returns `Err(Error::ConnectionFailed)` immediately (triggering cache miss) if not yet connected; marks `reconnecting` and spawns reconnect on Redis errors
- `set(&self, key, value, ttl)` — stores bytes with EX expiration; returns immediately on disconnect; respects `max_result_size` from live config
- `reconnect()` — spawns reconnect if not already running (CAS-guarded)
- `has_config_changed()` — returns `true` if cache config has changed (used for hotswap detection)
- `is_enabled()` — reads live `config().config.general.cache.enabled`
- Key prefix comes from `config().config.general.cache.redis.cache_key_prefix`
- `reconnecting: Arc<AtomicBool>` — prevents multiple concurrent reconnect tasks
- All Redis operations wrapped in `tokio::time::timeout(REDIS_OPERATION_TIMEOUT)` (2s)

**`policy.rs`** — 2-tier policy resolution:
- `CacheDirective` enum: `Cache { ttl_seconds }`, `ForceCache { ttl_seconds }`, `NoCache` (default)
- `CacheDecision` enum: `Skip`, `Cache(u64)`, `ForceCache(u64)`
- `resolve(client_request, params, is_read)` — main resolver function, chains all tiers
- `get_cache_directive(client_request, params)` — comment hint (from AST) has priority over connection parameter (`pgdog.cache`)
- `extract_parameter_directive(params)` — parses `pgdog.cache` parameter: `no_cache`, `cache`, `cache ttl=N`, `force_cache`, `force_cache ttl=N`
- Tier 1: Extractor directive (`CacheDirective::Cache { ttl }`, `CacheDirective::ForceCache { ttl }`, or `CacheDirective::NoCache`)
- Tier 2: Global config `CachePolicy` (`NoCache` / `Cache`)

**`context.rs`** — Cache context held in `QueryEngineContext`:
- `CacheContext` with `cache_miss: Option<CacheMiss>`, `response_buffer: Vec<Message>`, and `had_error: bool`
- `capture_response(message)` — stores message in buffer when cache miss is tracked; sets `had_error = true` on `E` messages
- `reset()` — clears all state for per-query isolation

**`integration.rs`** — Integration methods on `impl Cache`:
- `cache_check()` — main entry point, checks route, calls `policy::resolve()`, checks Redis
- `deserialize_cached(Vec<u8>) -> Vec<Message>` — parses a flat blob of concatenated PostgreSQL wire messages into individual `Message` values. Wire format: `[1B code][4B length (incl. itself)][payload]`. Named constants `HEADER_CODE_LEN`, `HEADER_LEN_SIZE`, `HEADER_TOTAL` replace the former magic numbers. Not Redis-specific — usable with any cache backend that stores raw bytes.
- `cache_response()` — serializes `Vec<Message>` into wire bytes and stores in Redis
- Cache key: XXH3 hash of `database_name + comment-stripped query string + bind params`

### Query Engine Integration

**`pgdog/src/frontend/client/query_engine/mod.rs`**
- Imports global `cache()` from `frontend::cache`
- `handle()` flow: after `route_query()` and before `before_execution()`, calls `cache().try_read_cache(context)`. If HIT: replays each cached `Message` through `process_server_message()` (same pipeline as live backend responses — stats, transaction state, hooks all fire correctly), then returns. On MISS: stores state in `context.cache_context`.
- After `match command`, calls `cache().save_response_in_cache(context)` to finalize caching.

**`pgdog/src/frontend/client/query_engine/query.rs`**
- `process_server_message()` calls `context.cache_context.capture_response(message.clone())`.

**`pgdog/src/frontend/client/query_engine/context.rs`**
- `QueryEngineContext` holds `cache_context: CacheContext` field.

### Backend and Config Integration

**`pgdog/src/backend/pool/cluster.rs`**
- `ClusterConfig` and `Cluster` hold `cache_enabled: bool` field
- Query parser requirement check includes `|| self.cache_enabled()` — when caching is on, the query parser is forced on.

**`pgdog-config/src/core.rs`**
- Startup warning emitted when `cache.is_enabled()` and parser is `Off` or `SessionControl`.

### Dependencies

**`pgdog/Cargo.toml`**
fred = { version = "9", features = ["enable-rustls"] }
xxhash-rust = { version = "0.8", features = ["xxh3"]}

---

## Key Design Decisions

| Decision | Choice |
|----------|--------|
| Interception point | Between `route_query()` and `before_execution()` in `handle()` |
| Cache config scope | **Global** (`config.general.cache`) |
| Redis client | `fred` crate v9 (async-native, tokio integration) |
| Cacheable queries | Only reads (`route.is_read()`) |
| Cache policy resolution | 2-tier: SQL comment/param → DB policy |
| Cache HIT flow | Deserialize wire bytes → `Vec<Message>` → replay each through `process_server_message()` |
| Cache MISS flow | Normal execute → capture response via `CacheContext` → store in Redis → respond |
| Cache key | XXH3 hash of `database_name + comment-stripped query string + bind params` |
| Wire format | Full PostgreSQL wire messages stored as raw bytes (one concatenated buffer) |

---

## How to Control Cache

### SQL Comments

Add a C-style comment before your query. The first matching directive wins:

```sql
-- Force bypass cache for this query
/* pgdog_cache: no_cache */
SELECT * FROM users WHERE id = 1;

-- Cache with database default TTL
/* pgdog_cache: cache */
SELECT * FROM products WHERE category = 'electronics';

-- Cache with custom TTL in seconds
/* pgdog_cache: cache ttl=300 */
SELECT * FROM orders;

-- Force cache with database default TTL
/* pgdog_cache: force_cache */
SELECT * FROM products WHERE category = 'electronics';

-- Force cache with custom TTL in seconds
/* pgdog_cache: force_cache ttl=300 */
SELECT * FROM orders;
```

> **Hash independence from comments:** All SQL comments (block `/* */` and line `--`) are stripped
> before computing the cache key hash. This means a query sent with a `/* pgdog_cache: cache */`
> comment produces **exactly the same cache key** as the same query sent without any comment but
> with the directive supplied via a connection parameter (`SET pgdog.cache = 'cache'`). There is
> no longer a need for special normalization of `force_cache` vs `cache` hints — both result in
> the same hash because comments are removed entirely.

### Connection Parameter

Set `pgdog.cache` at connection time (via DSN options) or with `SET` after connecting:

```sql
-- Session-wide: all queries in this connection bypass cache
SET pgdog.cache = 'no_cache';

-- Session-wide: cache all queries with default TTL
SET pgdog.cache = 'cache';

-- Session-wide: cache all queries with 5-minute TTL
SET pgdog.cache = 'cache ttl=300';

-- Session-wide: force cache all queries with default TTL
SET pgdog.cache = 'force_cache';

-- Session-wide: force cache all queries with 5-minute TTL
SET pgdog.cache = 'force_cache ttl=300';
```

```sh
# Session-wide: all queries in this connection bypass cache
psql postgresql://postgres:postgres@127.0.0.1:5432/postgres?options=-c%20pgdog.cache%3Dno_cache

# Session-wide: cache all queries with default TTL
psql postgresql://postgres:postgres@127.0.0.1:5432/postgres?options=-c%20pgdog.cache%3Dcache

# Session-wide: cache all queries with 5-minute TTL
psql postgresql://postgres:postgres@127.0.0.1:5432/postgres?options=-c%20pgdog.cache%3Dcache%20ttl%3D300

# Session-wide: force cache all queries with default TTL
psql postgresql://postgres:postgres@127.0.0.1:5432/postgres?options=-c%20pgdog.cache%3Dforce_cache

# Session-wide: force cache all queries with 5-minute TTL
psql postgresql://postgres:postgres@127.0.0.1:5432/postgres?options=-c%20pgdog.cache%3Dforce_cache%20ttl%3D300
```

### Priority Order

Sources are checked in order — first non-None result wins, then falls through to global config:

```
SQL comment  →  pgdog.cache parameter  →  DB policy config
(highest)                                           (lowest)
```

---

## Completed

1. **Redis client never connects** - Problem: CacheClient::new() built the client but never called init(). Fred requires explicit connection initialization. Fix: Added lazy `ensure_connected()` using `client.init().await`, guarded by `AtomicBool` so it runs exactly once on first get()/set(). Changed CacheClient from `#[derive(Debug)]` to manual Debug impl (contains `Arc<AtomicBool>`).

2. **Redis GET fails on NULL / cache miss** - Problem: `client.get::<bytes::Bytes>()` throws `Parse Error: Cannot parse into bytes` when the key doesn't exist. Fix: Use `client.get::<RedisValue, _>()` and check `val.is_null()` before extracting bytes. Later refined: `get()` now returns `Result<Vec<u8>, Error>` instead of `Result<Option<Vec<u8>>>` — a missing key yields `Err(Error::CacheMiss)`, which is matched explicitly in `cache_check()` and converted to `CacheCheckResult::Miss`. Other errors propagate as `Passthrough`.

3. **Wire format deserialization wrong in send_cached_response** - Problem: PostgreSQL wire message structure is `[1B code][4B length]` where length includes the 4B itself. I calculated `offset + 5 + msg_len` (treating length as payload-only), causing incorrect byte slicing. Fix: Corrected to `offset + 1 + msg_len`, then replaced magic numbers with named constants `HEADER_CODE_LEN`, `HEADER_LEN_SIZE`, `HEADER_TOTAL`.

4. **Route incorrectly reports read-only as write when parser is disabled** - Problem: `query_parser_bypass()` conservatively returns `Route::write()` for all SQL when the query parser is disabled. Since pgdog doesn't enable the parser by default for simple queries, `route.is_read()` was false for `SELECT 1`. Fix: When any database has `cache.enabled = true`, the query parser level is auto-upgraded to `On` in the cluster config. The `|| self.cache_enabled()` check in `cluster.rs:475` forces the parser on. Cache also emits a startup warning if parser is `Off` or `SessionControl`. The old `is_likely_read()` string-prefix heuristic has been removed entirely.

5. **DB cache config defaults** - Observation: `Cache.policy` defaults to `CachePolicy::NoCache`. Even with `enabled = true`, caching is skipped unless policy is explicitly set. User action taken: Add `policy = "cache"` to pgdog.toml.

6. **Query parser auto-upgrade for caching** — When caching is enabled and parser is `Auto`/`Off`/`SessionControl`, the parser is forced to `On` via `|| self.cache_enabled()` check in `cluster.rs`. A startup warning is emitted in `core.rs` if parser remains incompatible.

7. **Decoupled cache policy extraction** — Cache directives extracted via standalone regex in `cache/policy.rs`, works regardless of parser state. Supports `/* pgdog_cache: ... */` format with optional `ttl=` parameter. Unified with sharding hints via `comment()` function in `comment.rs`.

8. **Error handling / Reconnection** — Automatic reconnection with background task, CAS-guarded single reconnect, 2s operation timeout on all Redis calls, PING-based connection verification.

9. **Cache key collision across databases sharing one Redis** — Database name and query string (with all SQL comments stripped) are combined via a single XXH3 hash call, producing deterministic, collision-resistant per-database keys even on shared Redis. Different literal values in queries produce different cache keys. Because all comments are stripped before hashing, the cache key is identical whether the cache directive arrives via a SQL comment or a connection parameter.

10. **Wire format serialization/deserialization** — PostgreSQL wire messages stored as raw bytes. Correct byte slice calculation expressed via named constants (`HEADER_CODE_LEN = 1`, `HEADER_LEN_SIZE = 4`, `HEADER_TOTAL = 5`). Deserialization extracted into `deserialize_cached()` with inline comments explaining each boundary check.

11. **Do not cache error responses**.

12. **Setting pgdog.cache via connection url doesn't work** — now works.

13. **Moved all cache-related structs from QueryEngine to Client** — now all cache structs including redis client are creating for whole pgdog's lifetime.

14. **Use built-in query comment hints** — Cache hints (`pgdog_cache:`) are now extracted alongside sharding hints (`pgdog_shard:`, `pgdog_sharding_key:`, `pgdog_role:`) via the unified `comment()` function in `comment.rs`. The `comment_cache` field is stored in `AstInner` and accessed during cache checking via `client_request.ast.comment_cache`. Policy resolution simplified: trait-based extractors replaced with free functions (`resolve()`, `get_cache_directive()`, `extract_parameter_directive()`). Comment hint (from AST) has priority over connection parameter `pgdog.cache`. `Cache` struct no longer needs `policy_dispatcher` field. Parameter format unified to `no_cache` (underscore, not dash).

15. **Add cache config to .schema**.

16. **Force-cache hint support** — `/* pgdog_cache: force_cache */` and `/* pgdog_cache: force_cache ttl=N */` directives always attempt to cache. Because all comments are stripped before hashing, `force_cache` and `cache` directives produce the same cache key as the bare query with no comment at all.

17. **Cache HIT replays through the server-message pipeline** — Previously, cache hits sent responses directly to the stream, bypassing `process_server_message()`. Now `try_read_cache()` returns `Option<Vec<Message>>` and the caller (`handle()`) feeds each message through `process_server_message()` — giving correct stats accounting, transaction state updates from `ReadyForQuery`, and hook invocations on every cache hit.

18. **CacheClient error types refined** — `get()` now returns `Result<Vec<u8>, Error>` (no more `Option`). `Error::CacheMiss(u64)` is a dedicated variant for key-not-found; `Error::RedisError` is now a struct variant carrying `cmd: &'static str`, `key: u64`, and the underlying error for richer diagnostics. `Error::ConnectionFailed` uses `&'static str` instead of `String` to avoid heap allocation on the hot path.

19. **Config hotswap** — `Cache` singleton holds `Arc<tokio::sync::RwLock<Option<Box<dyn CacheStorage>>>>`. `hotswap_if_needed()` runs at the start of every `try_read_cache` and `save_response_in_cache` call: read-locks to compare the active backend's URL against `config().config.general.cache.redis.url`; if they differ (or the backend type changes) it write-locks and rebuilds the storage. Fast path is a read-lock-only check with no allocation.

20. **CacheClient rewritten as `RedisCacheStorage`** — Replaced `CacheClient` with `RedisCacheStorage` implementing the `CacheStorage` trait. Key improvements: background connect task is spawned immediately in `new()` so the first query never blocks on init; `get`/`set` check only one atomic flag (`reconnecting`) and return immediately if `true` returned instead of running `ensure_connected`; the `Option<RedisClient>` field and the three-condition guard at the top of every operation are gone; `reconnect` is the single place that sets the flag and CAS-guards the reconnect spawn.

21. **Abstract storage backend** — `storage/mod.rs` defines the `CacheStorage` trait (`get`, `set`, `is_enabled`, `has_config_changed`) and the shared `Error` enum. `storage/redis.rs` is the Redis implementation. `Cache` holds `Box<dyn CacheStorage>` behind a tokio `RwLock` so any backend (e.g. Memcached) can be plugged in by adding a sub-module under `storage/` and a variant to `CacheBackend`. `deserialize_cached()` remains backend-agnostic in `integration.rs`.

22. **Nested backend config** — Backend-specific settings live in their own TOML subtable (`[general.cache.redis]`) rather than flat fields on `[general.cache]`. `RedisConfig` holds `url` and `cache_key_prefix`. When a new backend is added, it gets its own subtable (e.g. `[general.cache.memcached]`) without polluting the top-level cache section. `client.rs` renamed to `storage/redis.rs`.

23. **Cache key must include Bind parameters for extended protocol** — For simple `Query` messages, parameter values are embedded in the SQL string, so the XXH3 hash of `database + query_text` is naturally unique per value. For extended protocol (Parse/Bind/Execute), the SQL contains `$1`/`$2` placeholders and the actual values arrive in the `Bind` message separately. The current hash ignores them, so `SELECT * FROM users WHERE id = $1` with `id = 1` and `id = 2` produce the same cache key — wrong rows are returned on the second call. Fix: hash `param.len` (the `i32` field, not the `len()` method which returns wire size) and `param.data` for each entry in `bind.params_raw()` into the hasher in `cache_check()` in `integration.rs`. This affects all production drivers that use extended protocol by default: psycopg3, asyncpg, JDBC, npgsql. Note: pgdog's built-in prepared statement cache (`PreparedStatements` / `GlobalCache`) is a proxy-level plan cache only — it deduplicates backend `Parse` round-trips. It does not cache result rows and is orthogonal to the Redis result cache.

24. **Comments stripped from query before hashing** — All SQL block comments (`/* … */`, including nested) and line comments (`-- …`) are removed from the query string before computing the XXH3 cache key. This makes the cache key independent of whether the cache directive was supplied via a SQL comment or a connection parameter. `compute_cache_key_hash` is a standalone public function in `integration.rs` so it can be unit-tested directly. `strip_sql_comments` returns `Cow<'_, str>`: when no comment markers are present the original string slice is returned without any allocation; only queries that actually contain `/*` or `--` incur a heap allocation. The `FORCE_CACHE_RE` regex normalization that previously converted `force_cache` to `cache` in the hash input has been removed — stripping all comments achieves the same result in a more general way.

---

## What's Left To Do

1. **Redis disconnect/reconnect under heavy load** — The reconnection logic works, but timing edge cases under rapid disconnect/reconnect cycles still need stress-testing.

2. **Integration tests**.

3. **Set redis query timeout from config**

4. **Add hint for query hash key**

5. **Add flag for required cache storage available** — query will fall with error if redis (or another cache storage) unavaliable. And subtask: first query inits cache client, but connection is established later, which is why the cache storage is unavailable for the first query — so need to wait for established connection.

6. **Hash query without comments on the fly instead of normalizing it first** — with this no `String` will be allocated. But must deal somehow with getting same hash for "SELECT 1;" and "/* pgdog_cache: cache */ SELECT 1;" because the second one transforms to " SELECT 1;" (with space at the start).

# Tests

## Running the tests

Unit tests (no PostgreSQL or Redis needed)
```sh
cargo nextest run -p pgdog frontend::cache
```

## Integration tests (PostgreSQL + Redis + pgdog required)

```sh
bash integration/cache/run.sh
```

Or if you already have pgdog running on port 6432 with that config:
```sh
bash integration/cache/dev.sh
```