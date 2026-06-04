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
- `operation_timeout: NonZeroU64` — timeout in seconds for individual Redis operations (GET/SET/ping) (default `2`)

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
url               = "redis://localhost:6379"
cache_key_prefix  = "pgdog:"
operation_timeout = 2
```

**`general.rs`** — `General` struct holds `cache: Cache` field. **Cache config is global.**

**`lib.rs`** — Exports `pub use cache::{CacheBackend, CachePolicy, Cache, RedisConfig as CacheRedisConfig};`.

### Cache Module (`pgdog/src/frontend/cache/`)

**`mod.rs`** — Module exports, global singleton, and main `Cache` struct:
```rust
pub mod context;
pub mod integration;
pub mod directive;
pub mod storage;

pub use context::CacheContext;
pub use integration::CacheCheckResult;
pub use directive::CacheDirective;
pub use storage::{CacheStorage, RedisCacheStorage};
```

`Cache` struct wraps `RwLock<Option<Box<dyn CacheStorage>>>` (tokio `RwLock`).

**Global singleton:** Cache is global-scoped, not connection-scoped. Accessed via async `cache()` function which returns `Arc<Cache>` from a `tokio::sync::OnceCell` static. `Cache::new()` is async and reads config internally — no parameters needed.

**Config hotswap:** `hotswap_if_needed()` is called at the top of `try_read_cache` and `save_response_in_cache`. It fast-paths with a read-lock; acquires write-lock only if `is_actual()` returns true, then rebuilds the storage. The write-lock path re-checks to guard against concurrent swaps. `is_actual()` is a no-argument method on `CacheStorage` that reads current config internally — callers do not pass a config snapshot.

Key methods:
- `new()` — async; creates storage from current config (or `None` if disabled); waits up to `operation_timeout` for initial Redis connection
- `hotswap_if_needed()` — compares live config against the active storage via `is_actual()`; swaps if `true`
- `try_read_cache(cache_context, in_transaction, client_request, params)` — hotswaps, calls `cache_check()`, returns `Ok(Some(Vec<Message>))` on HIT (caller replays through pipeline), `Ok(None)` on MISS/PASSTHROUGH
- `save_response_in_cache(cache_context)` — hotswaps, finalizes by storing the captured response

**`storage/mod.rs`** — Abstract storage trait and error type:
- `CacheStorage` trait: `get`, `set`, `is_enabled`, `is_actual` — implemented by all cache backends
- `is_actual(&self) -> bool` — takes no arguments; reads live config internally; should only check parameters that require a storage rebuild (e.g. `backend` type and storage-specific settings like `redis.url`); TTL and other runtime settings do not require a rebuild and are read from live config on every call
- `Error` enum shared across all backends: `RedisError`, `ConnectionFailed`, `CacheMiss`

**`storage/redis.rs`** — Redis storage backend (`RedisCacheStorage`) implementing `CacheStorage`:
- `RedisCacheStorage::new(config)` — async; builds client from given URL; spawns background connection task and waits up to `operation_timeout` ms for it to complete; if timeout expires, task continues in background; returns `None` if URL is invalid
- Background connect task: retries `init()` in a loop (5ms to 5s exponential backoff); sets `reconnecting = false` on success; CAS-guarded so only one task runs at a time; timeout for `init()` read from live config (`config.redis.operation_timeout`)
- `get(&self, key)` — returns `Result<Vec<u8>, Error>`; returns `Err(Error::ConnectionFailed)` immediately (triggering cache miss) if not yet connected; marks `reconnecting` and spawns reconnect on Redis errors; operation timeout read from live config
- `set(&self, key, value, ttl)` — stores bytes with EX expiration; returns immediately on disconnect; respects `max_result_size` from live config; operation timeout read from live config
- `reconnect()` — spawns reconnect task fire-and-forget (no waiting) if not already running (CAS-guarded)
- `is_actual()` — returns `true` if `backend != Redis` or `self.url != live config url`; only URL triggers a rebuild (all other redis settings including `cache_key_prefix`, `operation_timeout` are read from live config on every call)
- `is_enabled()` — reads live `config().config.general.cache.enabled`
- Key prefix comes from `config().config.general.cache.redis.cache_key_prefix`
- `reconnecting: Arc<AtomicBool>` — prevents multiple concurrent reconnect tasks
- All Redis operations wrapped in `tokio::time::timeout(Duration::from_millis(operation_timeout))` where `operation_timeout` is read from live config on every call (no compile-time constant)
- `RedisCacheStorage` stores only `url: String` (not the full `CacheConfig`) — all other settings are read from live config; this means `cache_key_prefix` and `operation_timeout` changes take effect immediately without a storage rebuild

**`directive.rs`** — Position-agnostic cache directive parsing and resolution:
- `CacheMode` enum: `Cache`, `ForceCache`, `NoCache` (default)
- `CacheDirective` struct: `{ mode: Option<CacheMode>, ttl_seconds: Option<u64> }` — flat structure where each field is independently optional
- `CacheDirective::parse(s: &str)` — parses space-separated tokens in any order: `cache`, `force_cache`, `no_cache`, `ttl=N`, and ignores unknown tokens
- `CacheDirective::or(fallback)` — merges two directives field-by-field: if a field is `Some`, it wins; otherwise fall back to the other directive's value
- `resolve(client_request, params)` — extracts directive from SQL comment (via AST `comment_cache` field) and connection parameter (`pgdog.cache`), merges them (comment wins per-field), returns the merged `CacheDirective`
- Arguments can appear in any order: `ttl=17 force_cache` and `force_cache ttl=17` are equivalent
- Missing fields cascade through resolution tiers: comment → parameter → global config

**`context.rs`** — Cache context held in `QueryEngineContext`:
- `CacheContext` with `cache_miss: Option<CacheMiss>`, `response_buffer: Vec<Message>`, and `had_error: bool`
- `capture_response(message)` — stores message in buffer when cache miss is tracked; sets `had_error = true` on `E` messages
- `reset()` — clears all state for per-query isolation

**`integration.rs`** — Integration methods on `impl Cache`:
- `cache_check()` — main entry point: checks route, calls `directive::resolve()`, resolves final mode and TTL:
  - Mode resolution: directive mode → global `CachePolicy` (converted to `CacheMode`)
  - TTL resolution: directive TTL → global `cache.ttl`
  - `NoCache` → `Passthrough`
  - `ForceCache` → returns `Miss` immediately (bypasses Redis lookup, always repopulates)
  - `Cache` → computes hash, acquires read-lock on storage, calls `storage.get()`; `CacheMiss` → `Miss`; other errors → `Passthrough`
- `deserialize_cached(Vec<u8>) -> Vec<Message>` — parses a flat blob of concatenated PostgreSQL wire messages into individual `Message` values. Wire format: `[1B code][4B length (incl. itself)][payload]`. Named constants `HEADER_CODE_LEN`, `HEADER_LEN_SIZE`, `HEADER_TOTAL` replace magic numbers. Not Redis-specific — usable with any cache backend that stores raw bytes.
- `cache_response()` — serializes `Vec<Message>` into wire bytes and stores in Redis
- Cache key: XXH3 hash of `database_name + normalized query + bind params` — computed by `compute_cache_key_hash`

**Cache key hashing (`compute_cache_key_hash` / `hash_query_without_comments`):**

`hash_query_without_comments` feeds the query directly into the XXH3 hasher without allocating a `String`. It implements a state machine over the character stream:
- **Block comments** (`/* … */`, including PostgreSQL nested variants) — skipped entirely; treated as a token separator (sets `pending_space`)
- **Line comments** (`-- … \n`) — skipped entirely; treated as a token separator
- **Whitespace** outside string literals — collapsed: any run of whitespace sets `pending_space = true` but is not hashed directly; leading and trailing whitespace is suppressed naturally
- **String literals** (`'…'` with `''` escapes) — passed through verbatim; spaces inside strings are never collapsed or removed
- **Regular characters** — if `pending_space` is set and at least one character has already been emitted, a single space is hashed first, then the character; this ensures `SELECT/*c*/1` and `SELECT 1` produce the same hash without merging tokens into `SELECT1`

This means `"/* pgdog_cache: cache */ SELECT 1"` and `"SELECT 1"` hash identically: the comment is dropped and the leading whitespace it would have left is suppressed because `emitted = false` at that point.

### Query Engine Integration

**`pgdog/src/frontend/client/query_engine/mod.rs`**
- Imports global async `cache()` from `frontend::cache`
- `handle()` flow: after `route_query()` and before `before_execution()`, calls `cache().await.try_read_cache(context)`. If HIT: replays each cached `Message` through `process_server_message()` (same pipeline as live backend responses — stats, transaction state, hooks all fire correctly), then returns. On MISS: stores state in `context.cache_context`.
- After `match command`, calls `cache().await.save_response_in_cache(context)` to finalize caching.

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
| Cache policy resolution | Field-by-field merge: SQL comment → connection param → global config |
| Cache HIT flow | Deserialize wire bytes → `Vec<Message>` → replay each through `process_server_message()` |
| Cache MISS flow | Normal execute → capture response via `CacheContext` → store in Redis → respond |
| Cache key | XXH3 hash of `database_name + normalized query + bind params` |
| Query normalization | On-the-fly in hasher: comments stripped, whitespace collapsed (except inside string literals), no `String` allocated |
| Wire format | Full PostgreSQL wire messages stored as raw bytes (one concatenated buffer) |
| Config hotswap | `is_actual()` reads live config internally; only those config parameters, that require rebuild, triggers it |
| Redis operation timeout | Configurable via `redis.operation_timeout` (seconds, default `2`); read from live config on every call — no rebuild needed to change it |

---

## How to Control Cache

### SQL Comments

Add a C-style comment before your query. Arguments can appear in any order:

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

-- TTL before mode (same as above)
/* pgdog_cache: ttl=300 cache */
SELECT * FROM orders;

-- Force cache with database default TTL
/* pgdog_cache: force_cache */
SELECT * FROM products WHERE category = 'electronics';

-- Force cache with custom TTL in seconds
/* pgdog_cache: force_cache ttl=300 */
SELECT * FROM orders;

-- Only specify TTL, inherit mode from connection parameter or global config
/* pgdog_cache: ttl=60 */
SELECT * FROM sessions;

-- Multiple directives in one comment (cache directive does not consume other directives)
/* pgdog_cache: force_cache ttl=10 pgdog_role: replica */
SELECT * FROM analytics;
```

> **Position-agnostic parsing:** Arguments like `cache`, `force_cache`, `no_cache`, and `ttl=N` 
> can appear in any order. Unknown tokens are silently ignored for forward compatibility.
>
> **Field-by-field fallback:** If you specify only `ttl=60` in a comment without a mode, the mode 
> will be taken from the `pgdog.cache` connection parameter. If the parameter also lacks a mode, 
> the global config's `policy` is used. Each field cascades independently through the resolution 
> tiers: comment → parameter → global config.
>
> **Hash independence from comments:** SQL comments are skipped on-the-fly while hashing, with no
> intermediate `String` allocation. Surrounding whitespace left by a stripped comment is also
> collapsed, so `"/* pgdog_cache: cache */ SELECT 1"` and `"SELECT 1"` produce exactly the same
> cache key. Spaces inside string literals (`WHERE name = 'hello world'`) are never affected.

### Connection Parameter

Set `pgdog.cache` at connection time (via DSN options) or with `SET` after connecting. Arguments can appear in any order:

```sql
-- Session-wide: all queries in this connection bypass cache
SET pgdog.cache = 'no_cache';

-- Session-wide: cache all queries with default TTL
SET pgdog.cache = 'cache';

-- Session-wide: cache all queries with 5-minute TTL
SET pgdog.cache = 'cache ttl=300';

-- TTL before mode (same as above)
SET pgdog.cache = 'ttl=300 cache';

-- Session-wide: force cache all queries with default TTL
SET pgdog.cache = 'force_cache';

-- Session-wide: force cache all queries with 5-minute TTL
SET pgdog.cache = 'force_cache ttl=300';

-- Only specify TTL, inherit mode from global config
SET pgdog.cache = 'ttl=120';
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

# Only specify TTL, inherit mode from global config
psql postgresql://postgres:postgres@127.0.0.1:5432/postgres?options=-c%20pgdog.cache%3Dttl%3D120
```

### Priority Order

Cache directives are resolved field-by-field. For each field (`mode` and `ttl_seconds`), the first non-`None` value wins:

```
SQL comment  →  pgdog.cache parameter  →  global config
(highest)                                      (lowest)
```

**Example 1:** Comment specifies `ttl=60` but no mode; parameter specifies `cache` but no TTL.
- Final mode: `cache` (from parameter)
- Final TTL: `60` (from comment)

**Example 2:** Comment specifies `force_cache ttl=10`; parameter specifies `cache ttl=300`.
- Final mode: `force_cache` (comment wins)
- Final TTL: `10` (comment wins)

**Example 3:** Comment specifies `ttl=120`; parameter is not set; global config has `policy = "cache"` and `ttl = 300`.
- Final mode: `cache` (from global config)
- Final TTL: `120` (comment wins)

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

19. **Config hotswap** — `Cache` singleton holds `Arc<tokio::sync::RwLock<Option<Box<dyn CacheStorage>>>>`. `hotswap_if_needed()` runs at the start of every `try_read_cache` and `save_response_in_cache` call: read-locks and calls `has_config_changed()` on the active backend; if true, write-locks, re-checks (to guard against concurrent swaps), and rebuilds the storage. `has_config_changed()` is a no-argument method — each implementation reads the live config internally so callers never pass a config snapshot.

20. **CacheClient rewritten as `RedisCacheStorage`** — Replaced `CacheClient` with `RedisCacheStorage` implementing the `CacheStorage` trait. Key improvements: background connect task is spawned in `new()` and `new()` waits up to `operation_timeout` ms for the connection to establish (if timeout expires, task continues in background); `get`/`set` check only one atomic flag (`reconnecting`) and return immediately if `true` instead of running `ensure_connected`; the `Option<RedisClient>` field and the three-condition guard at the top of every operation are gone; `reconnect` is the single place that sets the flag and CAS-guards the reconnect spawn; reconnect spawns fire-and-forget without waiting.

21. **Abstract storage backend** — `storage/mod.rs` defines the `CacheStorage` trait (`get`, `set`, `is_enabled`, `has_config_changed`) and the shared `Error` enum. `storage/redis.rs` is the Redis implementation. `Cache` holds `Box<dyn CacheStorage>` behind a tokio `RwLock` so any backend (e.g. Memcached) can be plugged in by adding a sub-module under `storage/` and a variant to `CacheBackend`. `deserialize_cached()` remains backend-agnostic in `integration.rs`.

22. **Nested backend config** — Backend-specific settings live in their own TOML subtable (`[general.cache.redis]`) rather than flat fields on `[general.cache]`. `RedisConfig` holds `url` and `cache_key_prefix`. When a new backend is added, it gets its own subtable (e.g. `[general.cache.memcached]`) without polluting the top-level cache section. `client.rs` renamed to `storage/redis.rs`.

23. **Cache key must include Bind parameters for extended protocol** — For simple `Query` messages, parameter values are embedded in the SQL string, so the XXH3 hash of `database + query_text` is naturally unique per value. For extended protocol (Parse/Bind/Execute), the SQL contains `$1`/`$2` placeholders and the actual values arrive in the `Bind` message separately. The current hash ignores them, so `SELECT * FROM users WHERE id = $1` with `id = 1` and `id = 2` produce the same cache key — wrong rows are returned on the second call. Fix: hash `param.len` (the `i32` field, not the `len()` method which returns wire size) and `param.data` for each entry in `bind.params_raw()` into the hasher in `cache_check()` in `integration.rs`. This affects all production drivers that use extended protocol by default: psycopg3, asyncpg, JDBC, npgsql. Note: pgdog's built-in prepared statement cache (`PreparedStatements` / `GlobalCache`) is a proxy-level plan cache only — it deduplicates backend `Parse` round-trips. It does not cache result rows and is orthogonal to the Redis result cache.

24. **Comments stripped from query before hashing** — All SQL block comments (`/* … */`, including nested) and line comments (`-- …`) are removed from the query string before computing the XXH3 cache key. This makes the cache key independent of whether the cache directive was supplied via a SQL comment or a connection parameter.

25. **Zero-allocation query hashing** — `hash_query_without_comments` feeds the query directly into the XXH3 hasher without allocating a `String`. A `pending_space` / `emitted` state machine collapses whitespace runs and suppresses leading/trailing whitespace on-the-fly. Spaces inside SQL string literals (`'…'`) are never collapsed or removed. `strip_sql_comments` (which returned a `Cow<str>`) has been removed; the old string-comparison unit tests have been rewritten as hash-equality assertions.

26. **`has_config_changed` reads live config internally** — The method signature changed from `has_config_changed(&self, new_config: &CacheConfig) -> bool` to `has_config_changed(&self) -> bool`. Each implementation reads `config()` directly. For Redis, only `redis.url` is compared (not the full `RedisConfig`): `cache_key_prefix` and other runtime settings are read from live config on every call and do not require a storage rebuild.

27. **Set redis query timeout from config** — `RedisConfig` gains `operation_timeout: NonZeroU64` (default `2` seconds). The `REDIS_OPERATION_TIMEOUT` compile-time constant is removed. All `tokio::time::timeout` calls in `storage/redis.rs` (init, GET, SET) read `config().config.general.cache.redis.operation_timeout` from live config on every invocation. `RedisCacheStorage` no longer stores the full `CacheConfig`; it stores only `url: String` for change-detection — all other settings (`cache_key_prefix`, `operation_timeout`) are fetched from live config on each call, so they take effect immediately without a storage rebuild. Schema updated with the new field.

28. **Wait for initial Redis connection** — `Cache::new()` and `RedisCacheStorage::new()` are now async. `RedisCacheStorage::new()` spawns the connection task and waits up to `operation_timeout` ms for it to complete. If the timeout expires, the task continues in background without cancellation. This prevents the first query from immediately failing with `ConnectionFailed`. The wait applies to both initial startup (via `tokio::sync::OnceCell`) and config hotswap rebuilds. `cache()` function is now async; `reconnect()` remains fire-and-forget (no waiting).

29. **Changed `has_config_changed` to `is_actual`** — config can be changed, but that doesn't mean, that cache storage should be rebuilt. And the function doesn't actually say if config has changed. It says if some specific parameters have changed. So `is_actual` is more correct.

30. **Position-agnostic cache directive parsing** — `CacheDirective` refactored from an enum (`Cache { ttl }`, `ForceCache { ttl }`, `NoCache`) to a flat struct `{ mode: Option<CacheMode>, ttl_seconds: Option<u64> }` where `CacheMode` is a sub-enum (`Cache`, `ForceCache`, `NoCache`). `CacheDirective::parse()` tokenizes space-separated arguments in any order: `cache`, `force_cache`, `no_cache`, `ttl=N`. Unknown tokens are silently ignored for forward compatibility (e.g., future `hash_key=foo` argument). Field-by-field resolution: comment directive `.or()` merges with parameter directive, then global config. This means `/* pgdog_cache: ttl=60 */` (mode absent) can combine with `SET pgdog.cache = 'cache'` (TTL absent) to produce `mode=Cache, ttl=60`. Module renamed from `policy.rs` to `directive.rs`. `CacheDecision` enum removed; resolution happens inline in `integration.rs` `cache_check()`. Regex in `comment.rs` simplified to capture the raw body after `pgdog_cache:` up to the next `pgdog_\w+:` directive or end-of-comment, then delegates parsing to `CacheDirective::parse()`.

---

## What's Left To Do

1. **Redis disconnect/reconnect under heavy load** — The reconnection logic works, but timing edge cases under rapid disconnect/reconnect cycles still need stress-testing.

2. **Add hint for query hash key**

3. **Add config flag for mandatory availability of cache storage** — query will fail with error if Redis (or another cache storage) is unavailable.

4. **Split `integration.rs` into several modules** — it is too big.

# Tests

## Running unit tests

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
