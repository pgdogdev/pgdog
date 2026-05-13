# Cache for pgdog — State of Implementation

## Architecture

Cache SELECT queries in Redis, bypass PostgreSQL on cache hit, populate cache on cache miss. Two-tier policy resolution: SQL comment/connection parameter → pgdog's config.

---

## Implementation

### Configuration (`pgdog-config`)

**`cache.rs`** — Cache configuration types:

**CachePolicy enum:** `NoCache` (default), `Cache`. Implements `FromStr`, `Display`, `Serialize`, `Deserialize`, `Copy`, `JsonSchema`.

**Cache struct:**
- `enabled: bool` — is caching on?
- `policy: CachePolicy` — which policy?
- `ttl: u64` — default TTL seconds (default 300)
- `redis_url: String` — Redis connection URL
- `max_result_size: usize` — max cached result bytes

**`general.rs`** — `General` struct holds `cache: Cache` field. **Cache config is global.**

**`lib.rs`** — Exports `pub mod cache;` and `pub use cache::{CachePolicy, Cache};`.

### Cache Module (`pgdog/src/frontend/cache/`)

**`mod.rs`** — Module exports, global singleton, and main `Cache` struct:
```rust
pub mod client;
pub mod context;
pub mod integration;
pub mod policy;

pub use client::CacheClient;
pub use context::CacheContext;
pub use integration::CacheCheckResult;
pub use policy::CacheDecision;
```

`Cache` struct wraps: `CacheClient`.

**Global singleton:** Cache is global-scoped, not connection-scoped. Accessed via `cache()` function which returns `Arc<Cache>` from a `Lazy<Arc<Cache>>` static. `Cache::new()` reads config internally — no parameters needed.

Key methods:
- `new()` — creates client (reads config internally)
- `try_read_cache(cache_context, in_transaction, client_request, params, stream)` — calls `cache_check()`, handles HIT/MISS/PASS-through
- `save_response_in_cache(cache_context)` — finalizes by storing the captured response

**`client.rs`** — Redis client wrapper using `fred` v9:
- `CacheClient::new()` — builds client from global `config().config.general.cache`, returns disabled stub if no config/URL
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
- `send_cached_response()` — deserializes wire-format bytes and sends to client
- `cache_response()` — serializes `Vec<Message>` into wire bytes and stores in Redis
- Cache key: XXH3 hash of `database_name + raw_query_string`

### Query Engine Integration

**`pgdog/src/frontend/client/query_engine/mod.rs`**
- Imports global `cache()` from `frontend::cache`
- `handle()` flow: after `route_query()` and before `before_execution()`, calls `cache().try_read_cache(context)`. If HIT: sends cached response and returns. On MISS: stores state in `context.cache_context`.
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
| Cache HIT flow | Deserialize wire bytes → parse messages → send to client → return `Ok(true)` |
| Cache MISS flow | Normal execute → capture response via `CacheContext` → store in Redis → respond |
| Cache key | XXH3 hash of `database_name + raw_query_string` |
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
-- Query hash computed as if comment were like "/* pgdog_cache: cache */"
/* pgdog_cache: force_cache */
SELECT * FROM products WHERE category = 'electronics';

-- Force cache with custom TTL in seconds
-- Query hash computed as if comment were like "/* pgdog_cache: cache ttl=300*/"
/* pgdog_cache: force_cache ttl=300 */
SELECT * FROM orders;
```

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

2. **Redis GET fails on NULL / cache miss** - Problem: `client.get::<bytes::Bytes>()` throws `Parse Error: Cannot parse into bytes` when the key doesn't exist. Fix: Use `client.get::<RedisValue, _>()` and check `val.is_null()` before extracting bytes.

3. **Wire format deserialization wrong in send_cached_response** - Problem: PostgreSQL wire message structure is `[1B code][4B length]` where length includes the 4B itself. I calculated `offset + 5 + msg_len` (treating length as payload-only), causing incorrect byte slicing. Fix: Corrected to `offset + 1 + msg_len`.

4. **Route incorrectly reports read-only as write when parser is disabled** - Problem: `query_parser_bypass()` conservatively returns `Route::write()` for all SQL when the query parser is disabled. Since pgdog doesn't enable the parser by default for simple queries, `route.is_read()` was false for `SELECT 1`. Fix: When any database has `cache.enabled = true`, the query parser level is auto-upgraded to `On` in the cluster config. The `|| self.cache_enabled()` check in `cluster.rs:475` forces the parser on. Cache also emits a startup warning if parser is `Off` or `SessionControl`. The old `is_likely_read()` string-prefix heuristic has been removed entirely.

5. **DB cache config defaults** - Observation: `Cache.policy` defaults to `CachePolicy::NoCache`. Even with `enabled = true`, caching is skipped unless policy is explicitly set. User action taken: Add `policy = "cache"` to pgdog.toml.

6. **Query parser auto-upgrade for caching** — When caching is enabled and parser is `Auto`/`Off`/`SessionControl`, the parser is forced to `On` via `|| self.cache_enabled()` check in `cluster.rs`. A startup warning is emitted in `core.rs` if parser remains incompatible.

7. **Decoupled cache policy extraction** — Cache directives extracted via standalone regex in `cache/policy.rs`, works regardless of parser state. Supports `/* pgdog_cache: ... */` format with optional `ttl=` parameter. Unified with sharding hints via `comment()` function in `comment.rs`.

8. **Error handling / Reconnection** — Automatic reconnection with background task, CAS-guarded single reconnect, 2s operation timeout on all Redis calls, PING-based connection verification.

9. **Cache key collision across databases sharing one Redis** — Database name and raw query string are combined via a single XXH3 hash call, producing deterministic, collision-resistant per-database keys even on shared Redis. Different literal values in queries produce different cache keys. `force_cache` hints normalize the query in the hash to use the same key as regular `cache`.

10. **Wire format serialization/deserialization** — PostgreSQL wire messages stored as raw bytes. Correct byte slice calculation: `offset + 1 + msg_len`.

11. **Do not cache error responses**.

12. **Setting pgdog.cache via connection url doesn't work** — now works.

13. **Moved all cache-related structs from QueryEngine to Client** — now all cache structs including redis client are creating for whole pgdog's lifetime.

14. **Use built-in query comment hints** — Cache hints (`pgdog_cache:`) are now extracted alongside sharding hints (`pgdog_shard:`, `pgdog_sharding_key:`, `pgdog_role:`) via the unified `comment()` function in `comment.rs`. The `comment_cache` field is stored in `AstInner` and accessed during cache checking via `client_request.ast.comment_cache`. Policy resolution simplified: trait-based extractors replaced with free functions (`resolve()`, `get_cache_directive()`, `extract_parameter_directive()`). Comment hint (from AST) has priority over connection parameter `pgdog.cache`. `Cache` struct no longer needs `policy_dispatcher` field. Parameter format unified to `no_cache` (underscore, not dash).

15. **Add cache config to .schema**.

16. **Force-cache hint support** — `/* pgdog_cache: force_cache */` and `/* pgdog_cache: force_cache ttl=N */` directives always attempt to cache (cache key normalized), bypassing normal cache miss flow considerations.

---

## What's Left To Do

1. **Response capture for prepared statements** — Extended protocol (Parse/Bind/Execute) response capture works through process_server_message() but hasn't been tested with PREPARE/EXECUTE. (Note: pgdog implements prepared statements caching. But unknown what kind of caching this is: just query cache or result cache. And if we implement our cache, will this break this prepared statement cache?)

2. **Redis disconnect/reconnect under heavy load** — The reconnection logic works, but the fast-path check (`ensure_connected`) and the reconnect task can have timing edge cases under rapid disconnect/reconnect cycles. Need to stress-test. 

3. **Integration tests** — Tests live in `integration/rust/tests/integration/`. Redis must be running on 127.0.0.1:6379 before tests. Run with: `cd integration/rust && cargo nextest run --no-fail-fast --test-threads=1`

4. **Magic numbers in send_cached_response()**.

5. **Provide config hotswap**.

6. **Review and rewrite CacheClient**.

### Planned Tests

1. **Database key namespace collision** — Two databases sharing one Redis, both running same query but with different underlying PG data. Verify correct isolation.
2. **Basic cache hit/miss** — Run a SELECT once (expect miss), run again (expect hit), verify metrics.
3. **TTL expiration** — Cache a query with short TTL, wait for expiry, verify miss on third call.
4. **Write bypasses cache** — Execute INSERT/UPDATE/DELETE, verify these do not populate or consume the cache.
5. **Redis unavailable** — Stop Redis mid-flight, verify queries pass through to PG without blocking.
6. **Redis reconnection** — Restart Redis after disconnect, verify cache recovers automatically.