use lru::LruCache;
use once_cell::sync::Lazy;
use pg_raw_parse::normalize::normalize;
use std::collections::HashMap;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use std::sync::Arc;
use tracing::debug;

use super::super::{Error, Route};
use super::{super::parse_edge_comment, Ast, AstContext, AstQuery};
use crate::frontend::{BufferedQuery, PreparedStatements};

static CACHE: Lazy<Cache> = Lazy::new(Cache::new);

/// Cache statistics.
#[derive(Default, Debug, Clone, Copy)]
pub struct Stats {
    /// Cache hits.
    pub hits: usize,
    /// Cache misses (new queries).
    pub misses: usize,
    /// Direct shard queries.
    pub direct: usize,
    /// Multi-shard queries.
    pub multi: usize,
    /// Parse time.
    pub parse_time: Duration,
    /// Fingerprints calculated.
    pub fingerprints: usize,
}

impl Stats {
    /// Create new statistics record for an AST entry.
    pub fn new() -> Self {
        Self {
            hits: 1,
            ..Default::default()
        }
    }
}

/// Query cache entry: the AST plus bookkeeping for the memory budget
/// (`size`) and the idle-expiry sweep (`accessed`).
#[derive(Debug, Clone)]
struct Entry {
    ast: Ast,
    accessed: Instant,
    size: usize,
}

/// Mutex-protected query cache.
#[derive(Debug)]
pub(super) struct Inner {
    /// Least-recently-used cache. Kept unbounded; the count and memory limits
    /// are enforced together in `enforce`.
    queries: LruCache<Arc<str>, Entry>,
    /// Maximum number of cached entries (0 = unlimited).
    count_limit: usize,
    /// Approximate memory budget in bytes (0 = unlimited).
    byte_limit: usize,
    /// Sum of `Entry::size` across all cached entries.
    bytes: usize,
    /// Idle expiry: entries untouched for longer than this are swept (None = off).
    idle_timeout: Option<Duration>,
    /// Cache global stats.
    pub(super) stats: Stats,
}

impl Inner {
    /// Insert (or replace) an entry and enforce the count and memory limits.
    /// `size` is the measured byte footprint (0 falls back to an estimate).
    fn insert(&mut self, key: Arc<str>, ast: Ast, size: usize) {
        let size = if size > 0 { size } else { ast.approx_size() };
        if let Some(old) = self.queries.put(
            key,
            Entry {
                ast,
                accessed: Instant::now(),
                size,
            },
        ) {
            self.bytes = self.bytes.saturating_sub(old.size);
        }
        self.bytes = self.bytes.saturating_add(size);
        self.enforce();
    }

    /// Evict least-recently-used entries until both limits are satisfied.
    fn enforce(&mut self) {
        while (self.count_limit > 0 && self.queries.len() > self.count_limit)
            || (self.byte_limit > 0 && self.bytes > self.byte_limit)
        {
            match self.queries.pop_lru() {
                Some((_, evicted)) => self.bytes = self.bytes.saturating_sub(evicted.size),
                None => break,
            }
        }
    }

    /// Drop entries not accessed within the idle window.
    fn sweep(&mut self) {
        let Some(idle_timeout) = self.idle_timeout else {
            return;
        };
        let now = Instant::now();
        // Two-pass: the LRU cache can't be mutated while iterating. `collect`
        // into an empty Vec does not allocate, so a sweep with nothing idle
        // (the common case) is allocation-free.
        let stale: Vec<Arc<str>> = self
            .queries
            .iter()
            .filter(|(_, e)| now.duration_since(e.accessed) >= idle_timeout)
            .map(|(k, _)| k.clone())
            .collect();
        for key in stale {
            if let Some(e) = self.queries.pop(&key) {
                self.bytes = self.bytes.saturating_sub(e.size);
            }
        }
    }
}

/// Measure the net bytes a build closure keeps allocated, using jemalloc's
/// per-thread allocation counters. The parse is synchronous (no `.await`
/// between the reads), so the delta is attributable to the entry it builds.
/// Returns 0 when the counters are unavailable, so callers fall back to the
/// `approx_size` estimate.
#[cfg(all(not(test), not(target_env = "msvc")))]
fn measure_build<T>(f: impl FnOnce() -> T) -> (T, usize) {
    use tikv_jemalloc_ctl::thread::{allocatedp, allocatedp_mib, deallocatedp, deallocatedp_mib};
    // Cache the MIBs once so each measurement skips the name-to-MIB lookup.
    static ALLOCATED: Lazy<Option<allocatedp_mib>> = Lazy::new(|| allocatedp::mib().ok());
    static DEALLOCATED: Lazy<Option<deallocatedp_mib>> = Lazy::new(|| deallocatedp::mib().ok());
    let net = || -> Option<u64> {
        let a = ALLOCATED.as_ref()?.read().ok()?.get();
        let d = DEALLOCATED.as_ref()?.read().ok()?.get();
        Some(a.wrapping_sub(d))
    };
    match net() {
        Some(before) => {
            let r = f();
            let after = net().unwrap_or(before);
            // `deallocated` counts frees of memory allocated on other threads,
            // so the delta can be negative under a work-stealing runtime.
            let delta = after.wrapping_sub(before) as i64;
            (r, delta.max(0) as usize)
        }
        None => (f(), 0),
    }
}

#[cfg(any(test, target_env = "msvc"))]
fn measure_build<T>(f: impl FnOnce() -> T) -> (T, usize) {
    (f(), 0)
}

/// AST cache.
#[derive(Clone, Debug)]
pub struct Cache {
    inner: Arc<Mutex<Inner>>,
}

impl Cache {
    /// Create new cache. Should only be done once at pooler startup.
    fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner {
                queries: LruCache::unbounded(),
                count_limit: 0,
                byte_limit: 0,
                bytes: 0,
                idle_timeout: None,
                stats: Stats::default(),
            })),
        }
    }

    /// Apply cache limits from configuration, evicting anything over the new
    /// caps. A `count`, `bytes`, or `idle_timeout_ms` of 0 disables that limit.
    pub fn configure(count: usize, bytes: usize, idle_timeout_ms: usize) {
        let mut guard = CACHE.inner.lock();
        guard.count_limit = count;
        guard.byte_limit = bytes;
        guard.idle_timeout = if idle_timeout_ms == 0 {
            None
        } else {
            Some(Duration::from_millis(idle_timeout_ms as u64))
        };
        guard.enforce();
        debug!(
            "ast cache limits: count={} bytes={} idle_timeout={}ms",
            count, bytes, idle_timeout_ms
        );
    }

    /// Resize cache to a count capacity, keeping the memory and idle limits.
    pub fn resize(capacity: usize) {
        let mut guard = CACHE.inner.lock();
        guard.count_limit = capacity.max(1);
        guard.enforce();
        debug!("ast cache size set to {}", capacity);
    }

    /// Run the idle-expiry sweep. Called periodically by maintenance.
    pub fn sweep() {
        CACHE.inner.lock().sweep();
    }

    /// Handle parsing a query.
    pub fn query(
        &self,
        query: &BufferedQuery,
        ctx: &AstContext<'_>,
        prepared_statements: &mut PreparedStatements,
    ) -> Result<Ast, Error> {
        match query {
            BufferedQuery::Prepared(_) => self.parse(query, ctx, prepared_statements),
            BufferedQuery::Query(_) => self.simple(query, ctx, prepared_statements),
        }
    }

    /// Parse a statement by either getting it from cache
    /// or parsing it.
    ///
    /// N.B. There is a race here that allows multiple threads to
    /// parse the same query. That's better imo than locking the data structure
    /// while we parse the query.
    fn parse(
        &self,
        query: &BufferedQuery,
        ctx: &AstContext<'_>,
        prepared_statements: &mut PreparedStatements,
    ) -> Result<Ast, Error> {
        // Separate query from comment, if one is present.
        let query_and_comment = parse_edge_comment(query.query(), &ctx.sharding_schema)?;
        {
            let mut guard = self.inner.lock();
            let now = Instant::now();
            let ast = guard.queries.get_mut(query_and_comment.query).map(|entry| {
                entry.accessed = now;
                entry.ast.stats.lock().hits += 1; // No contention on this.
                entry.ast.clone()
            });
            if let Some(mut ast) = ast {
                guard.stats.hits += 1;
                ast.comment_role = query_and_comment.role;
                ast.comment_shard = query_and_comment.shard;
                ast.comment_sharding_key = query_and_comment.sharding_key;

                return Ok(ast);
            }
        }

        // Parse query without holding lock, measuring the entry's footprint.
        let (built, size) = measure_build(|| {
            Ast::with_context(
                &AstQuery {
                    original_query: query,
                    query_without_comment: query_and_comment.query,
                },
                ctx,
                prepared_statements,
            )
        });
        let mut entry = built?;
        entry.comment_role = query_and_comment.role;
        entry.comment_shard = query_and_comment.shard;
        entry.comment_sharding_key = query_and_comment.sharding_key;

        let parse_time = entry.stats.lock().parse_time;

        let mut guard = self.inner.lock();
        // Don't cache when a shard comment routed the query AND a rewrite
        // was applied: the cache key is the comment-stripped body, so a
        // subsequent uncommented lookup would hit this entry and receive an
        // already-rewritten plan that was built against the commented
        // (direct-shard) variant.
        let cacheable = entry.comment_shard.is_none() || entry.rewrite_plan.is_empty();
        if cacheable {
            guard.insert(entry.query_without_comment.clone(), entry.clone(), size);
        }
        guard.stats.misses += 1;
        guard.stats.parse_time += parse_time;

        Ok(entry)
    }

    /// Parse and rewrite a statement but do not store it in the cache,
    /// because it may contain parameter values.
    fn simple(
        &self,
        query: &BufferedQuery,
        ctx: &AstContext<'_>,
        prepared_statements: &mut PreparedStatements,
    ) -> Result<Ast, Error> {
        let query_and_comment = parse_edge_comment(query.query(), &ctx.sharding_schema)?;

        let mut entry = Ast::with_context(
            &AstQuery {
                original_query: query,
                query_without_comment: query_and_comment.query,
            },
            ctx,
            prepared_statements,
        )?;
        entry.cached = false;
        entry.comment_role = query_and_comment.role;
        entry.comment_shard = query_and_comment.shard;
        entry.comment_sharding_key = query_and_comment.sharding_key;

        let parse_time = entry.stats.lock().parse_time;

        let mut guard = self.inner.lock();
        guard.stats.misses += 1;
        guard.stats.parse_time += parse_time;
        Ok(entry)
    }

    pub(crate) fn record(&self, query: &str) -> Result<Ast, Error> {
        {
            let mut guard = self.inner.lock();
            if let Some(entry) = guard.queries.get_mut(query) {
                entry.accessed = Instant::now();
                entry.ast.stats.lock().hits += 1;
                return Ok(entry.ast.clone());
            }
        }

        let (built, size) = measure_build(|| Ast::new_record(query));
        let entry = built?;

        let mut guard = self.inner.lock();
        guard.insert(query.into(), entry.clone(), size);
        guard.stats.misses += 1;

        Ok(entry)
    }

    /// Record a query sent over the simple protocol, while removing parameters.
    ///
    /// Used by dry run mode to keep stats on what queries are routed correctly,
    /// and which are not.
    ///
    pub fn record_normalized(&self, query: &str, route: &Route) -> Result<(), Error> {
        let normalized = normalize(query)?;

        {
            let mut guard = self.inner.lock();
            let now = Instant::now();
            if let Some(entry) = guard.queries.get_mut(normalized.as_str()) {
                entry.accessed = now;
                entry.ast.update_stats(route);
                guard.stats.hits += 1;
                return Ok(());
            }
        }

        let (built, size) = measure_build(|| Ast::new_record(&normalized));
        let entry = built?;
        entry.update_stats(route);

        let mut guard = self.inner.lock();
        guard.insert(normalized.into(), entry, size);
        guard.stats.misses += 1;

        Ok(())
    }

    /// Get global cache instance.
    pub fn get() -> Self {
        CACHE.clone()
    }

    /// Get cache stats.
    pub fn stats() -> (Stats, usize) {
        let cache = Self::get();
        let (len, query_stats, mut stats) = {
            let guard = cache.inner.lock();
            (
                guard.queries.len(),
                guard
                    .queries
                    .iter()
                    .map(|c| *c.1.ast.stats.lock())
                    .collect::<Vec<_>>(),
                guard.stats,
            )
        };
        for stat in query_stats {
            stats.direct += stat.direct;
            stats.multi += stat.multi;
        }
        (stats, len)
    }

    /// Get a copy of all queries stored in the cache.
    pub fn queries() -> HashMap<Arc<str>, Ast> {
        Self::get()
            .inner
            .lock()
            .queries
            .iter()
            .map(|i| (i.0.clone(), i.1.ast.clone()))
            .collect()
    }

    /// Reset cache, removing all statements and setting stats to 0. The
    /// configured count/memory/idle limits are kept.
    pub fn reset() {
        let cache = Self::get();
        let mut guard = cache.inner.lock();
        guard.queries.clear();
        guard.bytes = 0;
        guard.stats.hits = 0;
        guard.stats.misses = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A minimal valid AST entry; its content is irrelevant to the limit
    /// logic, which is driven by the explicit `size` passed to `insert`.
    fn ast() -> Ast {
        Ast::new_record("SELECT 1").expect("parse")
    }

    fn inner(count_limit: usize, byte_limit: usize, idle_timeout: Option<Duration>) -> Inner {
        Inner {
            queries: LruCache::unbounded(),
            count_limit,
            byte_limit,
            bytes: 0,
            idle_timeout,
            stats: Stats::default(),
        }
    }

    #[test]
    fn count_limit_caps_and_evicts_lru() {
        let mut c = inner(3, 0, None);
        for i in 0..5 {
            c.insert(format!("q{i}").into(), ast(), 10);
        }
        assert_eq!(c.queries.len(), 3);
        assert!(c.queries.peek("q4").is_some(), "newest kept");
        assert!(c.queries.peek("q2").is_some());
        assert!(c.queries.peek("q1").is_none(), "oldest evicted");
        assert_eq!(c.bytes, 30, "byte total tracks surviving entries");
    }

    #[test]
    fn count_limit_zero_is_unlimited() {
        let mut c = inner(0, 0, None);
        for i in 0..1000 {
            c.insert(format!("q{i}").into(), ast(), 1);
        }
        assert_eq!(c.queries.len(), 1000);
        assert_eq!(c.bytes, 1000);
    }

    #[test]
    fn byte_limit_caps_and_evicts_lru() {
        let mut c = inner(0, 25, None);
        for i in 0..5 {
            c.insert(format!("q{i}").into(), ast(), 10);
        }
        assert!(c.bytes <= 25, "stays within byte budget");
        assert_eq!(c.queries.len(), 2);
        assert!(c.queries.peek("q4").is_some(), "newest kept");
        assert!(c.queries.peek("q0").is_none(), "oldest evicted");
    }

    #[test]
    fn entry_larger_than_byte_budget_is_not_cached() {
        let mut c = inner(0, 25, None);
        c.insert("big".into(), ast(), 100);
        assert_eq!(
            c.queries.len(),
            0,
            "an entry over the whole budget is evicted"
        );
        assert_eq!(c.bytes, 0);
    }

    #[test]
    fn replacing_a_key_updates_byte_total() {
        let mut c = inner(0, 0, None);
        c.insert("q".into(), ast(), 10);
        assert_eq!(c.bytes, 10);
        c.insert("q".into(), ast(), 30);
        assert_eq!(c.queries.len(), 1);
        assert_eq!(c.bytes, 30, "old size subtracted, new size added");
    }

    #[test]
    fn byte_total_saturates_instead_of_overflowing() {
        let mut c = inner(0, 0, None);
        c.insert("q1".into(), ast(), usize::MAX);
        c.insert("q2".into(), ast(), usize::MAX);
        assert_eq!(c.bytes, usize::MAX);
        assert_eq!(c.queries.len(), 2);
    }

    #[test]
    fn zero_size_falls_back_to_query_length() {
        let mut c = inner(0, 0, None);
        c.insert("q".into(), ast(), 0);
        assert_eq!(
            c.bytes,
            "SELECT 1".len(),
            "record entries carry their query text"
        );
    }

    #[test]
    fn no_limits_keeps_everything() {
        let mut c = inner(0, 0, None);
        for i in 0..50 {
            c.insert(format!("q{i}").into(), ast(), 7);
        }
        assert_eq!(c.queries.len(), 50);
        assert_eq!(c.bytes, 350);
    }

    #[test]
    fn sweep_is_noop_when_idle_timeout_disabled() {
        let mut c = inner(0, 0, None);
        for i in 0..3 {
            c.insert(format!("q{i}").into(), ast(), 5);
        }
        c.sweep();
        assert_eq!(c.queries.len(), 3);
        assert_eq!(c.bytes, 15);
    }

    #[test]
    fn sweep_keeps_fresh_entries() {
        // Idle window far larger than the entries' age: nothing is idle yet.
        let mut c = inner(0, 0, Some(Duration::from_secs(3600)));
        for i in 0..3 {
            c.insert(format!("q{i}").into(), ast(), 5);
        }
        c.sweep();
        assert_eq!(c.queries.len(), 3);
        assert_eq!(c.bytes, 15);
    }

    #[test]
    fn sweep_drops_idle_entries_and_updates_bytes() {
        // Zero idle window: every entry is at or past it.
        let mut c = inner(0, 0, Some(Duration::ZERO));
        for i in 0..3 {
            c.insert(format!("q{i}").into(), ast(), 5);
        }
        c.sweep();
        assert_eq!(c.queries.len(), 0);
        assert_eq!(c.bytes, 0);
    }

    #[test]
    fn configure_and_resize_apply_limits_to_global_cache() {
        // The cache is a process-wide singleton, so save the live limits and
        // put them back at the end; assertions on entry counts are `<=` since
        // other tests may share the cache.
        let (count, bytes, idle) = {
            let guard = CACHE.inner.lock();
            (guard.count_limit, guard.byte_limit, guard.idle_timeout)
        };
        let keys: Vec<Arc<str>> = (0..5).map(|i| format!("__configure_q{i}").into()).collect();

        Cache::configure(3, 500, 30_000);
        {
            let mut guard = CACHE.inner.lock();
            assert_eq!(guard.count_limit, 3);
            assert_eq!(guard.byte_limit, 500);
            assert_eq!(guard.idle_timeout, Some(Duration::from_millis(30_000)));
            for key in &keys {
                guard.insert(key.clone(), ast(), 10);
            }
            assert!(guard.queries.len() <= 3, "configure() caps are enforced");
        }

        Cache::resize(1);
        {
            let guard = CACHE.inner.lock();
            assert_eq!(guard.count_limit, 1);
            assert!(
                guard.queries.len() <= 1,
                "resize() evicts down to the new capacity"
            );
        }

        // Zero means "unlimited" in configure(), but resize() keeps at least
        // one entry.
        Cache::resize(0);
        assert_eq!(CACHE.inner.lock().count_limit, 1);

        Cache::configure(
            count,
            bytes,
            idle.map(|d| d.as_millis() as usize).unwrap_or(0),
        );
        {
            let mut guard = CACHE.inner.lock();
            assert_eq!(
                guard.idle_timeout, idle,
                "restored, including the None branch"
            );
            for key in &keys {
                if let Some(entry) = guard.queries.pop(key.as_ref()) {
                    guard.bytes = guard.bytes.saturating_sub(entry.size);
                }
            }
        }
    }
}
