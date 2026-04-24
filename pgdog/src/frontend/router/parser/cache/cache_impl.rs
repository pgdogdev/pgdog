use lru::LruCache;
use once_cell::sync::Lazy;
use parking_lot::lock_api::MutexGuard;
use pg_query::normalize;
use pgdog_config::QueryParserEngine;
use std::borrow::Borrow;
use std::collections::HashMap;
use std::ops::Deref;
use std::time::Duration;

use parking_lot::{Mutex, RawMutex};
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

/// Newtype wrapper around `Arc<String>` that lets us look up cache entries
/// with any `&str` (e.g. a `QueryWithoutComment`, which derefs to `str`).
/// Stdlib only provides `Arc<T>: Borrow<T>`, not `Arc<String>: Borrow<str>`.
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub(super) struct CacheKey(pub(super) Arc<String>);

impl Borrow<str> for CacheKey {
    fn borrow(&self) -> &str {
        self.0.as_str()
    }
}

/// Mutex-protected query cache.
#[derive(Debug)]
pub(super) struct Inner {
    /// Least-recently-used cache.
    queries: LruCache<CacheKey, Ast>,
    /// Cache global stats.
    pub(super) stats: Stats,
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
                stats: Stats::default(),
            })),
        }
    }

    /// Resize cache to capacity, evicting any statements exceeding the capacity.
    ///
    /// Minimum capacity is 1.
    pub fn resize(capacity: usize) {
        let capacity = if capacity == 0 { 1 } else { capacity };

        CACHE
            .inner
            .lock()
            .queries
            .resize(capacity.try_into().unwrap());

        debug!("ast cache size set to {}", capacity);
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

    /// Get the inner struct.
    pub(super) fn lock<'a>(&'a self) -> MutexGuard<'a, RawMutex, Inner> {
        self.inner.lock()
    }

    /// Parse a statement by either getting it from cache
    /// or using pg_query parser.
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
        let comment_parser_result = parse_edge_comment(query.query(), &ctx.sharding_schema)?;
        let cache_key = &comment_parser_result.query;

        {
            let mut guard = self.inner.lock();
            let ast = guard.queries.get_mut(cache_key.deref()).map(|entry| {
                entry.stats.lock().hits += 1; // No contention on this.
                entry.clone()
            });
            if let Some(mut ast) = ast {
                guard.stats.hits += 1;
                ast.comment_role = comment_parser_result.role;
                ast.comment_shard = comment_parser_result.shard.clone();

                return Ok(ast);
            }
        }

        // Parse query without holding lock.
        let mut entry = Ast::with_context(
            &AstQuery {
                query,
                cache_key,
                comment_shard: comment_parser_result.shard.as_ref(),
            },
            ctx,
            prepared_statements,
        )?;
        entry.comment_role = comment_parser_result.role;
        entry.comment_shard = comment_parser_result.shard.clone();
        let parse_time = entry.stats.lock().parse_time;

        let mut guard = self.inner.lock();
        guard
            .queries
            .put(CacheKey(entry.original_query.clone()), entry.clone());
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
        let comment_parser_result = parse_edge_comment(query.query(), &ctx.sharding_schema)?;
        let cache_key = &comment_parser_result.query;

        let mut entry = Ast::with_context(
            &AstQuery {
                query,
                cache_key,
                comment_shard: comment_parser_result.shard.as_ref(),
            },
            ctx,
            prepared_statements,
        )?;
        entry.cached = false;
        entry.comment_role = comment_parser_result.role;
        entry.comment_shard = comment_parser_result.shard.clone();

        let parse_time = entry.stats.lock().parse_time;

        let mut guard = self.inner.lock();
        guard.stats.misses += 1;
        guard.stats.parse_time += parse_time;
        Ok(entry)
    }

    /// Record a query sent over the simple protocol, while removing parameters.
    ///
    /// Used by dry run mode to keep stats on what queries are routed correctly,
    /// and which are not.
    ///
    pub fn record_normalized(
        &self,
        query: &str,
        route: &Route,
        query_parser_engine: QueryParserEngine,
    ) -> Result<(), Error> {
        let normalized = normalize(query).map_err(Error::PgQuery)?;

        {
            let mut guard = self.inner.lock();
            if let Some(entry) = guard.queries.get(normalized.as_str()) {
                entry.update_stats(route);
                guard.stats.hits += 1;
                return Ok(());
            }
        }

        let entry = Ast::new_record(&normalized, query_parser_engine)?;
        entry.update_stats(route);

        let mut guard = self.inner.lock();
        guard.queries.put(CacheKey(Arc::new(normalized)), entry);
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
                    .map(|c| *c.1.stats.lock())
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
    pub fn queries() -> HashMap<Arc<String>, Ast> {
        Self::get()
            .inner
            .lock()
            .queries
            .iter()
            .map(|i| (i.0 .0.clone(), i.1.clone()))
            .collect()
    }

    /// Reset cache, removing all statements
    /// and setting stats to 0.
    pub fn reset() {
        let cache = Self::get();
        let mut guard = cache.inner.lock();
        guard.queries.clear();
        guard.stats.hits = 0;
        guard.stats.misses = 0;
    }
}
