use lru::LruCache;
use once_cell::sync::Lazy;
use pg_raw_parse::normalize::normalize;
use std::collections::HashMap;
use std::time::Duration;

use parking_lot::Mutex;
use pg_raw_parse::{Error as ParseError, deparse, nodes};
use std::sync::Arc;
use tracing::debug;

use super::super::{Error, Route};
use super::{super::parse_edge_comment, Ast, AstContext, AstQuery, ClientQuery};
use crate::frontend::{BufferedQuery, PreparedStatements};

static CACHE: Lazy<Cache> = Lazy::new(Cache::new);

/// Cache statistics.
#[derive(Default, Debug, Clone, Copy)]
pub(crate) struct Stats {
    /// Cache hits.
    pub(crate) hits: usize,
    /// Cache misses (new queries).
    pub(crate) misses: usize,
    /// Direct shard queries.
    pub(crate) direct: usize,
    /// Multi-shard queries.
    pub(crate) multi: usize,
    /// Parse time.
    pub(crate) parse_time: Duration,
    /// Fingerprints calculated.
    pub(crate) fingerprints: usize,
    pub(crate) memory_allocated: usize,
}

impl Stats {
    /// Create new statistics record for an AST entry.
    pub(crate) fn new() -> Self {
        Self {
            hits: 1,
            ..Default::default()
        }
    }
}

/// Mutex-protected query cache.
#[derive(Debug)]
pub(super) struct Inner {
    /// Least-recently-used cache.
    queries: LruCache<Arc<str>, Arc<Ast>>,
    /// Cache global stats.
    pub(super) stats: Stats,
}

/// AST cache.
#[derive(Clone, Debug)]
pub(crate) struct Cache {
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
    pub(crate) fn resize(capacity: usize) {
        let capacity = if capacity == 0 { 1 } else { capacity };

        CACHE
            .inner
            .lock()
            .queries
            .resize(capacity.try_into().unwrap());

        debug!("ast cache size set to {}", capacity);
    }

    /// Handle parsing a query.
    pub(crate) fn query(
        &self,
        query: &BufferedQuery,
        ctx: &AstContext<'_>,
        prepared_statements: &mut PreparedStatements,
    ) -> Result<ClientQuery, Error> {
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
    ) -> Result<ClientQuery, Error> {
        // Separate query from comment, if one is present.
        let query_and_comment = parse_edge_comment(query.query(), &ctx.sharding_schema)?;
        let ast = {
            let mut guard = self.inner.lock();
            let ast = guard.queries.get(query_and_comment.query).map(|entry| {
                entry.stats.lock().hits += 1; // No contention on this.
                Ok::<_, Error>(Arc::clone(entry))
            });
            if ast.is_some() {
                guard.stats.hits += 1;
            }
            ast
        }
        .unwrap_or_else(|| {
            // Parse query without holding lock.
            let ast = Arc::new(Ast::parse_and_rewrite(
                &AstQuery {
                    original_query: query,
                    query_without_comment: query_and_comment.query,
                },
                ctx,
                prepared_statements,
            )?);
            let parse_time = ast.stats.lock().parse_time;

            let mut guard = self.inner.lock();
            // Don't cache when a shard comment routed the query AND a rewrite
            // was applied: the cache key is the comment-stripped body, so a
            // subsequent uncommented lookup would hit this entry and receive an
            // already-rewritten plan that was built against the commented
            // (direct-shard) variant.
            let cacheable =
                query_and_comment.comment.shard.is_none() || ast.rewrite_plan.is_empty();
            if cacheable {
                guard
                    .queries
                    .put(ast.query_without_comment.clone(), Arc::clone(&ast));
            }
            guard.stats.misses += 1;
            guard.stats.parse_time += parse_time;
            Ok(ast)
        })?;

        Ok(ClientQuery {
            cached: true,
            comment: Arc::new(query_and_comment.comment),
            ast,
        })
    }

    /// Parse and rewrite a statement but do not store it in the cache,
    /// because it may contain parameter values.
    fn simple(
        &self,
        query: &BufferedQuery,
        ctx: &AstContext<'_>,
        prepared_statements: &mut PreparedStatements,
    ) -> Result<ClientQuery, Error> {
        let query_and_comment = parse_edge_comment(query.query(), &ctx.sharding_schema)?;

        let ast = Arc::new(Ast::parse_and_rewrite(
            &AstQuery {
                original_query: query,
                query_without_comment: query_and_comment.query,
            },
            ctx,
            prepared_statements,
        )?);

        let mut guard = self.inner.lock();
        guard.stats.misses += 1;
        guard.stats.parse_time += ast.stats.lock().parse_time;

        Ok(ClientQuery {
            cached: false,
            comment: Arc::new(query_and_comment.comment),
            ast,
        })
    }

    pub(crate) fn record(&self, query: &str) -> Result<ClientQuery, ParseError> {
        let ast = {
            let mut guard = self.inner.lock();
            guard.queries.get(query).map(|ast| {
                ast.stats.lock().hits += 1;
                Ok::<_, ParseError>(Arc::clone(ast))
            })
        }
        .unwrap_or_else(|| {
            let ast = Arc::new(Ast::parse(query)?);
            let mut guard = self.inner.lock();
            guard.queries.put(query.into(), Arc::clone(&ast));
            guard.stats.misses += 1;
            Ok(ast)
        })?;

        Ok(ClientQuery {
            cached: true,
            comment: Default::default(),
            ast,
        })
    }

    /// Record a query sent over the simple protocol, while removing parameters.
    ///
    /// Used by dry run mode to keep stats on what queries are routed correctly,
    /// and which are not.
    ///
    pub(crate) fn record_normalized(
        &self,
        query: &nodes::RawStmt,
        route: &Route,
    ) -> Result<(), Error> {
        let normalized = normalize(query);
        let normalized = deparse(normalized.stmt())?;
        let normalized = normalized.as_str();

        {
            let mut guard = self.inner.lock();
            if let Some(entry) = guard.queries.get(normalized) {
                entry.update_stats(route);
                guard.stats.hits += 1;
                return Ok(());
            }
        }

        let entry = Ast::parse(normalized)?;
        entry.update_stats(route);

        let mut guard = self.inner.lock();
        guard.queries.put(normalized.into(), Arc::new(entry));
        guard.stats.misses += 1;

        Ok(())
    }

    /// Get global cache instance.
    pub(crate) fn get() -> Self {
        CACHE.clone()
    }

    /// Get cache stats.
    pub(crate) fn stats() -> (Stats, usize) {
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
            stats.memory_allocated += stat.memory_allocated;
        }
        (stats, len)
    }

    /// Get a copy of all queries stored in the cache.
    pub(crate) fn queries() -> HashMap<Arc<str>, Arc<Ast>> {
        Self::get()
            .inner
            .lock()
            .queries
            .iter()
            .map(|i| (i.0.clone(), i.1.clone()))
            .collect()
    }

    /// Reset cache, removing all statements
    /// and setting stats to 0.
    pub(crate) fn reset() {
        let cache = Self::get();
        let mut guard = cache.inner.lock();
        guard.queries.clear();
        guard.stats.hits = 0;
        guard.stats.misses = 0;
    }
}
