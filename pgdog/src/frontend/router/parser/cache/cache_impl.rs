use lru::LruCache;
use once_cell::sync::Lazy;
use pg_raw_parse::normalize::normalize;
use std::collections::HashMap;
use std::time::Duration;

use parking_lot::RwLock;
use pg_raw_parse::{Error as ParseError, deparse, nodes};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
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

/// Query cache, read by all clients at once.
#[derive(Debug)]
pub(super) struct Inner {
    /// Least-recently-used cache.
    queries: LruCache<Arc<str>, Arc<Ast>>,
    /// Cache global stats.
    pub(super) stats: Stats,
    /// Cache hits.
    hits: AtomicUsize,
}

impl Inner {
    /// Insert a statement, evicting one if the cache is full.
    /// Eviction follows the CLOCK (second-chance) policy:
    ///
    /// - cache hits mark the statement as used (instead of moving the statement to front of list)
    ///   allowing hits to just need the read lock
    ///
    /// - when the cache is full, used statements at the back of the list move to the front with the mark
    ///   cleared, and when the first **unmarked** statement is found, it's evicted.
    fn insert(&mut self, query: Arc<str>, ast: Arc<Ast>) {
        if self.queries.len() == self.queries.cap().get() {
            while let Some((oldest, entry)) = self.queries.peek_lru()
                && entry.used.swap(false, Ordering::Relaxed)
            {
                let oldest = Arc::clone(oldest);
                self.queries.promote(&oldest);
            }
        }
        self.queries.put(query, ast);
    }
}

/// Statements parsed without being cached.
#[derive(Debug, Default)]
struct Uncached {
    parses: AtomicUsize,
    parse_time_nanos: AtomicU64,
}

/// AST cache shared through a static reference.
#[derive(Debug)]
pub(crate) struct Cache {
    inner: RwLock<Inner>,
    uncached: Uncached,
}

impl Cache {
    /// Create new cache. Should only be done once at pooler startup.
    fn new() -> Self {
        Self {
            inner: RwLock::new(Inner {
                queries: LruCache::unbounded(),
                stats: Stats::default(),
                hits: AtomicUsize::default(),
            }),
            uncached: Uncached::default(),
        }
    }

    /// Resize cache to capacity, evicting any statements exceeding the capacity.
    ///
    /// Minimum capacity is 1.
    pub(crate) fn resize(capacity: usize) {
        let capacity = if capacity == 0 { 1 } else { capacity };

        CACHE
            .inner
            .write()
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
        let query_and_comment = parse_edge_comment(query.query(), ctx.sharding_schema)?;
        let ast = {
            let guard = self.inner.read();

            let ast = guard.queries.peek(query_and_comment.query).map(|entry| {
                entry.record_hit();
                Ok::<_, Error>(Arc::clone(entry))
            });

            if ast.is_some() {
                guard.hits.fetch_add(1, Ordering::Relaxed);
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

            let parse_time = ast.stats.parse_time;

            let mut guard = self.inner.write();
            guard.insert(ast.query_without_comment.clone(), Arc::clone(&ast));
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
        let query_and_comment = parse_edge_comment(query.query(), ctx.sharding_schema)?;

        let ast = Arc::new(Ast::parse_and_rewrite(
            &AstQuery {
                original_query: query,
                query_without_comment: query_and_comment.query,
            },
            ctx,
            prepared_statements,
        )?);

        self.uncached.parses.fetch_add(1, Ordering::Relaxed);
        self.uncached
            .parse_time_nanos
            .fetch_add(ast.stats.parse_time.as_nanos() as u64, Ordering::Relaxed);

        Ok(ClientQuery {
            cached: false,
            comment: Arc::new(query_and_comment.comment),
            ast,
        })
    }

    pub(crate) fn record(&self, query: &str) -> Result<ClientQuery, ParseError> {
        let ast = {
            let mut guard = self.inner.write();
            guard.queries.get(query).map(|ast| {
                ast.record_hit();
                Ok::<_, ParseError>(Arc::clone(ast))
            })
        }
        .unwrap_or_else(|| {
            let ast = Arc::new(Ast::parse(query)?);

            let mut guard = self.inner.write();
            guard.insert(query.into(), Arc::clone(&ast));
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
            let mut guard = self.inner.write();
            if let Some(entry) = guard.queries.get(normalized) {
                entry.update_stats(route);
                guard.hits.fetch_add(1, Ordering::Relaxed);

                return Ok(());
            }
        }

        let entry = Ast::parse(normalized)?;
        entry.update_stats(route);

        let mut guard = self.inner.write();
        guard.insert(normalized.into(), Arc::new(entry));
        guard.stats.misses += 1;

        Ok(())
    }

    /// Get global cache instance.
    pub(crate) fn get() -> &'static Self {
        &CACHE
    }

    /// Get cache stats.
    pub(crate) fn stats() -> (Stats, usize) {
        let cache = Self::get();
        let (len, query_stats, mut stats) = {
            let guard = cache.inner.read();
            (
                guard.queries.len(),
                guard
                    .queries
                    .iter()
                    .map(|c| c.1.stats())
                    .collect::<Vec<_>>(),
                Stats {
                    hits: guard.hits.load(Ordering::Relaxed),
                    ..guard.stats
                },
            )
        };
        stats.misses += cache.uncached.parses.load(Ordering::Relaxed);
        stats.parse_time +=
            Duration::from_nanos(cache.uncached.parse_time_nanos.load(Ordering::Relaxed));
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
            .read()
            .queries
            .iter()
            .map(|i| (i.0.clone(), i.1.clone()))
            .collect()
    }

    /// Reset cache, removing all statements
    /// and setting stats to 0.
    pub(crate) fn reset() {
        let cache = Self::get();
        let mut guard = cache.inner.write();
        guard.queries.clear();
        guard.hits.store(0, Ordering::Relaxed);
        guard.stats.misses = 0;
        cache.uncached.parses.store(0, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod test {
    use super::*;

    /// The cache here has a size of 2. We insert 1, 2, record a hit on 1, then insert 3.
    /// 2 should be evicted instead of 1, since we used 1 after 2.
    #[test]
    fn test_used_statement_survives_eviction() {
        let mut inner = Inner {
            queries: LruCache::new(2.try_into().unwrap()),
            stats: Stats::default(),
            hits: AtomicUsize::default(),
        };

        let used = Arc::new(Ast::parse("SELECT 1").unwrap());

        inner.insert("SELECT 1".into(), Arc::clone(&used));
        inner.insert("SELECT 2".into(), Arc::new(Ast::parse("SELECT 2").unwrap()));
        used.record_hit();
        inner.insert("SELECT 3".into(), Arc::new(Ast::parse("SELECT 3").unwrap()));

        assert!(inner.queries.contains("SELECT 1"));
        assert!(!inner.queries.contains("SELECT 2"));
    }
}
