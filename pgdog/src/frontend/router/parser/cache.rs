//! AST cache.
//!
//! Shared between all clients and databases.

use once_cell::sync::Lazy;
use pg_query::*;
use std::collections::HashMap;

use parking_lot::Mutex;
use std::sync::Arc;

static CACHE: Lazy<Cache> = Lazy::new(Cache::default);

/// AST cache statistics.
#[derive(Default, Debug, Copy, Clone)]
pub struct Stats {
    /// Cache hits.
    pub hits: usize,
    /// Cache misses (new queries).
    pub misses: usize,
}

#[derive(Debug, Clone)]
pub struct CachedAst {
    pub ast: Arc<ParseResult>,
    pub hits: usize,
}

impl CachedAst {
    fn new(ast: ParseResult) -> Self {
        Self {
            ast: Arc::new(ast),
            hits: 1,
        }
    }
}

#[derive(Default, Debug)]
struct Inner {
    queries: HashMap<String, CachedAst>,
    stats: Stats,
}

/// AST cache.
#[derive(Default, Clone, Debug)]
pub struct Cache {
    inner: Arc<Mutex<Inner>>,
}

impl Cache {
    /// Parse a statement by either getting it from cache
    /// or using pg_query parser.
    ///
    /// N.B. There is a race here that allows multiple threads to
    /// parse the same query. That's better imo than locking the data structure
    /// while we parse the query.
    pub fn parse(&mut self, query: &str) -> Result<Arc<ParseResult>> {
        {
            let mut guard = self.inner.lock();
            let ast = guard.queries.get_mut(query).map(|entry| {
                entry.hits += 1;
                entry.ast.clone()
            });
            if let Some(ast) = ast {
                guard.stats.hits += 1;
                return Ok(ast);
            }
        }

        // Parse query without holding lock.
        let entry = CachedAst::new(parse(query)?);
        let ast = entry.ast.clone();

        let mut guard = self.inner.lock();
        guard.queries.insert(query.to_owned(), entry);
        guard.stats.misses += 1;

        Ok(ast)
    }

    /// Get global cache instance.
    pub fn get() -> Self {
        CACHE.clone()
    }

    /// Get cache stats.
    pub fn stats() -> Stats {
        Self::get().inner.lock().stats.clone()
    }

    /// Get a copy of all queries stored in the cache.
    pub fn queries() -> HashMap<String, CachedAst> {
        Self::get().inner.lock().queries.clone()
    }

    /// Reset cache.
    pub fn reset() {
        let cache = Self::get();
        let mut guard = cache.inner.lock();
        guard.queries.clear();
        guard.queries.shrink_to_fit();
        guard.stats.hits = 0;
        guard.stats.misses = 0;
    }
}
