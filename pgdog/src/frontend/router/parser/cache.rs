//! AST cache.
//!
//! Shared between all clients and databases.

use lru::LruCache;
use once_cell::sync::Lazy;
use pg_query::*;
use std::collections::HashMap;

use parking_lot::Mutex;
use std::sync::Arc;
use tracing::debug;

use crate::{
    backend::ShardingSchema,
    config::Role,
    frontend::router::parser::{comment::comment, Shard},
    net::Parse,
};

use super::Route;

static CACHE: Lazy<Cache> = Lazy::new(Cache::new);

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
}

/// Abstract syntax tree (query) cache entry,
/// with statistics.
#[derive(Debug, Clone)]
pub struct CachedAst {
    /// pg_query-produced AST.
    pub ast: Arc<ParseResult>,
    /// Statistics. Use a separate Mutex to avoid
    /// contention when updating them.
    pub stats: Arc<Mutex<Stats>>,
    /// Was this entry cached?
    pub cached: bool,
    /// Shard.
    pub shard: Shard,
    /// Role.
    pub role: Option<Role>,
}

impl CachedAst {
    /// Create new cache entry from pg_query's AST.
    fn new(query: &str, schema: &ShardingSchema) -> std::result::Result<Self, super::Error> {
        let ast = parse(query).map_err(super::Error::PgQuery)?;
        let (shard, role) = comment(query, schema)?;

        Ok(Self {
            cached: true,
            ast: Arc::new(ast),
            shard,
            role,
            stats: Arc::new(Mutex::new(Stats {
                hits: 1,
                ..Default::default()
            })),
        })
    }

    /// Get the reference to the AST.
    pub fn ast(&self) -> &ParseResult {
        &self.ast
    }

    /// Update stats for this statement, given the route
    /// calculated by the query parser.
    pub fn update_stats(&self, route: &Route) {
        let mut guard = self.stats.lock();

        if route.is_cross_shard() {
            guard.multi += 1;
        } else {
            guard.direct += 1;
        }
    }
}

/// Mutex-protected query cache.
#[derive(Debug)]
struct Inner {
    /// Least-recently-used cache.
    queries: LruCache<String, CachedAst>,
    /// Cache global stats.
    stats: Stats,
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

    /// Parse a statement by either getting it from cache
    /// or using pg_query parser.
    ///
    /// N.B. There is a race here that allows multiple threads to
    /// parse the same query. That's better imo than locking the data structure
    /// while we parse the query.
    pub fn parse(
        &self,
        parse: &Parse,
        schema: &ShardingSchema,
    ) -> std::result::Result<CachedAst, super::Error> {
        {
            let mut guard = self.inner.lock();
            let ast = guard.queries.get_mut(parse.name()).map(|entry| {
                entry.stats.lock().hits += 1; // No contention on this.
                entry.clone()
            });
            if let Some(ast) = ast {
                guard.stats.hits += 1;
                return Ok(ast);
            }
        }

        // Parse query without holding lock.
        let entry = CachedAst::new(parse.query(), schema)?;

        let mut guard = self.inner.lock();
        guard.queries.put(parse.name().to_owned(), entry.clone());
        guard.stats.misses += 1;

        Ok(entry)
    }

    /// Parse a statement but do not store it in the cache.
    pub fn parse_uncached(
        &self,
        query: &str,
        schema: &ShardingSchema,
    ) -> std::result::Result<CachedAst, super::Error> {
        let mut entry = CachedAst::new(query, schema)?;
        entry.cached = false;
        Ok(entry)
    }

    /// Record a query sent over the simple protocol, while removing parameters.
    pub fn record_normalized(
        &self,
        query: &str,
        route: &Route,
        schema: &ShardingSchema,
    ) -> std::result::Result<(), super::Error> {
        let normalized = pg_query::normalize(query).map_err(super::Error::PgQuery)?;

        {
            let mut guard = self.inner.lock();
            if let Some(entry) = guard.queries.get(&normalized) {
                entry.update_stats(route);
                guard.stats.hits += 1;
                return Ok(());
            }
        }

        let entry = CachedAst::new(query, schema)?;
        entry.update_stats(route);

        let mut guard = self.inner.lock();
        guard.queries.put(normalized, entry);
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
                    .map(|c| c.1.stats.clone())
                    .collect::<Vec<_>>(),
                guard.stats.clone(),
            )
        };
        for stat in query_stats {
            let guard = stat.lock();
            stats.direct += guard.direct;
            stats.multi += guard.multi;
        }
        (stats, len)
    }

    /// Get a copy of all queries stored in the cache.
    pub fn queries() -> HashMap<String, CachedAst> {
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
    pub fn reset() {
        let cache = Self::get();
        let mut guard = cache.inner.lock();
        guard.queries.clear();
        guard.stats.hits = 0;
        guard.stats.misses = 0;
    }
}

#[cfg(test)]
mod test {
    use tokio::spawn;

    use super::*;
    use std::time::{Duration, Instant};

    #[tokio::test(flavor = "multi_thread")]
    async fn bench_ast_cache() {
        let query = Parse::named(
            "test",
            "SELECT
            u.username,
            p.product_name,
            SUM(oi.quantity * oi.price) AS total_revenue,
            AVG(r.rating) AS average_rating,
            COUNT(DISTINCT c.country) AS countries_purchased_from
        FROM users u
        INNER JOIN orders o ON u.user_id = o.user_id
        INNER JOIN order_items oi ON o.order_id = oi.order_id
        INNER JOIN products p ON oi.product_id = p.product_id
        LEFT JOIN reviews r ON o.order_id = r.order_id
        LEFT JOIN customer_addresses c ON o.shipping_address_id = c.address_id
        WHERE
            o.order_date BETWEEN '2023-01-01' AND '2023-12-31'
            AND p.category IN ('Electronics', 'Clothing')
            AND (r.rating > 4 OR r.rating IS NULL)
        GROUP BY u.username, p.product_name
        HAVING COUNT(DISTINCT c.country) > 2
        ORDER BY total_revenue DESC;
",
        );

        let times = 10_000;
        let threads = 5;

        let mut tasks = vec![];
        for _ in 0..threads {
            let query = query.clone();
            let handle = spawn(async move {
                let mut parse_time = Duration::ZERO;
                for _ in 0..(times / threads) {
                    let start = Instant::now();
                    parse(query.query()).unwrap();
                    parse_time += start.elapsed();
                }

                parse_time
            });
            tasks.push(handle);
        }

        let mut parse_time = Duration::ZERO;
        for task in tasks {
            parse_time += task.await.unwrap();
        }

        println!("[bench_ast_cache]: parse time: {:?}", parse_time);

        // Simulate lock contention.
        let mut tasks = vec![];

        for _ in 0..threads {
            let query = query.clone();
            let handle = spawn(async move {
                let mut cached_time = Duration::ZERO;
                for _ in 0..(times / threads) {
                    let start = Instant::now();
                    Cache::get()
                        .parse(&query, &ShardingSchema::default())
                        .unwrap();
                    cached_time += start.elapsed();
                }

                cached_time
            });
            tasks.push(handle);
        }

        let mut cached_time = Duration::ZERO;
        for task in tasks {
            cached_time += task.await.unwrap();
        }

        println!("[bench_ast_cache]: cached time: {:?}", cached_time);

        let faster = parse_time.as_micros() as f64 / cached_time.as_micros() as f64;
        println!(
            "[bench_ast_cache]: cached is {:.4} times faster than parsed",
            faster
        ); // 32x on my M1

        assert!(faster > 10.0);
    }

    #[test]
    fn test_normalize() {
        let q = "SELECT * FROM users WHERE id = 1";
        let normalized = normalize(q).unwrap();
        assert_eq!(normalized, "SELECT * FROM users WHERE id = $1");
    }
}
