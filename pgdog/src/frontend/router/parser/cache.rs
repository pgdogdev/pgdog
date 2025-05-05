//! AST cache.
//!
//! Shared between all clients and databases.

use once_cell::sync::Lazy;
use pg_query::*;
use std::{collections::HashMap, time::Duration};

use parking_lot::Mutex;
use std::sync::Arc;

use super::{Error, Route};
use crate::frontend::buffer::BufferedQuery;

static CACHE: Lazy<Cache> = Lazy::new(Cache::default);

/// AST cache statistics.
#[derive(Default, Debug, Copy, Clone)]
pub struct Stats {
    /// Cache hits.
    pub hits: usize,
    /// Cache misses (new queries).
    pub misses: usize,
    /// Direct shard queries.
    pub direct: usize,
    /// Multi-shard queries.
    pub multi: usize,
    /// Size of the cache.
    pub size: usize,
}

#[derive(Debug, Clone)]
pub struct CachedAst {
    pub ast: Arc<ParseResult>,
    pub hits: usize,
    pub direct: usize,
    pub multi: usize,
    /// Average duration.
    pub avg_exec: Duration,
    /// Max duration.
    pub max_exec: Duration,
    /// Min duration.
    pub min_exec: Duration,
}

impl CachedAst {
    fn new(ast: ParseResult) -> Self {
        Self {
            ast: Arc::new(ast),
            hits: 1,
            direct: 0,
            multi: 0,
            avg_exec: Duration::ZERO,
            max_exec: Duration::ZERO,
            min_exec: Duration::ZERO,
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
    pub fn parse(&self, query: &str) -> Result<Arc<ParseResult>> {
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

    pub fn record_command(
        &self,
        query: &BufferedQuery,
        route: &Route,
    ) -> std::result::Result<(), Error> {
        match query {
            BufferedQuery::Prepared(parse) => self
                .record_command_for_normalized(parse.query(), route, false)
                .map_err(Error::PgQuery),
            BufferedQuery::Query(query) => {
                let query = normalize(query.query()).map_err(Error::PgQuery)?;
                self.record_command_for_normalized(&query, route, true)
                    .map_err(Error::PgQuery)
            }
        }
    }

    fn record_command_for_normalized(
        &self,
        query: &str,
        route: &Route,
        normalized: bool,
    ) -> Result<()> {
        // Fast path for prepared statements.
        {
            let mut guard = self.inner.lock();
            let multi = route.is_all_shards() || route.is_multi_shard();
            if multi {
                guard.stats.multi += 1;
            } else {
                guard.stats.direct += 1;
            }
            if let Some(ast) = guard.queries.get_mut(query) {
                if multi {
                    ast.multi += 1;
                } else {
                    ast.direct += 1;
                }

                if normalized {
                    ast.hits += 1;
                }

                return Ok(());
            }
        }

        // Slow path for simple queries.
        let mut entry = CachedAst::new(parse(query)?);
        let mut guard = self.inner.lock();
        if route.is_all_shards() || route.is_multi_shard() {
            entry.multi += 1;
        } else {
            entry.direct += 1;
        }

        guard.queries.insert(query.to_string(), entry);

        Ok(())
    }

    /// Get cache stats.
    pub fn stats() -> Stats {
        let cache = Self::get();
        let guard = cache.inner.lock();
        let mut stats = guard.stats;
        stats.size = guard.queries.len();
        stats
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

#[cfg(test)]
mod test {
    use tokio::spawn;

    use super::*;
    use std::time::{Duration, Instant};

    #[tokio::test(flavor = "multi_thread")]
    async fn bench_ast_cache() {
        let query = "SELECT
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
";

        let times = 10_000;
        let threads = 5;

        let mut tasks = vec![];
        for _ in 0..threads {
            let handle = spawn(async move {
                let mut parse_time = Duration::ZERO;
                for _ in 0..(times / threads) {
                    let start = Instant::now();
                    parse(query).unwrap();
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
            let handle = spawn(async move {
                let mut cached_time = Duration::ZERO;
                for _ in 0..(times / threads) {
                    let start = Instant::now();
                    Cache::get().parse(query).unwrap();
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
