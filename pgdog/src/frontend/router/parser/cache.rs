//! AST cache.
//!
//! Shared between all clients and databases.

use dashmap::DashMap;
use once_cell::sync::Lazy;
use parking_lot::RwLock;
use pg_query::*;
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};

use std::sync::Arc;

static CACHE: Lazy<RwLock<Cache>> = Lazy::new(|| RwLock::new(Cache::default()));

/// AST cache statistics.
#[derive(Default, Debug, Clone)]
pub struct Stats {
    /// Cache hits.
    hits: Arc<AtomicUsize>,
    /// Cache misses (new queries).
    misses: Arc<AtomicUsize>,
}

impl Stats {
    pub fn hits(&self) -> usize {
        self.hits.load(Relaxed)
    }

    pub fn misses(&self) -> usize {
        self.misses.load(Relaxed)
    }
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

#[derive(Default, Debug, Clone)]
struct Inner {
    queries: Arc<DashMap<String, CachedAst>>,
    stats: Stats,
}

/// AST cache.
#[derive(Default, Clone, Debug)]
pub struct Cache {
    inner: Inner,
}

impl Cache {
    /// Parse a statement by either getting it from cache
    /// or using pg_query parser.
    ///
    /// N.B. There is a race here that allows multiple threads to
    /// parse the same query. That's better imo than locking the data structure
    /// while we parse the query.
    pub fn parse(&mut self, query: &str) -> Result<Arc<ParseResult>> {
        if let Some(mut entry) = self.inner.queries.get_mut(query) {
            entry.hits += 1;
            self.inner.stats.hits.fetch_add(1, Relaxed);
            return Ok(entry.ast.clone());
        }

        // Parse query without holding lock.
        let entry = CachedAst::new(parse(query)?);
        let ast = entry.ast.clone();

        self.inner.queries.insert(query.to_owned(), entry);
        self.inner.stats.misses.fetch_add(1, Relaxed);

        Ok(ast)
    }

    /// Get global cache instance.
    pub fn get() -> Self {
        CACHE.read().clone()
    }

    /// Get cache stats.
    pub fn stats() -> Stats {
        Self::get().inner.stats.clone()
    }

    /// Get a copy of all queries stored in the cache.
    pub fn queries() -> Arc<DashMap<String, CachedAst>> {
        Self::get().inner.queries.clone()
    }

    /// Reset cache.
    pub fn reset() {
        // This is the only place we acquire the write lock, in order to synchronize clearing the cache
        let cache = CACHE.write();
        cache.inner.queries.clear();
        cache.inner.queries.shrink_to_fit();
        cache.inner.stats.hits.store(0, Relaxed);
        cache.inner.stats.misses.store(0, Relaxed);
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
}
