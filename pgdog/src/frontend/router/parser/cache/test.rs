#![allow(clippy::print_stdout)]

use pg_query::{normalize, parse};
use tokio::spawn;

use crate::{
    backend::{schema::Schema, ShardingSchema},
    config::Role,
    frontend::router::parser::Shard,
    frontend::{BufferedQuery, PreparedStatements},
    net::{Parse, Query},
};

use super::*;
use std::time::{Duration, Instant};

fn test_context() -> AstContext<'static> {
    AstContext::empty()
}

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
            let mut prepared_statements = PreparedStatements::default();
            let ctx = test_context();
            for _ in 0..(times / threads) {
                let start = Instant::now();
                Cache::get()
                    .query(
                        &BufferedQuery::Prepared(Parse::new_anonymous(query)),
                        &ctx,
                        &mut prepared_statements,
                    )
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

// Serialize tests that read or write the global Cache stats. These tests
// reset the cache and assert on `hits`/`misses` counters, so they cannot run
// concurrently without clobbering each other.
static CACHE_STATS_LOCK: parking_lot::Mutex<()> = parking_lot::Mutex::new(());

fn run_prepared(query: &str) -> Ast {
    let ctx = test_context();
    let mut prepared_statements = PreparedStatements::default();
    Cache::get()
        .query(
            &BufferedQuery::Prepared(Parse::new_anonymous(query)),
            &ctx,
            &mut prepared_statements,
        )
        .unwrap()
}

#[test]
fn test_cache_hit_same_query_no_comment() {
    let _guard = CACHE_STATS_LOCK.lock();
    Cache::reset();

    // Sanity baseline: repeating an identical query with no comment should
    // produce exactly one miss and one hit.
    let q = "SELECT 1 FROM cache_comment_sanity";
    run_prepared(q);
    run_prepared(q);

    let (stats, _) = Cache::stats();
    assert_eq!(stats.misses, 1);
    assert_eq!(stats.hits, 1);
}

#[test]
fn test_cache_hit_different_leading_comments() {
    let _guard = CACHE_STATS_LOCK.lock();
    Cache::reset();

    // Two queries with the same body but different leading comments should
    // share a cache entry because comment stripping normalizes the key.
    run_prepared("/* client=foo */ SELECT 1 FROM cache_comment_leading");
    run_prepared("/* client=bar */ SELECT 1 FROM cache_comment_leading");

    let (stats, _) = Cache::stats();
    assert_eq!(
        stats.misses, 1,
        "second query should hit the cache — comments should be stripped"
    );
    assert_eq!(stats.hits, 1);
}

#[test]
fn test_cache_hit_different_trailing_comments() {
    let _guard = CACHE_STATS_LOCK.lock();
    Cache::reset();

    run_prepared("SELECT 1 FROM cache_comment_trailing /* client=foo */");
    run_prepared("SELECT 1 FROM cache_comment_trailing /* client=bar */");

    let (stats, _) = Cache::stats();
    assert_eq!(
        stats.misses, 1,
        "second query should hit the cache — trailing comments should be stripped"
    );
    assert_eq!(stats.hits, 1);
}

#[test]
fn test_cache_hit_commented_after_uncommented() {
    let _guard = CACHE_STATS_LOCK.lock();
    Cache::reset();

    // Seed with no comment, then query the same body with a comment. The
    // second lookup strips the comment and should hit the uncommented entry.
    run_prepared("SELECT 1 FROM cache_comment_mixed");
    run_prepared("/* traced */ SELECT 1 FROM cache_comment_mixed");

    let (stats, _) = Cache::stats();
    assert_eq!(stats.misses, 1);
    assert_eq!(stats.hits, 1);
}

#[test]
fn test_cache_hit_overrides_role_hint() {
    let _guard = CACHE_STATS_LOCK.lock();
    Cache::reset();

    // Seed with a primary role hint; cache stores an entry keyed by the
    // stripped body.
    let first = run_prepared("/* pgdog_role: primary */ SELECT 1 FROM cache_role_override");
    assert_eq!(first.comment_role, Some(Role::Primary));

    // Second query, same body, replica hint. Must hit cache AND return
    // an Ast whose role reflects the new hint, not the cached one.
    let second = run_prepared("/* pgdog_role: replica */ SELECT 1 FROM cache_role_override");
    let (stats, _) = Cache::stats();
    assert_eq!(stats.hits, 1, "second query should hit cache");
    assert_eq!(
        second.comment_role,
        Some(Role::Replica),
        "cached Ast must be overridden with the incoming role hint"
    );
}

#[test]
fn test_cache_hit_clears_role_hint_when_absent() {
    let _guard = CACHE_STATS_LOCK.lock();
    Cache::reset();

    // Seed with a primary hint.
    let _ = run_prepared("/* pgdog_role: primary */ SELECT 1 FROM cache_role_clear");

    // Re-run same body without any comment. Must hit cache and the role
    // must be cleared (None), not inherit the cached primary.
    let second = run_prepared("SELECT 1 FROM cache_role_clear");
    let (stats, _) = Cache::stats();
    assert_eq!(stats.hits, 1);
    assert_eq!(
        second.comment_role, None,
        "incoming query has no role hint — cached role must be cleared"
    );
}

#[test]
fn test_cache_hit_overrides_shard_hint() {
    let _guard = CACHE_STATS_LOCK.lock();
    Cache::reset();

    let first = run_prepared("/* pgdog_shard: 0 */ SELECT 1 FROM cache_shard_override");
    assert_eq!(first.comment_shard, Some(Shard::Direct(0)));

    let second = run_prepared("/* pgdog_shard: 1 */ SELECT 1 FROM cache_shard_override");
    let (stats, _) = Cache::stats();
    assert_eq!(stats.hits, 1, "second query should hit cache");
    assert_eq!(
        second.comment_shard,
        Some(Shard::Direct(1)),
        "cached Ast must be overridden with the incoming shard hint"
    );
}

#[test]
fn test_cache_miss_different_bodies() {
    let _guard = CACHE_STATS_LOCK.lock();
    Cache::reset();

    // Sanity: different statement bodies must not collide regardless of
    // comment stripping.
    run_prepared("/* x */ SELECT 1 FROM cache_comment_distinct_a");
    run_prepared("/* x */ SELECT 2 FROM cache_comment_distinct_b");

    let (stats, _) = Cache::stats();
    assert_eq!(stats.misses, 2);
    assert_eq!(stats.hits, 0);
}

#[test]
fn test_normalize() {
    let q = "SELECT * FROM users WHERE id = 1";
    let normalized = normalize(q).unwrap();
    assert_eq!(normalized, "SELECT * FROM users WHERE id = $1");
}

#[test]
fn test_tables_list() {
    let mut prepared_statements = PreparedStatements::default();
    let db_schema = Schema::default();
    for q in [
        "CREATE TABLE private_schema.test (id BIGINT)",
        "SELECT * FROM private_schema.test a INNER JOIN public_schema.test b ON a.id = b.id LIMIT 5",
        "INSERT INTO public_schema.test VALUES ($1, $2)",
        "DELETE FROM private_schema.test",
        "DROP TABLE private_schema.test",
    ] {
        let ast = Ast::new(&BufferedQuery::Query(Query::new(q)), &ShardingSchema::default(), &db_schema, &mut prepared_statements, "", None).unwrap();
        let tables = ast.tables();
        println!("{:?}", tables);
    }
}
