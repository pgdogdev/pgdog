use pg_query::{normalize, parse};
use tokio::spawn;

use crate::{
    backend::{schema::Schema, ShardingSchema},
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
