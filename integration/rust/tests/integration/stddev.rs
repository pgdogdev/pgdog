use std::collections::BTreeSet;

use ordered_float::OrderedFloat;
use rust::setup::{admin_sqlx, connections_sqlx, connections_tokio};
use sqlx::{Connection, Executor, PgConnection, Row, postgres::PgPool};

const SHARD_URLS: [&str; 2] = [
    "postgres://pgdog:pgdog@127.0.0.1:5432/shard_0",
    "postgres://pgdog:pgdog@127.0.0.1:5432/shard_1",
];

struct Moments {
    sum: f64,
    sum_sq: f64,
    count: i64,
}

impl Moments {
    fn variance_population(&self) -> Option<f64> {
        if self.count == 0 {
            return None;
        }

        let n = self.count as f64;
        let numerator = self.sum_sq - (self.sum * self.sum) / n;
        let variance = numerator / n;
        Some(if variance < 0.0 { 0.0 } else { variance })
    }

    fn variance_sample(&self) -> Option<f64> {
        if self.count <= 1 {
            return None;
        }

        let n = self.count as f64;
        let numerator = self.sum_sq - (self.sum * self.sum) / n;
        let variance = numerator / (n - 1.0);
        Some(if variance < 0.0 { 0.0 } else { variance })
    }

    fn stddev_population(&self) -> Option<f64> {
        self.variance_population().map(|variance| variance.sqrt())
    }

    fn stddev_sample(&self) -> Option<f64> {
        self.variance_sample().map(|variance| variance.sqrt())
    }
}

#[tokio::test]
async fn stddev_pop_merges_with_helpers() -> Result<(), Box<dyn std::error::Error>> {
    let conns = connections_sqlx().await;
    let sharded = conns.get(1).cloned().unwrap();

    reset_table(
        &sharded,
        "stddev_pop_reduce_test",
        "(price DOUBLE PRECISION, customer_id BIGINT)",
    )
    .await?;

    seed_stat_data(
        &sharded,
        &[
            (
                0,
                "INSERT INTO stddev_pop_reduce_test(price) VALUES (10.0), (14.0)",
            ),
            (
                1,
                "INSERT INTO stddev_pop_reduce_test(price) VALUES (18.0), (22.0)",
            ),
        ],
    )
    .await?;

    let rows = sharded
        .fetch_all("SELECT STDDEV_POP(price) AS stddev_pop FROM stddev_pop_reduce_test")
        .await?;

    assert_eq!(rows.len(), 1);
    let pgdog_stddev: f64 = rows[0].get("stddev_pop");

    let stats = combined_moments("stddev_pop_reduce_test", "price").await?;
    let expected = stats
        .stddev_population()
        .expect("population stddev should exist");

    assert!(
        (pgdog_stddev - expected).abs() < 1e-9,
        "STDDEV_POP mismatch: pgdog={}, expected={}",
        pgdog_stddev,
        expected
    );

    cleanup_table(&sharded, "stddev_pop_reduce_test").await;

    Ok(())
}

#[tokio::test]
async fn stddev_samp_aliases_should_merge() -> Result<(), Box<dyn std::error::Error>> {
    let conns = connections_sqlx().await;
    let sharded = conns.get(1).cloned().unwrap();

    reset_table(
        &sharded,
        "stddev_sample_reduce_test",
        "(price DOUBLE PRECISION, customer_id BIGINT)",
    )
    .await?;

    seed_stat_data(
        &sharded,
        &[
            (
                0,
                "INSERT INTO stddev_sample_reduce_test(price) VALUES (10.0), (14.0)",
            ),
            (
                1,
                "INSERT INTO stddev_sample_reduce_test(price) VALUES (18.0), (22.0)",
            ),
        ],
    )
    .await?;

    let rows = sharded
        .fetch_all("SELECT STDDEV(price) AS stddev_price, STDDEV_SAMP(price) AS stddev_samp_price FROM stddev_sample_reduce_test")
        .await?;

    assert_eq!(rows.len(), 1);
    let stddev_alias: f64 = rows[0].get("stddev_price");
    let stddev_samp: f64 = rows[0].get("stddev_samp_price");

    let stats = combined_moments("stddev_sample_reduce_test", "price").await?;
    let expected = stats
        .stddev_sample()
        .expect("sample stddev should exist for n > 1");

    for value in [stddev_alias, stddev_samp] {
        assert!(
            (value - expected).abs() < 1e-9,
            "STDDEV_SAMP mismatch: value={}, expected={}",
            value,
            expected
        );
    }

    cleanup_table(&sharded, "stddev_sample_reduce_test").await;

    Ok(())
}

#[tokio::test]
async fn variance_variants_should_merge() -> Result<(), Box<dyn std::error::Error>> {
    let conns = connections_sqlx().await;
    let sharded = conns.get(1).cloned().unwrap();

    reset_table(
        &sharded,
        "variance_reduce_test",
        "(price DOUBLE PRECISION, customer_id BIGINT)",
    )
    .await?;

    seed_stat_data(
        &sharded,
        &[
            (
                0,
                "INSERT INTO variance_reduce_test(price) VALUES (10.0), (14.0)",
            ),
            (
                1,
                "INSERT INTO variance_reduce_test(price) VALUES (18.0), (22.0)",
            ),
        ],
    )
    .await?;

    let rows = sharded
        .fetch_all(
            "SELECT VAR_POP(price) AS variance_pop, VAR_SAMP(price) AS variance_samp, VARIANCE(price) AS variance_alias FROM variance_reduce_test",
        )
        .await?;

    assert_eq!(rows.len(), 1);
    let variance_pop: f64 = rows[0].get("variance_pop");
    let variance_samp: f64 = rows[0].get("variance_samp");
    let variance_alias: f64 = rows[0].get("variance_alias");

    let stats = combined_moments("variance_reduce_test", "price").await?;
    let expected_pop = stats
        .variance_population()
        .expect("population variance should exist for n > 0");
    let expected_samp = stats
        .variance_sample()
        .expect("sample variance should exist for n > 1");

    assert!(
        (variance_pop - expected_pop).abs() < 1e-9,
        "VAR_POP mismatch: value={}, expected={}",
        variance_pop,
        expected_pop
    );

    for value in [variance_samp, variance_alias] {
        assert!(
            (value - expected_samp).abs() < 1e-9,
            "VAR_SAMP mismatch: value={}, expected={}",
            value,
            expected_samp
        );
    }

    cleanup_table(&sharded, "variance_reduce_test").await;

    Ok(())
}

#[tokio::test]
async fn stddev_multiple_columns_should_merge() -> Result<(), Box<dyn std::error::Error>> {
    let conns = connections_sqlx().await;
    let sharded = conns.get(1).cloned().unwrap();

    reset_table(
        &sharded,
        "stddev_multi_column",
        "(price DOUBLE PRECISION, discount DOUBLE PRECISION, customer_id BIGINT)",
    )
    .await?;

    seed_stat_data(
        &sharded,
        &[
            (
                0,
                "INSERT INTO stddev_multi_column(price, discount) VALUES (10.0, 1.0), (14.0, 3.0)",
            ),
            (
                1,
                "INSERT INTO stddev_multi_column(price, discount) VALUES (18.0, 2.0), (22.0, 6.0)",
            ),
        ],
    )
    .await?;

    let rows = sharded
        .fetch_all(
            "SELECT STDDEV_POP(price) AS stddev_price_pop, STDDEV_SAMP(discount) AS stddev_discount_samp FROM stddev_multi_column",
        )
        .await?;

    assert_eq!(rows.len(), 1);
    let price_pop: f64 = rows[0].get("stddev_price_pop");
    let discount_samp: f64 = rows[0].get("stddev_discount_samp");

    let price_stats = combined_moments("stddev_multi_column", "price").await?;
    let discount_stats = combined_moments("stddev_multi_column", "discount").await?;

    let expected_price_pop = price_stats
        .stddev_population()
        .expect("population stddev should exist");
    let expected_discount_samp = discount_stats
        .stddev_sample()
        .expect("sample stddev should exist");

    assert!(
        (price_pop - expected_price_pop).abs() < 1e-9,
        "STDDEV_POP(price) mismatch: value={}, expected={}",
        price_pop,
        expected_price_pop
    );

    assert!(
        (discount_samp - expected_discount_samp).abs() < 1e-9,
        "STDDEV_SAMP(discount) mismatch: value={}, expected={}",
        discount_samp,
        expected_discount_samp
    );

    cleanup_table(&sharded, "stddev_multi_column").await;

    Ok(())
}

#[tokio::test]
async fn stddev_prepared_statement_should_merge() -> Result<(), Box<dyn std::error::Error>> {
    let conns = connections_sqlx().await;
    let sharded = conns.get(1).cloned().unwrap();
    let pg_clients = connections_tokio().await;
    let tokio_client = pg_clients.into_iter().nth(1).unwrap();

    reset_table(
        &sharded,
        "stddev_prepared_params",
        "(price DOUBLE PRECISION, customer_id BIGINT)",
    )
    .await?;

    seed_stat_data(
        &sharded,
        &[
            (
                0,
                "INSERT INTO stddev_prepared_params(price) VALUES (10.0), (14.0)",
            ),
            (
                1,
                "INSERT INTO stddev_prepared_params(price) VALUES (18.0), (22.0)",
            ),
        ],
    )
    .await?;

    let statement = tokio_client
        .prepare("SELECT STDDEV_SAMP(price) AS stddev_price FROM stddev_prepared_params WHERE price >= $1")
        .await?;
    let column_names: Vec<_> = statement
        .columns()
        .iter()
        .map(|c| c.name().to_string())
        .collect();
    println!("tokio column names: {:?}", column_names);
    let tokio_rows = tokio_client.query(&statement, &[&12.0_f64]).await?;
    println!("tokio row count: {}", tokio_rows.len());

    let stddev_rows = sqlx::query(
        "SELECT STDDEV_SAMP(price) AS stddev_price FROM stddev_prepared_params WHERE price >= $1",
    )
    .bind(12.0_f64)
    .fetch_all(&sharded)
    .await?;

    assert_eq!(stddev_rows.len(), 1);
    let pgdog_stddev: f64 = stddev_rows[0].get("stddev_price");

    let stats =
        combined_moments_with_filter("stddev_prepared_params", "price", "price >= $1", 12.0_f64)
            .await?;
    let expected = stats
        .stddev_sample()
        .expect("sample stddev should exist for filtered rows");

    assert!(
        (pgdog_stddev - expected).abs() < 1e-9,
        "Prepared STDDEV_SAMP mismatch: pgdog={}, expected={}",
        pgdog_stddev,
        expected
    );

    cleanup_table(&sharded, "stddev_prepared_params").await;

    Ok(())
}

#[tokio::test]
async fn stddev_distinct_should_error_until_supported() -> Result<(), Box<dyn std::error::Error>> {
    let conns = connections_sqlx().await;
    let sharded = conns.get(1).cloned().unwrap();

    reset_table(
        &sharded,
        "stddev_distinct_error",
        "(price DOUBLE PRECISION, customer_id BIGINT)",
    )
    .await?;

    seed_stat_data(
        &sharded,
        &[
            (
                0,
                "INSERT INTO stddev_distinct_error(price) VALUES (10.0), (14.0)",
            ),
            (
                1,
                "INSERT INTO stddev_distinct_error(price) VALUES (18.0), (22.0)",
            ),
        ],
    )
    .await?;

    let result = sharded
        .fetch_all("SELECT STDDEV_SAMP(DISTINCT price) FROM stddev_distinct_error")
        .await;
    assert!(
        result.is_err(),
        "DISTINCT STDDEV should error until supported"
    );

    cleanup_table(&sharded, "stddev_distinct_error").await;

    Ok(())
}

#[tokio::test]
#[ignore]
async fn stddev_distinct_future_expectation() -> Result<(), Box<dyn std::error::Error>> {
    let conns = connections_sqlx().await;
    let sharded = conns.get(1).cloned().unwrap();

    reset_table(
        &sharded,
        "stddev_distinct_test",
        "(price DOUBLE PRECISION, customer_id BIGINT)",
    )
    .await?;

    seed_stat_data(
        &sharded,
        &[
            (
                0,
                "INSERT INTO stddev_distinct_test(price) VALUES (10.0), (14.0), (14.0)",
            ),
            (
                1,
                "INSERT INTO stddev_distinct_test(price) VALUES (14.0), (18.0), (22.0), (22.0)",
            ),
        ],
    )
    .await?;

    let rows = sharded
        .fetch_all(
            "SELECT STDDEV_SAMP(DISTINCT price) AS stddev_distinct FROM stddev_distinct_test",
        )
        .await?;

    assert_eq!(rows.len(), 1);
    let pgdog_stddev: f64 = rows[0].get("stddev_distinct");

    let stats = distinct_moments("stddev_distinct_test", "price").await?;
    let expected = stats
        .stddev_sample()
        .expect("sample stddev should exist for distinct data");

    assert!(
        (pgdog_stddev - expected).abs() < 1e-9,
        "DISTINCT STDDEV future expectation mismatch: pgdog={}, expected={}",
        pgdog_stddev,
        expected
    );

    cleanup_table(&sharded, "stddev_distinct_test").await;

    Ok(())
}

async fn reset_table(pool: &PgPool, table: &str, schema: &str) -> Result<(), sqlx::Error> {
    for shard in [0, 1] {
        let drop_stmt = format!(
            "/* pgdog_shard: {} */ DROP TABLE IF EXISTS {}",
            shard, table
        );
        pool.execute(drop_stmt.as_str()).await.ok();
    }

    for shard in [0, 1] {
        let create_stmt = format!(
            "/* pgdog_shard: {} */ CREATE TABLE {} {}",
            shard, table, schema
        );
        pool.execute(create_stmt.as_str()).await?;
    }

    admin_sqlx().await.execute("RELOAD").await?;

    Ok(())
}

async fn cleanup_table(pool: &PgPool, table: &str) {
    for shard in [0, 1] {
        let drop_stmt = format!("/* pgdog_shard: {} */ DROP TABLE {}", shard, table);
        let _ = pool.execute(drop_stmt.as_str()).await;
    }
}

async fn seed_stat_data(pool: &PgPool, inserts: &[(usize, &str)]) -> Result<(), sqlx::Error> {
    for (shard, statement) in inserts {
        let command = format!("/* pgdog_shard: {} */ {}", shard, statement);
        pool.execute(command.as_str()).await?;
    }

    Ok(())
}

async fn combined_moments(table: &str, column: &str) -> Result<Moments, sqlx::Error> {
    let mut totals = Moments {
        sum: 0.0,
        sum_sq: 0.0,
        count: 0,
    };

    for url in SHARD_URLS {
        let mut conn = PgConnection::connect(url).await?;
        let query = format!(
            "SELECT COALESCE(SUM({col}), 0)::float8, COALESCE(SUM(POWER({col}, 2)), 0)::float8, COUNT(*) FROM {table}",
            col = column,
            table = table
        );
        let (sum, sum_sq, count): (f64, f64, i64) = sqlx::query_as::<_, (f64, f64, i64)>(&query)
            .fetch_one(&mut conn)
            .await?;
        totals.sum += sum;
        totals.sum_sq += sum_sq;
        totals.count += count;
    }

    Ok(totals)
}

async fn combined_moments_with_filter(
    table: &str,
    column: &str,
    predicate: &str,
    value: f64,
) -> Result<Moments, sqlx::Error> {
    let mut totals = Moments {
        sum: 0.0,
        sum_sq: 0.0,
        count: 0,
    };

    for url in SHARD_URLS {
        let mut conn = PgConnection::connect(url).await?;
        let query = format!(
            "SELECT COALESCE(SUM({col}), 0)::float8, COALESCE(SUM(POWER({col}, 2)), 0)::float8, COUNT(*) FROM {table} WHERE {predicate}",
            col = column,
            table = table,
            predicate = predicate
        );
        let (sum, sum_sq, count): (f64, f64, i64) = sqlx::query_as::<_, (f64, f64, i64)>(&query)
            .bind(value)
            .fetch_one(&mut conn)
            .await?;
        totals.sum += sum;
        totals.sum_sq += sum_sq;
        totals.count += count;
    }

    Ok(totals)
}

async fn distinct_moments(table: &str, column: &str) -> Result<Moments, sqlx::Error> {
    let mut distinct_values: BTreeSet<OrderedFloat<f64>> = BTreeSet::new();

    for url in SHARD_URLS {
        let mut conn = PgConnection::connect(url).await?;
        let query = format!(
            "SELECT DISTINCT {col} FROM {table}",
            col = column,
            table = table
        );
        let rows: Vec<Option<f64>> = sqlx::query_scalar::<_, Option<f64>>(&query)
            .fetch_all(&mut conn)
            .await?;
        for value in rows.into_iter().flatten() {
            distinct_values.insert(OrderedFloat(value));
        }
    }

    let mut sum = 0.0;
    let mut sum_sq = 0.0;
    for value in &distinct_values {
        let v = value.0;
        sum += v;
        sum_sq += v * v;
    }

    Ok(Moments {
        sum,
        sum_sq,
        count: distinct_values.len() as i64,
    })
}
