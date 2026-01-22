use rust::setup::{admin_sqlx, connections_sqlx};
use sqlx::{Connection, Executor, PgConnection, Row};

#[tokio::test]
async fn avg_merges_with_helper_count() -> Result<(), Box<dyn std::error::Error>> {
    let conns = connections_sqlx().await;
    let sharded = conns.get(1).cloned().unwrap();

    // Ensure clean state on each shard.
    for shard in [0, 1] {
        let comment = format!(
            "/* pgdog_shard: {} */ DROP TABLE IF EXISTS avg_reduce_test",
            shard
        );
        sharded.execute(comment.as_str()).await.ok();
    }

    for shard in [0, 1] {
        let comment = format!(
            "/* pgdog_shard: {} */ CREATE TABLE avg_reduce_test(price DOUBLE PRECISION, customer_id BIGINT)",
            shard
        );
        sharded.execute(comment.as_str()).await?;
    }

    // Make sure sharded table is loaded in schema.
    admin_sqlx().await.execute("RELOAD").await?;

    // Insert data on each shard so the query spans multiple shards.
    sharded
        .execute("/* pgdog_shard: 0 */ INSERT INTO avg_reduce_test(price) VALUES (10.0), (14.0)")
        .await?;
    sharded
        .execute("/* pgdog_shard: 1 */ INSERT INTO avg_reduce_test(price) VALUES (18.0), (22.0)")
        .await?;

    let rows = sharded
        .fetch_all(
            "SELECT COUNT(price)::bigint AS total_count, AVG(price) AS avg_price FROM avg_reduce_test",
        )
        .await?;

    assert_eq!(rows.len(), 1);
    let total_count: i64 = rows[0].get("total_count");
    let avg_price: f64 = rows[0].get("avg_price");

    assert_eq!(total_count, 4);
    assert!(
        (avg_price - 16.0).abs() < 1e-9,
        "unexpected avg: {}",
        avg_price
    );

    // Cleanup tables per shard.
    for shard in [0, 1] {
        let comment = format!("/* pgdog_shard: {} */ DROP TABLE avg_reduce_test", shard);
        sharded.execute(comment.as_str()).await.ok();
    }

    Ok(())
}

#[tokio::test]
async fn avg_without_helper_should_still_merge() -> Result<(), Box<dyn std::error::Error>> {
    let conns = connections_sqlx().await;
    let sharded = conns.get(1).cloned().unwrap();

    // Clean slate on each shard.
    for shard in [0, 1] {
        let comment = format!(
            "/* pgdog_shard: {} */ DROP TABLE IF EXISTS avg_rewrite_expectation",
            shard
        );
        sharded.execute(comment.as_str()).await.ok();
    }

    for shard in [0, 1] {
        let comment = format!(
            "/* pgdog_shard: {} */ CREATE TABLE avg_rewrite_expectation(price DOUBLE PRECISION, customer_id BIGINT)",
            shard
        );
        sharded.execute(comment.as_str()).await?;
    }

    admin_sqlx().await.execute("RELOAD").await?;

    sharded
        .execute(
            "/* pgdog_shard: 0 */ INSERT INTO avg_rewrite_expectation(price) VALUES (10.0), (14.0)",
        )
        .await?;
    sharded
        .execute(
            "/* pgdog_shard: 1 */ INSERT INTO avg_rewrite_expectation(price) VALUES (18.0), (22.0)",
        )
        .await?;

    let rows = sharded
        .fetch_all("SELECT AVG(price) AS avg_price FROM avg_rewrite_expectation")
        .await?;

    // Desired behavior: rows should merge to a single average across all shards.
    assert_eq!(
        rows.len(),
        1,
        "AVG without helper COUNT should merge across shards"
    );

    let pgdog_avg: f64 = rows[0].get("avg_price");

    let mut shard0 = PgConnection::connect("postgres://pgdog:pgdog@127.0.0.1:5432/shard_0")
        .await
        .unwrap();
    let mut shard1 = PgConnection::connect("postgres://pgdog:pgdog@127.0.0.1:5432/shard_1")
        .await
        .unwrap();

    let (sum0, count0): (f64, i64) = sqlx::query_as::<_, (f64, i64)>(
        "SELECT COALESCE(SUM(price), 0)::float8, COUNT(*) FROM avg_rewrite_expectation",
    )
    .fetch_one(&mut shard0)
    .await?;
    let (sum1, count1): (f64, i64) = sqlx::query_as::<_, (f64, i64)>(
        "SELECT COALESCE(SUM(price), 0)::float8, COUNT(*) FROM avg_rewrite_expectation",
    )
    .fetch_one(&mut shard1)
    .await?;

    let expected = (sum0 + sum1) / (count0 + count1) as f64;
    assert!(
        (pgdog_avg - expected).abs() < 1e-9,
        "PgDog AVG should match Postgres"
    );

    Ok(())
}

#[tokio::test]
async fn avg_multiple_columns_should_merge() -> Result<(), Box<dyn std::error::Error>> {
    let conns = connections_sqlx().await;
    let sharded = conns.get(1).cloned().unwrap();

    for shard in [0, 1] {
        let comment = format!(
            "/* pgdog_shard: {} */ DROP TABLE IF EXISTS avg_multi_column",
            shard
        );
        sharded.execute(comment.as_str()).await.ok();
    }

    for shard in [0, 1] {
        let comment = format!(
            "/* pgdog_shard: {} */ CREATE TABLE avg_multi_column(price DOUBLE PRECISION, discount DOUBLE PRECISION, customer_id BIGINT)",
            shard
        );
        sharded.execute(comment.as_str()).await?;
    }

    admin_sqlx().await.execute("RELOAD").await?;

    sharded
        .execute(
            "/* pgdog_shard: 0 */ INSERT INTO avg_multi_column(price, discount) VALUES (10.0, 1.0), (14.0, 3.0)",
        )
        .await?;
    sharded
        .execute(
            "/* pgdog_shard: 1 */ INSERT INTO avg_multi_column(price, discount) VALUES (18.0, 2.0), (22.0, 6.0)",
        )
        .await?;

    let rows = sharded
        .fetch_all(
            "SELECT AVG(price) AS avg_price, AVG(discount) AS avg_discount FROM avg_multi_column",
        )
        .await?;

    assert_eq!(
        rows.len(),
        1,
        "rewritten AVG columns should merge across shards"
    );

    let avg_price: f64 = rows[0].get("avg_price");
    let avg_discount: f64 = rows[0].get("avg_discount");

    let mut shard0 = PgConnection::connect("postgres://pgdog:pgdog@127.0.0.1:5432/shard_0")
        .await
        .unwrap();
    let mut shard1 = PgConnection::connect("postgres://pgdog:pgdog@127.0.0.1:5432/shard_1")
        .await
        .unwrap();

    let (sum_price0, sum_discount0, count0): (f64, f64, i64) = sqlx::query_as(
        "SELECT COALESCE(SUM(price), 0)::float8, COALESCE(SUM(discount), 0)::float8, COUNT(*) FROM avg_multi_column",
    )
    .fetch_one(&mut shard0)
    .await?;
    let (sum_price1, sum_discount1, count1): (f64, f64, i64) = sqlx::query_as(
        "SELECT COALESCE(SUM(price), 0)::float8, COALESCE(SUM(discount), 0)::float8, COUNT(*) FROM avg_multi_column",
    )
    .fetch_one(&mut shard1)
    .await?;

    let total_count = (count0 + count1) as f64;
    let expected_price = (sum_price0 + sum_price1) / total_count;
    let expected_discount = (sum_discount0 + sum_discount1) / total_count;

    assert!(
        (avg_price - expected_price).abs() < 1e-9,
        "PgDog AVG(price) should match Postgres"
    );
    assert!(
        (avg_discount - expected_discount).abs() < 1e-9,
        "PgDog AVG(discount) should match Postgres"
    );

    for shard in [0, 1] {
        let comment = format!("/* pgdog_shard: {} */ DROP TABLE avg_multi_column", shard);
        sharded.execute(comment.as_str()).await.ok();
    }

    Ok(())
}

#[tokio::test]
async fn avg_prepared_statement_should_merge() -> Result<(), Box<dyn std::error::Error>> {
    let conns = connections_sqlx().await;
    let sharded = conns.get(1).cloned().unwrap();

    for shard in [0, 1] {
        let comment = format!(
            "/* pgdog_shard: {} */ DROP TABLE IF EXISTS avg_prepared_params",
            shard
        );
        sharded.execute(comment.as_str()).await.ok();
    }

    for shard in [0, 1] {
        let comment = format!(
            "/* pgdog_shard: {} */ CREATE TABLE avg_prepared_params(price DOUBLE PRECISION, customer_id BIGINT)",
            shard
        );
        sharded.execute(comment.as_str()).await?;
    }

    admin_sqlx().await.execute("RELOAD").await?;

    sharded
        .execute(
            "/* pgdog_shard: 0 */ INSERT INTO avg_prepared_params(price) VALUES (10.0), (14.0)",
        )
        .await?;
    sharded
        .execute(
            "/* pgdog_shard: 1 */ INSERT INTO avg_prepared_params(price) VALUES (18.0), (22.0)",
        )
        .await?;

    let avg_rows =
        sqlx::query("SELECT AVG(price) AS avg_price FROM avg_prepared_params WHERE price >= $1")
            .bind(10.0_f64)
            .fetch_all(&sharded)
            .await?;

    assert_eq!(avg_rows.len(), 1);
    let pgdog_avg: f64 = avg_rows[0].get("avg_price");

    let mut shard0 = PgConnection::connect("postgres://pgdog:pgdog@127.0.0.1:5432/shard_0")
        .await
        .unwrap();
    let mut shard1 = PgConnection::connect("postgres://pgdog:pgdog@127.0.0.1:5432/shard_1")
        .await
        .unwrap();

    let (sum0, count0): (f64, i64) = sqlx::query_as(
        "SELECT COALESCE(SUM(price), 0)::float8, COUNT(*) FROM avg_prepared_params WHERE price >= $1",
    )
    .bind(10.0_f64)
    .fetch_one(&mut shard0)
    .await?;
    let (sum1, count1): (f64, i64) = sqlx::query_as(
        "SELECT COALESCE(SUM(price), 0)::float8, COUNT(*) FROM avg_prepared_params WHERE price >= $1",
    )
    .bind(10.0_f64)
    .fetch_one(&mut shard1)
    .await?;

    let expected = (sum0 + sum1) / (count0 + count1) as f64;
    assert!(
        (pgdog_avg - expected).abs() < 1e-9,
        "Prepared AVG should match Postgres"
    );

    for shard in [0, 1] {
        let comment = format!(
            "/* pgdog_shard: {} */ DROP TABLE avg_prepared_params",
            shard
        );
        sharded.execute(comment.as_str()).await.ok();
    }

    Ok(())
}
