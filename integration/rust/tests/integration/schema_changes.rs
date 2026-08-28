use integration_tests_rust::setup::{admin_sqlx, connections_sqlx};
use integration_tests_rust::utils::{Message, startup};
use serial_test::serial;
use sqlx::{Connection, Executor, PgConnection, Pool, Postgres, Row};
use tokio::{io::AsyncWriteExt, net::TcpStream};

#[tokio::test]
async fn cached_query_alter_outside_transaction() -> Result<(), Box<dyn std::error::Error>> {
    let conns = connections_sqlx().await;
    let sharded = conns.get(1).cloned().unwrap();

    setup_scenario(sharded.clone()).await?;

    // Run the statement (OUTSIDE a transaction).
    // This will hit the invalidated cached query;
    // It should re-run the Parse, and return as normal.
    let rows = sqlx::query("SELECT * FROM sharded WHERE id = $1")
        .bind(1_i64)
        .fetch_all(&sharded)
        .await?;

    // Ensure that everything is accurate in the response after re-caching internally.
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<i64, _>("id"), 1);
    assert_eq!(rows[0].get::<String, _>("value"), "shard_0");

    Ok(())
}

#[tokio::test]
async fn cached_query_alter_outside_transaction_multi_shard()
-> Result<(), Box<dyn std::error::Error>> {
    let conns = connections_sqlx().await;
    let sharded = conns.get(1).cloned().unwrap();

    setup_scenario(sharded.clone()).await?;

    // Try a cached statement across 2 shards.
    let rows = sqlx::query("SELECT * FROM sharded WHERE id = $1 OR id = $2")
        .bind(1_i64)
        .bind(11_i64)
        .fetch_all(&sharded)
        .await?;

    // Got both successfully?
    let mut ids: Vec<i64> = rows.iter().map(|row| row.get("id")).collect();
    ids.sort();
    assert_eq!(ids, vec![1, 11]);

    Ok(())
}

#[tokio::test]
async fn cached_query_alter_parse_describe() -> Result<(), Box<dyn std::error::Error>> {
    let conns = connections_sqlx().await;
    let sharded = conns.get(1).cloned().unwrap();

    setup_scenario(sharded.clone()).await?;

    admin_sqlx()
        .await
        .execute("SET auth_type TO 'trust'")
        .await?;

    // Manually connect to the server.
    let mut stream = TcpStream::connect("127.0.0.1:6432").await?;
    stream.write_all(&startup("pgdog", "pgdog_sharded")).await?;
    loop {
        let message = Message::read(&mut stream).await?;
        assert_ne!(message.code, 'E');
        if message.code == 'Z' {
            break;
        }
    }

    // Send a [Parse, Describe, Sync]
    // This tests a situation where we have an invalidated statement, the Parse is already cached;
    // PgDog previously would use global cache to drop the Parse (regardless of it being invalidated on Postgres)
    // And, afterward, the Describe would try to use the statement, and fail.
    // Now it should work fine (the Describe is re-prepared).
    Message::new_parse("s1", "SELECT * FROM sharded WHERE id = $1")
        .send(&mut stream)
        .await?;
    Message::new_describe_statement("s1")
        .send(&mut stream)
        .await?;
    Message::new_sync().send(&mut stream).await?;

    // Get the RowDescription + Codes
    let mut codes = vec![];
    let mut row_description = None;
    loop {
        let message = Message::read(&mut stream).await?;
        codes.push(message.code);
        if message.code == 'T' {
            row_description = Some(message.payload.clone());
        }
        if message.code == 'Z' {
            break;
        }
    }

    assert!(!codes.contains(&'E'), "unexpected error: {codes:?}");

    let row_description = row_description.expect("missing RowDescription");
    let columns = i16::from_be_bytes([row_description[0], row_description[1]]);

    // Fetch the actual amount of columns
    let mut shard0 = PgConnection::connect("postgres://pgdog:pgdog@127.0.0.1:5432/shard_0").await?;
    let expected: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM information_schema.columns WHERE table_name = 'sharded'",
    )
    .fetch_one(&mut shard0)
    .await?;

    // Do they match?
    assert_eq!(columns as i64, expected);

    Ok(())
}

/// Helper function to (1) add the column, (2) cache the query, (3) remove the column
async fn setup_scenario(sharded: Pool<Postgres>) -> Result<(), Box<dyn std::error::Error>> {
    let mut shard0 = PgConnection::connect("postgres://pgdog:pgdog@127.0.0.1:5432/shard_0").await?;
    let mut shard1 = PgConnection::connect("postgres://pgdog:pgdog@127.0.0.1:5432/shard_1").await?;

    // Add the test column to the shards.
    {
        let query = "ALTER TABLE sharded ADD COLUMN IF NOT EXISTS test TEXT";
        for shard in [&mut shard0, &mut shard1] {
            shard.execute(query).await?;
        }
    }

    // Insert two rows (1 in shard 0, 1 in shard 1)
    sqlx::query("TRUNCATE TABLE sharded")
        .execute(&sharded)
        .await?;
    for (id, value) in [(1_i64, "shard_0"), (11_i64, "shard_1")] {
        sqlx::query("INSERT INTO sharded (id, value) VALUES ($1, $2)")
            .bind(id)
            .bind(value)
            .execute(&sharded)
            .await?;
    }

    // Ensure they were split properly and that both somehow didn't get to the same shard.
    // (in case of a potential integration config change in future, etc)
    let on_shard0: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM sharded WHERE id = 1")
        .fetch_one(&mut shard0)
        .await?;
    let on_shard1: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM sharded WHERE id = 11")
        .fetch_one(&mut shard1)
        .await?;
    assert_eq!(on_shard0, 1);
    assert_eq!(on_shard1, 1);

    // Cache the SELECT querys.
    // Each shard has a primary and a replica pool and reads round-robin
    // So, we hit them all! ( assertion's here to prevent flaky tests in future :) )
    integration_tests_rust::utils::assert_setting_str("min_pool_size", "1").await;
    for _ in 0..4 {
        sqlx::query("SELECT * FROM sharded WHERE id = $1")
            .bind(1_i64)
            .fetch_all(&sharded)
            .await?;
        sqlx::query("SELECT * FROM sharded WHERE id = $1 OR id = $2")
            .bind(1_i64)
            .bind(11_i64)
            .fetch_all(&sharded)
            .await?;
    }

    // Drop the test column. This will invalidate the cached query.
    let query = "ALTER TABLE sharded DROP COLUMN test";
    for mut shard in [shard0, shard1] {
        shard.execute(query).await?;
    }

    Ok(())
}
