use integration_tests_rust::setup::{admin_sqlx, connections_sqlx};
use sqlx::{Connection, Executor, PgConnection, Pool, Postgres};

#[tokio::test]
async fn cached_query_alter_outside_transaction() -> Result<(), Box<dyn std::error::Error>> {
    let conns = connections_sqlx().await;
    let sharded = conns.get(1).cloned().unwrap();

    setup_scenario(sharded.clone()).await?;

    // Run the statement (OUTSIDE a transaction).
    // This will hit the invalidated cached query;
    // It should re-run the Parse, and return as normal.
    sqlx::query("SELECT * FROM sharded WHERE id = $1")
        .bind(1_i64)
        .fetch_all(&sharded)
        .await?;

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

        admin_sqlx().await.execute("RELOAD").await?;
    }

    // Cache the SELECT query.
    // Each shard has a primary and a replica pool and reads round-robin
    // So, we hit them all! ( assertion's here to prevent flaky tests in future :) )
    integration_tests_rust::utils::assert_setting_str("min_pool_size", "1").await;
    for _ in 0..4 {
        sqlx::query("SELECT * FROM sharded WHERE id = $1")
            .bind(1_i64)
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
