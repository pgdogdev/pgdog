use std::time::Duration;

use crate::setup::{admin_sqlx, connections_sqlx};
use sqlx::{Connection, Executor};
use tokio::time::sleep;

#[tokio::test]
async fn test_cross_shard_disabled() {
    let admin = admin_sqlx().await;
    admin
        .execute("SET cross_shard_disabled TO false")
        .await
        .unwrap();

    let conns = connections_sqlx().await;

    for conn in &conns {
        sqlx::query("SELECT * FROM sharded")
            .fetch_optional(conn)
            .await
            .unwrap();
    }

    admin
        .execute("SET cross_shard_disabled TO true")
        .await
        .unwrap();

    sleep(Duration::from_millis(100)).await;
    // let conns = connections_sqlx().await;

    // Not sharded DB.
    sqlx::query("SELECT * FROM sharded")
        .fetch_optional(conns.first().unwrap())
        .await
        .unwrap();

    // Sharded DB.
    let err = sqlx::query("SELECT * FROM sharded")
        .fetch_optional(conns.get(1).unwrap())
        .await
        .err()
        .unwrap();
    assert!(err.to_string().contains("cross-shard queries are disabled"));

    // Query has sharding key.
    sqlx::query("SELECT * FROM sharded WHERE id = $1")
        .bind(1)
        .fetch_optional(conns.get(1).unwrap())
        .await
        .unwrap();

    // Still works with cross-shard disabled.
    admin.fetch_all("SHOW QUERY_CACHE").await.unwrap();

    // Set it back to default value.
    admin
        .execute("SET cross_shard_disabled TO false")
        .await
        .unwrap();
}

#[tokio::test]
async fn test_cross_shard_disabled_with_unknown_sharding_key() {
    let admin = admin_sqlx().await;

    let mut conn =
        sqlx::PgConnection::connect("postgres://pgdog:pgdog@127.0.0.1:6432/single_sharded_list")
            .await
            .unwrap();

    // Get everything setup to test.
    {
        admin
            .execute("SET cross_shard_disabled TO false")
            .await
            .unwrap();

        conn.execute("DROP TABLE IF EXISTS test_unknown_sharding_key")
            .await
            .unwrap();

        conn.execute(
            "CREATE TABLE IF NOT EXISTS test_unknown_sharding_key(id BIGINT, value VARCHAR)",
        )
        .await
        .unwrap();
    }

    // Query has sharding key that is unknown using list-based sharding.
    // 0-10 => shard 0, 11-20 => shard 1 in this instance
    // 25 doesn't map to a shard.
    // With a valid sharding key, SELECT * FROM statement would return the pertinent rows.
    // However, since we have an unknown one, and cross-shard queries are denied,
    // an error must be thrown (otherwise, it would be a cross-shard query).
    {
        admin
            .execute("SET cross_shard_disabled TO true")
            .await
            .unwrap();

        conn.execute("BEGIN").await.unwrap();

        conn.execute(format!("SET pgdog.sharding_key TO '{}'", 25).as_str())
            .await
            .unwrap();

        let err = sqlx::query("SELECT * FROM test_unknown_sharding_key")
            .fetch_one(&mut conn)
            .await
            .err()
            .unwrap();
        assert!(
            err.to_string()
                .contains("error returned from database: unknown sharding key was specified")
        );

        conn.execute("COMMIT").await.unwrap();
    }

    // Reset back to normal.
    {
        admin
            .execute("SET cross_shard_disabled TO false")
            .await
            .unwrap();

        conn.execute("DROP TABLE test_unknown_sharding_key")
            .await
            .unwrap();

        conn.close().await.unwrap();
    }
}
