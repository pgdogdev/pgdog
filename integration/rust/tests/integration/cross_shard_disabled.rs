use std::time::Duration;

use rust::setup::{admin_sqlx, connections_sqlx};
use sqlx::Executor;
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

    // Not sharded DB.
    sqlx::query("SELECT * FROM sharded")
        .fetch_optional(conns.get(0).unwrap())
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
