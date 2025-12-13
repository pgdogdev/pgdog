use sqlx::{Postgres, pool::Pool, postgres::PgPoolOptions};

async fn sharded_pool() -> Pool<Postgres> {
    PgPoolOptions::new()
        .max_connections(1)
        .connect("postgres://pgdog:pgdog@127.0.0.1:6432/pgdog_sharded?application_name=sqlx")
        .await
        .unwrap()
}

#[tokio::test]
async fn test_unique_id() {
    let conn = sharded_pool().await;

    let row: (i64,) = sqlx::query_as("SELECT pgdog.unique_id()")
        .fetch_one(&conn)
        .await
        .unwrap();

    assert!(row.0 > 0, "unique_id should be positive");

    conn.close().await;
}

#[tokio::test]
async fn test_unique_id_multiple() {
    let conn = sharded_pool().await;

    let row: (i64, i64) = sqlx::query_as("SELECT pgdog.unique_id(), pgdog.unique_id()")
        .fetch_one(&conn)
        .await
        .unwrap();

    assert!(row.0 > 0, "first unique_id should be positive");
    assert!(row.1 > 0, "second unique_id should be positive");
    assert_ne!(row.0, row.1, "unique_ids should be different");

    conn.close().await;
}

#[tokio::test]
async fn test_unique_id_uniqueness() {
    let conn = sharded_pool().await;

    let mut ids = Vec::new();

    for _ in 0..100 {
        let row: (i64,) = sqlx::query_as("SELECT pgdog.unique_id()")
            .fetch_one(&conn)
            .await
            .unwrap();
        ids.push(row.0);
    }

    ids.sort();
    ids.dedup();
    assert_eq!(ids.len(), 100, "all unique_ids should be unique");

    conn.close().await;
}
