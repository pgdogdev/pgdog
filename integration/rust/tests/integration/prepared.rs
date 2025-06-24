use rust::setup::{admin_sqlx, connections_sqlx};
use serial_test::serial;
use sqlx::{Executor, Pool, Postgres, Row};

#[tokio::test]
async fn test_prepared_counter() {
    let conns = connections_sqlx().await;
    let admin = admin_sqlx().await;

    for conn in conns {
        let prepared = conn.prepare("SELECT $1").await.unwrap();
    }
}
