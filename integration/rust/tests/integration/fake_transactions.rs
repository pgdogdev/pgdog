use rust::setup::{admin_sqlx, connections_sqlx};
use sqlx::{Executor, Pool, Postgres, Row};

#[tokio::test]
async fn test_fake_transactions() {
    let conn = connections_sqlx().await.into_iter().skip(1).next().unwrap();
    let admin = admin_sqlx().await;

    for _ in 0..5 {
        conn.execute("BEGIN").await.unwrap();
        check_state("idle in transaction", admin.clone()).await;
        conn.execute("ROLLBACK").await.unwrap();
        check_state("idle", admin.clone()).await;
    }
}

async fn check_state(expected: &str, admin: Pool<Postgres>) {
    let clients = admin.fetch_all("SHOW CLIENTS").await.unwrap();
    let mut ok = false;

    for client in clients {
        let state: String = client.get("state");
        let database: String = client.get("database");

        if database == "pgdog_sharded" {
            assert_eq!(state, expected);
            ok = true;
        }
    }

    assert!(ok);
}
