use rust::setup::{admin_sqlx, connections_sqlx};
use serial_test::serial;
use sqlx::{Executor, Pool, Postgres, Row};

#[tokio::test]
#[serial]
async fn test_fake_transactions() {
    let conn = connections_sqlx().await.into_iter().nth(1).unwrap();
    let admin = admin_sqlx().await;

    for _ in 0..5 {
        conn.execute("SET application_name TO 'test_fake_transactions'")
            .await
            .unwrap();
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
        let application_name: String = client.get("application_name");

        if database == "pgdog_sharded" && application_name == "test_fake_transactions" {
            assert_eq!(state, expected);
            ok = true;
        }
    }

    assert!(ok);
}
