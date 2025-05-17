use rust::setup::{admin_sqlx, connections_sqlx};
use serial_test::serial;
use sqlx::{Executor, Pool, Postgres, Row};

#[tokio::test]
#[serial]
async fn test_fake_transactions() {
    let conn = connections_sqlx().await.into_iter().nth(1).unwrap();
    let admin = admin_sqlx().await;

    admin
        .execute("SET read_write_strategy TO 'conservative'")
        .await
        .unwrap();

    for _ in 0..5 {
        conn.execute("SET application_name TO 'test_fake_transactions'")
            .await
            .unwrap();
        conn.execute("BEGIN").await.unwrap();
        check_client_state("idle in transaction", admin.clone()).await;
        assert!(check_server_state("idle in transaction", admin.clone()).await);
        conn.execute("ROLLBACK").await.unwrap();
        check_client_state("idle", admin.clone()).await;
        assert!(check_server_state("idle", admin.clone()).await);
    }

    admin
        .execute("SET read_write_strategy TO 'aggressive'")
        .await
        .unwrap();

    for _ in 0..5 {
        conn.execute("SET application_name TO 'test_fake_transactions'")
            .await
            .unwrap();
        conn.execute("BEGIN").await.unwrap();
        check_client_state("idle in transaction", admin.clone()).await;
        assert!(check_server_state("idle", admin.clone()).await);
        conn.execute("CREATE TABLE test_fake_transactions (id BIGINT)")
            .await
            .unwrap();
        check_client_state("idle in transaction", admin.clone()).await;
        assert!(check_server_state("idle in transaction", admin.clone()).await);
        conn.execute("ROLLBACK").await.unwrap();
        check_client_state("idle", admin.clone()).await;
        assert!(check_server_state("idle", admin.clone()).await);
    }
}

async fn check_client_state(expected: &str, admin: Pool<Postgres>) {
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

async fn check_server_state(expected: &str, admin: Pool<Postgres>) -> bool {
    let clients = admin.fetch_all("SHOW SERVERS").await.unwrap();
    let mut ok = false;

    for client in clients {
        let state: String = client.get("state");
        let database: String = client.get("database");
        let application_name: String = client.get("application_name");

        if database.starts_with("shard_") && application_name == "test_fake_transactions" {
            ok = state == expected;
        }
    }

    ok
}
