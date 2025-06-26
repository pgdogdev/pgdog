use rust::setup::{admin_sqlx, connections_sqlx};
use sqlx::Executor;
use sqlx::Row;

async fn ban_unban(database: &str, ban: bool, replica: bool) {
    let admin = admin_sqlx().await;

    let pools = admin.fetch_all("SHOW POOLS").await.unwrap();
    let id = pools
        .iter()
        .find(|p| {
            p.get::<String, &str>("database") == database
                && p.get::<String, &str>("user") == "pgdog"
                && p.get::<String, &str>("role") == if replica { "replica" } else { "primary" }
        })
        .map(|p| p.get::<i64, &str>("id"))
        .unwrap();

    if !ban {
        admin
            .execute(format!("UNBAN {}", id).as_str())
            .await
            .unwrap();
    } else {
        admin.execute(format!("BAN {}", id).as_str()).await.unwrap();
    }
}

async fn ensure_client_state(state: &str) {
    let admin = admin_sqlx().await;

    let clients = admin.fetch_all("SHOW CLIENTS").await.unwrap();
    let mut found = false;
    for client in clients {
        if client.get::<String, _>("database") != "admin" {
            assert_eq!(client.get::<String, _>("state"), state);
            found = true;
        }
    }

    assert!(found);
}

#[tokio::test]
async fn test_ban_unban() {
    let conns = connections_sqlx().await;

    for (pool, database) in conns
        .clone()
        .into_iter()
        .zip(["pgdog", "pgdog_sharded"].into_iter())
    {
        for replica in [true, false] {
            for _ in 0..25 {
                pool.execute("SELECT 1").await.unwrap();
            }

            ban_unban(database, true, replica).await;

            for _ in 0..25 {
                pool.execute("SELECT 1").await.unwrap();
            }

            ban_unban(database, false, replica).await;

            for _ in 0..25 {
                pool.execute("SELECT 1").await.unwrap();
            }
        }
    }

    ensure_client_state("idle").await;

    for (pool, database) in conns
        .into_iter()
        .zip(["pgdog", "pgdog_sharded"].into_iter())
    {
        for _ in 0..25 {
            pool.execute("SELECT 1").await.unwrap();
        }

        ban_unban(database, true, false).await;

        for _ in 0..25 {
            let err = pool.execute("CREATE TABLE test (id BIGINT)").await;
            assert!(err.err().unwrap().to_string().contains("pool is banned"));
        }

        ban_unban(database, false, false).await;

        let mut t = pool.begin().await.unwrap();
        t.execute("CREATE TABLE test_ban_unban (id BIGINT)")
            .await
            .unwrap();
        t.rollback().await.unwrap();

        pool.close().await;
    }
}
