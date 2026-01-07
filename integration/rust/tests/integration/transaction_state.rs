use rust::setup::admin_sqlx;
use serial_test::serial;
use sqlx::{Executor, Pool, Postgres, Row};
use tokio::time::{Duration, Instant, sleep};

const APP_NAME: &str = "test_transaction_state_flow";

#[tokio::test]
#[serial]
async fn test_transaction_state_transitions() {
    let admin = admin_sqlx().await;
    assert!(fetch_client_state(&admin, APP_NAME).await.is_none());

    let (client, connection) = tokio_postgres::connect(
        "host=127.0.0.1 user=pgdog dbname=pgdog password=pgdog port=6432",
        tokio_postgres::NoTls,
    )
    .await
    .unwrap();

    let connection_handle = tokio::spawn(async move {
        if let Err(e) = connection.await {
            panic!("connection error: {}", e);
        }
    });

    client
        .batch_execute(&format!("SET application_name TO '{}';", APP_NAME))
        .await
        .unwrap();
    client
        .batch_execute("SET statement_timeout TO '10s';")
        .await
        .unwrap();
    client.batch_execute("SELECT 1;").await.unwrap();

    wait_for_client_state(&admin, APP_NAME, "idle").await;

    client.batch_execute("BEGIN;").await.unwrap();
    wait_for_client_state(&admin, APP_NAME, "idle in transaction").await;

    {
        let query = client.simple_query("SELECT pg_sleep(0.25);");
        tokio::pin!(query);

        let deadline = Instant::now() + Duration::from_secs(5);
        let mut saw_active = false;
        loop {
            tokio::select! {
                result = &mut query => {
                    result.unwrap();
                    break;
                }
                _ = sleep(Duration::from_millis(10)) => {
                    if let Some(state) = fetch_client_state(&admin, APP_NAME).await {
                        if state == "active" {
                            saw_active = true;
                        }
                    }

                    if Instant::now() >= deadline {
                        panic!("timed out waiting for client to become active");
                    }
                }
            }
        }

        assert!(
            saw_active,
            "client never reported active state during query"
        );
    }

    wait_for_client_state(&admin, APP_NAME, "idle in transaction").await;

    client.batch_execute("COMMIT;").await.unwrap();
    wait_for_client_state(&admin, APP_NAME, "idle").await;

    drop(client);

    wait_for_no_client(&admin, APP_NAME).await;

    admin.close().await;
    connection_handle.await.unwrap();
}

async fn fetch_client_state(admin: &Pool<Postgres>, application_name: &str) -> Option<String> {
    let rows = admin.fetch_all("SHOW CLIENTS").await.unwrap();
    for row in rows {
        let db: String = row.get::<String, _>("database");
        let app: String = row.get::<String, _>("application_name");
        if db == "pgdog" && app == application_name {
            return Some(row.get::<String, _>("state"));
        }
    }
    None
}

async fn wait_for_client_state(admin: &Pool<Postgres>, application_name: &str, expected: &str) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(state) = fetch_client_state(admin, application_name).await {
            if state == expected {
                return;
            }
        }

        if Instant::now() >= deadline {
            panic!(
                "timed out waiting for client state '{}' (expected '{}')",
                fetch_client_state(admin, application_name)
                    .await
                    .unwrap_or_else(|| "<none>".to_string()),
                expected
            );
        }

        sleep(Duration::from_millis(25)).await;
    }
}

async fn wait_for_no_client(admin: &Pool<Postgres>, application_name: &str) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if fetch_client_state(admin, application_name).await.is_none() {
            return;
        }

        if Instant::now() >= deadline {
            panic!("client '{}' still present", application_name);
        }

        sleep(Duration::from_millis(25)).await;
    }
}
