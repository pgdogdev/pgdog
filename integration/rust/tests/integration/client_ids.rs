use futures_util::future::join_all;
use rust::setup::admin_sqlx;
use serial_test::serial;
use sqlx::{Executor, Row};
use std::collections::HashSet;
use tokio::task::JoinHandle;
use tokio_postgres::{Client, NoTls};

/// Number of client connections to create for testing unique IDs.
const NUM_CLIENTS: usize = 500;

#[tokio::test]
#[serial]
async fn test_client_ids_unique() {
    // Set auth type to md5 for faster connection setup.
    let admin = admin_sqlx().await;
    admin.execute("SET auth_type TO 'md5'").await.unwrap();
    admin.close().await;

    // Spawn all connection attempts in parallel.
    let connect_futures: Vec<_> = (0..NUM_CLIENTS)
        .map(|_| async {
            let (client, connection) = tokio_postgres::connect(
                "host=127.0.0.1 user=pgdog dbname=pgdog password=pgdog port=6432 application_name=test_client_ids",
                NoTls,
            )
            .await
            .unwrap();

            let handle = tokio::spawn(async move {
                if let Err(e) = connection.await {
                    eprintln!("connection error: {}", e);
                }
            });

            (client, handle)
        })
        .collect();

    let results: Vec<(Client, JoinHandle<()>)> = join_all(connect_futures).await;
    let (clients, connection_handles): (Vec<_>, Vec<_>) = results.into_iter().unzip();

    // Connect to admin DB and run SHOW CLIENTS.
    let admin = admin_sqlx().await;
    let rows = admin.fetch_all("SHOW CLIENTS").await.unwrap();

    // Collect IDs for our test clients (filter by application_name).
    let mut client_ids: Vec<i64> = Vec::new();
    for row in &rows {
        let application_name: String = row.get("application_name");
        if application_name == "test_client_ids" {
            let id: i64 = row.get("id");
            client_ids.push(id);
        }
    }

    // Verify we have exactly NUM_CLIENTS test clients.
    assert_eq!(
        client_ids.len(),
        NUM_CLIENTS,
        "expected {} clients, found {}",
        NUM_CLIENTS,
        client_ids.len()
    );

    // Verify all client IDs are unique.
    let unique_ids: HashSet<i64> = client_ids.iter().copied().collect();
    assert_eq!(
        unique_ids.len(),
        NUM_CLIENTS,
        "expected {} unique client IDs, found {} (duplicates exist)",
        NUM_CLIENTS,
        unique_ids.len()
    );

    // Drop clients to close connections gracefully.
    drop(clients);

    // Wait for all connection handlers to complete.
    for handle in connection_handles {
        let _ = handle.await;
    }

    // Restore auth type to scram.
    admin.execute("SET auth_type TO 'scram'").await.unwrap();
    admin.close().await;
}
