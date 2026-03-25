use std::time::Duration;

use pgdog_postgres_types::Format;
use tokio::time::sleep;

use crate::{
    backend::{server::test::test_server, Server},
    expect_message,
    net::{DataRow, RowDescription},
};

use super::prelude::*;

#[tokio::test]
async fn test_idle_in_transaction_partial_recovery() {
    crate::logger();

    // Direct connection for testing state.
    let mut test_server = test_server().await;

    let mut client = TestClient::new_replicas(Parameters::default())
        .await
        .leak_pool();

    client.send_simple(Query::new("BEGIN")).await;
    client.read_until('Z').await.unwrap();

    client
        .send_simple(Query::new("SELECT pg_backend_pid()::text"))
        .await;
    expect_message!(client.read().await, RowDescription);
    let rd = expect_message!(client.read().await, DataRow);
    let pid: String = rd.get(0, Format::Text).unwrap();

    client.read_until('Z').await.unwrap();

    // This won't fire because we'll be stuck inside the extended exchange.
    client
        .send_simple(Query::new("SET idle_in_transaction_session_timeout TO 100"))
        .await;
    client.read_until('Z').await.unwrap();

    client.send(Parse::named("__test_1", "SELECT $1")).await;
    client.send(Flush).await;
    client.try_process().await.unwrap();
    client.read_until('1').await.unwrap();

    // Stuck inside extended exchange, idle in transaction timeout will not fire.
    sleep(Duration::from_millis(100)).await;

    client.send(Parse::named("__test_2", "SELECT $1")).await;
    client.send(Flush).await;

    client.try_process().await.unwrap();
    client.read_until('1').await.unwrap();

    // Server in active state.
    assert_server_state(&mut test_server, &pid, "active").await;

    client.send(Terminate).await;
    drop(client);

    sleep(Duration::from_millis(50)).await;

    // Cleanup works.
    assert_server_state(&mut test_server, &pid, "idle").await;
}

async fn assert_server_state(conn: &mut Server, pid: &str, expected: &str) {
    let response: Vec<String> = conn
        .fetch_all(format!(
            "SELECT state::text FROM pg_stat_activity WHERE pid = {}",
            pid
        ))
        .await
        .unwrap();
    assert_eq!(response[0], expected);
}
