use crate::{
    backend::databases::reload_from_existing,
    config::{config, load_test, set},
    expect_message,
    net::{CommandComplete, ErrorResponse, ReadyForQuery},
};

use super::prelude::*;

async fn run_simple(client: &mut TestClient, query: &str) -> ReadyForQuery {
    client.send_simple(Query::new(query)).await;
    expect_message!(client.read().await, CommandComplete);
    expect_message!(client.read().await, ReadyForQuery)
}

fn load_single_connection_test_pool() {
    load_test();

    let mut config = (*config()).clone();
    config.config.general.default_pool_size = 1;
    config.config.general.min_pool_size = 0;
    set(config).unwrap();
    reload_from_existing().unwrap();
}

fn assert_single_connection_pool(client: &mut TestClient) {
    let cluster = client.engine.backend().cluster().unwrap();
    let pools: Vec<_> = cluster
        .shards()
        .iter()
        .flat_map(|shard| shard.pool_iter())
        .collect();

    assert_eq!(pools.len(), 1, "test cluster should use one pool");
    assert_eq!(
        pools[0].config().max,
        1,
        "test pool should allow one connection"
    );
    assert_eq!(pools[0].config().min, 0);
}

#[tokio::test]
async fn test_pgdog_pin_holds_backend_until_false_or_reset() {
    let mut client = TestClient::new_sharded(Parameters::default()).await;

    let rfq = run_simple(&mut client, "SET pgdog.pin TO true").await;
    assert_eq!(rfq.status, 'I');
    assert!(!client.backend_connected());

    client.send_simple(Query::new("SELECT 1")).await;
    client.read_until('Z').await.unwrap();
    assert!(
        client.backend_locked(),
        "pgdog.pin=true should lock the backend"
    );
    assert!(
        client.backend_connected(),
        "transaction-mode backend should not be released while pinned"
    );

    let rfq = run_simple(&mut client, "RESET pgdog.pin").await;
    assert_eq!(rfq.status, 'I');
    assert!(
        !client.backend_locked(),
        "RESET pgdog.pin should unlock the backend"
    );
    assert!(
        !client.backend_connected(),
        "transaction-mode backend should be released after RESET pgdog.pin"
    );

    let rfq = run_simple(&mut client, "SET pgdog.pin TO true").await;
    assert_eq!(rfq.status, 'I');

    client.send_simple(Query::new("SELECT 1")).await;
    client.read_until('Z').await.unwrap();
    assert!(client.backend_locked());
    assert!(client.backend_connected());

    let rfq = run_simple(&mut client, "SET pgdog.pin TO false").await;
    assert_eq!(rfq.status, 'I');
    assert!(
        !client.backend_locked(),
        "pgdog.pin=false should unlock the backend"
    );
    assert!(
        !client.backend_connected(),
        "transaction-mode backend should be released after pgdog.pin=false"
    );
}

#[tokio::test]
async fn test_pgdog_pin_release_discards_temp_tables() {
    load_single_connection_test_pool();
    let mut client = TestClient::new(Parameters::default()).await;
    assert_single_connection_pool(&mut client);

    let rfq = run_simple(&mut client, "SET pgdog.pin TO true").await;
    assert_eq!(rfq.status, 'I');

    let rfq = run_simple(
        &mut client,
        "CREATE TEMP TABLE pgdog_manual_lock_temp AS SELECT 1 AS id",
    )
    .await;
    assert_eq!(rfq.status, 'I');
    assert!(client.backend_locked());
    assert!(client.backend_connected());
    let pinned_backend_pid = client.backend_pid().await;

    let rfq = run_simple(&mut client, "SET pgdog.pin TO false").await;
    assert_eq!(rfq.status, 'I');
    assert!(!client.backend_locked());
    assert!(!client.backend_connected());

    let reused_backend_pid = client.backend_pid().await;
    assert_eq!(
        pinned_backend_pid, reused_backend_pid,
        "single-connection test pool should reuse the same backend"
    );

    client
        .send_simple(Query::new("SELECT * FROM pgdog_manual_lock_temp"))
        .await;
    let err = expect_message!(client.read().await, ErrorResponse);
    assert_eq!(err.severity, "ERROR");
    assert_eq!(err.code, "42P01");
    assert_eq!(
        err.message,
        r#"relation "pgdog_manual_lock_temp" does not exist"#
    );
    assert_eq!(
        expect_message!(client.read().await, ReadyForQuery).status,
        'I'
    );
}
