use crate::{
    backend::databases::reload_from_existing,
    config::{config, load_test_sharded, set},
    expect_message,
    net::{CommandComplete, ErrorResponse, ReadyForQuery, parameter::ParameterValue},
};

use super::prelude::*;

/// Number of shards the client is currently connected to.
fn connected_servers(client: &mut TestClient) -> usize {
    client.engine.backend().connected_servers()
}

/// Total number of shards in the client's cluster.
fn shard_count(client: &mut TestClient) -> usize {
    client.engine.backend().cluster().unwrap().shards().len()
}

#[tokio::test]
async fn test_set() {
    let mut test_client = TestClient::new_sharded(Parameters::default()).await;

    test_client
        .send_simple(Query::new("SET application_name TO 'test_set'"))
        .await;

    assert_eq!(
        expect_message!(test_client.read().await, CommandComplete).command(),
        "SET"
    );
    assert_eq!(
        expect_message!(test_client.read().await, ReadyForQuery).status,
        'I'
    );

    assert_eq!(
        test_client.client().params.get("application_name").unwrap(),
        &ParameterValue::String("test_set".into()),
    );

    assert!(!test_client.backend_locked());
}

#[tokio::test]
async fn test_set_search_path() {
    let mut test_client = TestClient::new_sharded(Parameters::default()).await;

    test_client
        .send_simple(Query::new(
            "SET search_path TO \"$user\", public, acustomer",
        ))
        .await;

    assert_eq!(
        expect_message!(test_client.read().await, CommandComplete).command(),
        "SET"
    );
    assert_eq!(
        expect_message!(test_client.read().await, ReadyForQuery).status,
        'I'
    );

    assert_eq!(
        test_client.client().params.get("search_path").unwrap(),
        &ParameterValue::Tuple(vec!["$user".into(), "public".into(), "acustomer".into()]),
    );
}

#[tokio::test]
async fn test_set_inside_transaction() {
    let mut test_client = TestClient::new_sharded(Parameters::default()).await;

    test_client.send_simple(Query::new("BEGIN")).await;

    assert_eq!(
        expect_message!(test_client.read().await, CommandComplete).command(),
        "BEGIN"
    );
    assert_eq!(
        expect_message!(test_client.read().await, ReadyForQuery).status,
        'T'
    );

    assert!(!test_client.backend_locked());

    test_client
        .send_simple(Query::new("SET search_path TO acustomer, public"))
        .await;

    assert_eq!(
        expect_message!(test_client.read().await, CommandComplete).command(),
        "SET"
    );
    assert_eq!(
        expect_message!(test_client.read().await, ReadyForQuery).status,
        'T'
    );

    test_client.send_simple(Query::new("COMMIT")).await;

    assert_eq!(
        expect_message!(test_client.read().await, CommandComplete).command(),
        "COMMIT"
    );
    assert_eq!(
        expect_message!(test_client.read().await, ReadyForQuery).status,
        'I'
    );

    assert_eq!(
        test_client.client().params.get("search_path").unwrap(),
        &ParameterValue::Tuple(vec!["acustomer".into(), "public".into()]),
    );

    assert!(!test_client.backend_locked());
}

#[tokio::test]
async fn test_set_inside_transaction_rollback() {
    let mut test_client = TestClient::new_sharded(Parameters::default()).await;

    test_client.send_simple(Query::new("BEGIN")).await;

    assert_eq!(
        expect_message!(test_client.read().await, CommandComplete).command(),
        "BEGIN"
    );
    assert_eq!(
        expect_message!(test_client.read().await, ReadyForQuery).status,
        'T'
    );

    test_client
        .send_simple(Query::new("SET search_path TO acustomer, public"))
        .await;

    assert_eq!(
        expect_message!(test_client.read().await, CommandComplete).command(),
        "SET"
    );
    assert_eq!(
        expect_message!(test_client.read().await, ReadyForQuery).status,
        'T'
    );

    test_client.send_simple(Query::new("ROLLBACK")).await;

    assert_eq!(
        expect_message!(test_client.read().await, CommandComplete).command(),
        "ROLLBACK"
    );
    assert_eq!(
        expect_message!(test_client.read().await, ReadyForQuery).status,
        'I'
    );

    assert!(
        test_client.client().params.get("search_path").is_none(),
        "search_path should not be set",
    );
}

#[tokio::test]
async fn test_reset() {
    let mut test_client = TestClient::new_sharded(Parameters::default()).await;

    // First set a parameter
    test_client
        .send_simple(Query::new("SET application_name TO 'test_reset'"))
        .await;

    assert_eq!(
        expect_message!(test_client.read().await, CommandComplete).command(),
        "SET"
    );
    assert_eq!(
        expect_message!(test_client.read().await, ReadyForQuery).status,
        'I'
    );

    assert_eq!(
        test_client.client().params.get("application_name").unwrap(),
        &ParameterValue::String("test_reset".into()),
    );

    // Now reset it
    test_client
        .send_simple(Query::new("RESET application_name"))
        .await;

    assert_eq!(
        expect_message!(test_client.read().await, CommandComplete).command(),
        "RESET"
    );
    assert_eq!(
        expect_message!(test_client.read().await, ReadyForQuery).status,
        'I'
    );

    assert!(
        test_client
            .client()
            .params
            .get("application_name")
            .is_none(),
        "application_name should be reset"
    );
}

#[tokio::test]
async fn test_reset_all() {
    let mut test_client = TestClient::new_sharded(Parameters::default()).await;

    // Set multiple parameters
    test_client
        .send_simple(Query::new("SET application_name TO 'test_reset_all'"))
        .await;
    expect_message!(test_client.read().await, CommandComplete);
    expect_message!(test_client.read().await, ReadyForQuery);

    test_client
        .send_simple(Query::new("SET statement_timeout TO 5000"))
        .await;
    expect_message!(test_client.read().await, CommandComplete);
    expect_message!(test_client.read().await, ReadyForQuery);

    test_client
        .send_simple(Query::new("SET lock_timeout TO 5000"))
        .await;
    expect_message!(test_client.read().await, CommandComplete);
    expect_message!(test_client.read().await, ReadyForQuery);

    assert!(
        test_client
            .client()
            .params
            .get("application_name")
            .is_some()
    );
    assert!(
        test_client
            .client()
            .params
            .get("statement_timeout")
            .is_some()
    );
    assert!(test_client.client().params.get("lock_timeout").is_some());

    // Reset all
    test_client.send_simple(Query::new("RESET ALL")).await;

    assert_eq!(
        expect_message!(test_client.read().await, CommandComplete).command(),
        "RESET"
    );
    assert_eq!(
        expect_message!(test_client.read().await, ReadyForQuery).status,
        'I'
    );

    assert!(
        test_client
            .client()
            .params
            .get("application_name")
            .is_none(),
        "application_name should be reset"
    );
    assert!(
        test_client
            .client()
            .params
            .get("statement_timeout")
            .is_none(),
        "statement_timeout should be reset"
    );
    assert!(
        test_client.client().params.get("lock_timeout").is_none(),
        "lock_timeout should be reset"
    );
}

#[tokio::test]
async fn test_reset_inside_transaction_commit() {
    let mut test_client = TestClient::new_sharded(Parameters::default()).await;

    // Set a parameter outside transaction
    test_client
        .send_simple(Query::new("SET application_name TO 'before_reset'"))
        .await;
    expect_message!(test_client.read().await, CommandComplete);
    expect_message!(test_client.read().await, ReadyForQuery);

    assert_eq!(
        test_client.client().params.get("application_name").unwrap(),
        &ParameterValue::String("before_reset".into()),
    );

    // Begin transaction
    test_client.send_simple(Query::new("BEGIN")).await;
    expect_message!(test_client.read().await, CommandComplete);
    assert_eq!(
        expect_message!(test_client.read().await, ReadyForQuery).status,
        'T'
    );

    // Reset inside transaction
    test_client
        .send_simple(Query::new("RESET application_name"))
        .await;
    assert_eq!(
        expect_message!(test_client.read().await, CommandComplete).command(),
        "RESET"
    );
    assert_eq!(
        expect_message!(test_client.read().await, ReadyForQuery).status,
        'T'
    );

    // Commit
    test_client.send_simple(Query::new("COMMIT")).await;
    expect_message!(test_client.read().await, CommandComplete);
    assert_eq!(
        expect_message!(test_client.read().await, ReadyForQuery).status,
        'I'
    );

    // Parameter should be reset after commit
    assert!(
        test_client
            .client()
            .params
            .get("application_name")
            .is_none(),
        "application_name should be reset after commit"
    );
}

#[tokio::test]
async fn test_reset_inside_transaction_rollback() {
    let mut test_client = TestClient::new_sharded(Parameters::default()).await;

    // Set a parameter outside transaction
    test_client
        .send_simple(Query::new("SET application_name TO 'before_reset'"))
        .await;
    expect_message!(test_client.read().await, CommandComplete);
    expect_message!(test_client.read().await, ReadyForQuery);

    assert_eq!(
        test_client.client().params.get("application_name").unwrap(),
        &ParameterValue::String("before_reset".into()),
    );

    // Begin transaction
    test_client.send_simple(Query::new("BEGIN")).await;
    expect_message!(test_client.read().await, CommandComplete);
    assert_eq!(
        expect_message!(test_client.read().await, ReadyForQuery).status,
        'T'
    );

    // Reset inside transaction
    test_client
        .send_simple(Query::new("RESET application_name"))
        .await;
    assert_eq!(
        expect_message!(test_client.read().await, CommandComplete).command(),
        "RESET"
    );
    assert_eq!(
        expect_message!(test_client.read().await, ReadyForQuery).status,
        'T'
    );

    // Rollback
    test_client.send_simple(Query::new("ROLLBACK")).await;
    expect_message!(test_client.read().await, CommandComplete);
    assert_eq!(
        expect_message!(test_client.read().await, ReadyForQuery).status,
        'I'
    );

    // Parameter should be restored after rollback
    assert_eq!(
        test_client.client().params.get("application_name").unwrap(),
        &ParameterValue::String("before_reset".into()),
        "application_name should be restored after rollback"
    );
}

/// `SET pgdog.shard` pins the transaction to a single shard: a subsequent query
/// connects to exactly one backend, even on a multi-shard cluster.
#[tokio::test]
async fn test_set_shard_pins_transaction_to_one_shard() {
    let mut test_client = TestClient::new_sharded(Parameters::default()).await;
    assert!(
        shard_count(&mut test_client) > 1,
        "test requires a multi-shard cluster"
    );

    test_client.send_simple(Query::new("BEGIN")).await;
    expect_message!(test_client.read().await, CommandComplete);
    assert_eq!(
        expect_message!(test_client.read().await, ReadyForQuery).status,
        'T'
    );

    test_client
        .send_simple(Query::new("SET pgdog.shard TO 0"))
        .await;
    assert_eq!(
        expect_message!(test_client.read().await, CommandComplete).command(),
        "SET"
    );
    expect_message!(test_client.read().await, ReadyForQuery);

    // Force the backend to actually connect to the pinned shard.
    test_client.send_simple(Query::new("SELECT 1")).await;
    test_client.read_until('Z').await.unwrap();

    assert_eq!(
        connected_servers(&mut test_client),
        1,
        "SET pgdog.shard should pin the transaction to a single shard"
    );

    test_client.send_simple(Query::new("ROLLBACK")).await;
    test_client.read_until('Z').await.unwrap();
}

/// `SET pgdog.sharding_key` pins the transaction to a single shard: a subsequent
/// query touching a sharded table connects to exactly one backend.
///
/// Uses a config without sharded schemas (and a single sharding function) so the
/// key resolves via its hash rather than being interpreted as a schema name.
#[tokio::test]
async fn test_set_sharding_key_pins_transaction_to_one_shard() {
    load_test_sharded();
    let mut cfg = (*config()).clone();
    cfg.config.sharded_schemas.clear();
    cfg.config
        .sharded_tables
        .retain(|t| t.name.as_deref() == Some("sharded"));
    set(cfg).unwrap();
    reload_from_existing().unwrap();

    let mut test_client = TestClient::new(Parameters::default()).await;
    assert!(
        shard_count(&mut test_client) > 1,
        "test requires a multi-shard cluster"
    );

    test_client.send_simple(Query::new("BEGIN")).await;
    expect_message!(test_client.read().await, CommandComplete);
    assert_eq!(
        expect_message!(test_client.read().await, ReadyForQuery).status,
        'T'
    );

    test_client
        .send_simple(Query::new("SET pgdog.sharding_key TO '1'"))
        .await;
    assert_eq!(
        expect_message!(test_client.read().await, CommandComplete).command(),
        "SET"
    );
    expect_message!(test_client.read().await, ReadyForQuery);

    // The sharding key only resolves to a single shard for queries that touch a
    // sharded table, so connect with a read from `sharded`.
    test_client
        .send_simple(Query::new("SELECT * FROM sharded"))
        .await;
    test_client.read_until('Z').await.unwrap();

    assert_eq!(
        connected_servers(&mut test_client),
        1,
        "SET pgdog.sharding_key should pin the transaction to a single shard"
    );

    test_client.send_simple(Query::new("ROLLBACK")).await;
    test_client.read_until('Z').await.unwrap();
}

/// Once a query has connected the transaction to a server, `SET pgdog.shard` is
/// rejected: the connection is already pinned.
#[tokio::test]
async fn test_set_shard_rejected_after_connect() {
    let mut test_client = TestClient::new_sharded(Parameters::default()).await;

    test_client.send_simple(Query::new("BEGIN")).await;
    expect_message!(test_client.read().await, CommandComplete);
    expect_message!(test_client.read().await, ReadyForQuery);

    // Connect the transaction to a server.
    test_client.send_simple(Query::new("SELECT 1")).await;
    test_client.read_until('Z').await.unwrap();
    assert!(test_client.backend_connected());

    test_client
        .send_simple(Query::new("SET pgdog.shard TO 0"))
        .await;
    let err = expect_message!(test_client.read().await, ErrorResponse);
    assert_eq!(
        err.message,
        "cannot use \"SET pgdog.shard\" after connecting to a server; \
         set it before running any queries"
    );
    expect_message!(test_client.read().await, ReadyForQuery);

    test_client.send_simple(Query::new("ROLLBACK")).await;
    test_client.read_until('Z').await.unwrap();
}

/// Same as above for `SET pgdog.sharding_key`.
#[tokio::test]
async fn test_set_sharding_key_rejected_after_connect() {
    let mut test_client = TestClient::new_sharded(Parameters::default()).await;

    test_client.send_simple(Query::new("BEGIN")).await;
    expect_message!(test_client.read().await, CommandComplete);
    expect_message!(test_client.read().await, ReadyForQuery);

    test_client.send_simple(Query::new("SELECT 1")).await;
    test_client.read_until('Z').await.unwrap();
    assert!(test_client.backend_connected());

    test_client
        .send_simple(Query::new("SET pgdog.sharding_key TO '1'"))
        .await;
    let err = expect_message!(test_client.read().await, ErrorResponse);
    assert_eq!(
        err.message,
        "cannot use \"SET pgdog.sharding_key\" after connecting to a server; \
         set it before running any queries"
    );
    expect_message!(test_client.read().await, ReadyForQuery);

    test_client.send_simple(Query::new("ROLLBACK")).await;
    test_client.read_until('Z').await.unwrap();
}

#[tokio::test]
async fn test_lock_timeout() {
    let mut test_client = TestClient::new_sharded(Parameters::default()).await;

    test_client
        .send_simple(Query::new("SET lock_timeout TO 3000"))
        .await;

    assert_eq!(
        expect_message!(test_client.read().await, CommandComplete).command(),
        "SET"
    );
    assert_eq!(
        expect_message!(test_client.read().await, ReadyForQuery).status,
        'I'
    );

    assert_eq!(
        test_client.client().params.get("lock_timeout"),
        Some(&ParameterValue::String("3000".into())),
        "lock_timeout should be tracked with the correct value after SET"
    );

    // Reset clears it.
    test_client
        .send_simple(Query::new("RESET lock_timeout"))
        .await;

    assert_eq!(
        expect_message!(test_client.read().await, CommandComplete).command(),
        "RESET"
    );
    assert_eq!(
        expect_message!(test_client.read().await, ReadyForQuery).status,
        'I'
    );

    assert!(
        test_client.client().params.get("lock_timeout").is_none(),
        "lock_timeout should be cleared after RESET"
    );
}
