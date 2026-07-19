//! Tests for blocking writes to omnisharded tables inside a direct-to-shard
//! transaction.
//!
//! Omnisharded tables hold the same data on every shard. A write to an
//! omnisharded table must therefore reach all shards. If a transaction has
//! already pinned itself to a single shard (a direct-to-shard transaction), an
//! omni write can only reach that one shard, which would leave the shards
//! inconsistent. We block such writes with an error instead.
//!
//! See [`crate::frontend::client::query_engine::route_query`].

use crate::{
    backend::databases::reload_from_existing,
    config::{config, load_test_sharded, set},
    expect_message,
    net::{CommandComplete, ErrorResponse, Parameters, Query, ReadyForQuery},
};

use super::prelude::*;

/// Message returned by [`ErrorResponse::omni_in_direct_to_shard`]. The SQLSTATE
/// (58000) is shared by several errors, so we match on the message instead.
const OMNI_IN_DIRECT_TO_SHARD_MESSAGE: &str =
    "cannot write to an omnisharded table in a direct-to-shard transaction";

/// Assert that `err` is the omni-in-direct-to-shard error.
fn assert_omni_in_direct_to_shard(err: &ErrorResponse) {
    assert_eq!(
        err.message, OMNI_IN_DIRECT_TO_SHARD_MESSAGE,
        "unexpected error: {:?}",
        err
    );
}

/// A sharded client whose config has no sharded schemas, so that
/// `SET pgdog.sharding_key` resolves via the hash of the key (the documented
/// sharding-key path) rather than being interpreted as a schema name.
async fn new_sharded_client_without_schemas() -> TestClient {
    load_test_sharded();
    let mut cfg = (*config()).clone();
    cfg.config.sharded_schemas.clear();
    // Keep a single sharding function so `SET pgdog.sharding_key` can resolve a
    // numeric key without ambiguity ("more than one sharding function").
    cfg.config
        .sharded_tables
        .retain(|t| t.name.as_deref() == Some("sharded"));
    set(cfg).unwrap();
    reload_from_existing().unwrap();
    TestClient::new(Parameters::default()).await
}

/// Ensure both tables exist on every shard and are empty.
async fn reset_tables(client: &mut TestClient) {
    for table in ["sharded", "sharded_omni"] {
        client
            .send_simple(Query::new(format!(
                "CREATE TABLE IF NOT EXISTS {} (id BIGINT PRIMARY KEY, value TEXT)",
                table
            )))
            .await;
        client.read_until('Z').await.unwrap();

        client
            .send_simple(Query::new(format!("DELETE FROM {}", table)))
            .await;
        client.read_until('Z').await.unwrap();
    }
}

/// Pin the current transaction to a single shard via `SET pgdog.shard` and force
/// the backend to connect to it with a trivial query.
async fn pin_to_shard(client: &mut TestClient, shard: usize) {
    client
        .send_simple(Query::new(format!("SET pgdog.shard TO {}", shard)))
        .await;
    client.read_until('Z').await.unwrap();

    // Force the backend to actually connect to the single shard.
    client.send_simple(Query::new("SELECT 1")).await;
    client.read_until('Z').await.unwrap();
}

/// Pin the current transaction to a single shard via `SET pgdog.sharding_key` and
/// force the backend to connect to it with a trivial query.
async fn pin_to_sharding_key(client: &mut TestClient, key: i64) {
    client
        .send_simple(Query::new(format!("SET pgdog.sharding_key TO '{}'", key)))
        .await;
    client.read_until('Z').await.unwrap();

    // Force the backend to connect to the single shard the key resolves to.
    // The sharding key only resolves to one shard for queries that touch a
    // sharded table, so we read from `sharded` here (not `SELECT 1`).
    client
        .send_simple(Query::new("SELECT * FROM sharded"))
        .await;
    client.read_until('Z').await.unwrap();
}

/// A write to an omnisharded table inside a transaction that has already pinned
/// itself to a single shard is rejected.
#[tokio::test]
async fn test_omni_write_blocked_in_direct_to_shard_transaction() {
    let mut client = TestClient::new_sharded(Parameters::default()).await;
    reset_tables(&mut client).await;

    client.send_simple(Query::new("BEGIN")).await;
    client.read_until('Z').await.unwrap();

    // Pin the transaction to a single shard. From now on the backend is
    // connected to one shard only.
    pin_to_shard(&mut client, 0).await;

    // The omni write cannot reach all shards anymore, so it must be blocked.
    client
        .send_simple(Query::new(
            "INSERT INTO sharded_omni (id, value) VALUES (1, 'a')",
        ))
        .await;

    let err = expect_message!(client.read().await, ErrorResponse);
    assert_omni_in_direct_to_shard(&err);
    // Transaction is now in the aborted state.
    let rfq = expect_message!(client.read().await, ReadyForQuery);
    assert_eq!(rfq.status, 'E');

    client.send_simple(Query::new("ROLLBACK")).await;
    client.read_until('Z').await.unwrap();

    reset_tables(&mut client).await;
}

/// A write to an omnisharded table outside of any transaction is allowed: it
/// fans out to every shard.
#[tokio::test]
async fn test_omni_write_allowed_outside_transaction() {
    let mut client = TestClient::new_sharded(Parameters::default()).await;
    reset_tables(&mut client).await;

    client
        .send_simple(Query::new(
            "INSERT INTO sharded_omni (id, value) VALUES (1, 'a')",
        ))
        .await;

    let cc = expect_message!(client.read().await, CommandComplete);
    assert_eq!(cc.command(), "INSERT 0 1");
    expect_message!(client.read().await, ReadyForQuery);

    reset_tables(&mut client).await;
}

/// A write to an omnisharded table as the FIRST statement of a transaction is
/// allowed: the transaction has not pinned itself to a single shard yet, so the
/// write still fans out to every shard.
#[tokio::test]
async fn test_omni_write_allowed_as_first_statement_in_transaction() {
    let mut client = TestClient::new_sharded(Parameters::default()).await;
    reset_tables(&mut client).await;

    client.send_simple(Query::new("BEGIN")).await;
    client.read_until('Z').await.unwrap();

    client
        .send_simple(Query::new(
            "INSERT INTO sharded_omni (id, value) VALUES (1, 'a')",
        ))
        .await;
    let cc = expect_message!(client.read().await, CommandComplete);
    assert_eq!(cc.command(), "INSERT 0 1");
    expect_message!(client.read().await, ReadyForQuery);

    client.send_simple(Query::new("COMMIT")).await;
    client.read_until('Z').await.unwrap();

    reset_tables(&mut client).await;
}

/// A SELECT against an omnisharded table inside a direct-to-shard transaction is
/// also blocked: inside an explicit transaction, reads are routed to the primary
/// (treated as writes), so they hit the same omni-in-direct-to-shard guard.
#[tokio::test]
async fn test_omni_read_blocked_in_direct_to_shard_transaction() {
    let mut client = TestClient::new_sharded(Parameters::default()).await;
    reset_tables(&mut client).await;

    client.send_simple(Query::new("BEGIN")).await;
    client.read_until('Z').await.unwrap();

    // Pin the transaction to a single shard.
    pin_to_shard(&mut client, 0).await;

    client
        .send_simple(Query::new("SELECT * FROM sharded_omni"))
        .await;
    let err = expect_message!(client.read().await, ErrorResponse);
    assert_omni_in_direct_to_shard(&err);
    let rfq = expect_message!(client.read().await, ReadyForQuery);
    assert_eq!(rfq.status, 'E');

    client.send_simple(Query::new("ROLLBACK")).await;
    client.read_until('Z').await.unwrap();

    reset_tables(&mut client).await;
}

/// `SET pgdog.sharding_key` also pins the transaction to a single shard, so an
/// omni write afterwards is rejected just like with `SET pgdog.shard`.
#[tokio::test]
async fn test_omni_write_blocked_after_set_sharding_key() {
    let mut client = new_sharded_client_without_schemas().await;
    reset_tables(&mut client).await;

    client.send_simple(Query::new("BEGIN")).await;
    client.read_until('Z').await.unwrap();

    pin_to_sharding_key(&mut client, 1).await;

    client
        .send_simple(Query::new(
            "INSERT INTO sharded_omni (id, value) VALUES (1, 'a')",
        ))
        .await;

    let err = expect_message!(client.read().await, ErrorResponse);
    assert_omni_in_direct_to_shard(&err);
    let rfq = expect_message!(client.read().await, ReadyForQuery);
    assert_eq!(rfq.status, 'E');

    client.send_simple(Query::new("ROLLBACK")).await;
    client.read_until('Z').await.unwrap();

    reset_tables(&mut client).await;
}

/// A SELECT against an omnisharded table after `SET pgdog.sharding_key` is also
/// blocked, for the same reason as the `SET pgdog.shard` case: in-transaction
/// reads are routed to the primary and hit the omni-in-direct-to-shard guard.
#[tokio::test]
async fn test_omni_read_blocked_after_set_sharding_key() {
    let mut client = new_sharded_client_without_schemas().await;
    reset_tables(&mut client).await;

    client.send_simple(Query::new("BEGIN")).await;
    client.read_until('Z').await.unwrap();

    pin_to_sharding_key(&mut client, 1).await;

    client
        .send_simple(Query::new("SELECT * FROM sharded_omni"))
        .await;
    let err = expect_message!(client.read().await, ErrorResponse);
    assert_omni_in_direct_to_shard(&err);
    let rfq = expect_message!(client.read().await, ReadyForQuery);
    assert_eq!(rfq.status, 'E');

    client.send_simple(Query::new("ROLLBACK")).await;
    client.read_until('Z').await.unwrap();

    reset_tables(&mut client).await;
}
