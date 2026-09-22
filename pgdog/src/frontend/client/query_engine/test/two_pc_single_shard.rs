//! With `two_phase_commit` on, a write transaction pinned to one shard
//! commits with a plain COMMIT: a single server is atomic on its own,
//! and the direct binding it runs on has no 2pc path.

use crate::{
    expect_message,
    net::{CommandComplete, Parameters, Query, ReadyForQuery},
};

use super::prelude::*;

#[tokio::test]
async fn test_two_pc_single_shard_write_commits_plainly() {
    let mut client = TestClient::new_sharded_two_pc(Parameters::default()).await;

    let id = client.random_id_for_shard(0);

    // Cleanup first.
    client
        .send_simple(Query::new(format!("DELETE FROM sharded WHERE id = {}", id)))
        .await;
    client.read_until('Z').await.unwrap();

    client.send_simple(Query::new("BEGIN")).await;
    client.read_until('Z').await.unwrap();

    // Pin the transaction to one shard: the connection binds direct to
    // it instead of connecting to every shard.
    client.send_simple(Query::new("SET pgdog.shard TO 0")).await;
    client.read_until('Z').await.unwrap();

    client
        .send_simple(Query::new(format!(
            "INSERT INTO sharded (id, value) VALUES ({}, 'one_shard_2pc')",
            id
        )))
        .await;
    let messages = client.read_until('Z').await.unwrap();
    assert!(
        messages.iter().all(|m| m.code() != 'E'),
        "shard-pinned INSERT failed: {:?}",
        messages
    );

    // The whole point: this used to fail with "2pc commit supported
    // with multi-shard binding only".
    client.send_simple(Query::new("COMMIT")).await;
    let cc = expect_message!(client.read().await, CommandComplete);
    assert_eq!(
        cc.command(),
        "COMMIT",
        "a shard-pinned write transaction must commit without 2pc"
    );
    expect_message!(client.read().await, ReadyForQuery);

    // The write committed.
    client
        .send_simple(Query::new(format!(
            "SELECT value FROM sharded WHERE id = {}",
            id
        )))
        .await;
    let messages = client.read_until('Z').await.unwrap();
    assert_eq!(
        messages.iter().filter(|m| m.code() == 'D').count(),
        1,
        "committed row should be visible"
    );

    // No prepared transaction was left behind on any shard.
    client
        .send_simple(Query::new("SELECT gid FROM pg_prepared_xacts"))
        .await;
    let messages = client.read_until('Z').await.unwrap();
    assert_eq!(
        messages.iter().filter(|m| m.code() == 'D').count(),
        0,
        "a single-shard commit must not prepare a transaction"
    );

    // Cleanup.
    client
        .send_simple(Query::new(format!("DELETE FROM sharded WHERE id = {}", id)))
        .await;
    client.read_until('Z').await.unwrap();
}

#[tokio::test]
async fn test_two_pc_sharding_key_pinned_write_commits_plainly() {
    let mut client = TestClient::new_sharded_two_pc(Parameters::default()).await;

    let id = client.random_id_for_shard(1);

    client
        .send_simple(Query::new(format!("DELETE FROM sharded WHERE id = {}", id)))
        .await;
    client.read_until('Z').await.unwrap();

    client.send_simple(Query::new("BEGIN")).await;
    client.read_until('Z').await.unwrap();

    client
        .send_simple(Query::new(format!("SET pgdog.sharding_key TO '{}'", id)))
        .await;
    client.read_until('Z').await.unwrap();

    client
        .send_simple(Query::new(format!(
            "INSERT INTO sharded (id, value) VALUES ({}, 'key_pinned_2pc')",
            id
        )))
        .await;
    let messages = client.read_until('Z').await.unwrap();
    assert!(
        messages.iter().all(|m| m.code() != 'E'),
        "key-pinned INSERT failed: {:?}",
        messages
    );

    client.send_simple(Query::new("COMMIT")).await;
    let cc = expect_message!(client.read().await, CommandComplete);
    assert_eq!(
        cc.command(),
        "COMMIT",
        "a key-pinned write transaction must commit without 2pc"
    );
    expect_message!(client.read().await, ReadyForQuery);

    client
        .send_simple(Query::new(format!("DELETE FROM sharded WHERE id = {}", id)))
        .await;
    client.read_until('Z').await.unwrap();
}
