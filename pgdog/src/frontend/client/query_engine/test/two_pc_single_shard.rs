//! With `two_phase_commit` on, a write transaction pinned to one shard
//! commits with a plain COMMIT: a single server is atomic on its own,
//! and the direct binding it runs on has no 2pc path.

use crate::{
    backend::databases::{databases, reload_from_existing},
    config::{config, load_test_sharded, set},
    expect_message,
    net::{CommandComplete, Parameters, Query, ReadyForQuery},
};

use super::prelude::*;

/// Two-phase commits recorded across every pool. The response sent to
/// the client is rewritten to a plain `COMMIT` either way, so the pool
/// counters are what tell the two paths apart.
fn total_2pc_commits() -> usize {
    databases()
        .all()
        .values()
        .flat_map(|cluster| cluster.shards().iter())
        .flat_map(|shard| shard.pools())
        .map(|pool| pool.state().stats.counts.xact_2pc_count)
        .sum()
}

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

    // This used to fail with "2pc commit supported with multi-shard
    // binding only": the direct binding has no 2pc path.
    client.send_simple(Query::new("COMMIT")).await;
    let cc = expect_message!(client.read().await, CommandComplete);
    assert_eq!(
        cc.command(),
        "COMMIT",
        "a shard-pinned write transaction must commit"
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

    // Cleanup; also checks the pinned transaction's connection back in,
    // so the pool counters below are settled.
    client
        .send_simple(Query::new(format!("DELETE FROM sharded WHERE id = {}", id)))
        .await;
    client.read_until('Z').await.unwrap();

    assert_eq!(
        total_2pc_commits(),
        0,
        "a single-shard commit must not run two-phase commit"
    );
}

/// Like [`super::set::test_set_sharding_key_pins_transaction_to_one_shard`],
/// the config drops sharded schemas and keeps a single sharding function
/// so the key resolves via its hash and actually pins the transaction.
#[tokio::test]
async fn test_two_pc_sharding_key_pinned_write_commits_plainly() {
    load_test_sharded();
    let mut cfg = (*config()).clone();
    cfg.config.general.two_phase_commit = true;
    cfg.config.sharded_schemas.clear();
    cfg.config
        .sharded_tables
        .retain(|t| t.name.as_deref() == Some("sharded"));
    set(cfg).unwrap();
    reload_from_existing().unwrap();

    let mut client = TestClient::new(Parameters::default()).await;

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
        "a key-pinned write transaction must commit"
    );
    expect_message!(client.read().await, ReadyForQuery);

    client
        .send_simple(Query::new(format!("DELETE FROM sharded WHERE id = {}", id)))
        .await;
    client.read_until('Z').await.unwrap();

    assert_eq!(
        total_2pc_commits(),
        0,
        "a key-pinned commit must not run two-phase commit"
    );
}
