use crate::{
    expect_message,
    net::{CommandComplete, Parameters, Query, ReadyForQuery},
};

use super::prelude::*;

/// Regression test: Shard::Multi with fewer shards than total (e.g. 2 of 3)
/// used to get stuck because send() compared actual shard numbers against
/// positional indices in the pre-filtered servers vec.
///
/// With 3 shards, an IN clause targeting shards 0 and 2 creates a MultiShard
/// binding with only 2 servers. Before the fix, send() used positional indices
/// (0, 1) to match against actual shard numbers ([0, 2]), so server at index 1
/// (shard 2) never received the query — hanging the response.
#[tokio::test]
async fn test_multi_binding_select_subset_of_shards() {
    let mut client = TestClient::new_sharded_3(Parameters::default()).await;

    let id_shard0 = client.random_id_for_shard(0);
    let id_shard2 = client.random_id_for_shard(2);

    // Cleanup
    client
        .send_simple(Query::new(format!(
            "DELETE FROM sharded WHERE id IN ({}, {})",
            id_shard0, id_shard2
        )))
        .await;
    client.read_until('Z').await.unwrap();

    // Insert rows into shards 0 and 2 (skipping shard 1).
    client
        .send_simple(Query::new(format!(
            "INSERT INTO sharded (id, value) VALUES ({}, 'multi0'), ({}, 'multi2') ON CONFLICT (id) DO UPDATE SET value = EXCLUDED.value",
            id_shard0, id_shard2
        )))
        .await;
    client.read_until('Z').await.unwrap();

    // SELECT with IN clause targeting shards 0 and 2 only.
    // This creates a Shard::Multi([0, 2]) binding with 2 of 3 servers.
    client
        .send_simple(Query::new(format!(
            "SELECT * FROM sharded WHERE id IN ({}, {})",
            id_shard0, id_shard2
        )))
        .await;

    let messages = client.read_until('Z').await.unwrap();

    let data_rows: Vec<_> = messages.iter().filter(|m| m.code() == 'D').collect();
    assert_eq!(
        data_rows.len(),
        2,
        "should return rows from both targeted shards"
    );

    let cc_msg = messages.iter().find(|m| m.code() == 'C').unwrap();
    let cc = CommandComplete::try_from(cc_msg.clone()).unwrap();
    assert_eq!(cc.command(), "SELECT 2");

    // Cleanup
    client
        .send_simple(Query::new(format!(
            "DELETE FROM sharded WHERE id IN ({}, {})",
            id_shard0, id_shard2
        )))
        .await;
    client.read_until('Z').await.unwrap();
}

/// Test multi-binding DELETE targeting a subset of shards (2 of 3).
#[tokio::test]
async fn test_multi_binding_delete_subset_of_shards() {
    let mut client = TestClient::new_sharded_3(Parameters::default()).await;

    let id_shard0 = client.random_id_for_shard(0);
    let id_shard2 = client.random_id_for_shard(2);

    // Setup
    client
        .send_simple(Query::new(format!(
            "INSERT INTO sharded (id, value) VALUES ({}, 'del0'), ({}, 'del2') ON CONFLICT (id) DO UPDATE SET value = EXCLUDED.value",
            id_shard0, id_shard2
        )))
        .await;
    client.read_until('Z').await.unwrap();

    // DELETE targeting shards 0 and 2 via IN clause (2 of 3 shards).
    client
        .send_simple(Query::new(format!(
            "DELETE FROM sharded WHERE id IN ({}, {})",
            id_shard0, id_shard2
        )))
        .await;

    let cc = expect_message!(client.read().await, CommandComplete);
    assert_eq!(cc.command(), "DELETE 2");
    expect_message!(client.read().await, ReadyForQuery);
}
