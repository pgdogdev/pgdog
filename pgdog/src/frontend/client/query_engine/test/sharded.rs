use crate::{
    expect_message,
    net::{CommandComplete, Parameters, Query, ReadyForQuery},
};

use super::prelude::*;

#[tokio::test]
async fn test_sharded_insert_sums_row_counts() {
    let mut client = TestClient::new_sharded(Parameters::default()).await;

    let id_shard0 = client.random_id_for_shard(0);
    let id_shard1 = client.random_id_for_shard(1);

    // Cleanup first
    client
        .send_simple(Query::new(format!(
            "DELETE FROM sharded WHERE id IN ({}, {})",
            id_shard0, id_shard1
        )))
        .await;
    client.read_until('Z').await.unwrap();

    // Multi-row INSERT with rows going to different shards - should be split and summed
    client
        .send_simple(Query::new(format!(
            "INSERT INTO sharded (id, value) VALUES ({}, 'ins1'), ({}, 'ins2')",
            id_shard0, id_shard1
        )))
        .await;

    let cc = expect_message!(client.read().await, CommandComplete);
    assert_eq!(
        cc.command(),
        "INSERT 0 2",
        "sharded INSERT should return summed row count from all shards"
    );
    expect_message!(client.read().await, ReadyForQuery);

    // Cleanup
    client
        .send_simple(Query::new(format!(
            "DELETE FROM sharded WHERE id IN ({}, {})",
            id_shard0, id_shard1
        )))
        .await;
    client.read_until('Z').await.unwrap();
}

#[tokio::test]
async fn test_sharded_insert_returning_from_all_shards() {
    let mut client = TestClient::new_sharded(Parameters::default()).await;

    let id_shard0 = client.random_id_for_shard(0);
    let id_shard1 = client.random_id_for_shard(1);

    // Cleanup first
    client
        .send_simple(Query::new(format!(
            "DELETE FROM sharded WHERE id IN ({}, {})",
            id_shard0, id_shard1
        )))
        .await;
    client.read_until('Z').await.unwrap();

    // Multi-row INSERT with RETURNING - should return rows from ALL shards
    client
        .send_simple(Query::new(format!(
            "INSERT INTO sharded (id, value) VALUES ({}, 'ret1'), ({}, 'ret2') RETURNING id, value",
            id_shard0, id_shard1
        )))
        .await;

    let messages = client.read_until('Z').await.unwrap();

    let data_rows: Vec<_> = messages.iter().filter(|m| m.code() == 'D').collect();

    assert_eq!(
        data_rows.len(),
        2,
        "sharded INSERT RETURNING should return rows from all shards, got {} rows",
        data_rows.len()
    );

    let cc_msg = messages.iter().find(|m| m.code() == 'C').unwrap();
    let cc = CommandComplete::try_from(cc_msg.clone()).unwrap();
    assert_eq!(cc.command(), "INSERT 0 2");

    // Cleanup
    client
        .send_simple(Query::new(format!(
            "DELETE FROM sharded WHERE id IN ({}, {})",
            id_shard0, id_shard1
        )))
        .await;
    client.read_until('Z').await.unwrap();
}

#[tokio::test]
async fn test_sharded_update_sums_row_counts() {
    let mut client = TestClient::new_sharded(Parameters::default()).await;

    // Use the 'sharded' table which is a regular sharded table
    // Insert data that will go to different shards
    let id_shard0 = client.random_id_for_shard(0);
    let id_shard1 = client.random_id_for_shard(1);

    client
        .send_simple(Query::new(format!(
            "INSERT INTO sharded (id, value) VALUES ({}, 'test'), ({}, 'test') ON CONFLICT (id) DO UPDATE SET value = 'test'",
            id_shard0, id_shard1
        )))
        .await;
    client.read_until('Z').await.unwrap();

    // Update all rows - should return SUMMED count from both shards
    client
        .send_simple(Query::new(
            "UPDATE sharded SET value = 'updated' WHERE value = 'test'",
        ))
        .await;

    let cc = expect_message!(client.read().await, CommandComplete);
    // Should be "UPDATE 2" (1 from each shard, summed)
    assert_eq!(
        cc.command(),
        "UPDATE 2",
        "sharded UPDATE should return summed row count from all shards"
    );
    expect_message!(client.read().await, ReadyForQuery);

    // Cleanup
    client
        .send_simple(Query::new(format!(
            "DELETE FROM sharded WHERE id IN ({}, {})",
            id_shard0, id_shard1
        )))
        .await;
    client.read_until('Z').await.unwrap();
}

#[tokio::test]
async fn test_sharded_delete_sums_row_counts() {
    let mut client = TestClient::new_sharded(Parameters::default()).await;

    let id_shard0 = client.random_id_for_shard(0);
    let id_shard1 = client.random_id_for_shard(1);

    client
        .send_simple(Query::new(format!(
            "INSERT INTO sharded (id, value) VALUES ({}, 'del'), ({}, 'del') ON CONFLICT (id) DO UPDATE SET value = 'del'",
            id_shard0, id_shard1
        )))
        .await;
    client.read_until('Z').await.unwrap();

    // Delete all rows - should return SUMMED count
    client
        .send_simple(Query::new("DELETE FROM sharded WHERE value = 'del'"))
        .await;

    let cc = expect_message!(client.read().await, CommandComplete);
    // Should be "DELETE 2" (1 from each shard, summed)
    assert_eq!(
        cc.command(),
        "DELETE 2",
        "sharded DELETE should return summed row count from all shards"
    );
    expect_message!(client.read().await, ReadyForQuery);
}

#[tokio::test]
async fn test_sharded_update_returning_from_all_shards() {
    let mut client = TestClient::new_sharded(Parameters::default()).await;

    let id_shard0 = client.random_id_for_shard(0);
    let id_shard1 = client.random_id_for_shard(1);

    client
        .send_simple(Query::new(format!(
            "INSERT INTO sharded (id, value) VALUES ({}, 'ret'), ({}, 'ret') ON CONFLICT (id) DO UPDATE SET value = 'ret'",
            id_shard0, id_shard1
        )))
        .await;
    client.read_until('Z').await.unwrap();

    // UPDATE with RETURNING - should return rows from ALL shards
    client
        .send_simple(Query::new(
            "UPDATE sharded SET value = 'returned' WHERE value = 'ret' RETURNING id",
        ))
        .await;

    let messages = client.read_until('Z').await.unwrap();

    let data_rows: Vec<_> = messages.iter().filter(|m| m.code() == 'D').collect();

    // Should be 2 rows (1 from each shard)
    assert_eq!(
        data_rows.len(),
        2,
        "sharded UPDATE RETURNING should return rows from all shards, got {} rows",
        data_rows.len()
    );

    let cc_msg = messages.iter().find(|m| m.code() == 'C').unwrap();
    let cc = CommandComplete::try_from(cc_msg.clone()).unwrap();
    assert_eq!(cc.command(), "UPDATE 2");

    // Cleanup
    client
        .send_simple(Query::new(format!(
            "DELETE FROM sharded WHERE id IN ({}, {})",
            id_shard0, id_shard1
        )))
        .await;
    client.read_until('Z').await.unwrap();
}

#[tokio::test]
async fn test_sharded_delete_returning_from_all_shards() {
    let mut client = TestClient::new_sharded(Parameters::default()).await;

    let id_shard0 = client.random_id_for_shard(0);
    let id_shard1 = client.random_id_for_shard(1);

    client
        .send_simple(Query::new(format!(
            "INSERT INTO sharded (id, value) VALUES ({}, 'delret'), ({}, 'delret') ON CONFLICT (id) DO UPDATE SET value = 'delret'",
            id_shard0, id_shard1
        )))
        .await;
    client.read_until('Z').await.unwrap();

    // DELETE with RETURNING - should return rows from ALL shards
    client
        .send_simple(Query::new(
            "DELETE FROM sharded WHERE value = 'delret' RETURNING id",
        ))
        .await;

    let messages = client.read_until('Z').await.unwrap();

    let data_rows: Vec<_> = messages.iter().filter(|m| m.code() == 'D').collect();

    // Should be 2 rows (1 from each shard)
    assert_eq!(
        data_rows.len(),
        2,
        "sharded DELETE RETURNING should return rows from all shards, got {} rows",
        data_rows.len()
    );

    let cc_msg = messages.iter().find(|m| m.code() == 'C').unwrap();
    let cc = CommandComplete::try_from(cc_msg.clone()).unwrap();
    assert_eq!(cc.command(), "DELETE 2");
}
