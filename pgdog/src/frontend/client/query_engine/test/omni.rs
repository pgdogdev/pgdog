use crate::{
    expect_message,
    net::{CommandComplete, Parameters, Query, ReadyForQuery},
};

use super::prelude::*;

#[tokio::test]
async fn test_omni_update_returns_single_shard_count() {
    let mut client = TestClient::new_sharded(Parameters::default()).await;

    // Setup: create table and insert data on both shards
    client
        .send_simple(Query::new(
            "CREATE TABLE IF NOT EXISTS sharded_omni (id BIGINT PRIMARY KEY, value TEXT)",
        ))
        .await;
    client.read_until('Z').await.unwrap();

    client
        .send_simple(Query::new("DELETE FROM sharded_omni"))
        .await;
    client.read_until('Z').await.unwrap();

    client
        .send_simple(Query::new(
            "INSERT INTO sharded_omni (id, value) VALUES (1, 'test'), (2, 'test')",
        ))
        .await;
    client.read_until('Z').await.unwrap();

    // Update all rows - should return count from ONE shard, not sum of all shards
    client
        .send_simple(Query::new(
            "UPDATE sharded_omni SET value = 'updated' WHERE value = 'test'",
        ))
        .await;

    let cc = expect_message!(client.read().await, CommandComplete);
    // Should be "UPDATE 2" (from one shard), not "UPDATE 4" (summed from 2 shards)
    assert_eq!(
        cc.command(),
        "UPDATE 2",
        "omni UPDATE should return row count from one shard only"
    );
    expect_message!(client.read().await, ReadyForQuery);

    // Cleanup
    client
        .send_simple(Query::new("DROP TABLE IF EXISTS sharded_omni"))
        .await;
    client.read_until('Z').await.unwrap();
}

#[tokio::test]
async fn test_omni_delete_returns_single_shard_count() {
    let mut client = TestClient::new_sharded(Parameters::default()).await;

    // Setup
    client
        .send_simple(Query::new(
            "CREATE TABLE IF NOT EXISTS sharded_omni (id BIGINT PRIMARY KEY, value TEXT)",
        ))
        .await;
    client.read_until('Z').await.unwrap();

    client
        .send_simple(Query::new("DELETE FROM sharded_omni"))
        .await;
    client.read_until('Z').await.unwrap();

    client
        .send_simple(Query::new(
            "INSERT INTO sharded_omni (id, value) VALUES (1, 'test'), (2, 'test'), (3, 'test')",
        ))
        .await;
    client.read_until('Z').await.unwrap();

    // Delete all rows - should return count from ONE shard
    client
        .send_simple(Query::new("DELETE FROM sharded_omni WHERE value = 'test'"))
        .await;

    let cc = expect_message!(client.read().await, CommandComplete);
    // Should be "DELETE 3" (from one shard), not "DELETE 6" (summed from 2 shards)
    assert_eq!(
        cc.command(),
        "DELETE 3",
        "omni DELETE should return row count from one shard only"
    );
    expect_message!(client.read().await, ReadyForQuery);

    // Cleanup
    client
        .send_simple(Query::new("DROP TABLE IF EXISTS sharded_omni"))
        .await;
    client.read_until('Z').await.unwrap();
}

#[tokio::test]
async fn test_omni_insert_returns_single_shard_count() {
    let mut client = TestClient::new_sharded(Parameters::default()).await;

    // Setup
    client
        .send_simple(Query::new(
            "CREATE TABLE IF NOT EXISTS sharded_omni (id BIGINT PRIMARY KEY, value TEXT)",
        ))
        .await;
    client.read_until('Z').await.unwrap();

    client
        .send_simple(Query::new("DELETE FROM sharded_omni"))
        .await;
    client.read_until('Z').await.unwrap();

    // Insert rows - should return count from ONE shard
    client
        .send_simple(Query::new(
            "INSERT INTO sharded_omni (id, value) VALUES (10, 'a'), (20, 'b')",
        ))
        .await;

    let cc = expect_message!(client.read().await, CommandComplete);
    // Should be "INSERT 0 2" (from one shard), not "INSERT 0 4" (summed from 2 shards)
    assert_eq!(
        cc.command(),
        "INSERT 0 2",
        "omni INSERT should return row count from one shard only"
    );
    expect_message!(client.read().await, ReadyForQuery);

    // Cleanup
    client
        .send_simple(Query::new("DROP TABLE IF EXISTS sharded_omni"))
        .await;
    client.read_until('Z').await.unwrap();
}

#[tokio::test]
async fn test_omni_update_returning_only_from_one_shard() {
    let mut client = TestClient::new_sharded(Parameters::default()).await;

    // Setup
    client
        .send_simple(Query::new(
            "CREATE TABLE IF NOT EXISTS sharded_omni (id BIGINT PRIMARY KEY, value TEXT)",
        ))
        .await;
    client.read_until('Z').await.unwrap();

    client
        .send_simple(Query::new("DELETE FROM sharded_omni"))
        .await;
    client.read_until('Z').await.unwrap();

    client
        .send_simple(Query::new(
            "INSERT INTO sharded_omni (id, value) VALUES (1, 'test'), (2, 'test')",
        ))
        .await;
    client.read_until('Z').await.unwrap();

    // UPDATE with RETURNING - should return rows from ONE shard only
    client
        .send_simple(Query::new(
            "UPDATE sharded_omni SET value = 'updated' RETURNING id, value",
        ))
        .await;

    let messages = client.read_until('Z').await.unwrap();

    // Count DataRow messages
    let data_rows: Vec<_> = messages.iter().filter(|m| m.code() == 'D').collect();

    // Should be 2 rows (from one shard), not 4 (from both shards)
    assert_eq!(
        data_rows.len(),
        2,
        "omni UPDATE RETURNING should return rows from one shard only, got {} rows",
        data_rows.len()
    );

    // Verify we got RowDescription, DataRows, CommandComplete, ReadyForQuery
    let codes: Vec<char> = messages.iter().map(|m| m.code()).collect();
    assert!(codes.contains(&'T'), "should have RowDescription");
    assert!(codes.contains(&'C'), "should have CommandComplete");
    assert!(codes.contains(&'Z'), "should have ReadyForQuery");

    // Verify CommandComplete shows correct count
    let cc_msg = messages.iter().find(|m| m.code() == 'C').unwrap();
    let cc = CommandComplete::try_from(cc_msg.clone()).unwrap();
    assert_eq!(cc.command(), "UPDATE 2");

    // Cleanup
    client
        .send_simple(Query::new("DROP TABLE IF EXISTS sharded_omni"))
        .await;
    client.read_until('Z').await.unwrap();
}

#[tokio::test]
async fn test_omni_delete_returning_only_from_one_shard() {
    let mut client = TestClient::new_sharded(Parameters::default()).await;

    // Setup
    client
        .send_simple(Query::new(
            "CREATE TABLE IF NOT EXISTS sharded_omni (id BIGINT PRIMARY KEY, value TEXT)",
        ))
        .await;
    client.read_until('Z').await.unwrap();

    client
        .send_simple(Query::new("DELETE FROM sharded_omni"))
        .await;
    client.read_until('Z').await.unwrap();

    client
        .send_simple(Query::new(
            "INSERT INTO sharded_omni (id, value) VALUES (1, 'del'), (2, 'del'), (3, 'del')",
        ))
        .await;
    client.read_until('Z').await.unwrap();

    // DELETE with RETURNING - should return rows from ONE shard only
    client
        .send_simple(Query::new("DELETE FROM sharded_omni RETURNING id"))
        .await;

    let messages = client.read_until('Z').await.unwrap();

    let data_rows: Vec<_> = messages.iter().filter(|m| m.code() == 'D').collect();

    // Should be 3 rows (from one shard), not 6 (from both shards)
    assert_eq!(
        data_rows.len(),
        3,
        "omni DELETE RETURNING should return rows from one shard only, got {} rows",
        data_rows.len()
    );

    let cc_msg = messages.iter().find(|m| m.code() == 'C').unwrap();
    let cc = CommandComplete::try_from(cc_msg.clone()).unwrap();
    assert_eq!(cc.command(), "DELETE 3");

    // Cleanup
    client
        .send_simple(Query::new("DROP TABLE IF EXISTS sharded_omni"))
        .await;
    client.read_until('Z').await.unwrap();
}

#[tokio::test]
async fn test_omni_insert_returning_only_from_one_shard() {
    let mut client = TestClient::new_sharded(Parameters::default()).await;

    // Setup
    client
        .send_simple(Query::new(
            "CREATE TABLE IF NOT EXISTS sharded_omni (id BIGINT PRIMARY KEY, value TEXT)",
        ))
        .await;
    client.read_until('Z').await.unwrap();

    client
        .send_simple(Query::new("DELETE FROM sharded_omni"))
        .await;
    client.read_until('Z').await.unwrap();

    // INSERT with RETURNING - should return rows from ONE shard only
    client
        .send_simple(Query::new(
            "INSERT INTO sharded_omni (id, value) VALUES (100, 'a'), (200, 'b') RETURNING id, value",
        ))
        .await;

    let messages = client.read_until('Z').await.unwrap();

    let data_rows: Vec<_> = messages.iter().filter(|m| m.code() == 'D').collect();

    // Should be 2 rows (from one shard), not 4 (from both shards)
    assert_eq!(
        data_rows.len(),
        2,
        "omni INSERT RETURNING should return rows from one shard only, got {} rows",
        data_rows.len()
    );

    let cc_msg = messages.iter().find(|m| m.code() == 'C').unwrap();
    let cc = CommandComplete::try_from(cc_msg.clone()).unwrap();
    assert_eq!(cc.command(), "INSERT 0 2");

    // Cleanup
    client
        .send_simple(Query::new("DROP TABLE IF EXISTS sharded_omni"))
        .await;
    client.read_until('Z').await.unwrap();
}
