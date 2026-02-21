//! End-to-end tests for COPY with FK lookup sharding.
//!
//! Tests that the query engine correctly routes COPY rows via FK relationships
//! when the target table doesn't have the sharding key directly.

use std::ops::Deref;

use crate::{
    backend::databases::reload_from_existing,
    config::{config, load_test_sharded, set, DataType, Hasher, ShardedTable},
    expect_message,
    frontend::client::test::TestClient,
    net::{CommandComplete, Parameters, Protocol, Query, ReadyForQuery},
};

/// Load test config with FK sharding tables.
fn load_test_fk_sharded() {
    load_test_sharded();

    let mut config = config().deref().clone();
    // Add the FK parent table as a sharded table
    config.config.sharded_tables.push(ShardedTable {
        database: "pgdog".into(),
        name: Some("copy_fk_users".into()),
        column: "customer_id".into(),
        primary: true,
        data_type: DataType::Bigint,
        hasher: Hasher::Postgres,
        ..Default::default()
    });
    set(config).unwrap();
    reload_from_existing().unwrap();
}

/// Test COPY with multi-hop FK lookup through the query engine.
///
/// This test uses 3 tables to verify FK traversal across multiple hops:
///   copy_fk_users (id, customer_id) - has sharding key
///   copy_fk_orders (id, user_id FK -> users) - 1 hop from sharding key
///   copy_fk_order_items (id, order_id FK -> orders) - 2 hops from sharding key
///
/// When we COPY into order_items, pgdog must traverse:
///   order_items -> orders -> users to find customer_id
#[tokio::test]
async fn test_copy_fk_lookup_end_to_end() {
    load_test_fk_sharded();
    let mut client = TestClient::new_sharded(Parameters::default()).await;

    // Drop and create FK tables (3 levels deep)
    client
        .send(Query::new(
            "DROP TABLE IF EXISTS copy_fk_order_items, copy_fk_orders, copy_fk_users",
        ))
        .await;
    client.try_process().await.unwrap();
    client.read_until('Z').await.unwrap();

    // Level 1: users with sharding key
    client
        .send(Query::new(
            "CREATE TABLE copy_fk_users (
                id BIGINT PRIMARY KEY,
                customer_id BIGINT NOT NULL
            )",
        ))
        .await;
    client.try_process().await.unwrap();
    client.read_until('Z').await.unwrap();

    // Level 2: orders with FK to users
    client
        .send(Query::new(
            "CREATE TABLE copy_fk_orders (
                id BIGINT PRIMARY KEY,
                user_id BIGINT REFERENCES copy_fk_users(id)
            )",
        ))
        .await;
    client.try_process().await.unwrap();
    client.read_until('Z').await.unwrap();

    // Level 3: order_items with FK to orders (2 hops from sharding key)
    client
        .send(Query::new(
            "CREATE TABLE copy_fk_order_items (
                id BIGINT PRIMARY KEY,
                order_id BIGINT REFERENCES copy_fk_orders(id)
            )",
        ))
        .await;
    client.try_process().await.unwrap();
    client.read_until('Z').await.unwrap();

    // Insert users with known customer_id values
    for i in 1i64..=10 {
        client
            .send(Query::new(&format!(
                "INSERT INTO copy_fk_users (id, customer_id) VALUES ({}, {})",
                i,
                i * 100
            )))
            .await;
        client.try_process().await.unwrap();
        client.read_until('Z').await.unwrap();
    }

    // Insert orders referencing users
    for i in 1i64..=10 {
        client
            .send(Query::new(&format!(
                "INSERT INTO copy_fk_orders (id, user_id) VALUES ({}, {})",
                i * 10,
                i
            )))
            .await;
        client.try_process().await.unwrap();
        client.read_until('Z').await.unwrap();
    }

    // Send COPY command for order_items table (2 hops from sharding key)
    client
        .send(Query::new(
            "COPY copy_fk_order_items (id, order_id) FROM STDIN",
        ))
        .await;
    client.try_process().await.unwrap();

    // Expect CopyInResponse (code 'G')
    let copy_in = client.read().await;
    assert_eq!(copy_in.code(), 'G', "expected CopyInResponse");

    // Send COPY data - 10 order_items referencing orders
    use crate::net::messages::CopyData;

    let copy_rows: String = (1i64..=10)
        .map(|i| format!("{}\t{}\n", i * 100, i * 10)) // item_id, order_id (FK)
        .collect();

    let copy_data = CopyData::new(copy_rows.as_bytes());
    client.send(copy_data).await;

    // Send CopyDone
    use crate::net::CopyDone;
    client.send(CopyDone).await;

    client.try_process().await.unwrap();

    // Expect CommandComplete and ReadyForQuery
    expect_message!(client.read().await, CommandComplete);
    let rfq = expect_message!(client.read().await, ReadyForQuery);
    assert_eq!(rfq.status, 'I');

    // Verify all order_items were inserted
    client
        .send(Query::new("SELECT COUNT(*) FROM copy_fk_order_items"))
        .await;
    client.try_process().await.unwrap();

    let messages = client.read_until('Z').await.unwrap();
    let data_row = messages
        .iter()
        .find(|m| m.code() == 'D')
        .expect("should have DataRow");
    let data_row = crate::net::DataRow::try_from(data_row.clone()).unwrap();
    let count = data_row.get_int(0, true).expect("should have count");
    assert_eq!(count, 10, "expected 10 order_items");

    // Verify data integrity
    client
        .send(Query::new(
            "SELECT id, order_id FROM copy_fk_order_items ORDER BY id",
        ))
        .await;
    client.try_process().await.unwrap();

    let messages = client.read_until('Z').await.unwrap();
    let data_rows: Vec<_> = messages
        .iter()
        .filter(|m| m.code() == 'D')
        .map(|m| crate::net::DataRow::try_from(m.clone()).unwrap())
        .collect();

    assert_eq!(data_rows.len(), 10, "expected 10 data rows");
    for (i, row) in data_rows.iter().enumerate() {
        let item_id = row.get_int(0, true).expect("item_id");
        let order_id = row.get_int(1, true).expect("order_id");
        assert_eq!(item_id, ((i + 1) * 100) as i64, "item_id mismatch");
        assert_eq!(order_id, ((i + 1) * 10) as i64, "order_id mismatch");
    }

    // Clean up
    client
        .send(Query::new(
            "DROP TABLE IF EXISTS copy_fk_order_items, copy_fk_orders, copy_fk_users",
        ))
        .await;
    client.try_process().await.unwrap();
    client.read_until('Z').await.unwrap();
}

/// Test COPY with direct sharding key (no FK lookup needed).
#[tokio::test]
async fn test_copy_direct_sharding_key_end_to_end() {
    load_test_fk_sharded();
    let mut client = TestClient::new_sharded(Parameters::default()).await;

    // Drop and create table with sharding key directly
    client
        .send(Query::new(
            "DROP TABLE IF EXISTS copy_fk_orders, copy_fk_users",
        ))
        .await;
    client.try_process().await.unwrap();
    client.read_until('Z').await.unwrap();

    client
        .send(Query::new(
            "CREATE TABLE copy_fk_users (
                id BIGINT PRIMARY KEY,
                customer_id BIGINT NOT NULL
            )",
        ))
        .await;
    client.try_process().await.unwrap();
    client.read_until('Z').await.unwrap();

    // Send COPY command for users table (has sharding key directly)
    client
        .send(Query::new(
            "COPY copy_fk_users (id, customer_id) FROM STDIN",
        ))
        .await;
    client.try_process().await.unwrap();

    // Expect CopyInResponse (code 'G')
    let copy_in = client.read().await;
    assert_eq!(copy_in.code(), 'G', "expected CopyInResponse");

    // Send COPY data - 10 users with customer_id (sharding key)
    use crate::net::messages::CopyData;

    let copy_rows: String = (1i64..=10)
        .map(|i| format!("{}\t{}\n", i, i * 100)) // id, customer_id
        .collect();

    let copy_data = CopyData::new(copy_rows.as_bytes());
    client.send(copy_data).await;

    // Send CopyDone
    use crate::net::CopyDone;
    client.send(CopyDone).await;

    client.try_process().await.unwrap();

    // Expect CommandComplete and ReadyForQuery
    expect_message!(client.read().await, CommandComplete);
    let rfq = expect_message!(client.read().await, ReadyForQuery);
    assert_eq!(rfq.status, 'I');

    // Verify all users were inserted
    client
        .send(Query::new("SELECT COUNT(*) FROM copy_fk_users"))
        .await;
    client.try_process().await.unwrap();

    let messages = client.read_until('Z').await.unwrap();
    let data_row_msg = messages
        .into_iter()
        .find(|m| m.code() == 'D')
        .expect("should have DataRow");
    let data_row = crate::net::DataRow::try_from(data_row_msg).unwrap();
    let count = data_row.get_int(0, true).expect("should have count");
    assert_eq!(count, 10, "expected 10 users");

    // Clean up
    client
        .send(Query::new("DROP TABLE IF EXISTS copy_fk_users"))
        .await;
    client.try_process().await.unwrap();
    client.read_until('Z').await.unwrap();
}
