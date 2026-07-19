use crate::{
    expect_message,
    net::{
        CloseComplete, CommandComplete, DataRow, Parameters, ParseComplete, ReadyForQuery,
        RowDescription,
    },
};

use super::prelude::*;

/// Test closing a non-existent prepared statement, then a query,
/// then close+parse+flush for the same name.
#[tokio::test]
async fn test_close_parse() {
    let mut client = TestClient::new_replicas(Parameters::default()).await;

    // Close non-existent statement + Sync
    client.send(Close::named("test")).await;
    client.send(Sync).await;
    client.try_process().await.unwrap();

    expect_message!(client.read().await, CloseComplete); // '3'
    expect_message!(client.read().await, ReadyForQuery); // 'Z'

    // Simple SELECT query
    client.send_simple(Query::new("SELECT 1")).await;

    expect_message!(client.read().await, RowDescription); // 'T'
    expect_message!(client.read().await, DataRow); // 'D'
    expect_message!(client.read().await, CommandComplete); // 'C'
    expect_message!(client.read().await, ReadyForQuery); // 'Z'

    // Close + Parse same name + Flush
    client.send(Close::named("test1")).await;
    client.send(Parse::named("test1", "SELECT $1")).await;
    client.send(Flush).await;
    client.try_process().await.unwrap();

    expect_message!(client.read().await, CloseComplete); // '3'
    expect_message!(client.read().await, ParseComplete); // '1'
}
