use crate::{
    expect_message,
    net::{CloseComplete, Parameters, ParseComplete},
};

use super::prelude::*;

/// Test that Close + Parse for the same name correctly updates
/// the global prepared statement cache.
#[tokio::test]
async fn test_close_parse_same_name_global_cache() {
    let mut client = TestClient::new_replicas(Parameters::default()).await;

    // Close + Parse same name + Flush
    client.send(Close::named("test_stmt")).await;
    client.send(Parse::named("test_stmt", "SELECT $1")).await;
    client.send(Flush).await;
    client.try_process().await.unwrap();

    expect_message!(client.read().await, CloseComplete); // '3'
    expect_message!(client.read().await, ParseComplete); // '1'

    // Verify the statement is registered correctly in the global cache
    let global_cache = client.client().prepared_statements.global.clone();
    assert_eq!(global_cache.read().len(), 1);
    let binding = global_cache.write();
    let (_, cached_stmt) = binding.statements().iter().next().unwrap();
    assert_eq!(cached_stmt.used, 1);

    // Verify the SQL content in the global cache
    let global_stmt_name = cached_stmt.name();
    let cached_query = binding.query(&global_stmt_name).unwrap();
    assert_eq!(cached_query, "SELECT $1");

    // Verify the client's local cache
    assert_eq!(client.client().prepared_statements.len_local(), 1);
    assert!(client
        .client()
        .prepared_statements
        .name("test_stmt")
        .is_some());
}
