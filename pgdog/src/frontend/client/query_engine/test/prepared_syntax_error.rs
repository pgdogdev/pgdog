use crate::{
    expect_message,
    net::{ErrorResponse, Parameters, ReadyForQuery},
};

use super::prelude::*;

/// Test that a syntax error in a prepared statement is tracked correctly
/// in the global cache, and that dropping the client decrements the use count.
#[tokio::test]
async fn test_prepared_syntax_error() {
    let mut client = TestClient::new_replicas(Parameters::default()).await;

    // Parse with invalid SQL + Sync
    client.send(Parse::named("test", "SELECT sdfsf")).await;
    client.send(Sync).await;
    client.try_process().await.unwrap();

    expect_message!(client.read().await, ErrorResponse); // 'E'
    expect_message!(client.read().await, ReadyForQuery); // 'Z'

    let stmts = client.client().prepared_statements.global.clone();
    assert_eq!(stmts.read().statements().iter().next().unwrap().1.used, 1);

    // Send Terminate to trigger graceful disconnect
    client.send(Terminate).await;
    // Drop client to decrement use count
    let stmts_clone = stmts.clone();
    drop(client);

    assert_eq!(
        stmts_clone
            .read()
            .statements()
            .iter()
            .next()
            .unwrap()
            .1
            .used,
        0
    );
}
