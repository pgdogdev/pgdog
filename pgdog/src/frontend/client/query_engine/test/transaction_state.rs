use crate::{
    expect_message,
    net::{
        bind::Parameter, BindComplete, CommandComplete, DataRow, Parameters, ParseComplete,
        ReadyForQuery,
    },
};

use super::prelude::*;

/// Test that transaction state is tracked correctly through
/// BEGIN, extended protocol operations, and COMMIT.
#[tokio::test]
async fn test_transaction_state() {
    let mut client = TestClient::new_replicas(Parameters::default()).await;

    // BEGIN
    client.send(Query::new("BEGIN")).await;
    client.try_process().await.unwrap();

    assert!(client.client().transaction.is_some());
    assert!(client.engine.router().route().is_write());

    expect_message!(client.read().await, CommandComplete);
    let rfq = expect_message!(client.read().await, ReadyForQuery);
    assert_eq!(rfq.status, 'T');

    // Parse + Describe + Sync (in transaction)
    client.send(Parse::named("test", "SELECT $1")).await;
    client.send(Describe::new_statement("test")).await;
    client.send(Sync).await;
    client.try_process().await.unwrap();

    assert!(client.client().transaction.is_some());
    assert!(client.engine.router().route().is_write());

    expect_message!(client.read().await, ParseComplete);
    client.read_until('Z').await.unwrap();

    // Bind + Execute + Sync (in transaction)
    client
        .send(Bind::new_params(
            "test",
            &[Parameter {
                len: 1,
                data: "1".as_bytes().into(),
            }],
        ))
        .await;
    client.send(Execute::new()).await;
    client.send(Sync).await;
    client.try_process().await.unwrap();

    assert!(client.client().transaction.is_some());
    assert!(client.engine.router().route().is_write());

    expect_message!(client.read().await, BindComplete);
    expect_message!(client.read().await, DataRow);
    expect_message!(client.read().await, CommandComplete);
    let rfq = expect_message!(client.read().await, ReadyForQuery);
    assert_eq!(rfq.status, 'T');

    // COMMIT
    client.send(Query::new("COMMIT")).await;
    client.try_process().await.unwrap();

    expect_message!(client.read().await, CommandComplete);
    let rfq = expect_message!(client.read().await, ReadyForQuery);
    assert_eq!(rfq.status, 'I');

    assert!(client.client().transaction.is_none());
}
