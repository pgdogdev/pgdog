use crate::{
    expect_message,
    frontend::client::TransactionType,
    net::{
        BindComplete, CommandComplete, NoData, NoticeResponse, ParameterDescription, ParseComplete,
        ReadyForQuery,
    },
};

use super::prelude::*;

/// Deferred BEGIN with a statement Describe replies with ParameterDescription + NoData.
#[tokio::test]
async fn begin_statement_describe() {
    let mut client = TestClient::new_replicas(Parameters::default()).await;

    client.send(Parse::named("tx", "BEGIN")).await;
    client.send(Bind::new_statement("tx")).await;
    client.send(Describe::new_statement("tx")).await;
    client.send(Execute::new()).await;
    client.send(Sync).await;
    client.try_process().await.unwrap();

    assert!(!client.backend_connected());
    assert!(client.client().transaction.is_some());

    expect_message!(client.read().await, ParseComplete);
    expect_message!(client.read().await, BindComplete);
    expect_message!(client.read().await, ParameterDescription);
    expect_message!(client.read().await, NoData);
    expect_message!(client.read().await, CommandComplete);
    let rfq = expect_message!(client.read().await, ReadyForQuery);
    assert_eq!(rfq.status, 'T');
}

/// Deferred BEGIN with a portal Describe replies with NoData only.
#[tokio::test]
async fn begin_portal_describe() {
    let mut client = TestClient::new_replicas(Parameters::default()).await;

    client.send(Parse::named("tx", "BEGIN")).await;
    client.send(Bind::new_statement("tx")).await;
    client.send(Describe::new_portal("")).await;
    client.send(Execute::new()).await;
    client.send(Sync).await;
    client.try_process().await.unwrap();

    assert!(!client.backend_connected());

    expect_message!(client.read().await, ParseComplete);
    expect_message!(client.read().await, BindComplete);
    expect_message!(client.read().await, NoData);
    expect_message!(client.read().await, CommandComplete);
    let rfq = expect_message!(client.read().await, ReadyForQuery);
    assert_eq!(rfq.status, 'T');
}

/// A statement and a portal Describe in one batch are answered independently.
#[tokio::test]
async fn begin_multiple_describes() {
    let mut client = TestClient::new_replicas(Parameters::default()).await;

    client.send(Parse::named("tx", "BEGIN")).await;
    client.send(Describe::new_statement("tx")).await;
    client.send(Bind::new_statement("tx")).await;
    client.send(Describe::new_portal("")).await;
    client.send(Execute::new()).await;
    client.send(Sync).await;
    client.try_process().await.unwrap();

    assert!(!client.backend_connected());

    expect_message!(client.read().await, ParseComplete);
    expect_message!(client.read().await, ParameterDescription);
    expect_message!(client.read().await, NoData);
    expect_message!(client.read().await, BindComplete);
    expect_message!(client.read().await, NoData);
    expect_message!(client.read().await, CommandComplete);
    let rfq = expect_message!(client.read().await, ReadyForQuery);
    assert_eq!(rfq.status, 'T');
}

/// Deferred COMMIT with a statement Describe, driven through end_not_connected.
#[tokio::test]
async fn commit_statement_describe() {
    let mut client = TestClient::new_replicas(Parameters::default()).await;
    client.client.transaction = Some(TransactionType::ReadWrite);
    client.client.client_request = ClientRequest::from(vec![
        Parse::named("c", "COMMIT").into(),
        Bind::new_statement("c").into(),
        Describe::new_statement("c").into(),
        Execute::new().into(),
        Sync.into(),
    ]);

    let mut context = QueryEngineContext::new(&mut client.client);
    client
        .engine
        .end_not_connected(&mut context, false, true)
        .await
        .unwrap();
    drop(context);

    expect_message!(client.read().await, ParseComplete);
    expect_message!(client.read().await, BindComplete);
    expect_message!(client.read().await, ParameterDescription);
    expect_message!(client.read().await, NoData);
    expect_message!(client.read().await, CommandComplete);
    let rfq = expect_message!(client.read().await, ReadyForQuery);
    assert_eq!(rfq.status, 'I');
}

/// Deferred ROLLBACK outside a transaction keeps the no-transaction notice.
#[tokio::test]
async fn rollback_without_transaction_describe() {
    let mut client = TestClient::new_replicas(Parameters::default()).await;
    client.client.transaction = None;
    client.client.client_request = ClientRequest::from(vec![
        Parse::named("r", "ROLLBACK").into(),
        Bind::new_statement("r").into(),
        Describe::new_statement("r").into(),
        Execute::new().into(),
        Sync.into(),
    ]);

    let mut context = QueryEngineContext::new(&mut client.client);
    client
        .engine
        .end_not_connected(&mut context, true, true)
        .await
        .unwrap();
    drop(context);

    expect_message!(client.read().await, ParseComplete);
    expect_message!(client.read().await, BindComplete);
    expect_message!(client.read().await, ParameterDescription);
    expect_message!(client.read().await, NoData);
    expect_message!(client.read().await, CommandComplete);
    expect_message!(client.read().await, NoticeResponse);
    let rfq = expect_message!(client.read().await, ReadyForQuery);
    assert_eq!(rfq.status, 'I');
}
