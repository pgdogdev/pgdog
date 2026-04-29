use pgdog_config::PreparedStatements as PreparedStatementsLevel;

use crate::{
    expect_message,
    net::{
        bind::Parameter, BindComplete, CommandComplete, DataRow, Parameters, ParseComplete,
        ReadyForQuery, RowDescription,
    },
};

use super::{change_config, prelude::*};

fn set_extended_anonymous() {
    change_config(|g| {
        g.prepared_statements = PreparedStatementsLevel::ExtendedAnonymous;
    });
}

/// Extended protocol with named prepared statement works in extended_anonymous mode.
/// The same sequence of messages that works in regular extended mode must also work here.
#[tokio::test]
async fn test_extended_anonymous_basic() {
    let mut client = TestClient::new_replicas(Parameters::default()).await;
    set_extended_anonymous();

    client.send(Parse::named("test", "SELECT $1")).await;
    client
        .send(Bind::new_params(
            "test",
            &[Parameter {
                len: 3,
                data: "123".into(),
            }],
        ))
        .await;
    client.send(Execute::new()).await;
    client.send(Sync).await;
    client.try_process().await.unwrap();

    expect_message!(client.read().await, ParseComplete); // '1'
    expect_message!(client.read().await, BindComplete); // '2'
    let row = expect_message!(client.read().await, DataRow); // 'D'
    assert_eq!(
        row.get::<String>(0, crate::net::Format::Text),
        Some("123".into())
    );
    expect_message!(client.read().await, CommandComplete); // 'C'
    let rfq = expect_message!(client.read().await, ReadyForQuery); // 'Z'
    assert_eq!(rfq.status, 'I');
}

/// Extended protocol with Describe works in extended_anonymous mode.
#[tokio::test]
async fn test_extended_anonymous_with_describe() {
    let mut client = TestClient::new_replicas(Parameters::default()).await;
    set_extended_anonymous();

    client.send(Parse::named("desc_test", "SELECT $1")).await;
    client
        .send(Bind::new_params(
            "desc_test",
            &[Parameter {
                len: 3,
                data: "456".into(),
            }],
        ))
        .await;
    client.send(Describe::new_statement("desc_test")).await;
    client.send(Execute::new()).await;
    client.send(Sync).await;
    client.try_process().await.unwrap();

    expect_message!(client.read().await, ParseComplete); // '1'
    expect_message!(client.read().await, BindComplete); // '2'
                                                        // Describe returns ParameterDescription + RowDescription
    client.read().await; // 't'
    expect_message!(client.read().await, RowDescription); // 'T'
    let row = expect_message!(client.read().await, DataRow); // 'D'
    assert_eq!(
        row.get::<String>(0, crate::net::Format::Text),
        Some("456".into())
    );
    expect_message!(client.read().await, CommandComplete); // 'C'
    let rfq = expect_message!(client.read().await, ReadyForQuery); // 'Z'
    assert_eq!(rfq.status, 'I');
}

/// Transaction state is tracked correctly in extended_anonymous mode.
#[tokio::test]
async fn test_extended_anonymous_transaction_state() {
    let mut client = TestClient::new_replicas(Parameters::default()).await;
    set_extended_anonymous();

    // BEGIN
    client.send(Query::new("BEGIN")).await;
    client.try_process().await.unwrap();

    assert!(client.client().transaction.is_some());
    expect_message!(client.read().await, CommandComplete);
    let rfq = expect_message!(client.read().await, ReadyForQuery);
    assert_eq!(rfq.status, 'T');

    // Extended protocol inside transaction
    client.send(Parse::named("tx_test", "SELECT $1")).await;
    client
        .send(Bind::new_params(
            "tx_test",
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

    expect_message!(client.read().await, ParseComplete);
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

/// Close message in extended_anonymous mode is handled gracefully.
#[tokio::test]
async fn test_extended_anonymous_close() {
    let mut client = TestClient::new_replicas(Parameters::default()).await;
    set_extended_anonymous();

    // Parse first
    client.send(Parse::named("close_test", "SELECT 1")).await;
    client.send(Sync).await;
    client.try_process().await.unwrap();

    expect_message!(client.read().await, ParseComplete);
    expect_message!(client.read().await, ReadyForQuery);

    // Close
    client.send(Close::named("close_test")).await;
    client.send(Sync).await;
    client.try_process().await.unwrap();

    client.read().await; // CloseComplete '3'
    expect_message!(client.read().await, ReadyForQuery);
}

/// Multiple rounds of the same statement name work in extended_anonymous mode.
/// This verifies there are no "prepared statement already exists" errors.
#[tokio::test]
async fn test_extended_anonymous_repeated_statement_name() {
    let mut client = TestClient::new_replicas(Parameters::default()).await;
    set_extended_anonymous();

    for i in 0..5 {
        let val = format!("{}", i);
        client.send(Parse::named("repeated", "SELECT $1")).await;
        client
            .send(Bind::new_params(
                "repeated",
                &[Parameter::new(val.as_bytes())],
            ))
            .await;
        client.send(Execute::new()).await;
        client.send(Sync).await;
        client.try_process().await.unwrap();

        expect_message!(client.read().await, ParseComplete);
        expect_message!(client.read().await, BindComplete);
        let row = expect_message!(client.read().await, DataRow);
        assert_eq!(row.get::<String>(0, crate::net::Format::Text), Some(val));
        expect_message!(client.read().await, CommandComplete);
        let rfq = expect_message!(client.read().await, ReadyForQuery);
        assert_eq!(rfq.status, 'I');
    }
}

/// Simple query protocol is unaffected by extended_anonymous mode.
#[tokio::test]
async fn test_extended_anonymous_simple_query_unaffected() {
    let mut client = TestClient::new_replicas(Parameters::default()).await;
    set_extended_anonymous();

    client.send_simple(Query::new("SELECT 42")).await;

    expect_message!(client.read().await, RowDescription);
    let row = expect_message!(client.read().await, DataRow);
    assert_eq!(row.get::<i32>(0, crate::net::Format::Text), Some(42));
    expect_message!(client.read().await, CommandComplete);
    expect_message!(client.read().await, ReadyForQuery);
}
