use crate::{
    expect_message,
    net::{
        BindComplete, CommandComplete, DataRow, ParameterDescription, Parameters, ParseComplete,
        ReadyForQuery, RowDescription,
    },
};

use super::prelude::*;

/// Test extended protocol with named prepared statement, parameter binding, and Describe.
#[tokio::test]
async fn test_extended_with_params_and_describe() {
    let mut client = TestClient::new_replicas(Parameters::default()).await;

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
    client.send(Describe::new_statement("test")).await;
    client.send(Execute::new()).await;
    client.send(Sync).await;
    client.try_process().await.unwrap();

    expect_message!(client.read().await, ParseComplete); // '1'
    expect_message!(client.read().await, BindComplete); // '2'
    expect_message!(client.read().await, ParameterDescription); // 't'
    expect_message!(client.read().await, RowDescription); // 'T'
    let row = expect_message!(client.read().await, DataRow); // 'D'
    assert_eq!(
        row.get::<String>(0, crate::net::Format::Text),
        Some("123".into())
    );
    expect_message!(client.read().await, CommandComplete); // 'C'
    let rfq = expect_message!(client.read().await, ReadyForQuery); // 'Z'
    assert_eq!(rfq.status, 'I');
}
