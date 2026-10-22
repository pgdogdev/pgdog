use super::prelude::*;
use super::{multi_statement::assert_connection_usable, test_client, test_sharded_client};
use crate::{
    expect_message,
    net::{ErrorResponse, ParseComplete, ReadyForQuery},
};

async fn run_test(messages: Vec<ProtocolMessage>) -> Vec<ProtocolMessage> {
    let mut results = vec![];

    for mut client in [test_client(), test_sharded_client()] {
        client.client_request = messages.clone().into();

        let mut engine = QueryEngine::from_client(&client).unwrap();
        let (mut context, client_request) = QueryEngineContext::new(&mut client);

        engine
            .rewrite_extended(&mut context, &mut client_request.messages)
            .unwrap();

        results.push(client_request.messages.clone());
    }

    assert_eq!(
        results[0], results[1],
        "expected rewrite_extended to work the same for sharded and unsharded databases"
    );

    results.pop().unwrap()
}

#[tokio::test]
async fn test_rewrite_prepared() {
    let messages = run_test(vec![
        ProtocolMessage::from(Parse::named("__test_1", "SELECT $1, $2, $3")),
        ProtocolMessage::from(Bind::new_statement("__test_1")),
        ProtocolMessage::from(Execute::new()),
        ProtocolMessage::from(Sync),
    ])
    .await;

    assert!(
        matches!(messages[0].clone(), ProtocolMessage::Parse(parse) if parse.name() == "__pgdog_1"),
        "parse should of been renamed to __pgdog_1",
    );

    assert!(
        matches!(messages[1].clone(), ProtocolMessage::Bind(bind) if bind.statement() =="__pgdog_1"),
        "bind should of been renamed to __pgdog_1",
    );

    assert_eq!(messages.len(), 4);
}

#[tokio::test]
async fn test_rewrite_extended() {
    let messages = run_test(vec![
        ProtocolMessage::from(Parse::named("", "SELECT $1, $2, $3")),
        ProtocolMessage::from(Bind::new_statement("")),
        ProtocolMessage::from(Execute::new()),
        ProtocolMessage::from(Sync),
    ])
    .await;

    assert!(
        matches!(messages[0].clone(), ProtocolMessage::Parse(parse) if parse.name() == ""),
        "parse should not be renamed",
    );

    assert!(
        matches!(messages[1].clone(), ProtocolMessage::Bind(bind) if bind.statement() ==""),
        "bind should not be renamed",
    );

    assert_eq!(messages.len(), 4);
}

#[tokio::test]
async fn test_rewrite_simple() {
    let messages = run_test(vec![ProtocolMessage::from(Query::new("SELECT 1, 2, 3"))]).await;

    assert!(
        matches!(messages[0].clone(), ProtocolMessage::Query(query) if query.query() == "SELECT 1, 2, 3"),
        "query should not be altered",
    );

    assert_eq!(messages.len(), 1);
}

#[tokio::test]
async fn test_reject_other_clients_prepared_statement() {
    let mut owner = TestClient::new_replicas(Parameters::default()).await;
    owner.send(Parse::named("owned", "SELECT 1")).await;
    owner.send(Sync).await;
    owner
        .try_process()
        .await
        .expect("prepare owner's statement");
    expect_message!(owner.read().await, ParseComplete);
    expect_message!(owner.read().await, ReadyForQuery);

    let name = owner
        .client()
        .prepared_statements
        .name("owned")
        .expect("owner's statement has a global name")
        .clone();
    let mut client = TestClient::new(Parameters::default()).await;

    for message in [
        ProtocolMessage::from(Bind::new_statement(&name)),
        ProtocolMessage::from(Describe::new_statement(&name)),
    ] {
        client.send(message).await;
        client.send(Sync).await;
        client.try_process().await.expect("return statement error");

        let error = expect_message!(client.read().await, ErrorResponse);
        assert_eq!(error.severity, "ERROR");
        assert_eq!(error.code, "26000");
        assert_eq!(
            error.message,
            format!("prepared statement \"{name}\" does not exist")
        );
        expect_message!(client.read().await, ReadyForQuery);
        assert!(!client.backend_connected());

        assert_connection_usable(&mut client).await;
    }
}
