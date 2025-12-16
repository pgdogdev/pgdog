use super::prelude::*;
use super::{test_client, test_sharded_client};

async fn run_test(messages: Vec<ProtocolMessage>) -> Vec<ProtocolMessage> {
    let mut results = vec![];

    for mut client in [test_client(), test_sharded_client()] {
        client.client_request = messages.clone().into();

        let mut engine = QueryEngine::from_client(&client).unwrap();
        let mut context = QueryEngineContext::new(&mut client);

        engine.rewrite_extended(&mut context).unwrap();

        results.push(client.client_request.messages.clone());
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
