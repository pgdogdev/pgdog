use pgdog_config::PreparedStatements;

use crate::config::load_test;

use super::change_config;
use super::prelude::*;

async fn run_test(client: &mut Client, messages: &[ProtocolMessage]) -> Vec<ProtocolMessage> {
    client.client_request = ClientRequest::from(messages.to_vec());
    let mut engine = QueryEngine::from_client(&client).unwrap();
    let mut context = QueryEngineContext::new(client);

    assert!(engine.parse_and_rewrite(&mut context).await.unwrap());

    client.client_request.messages.clone()
}

#[tokio::test]
async fn test_rewrite_prepare() {
    load_test();

    change_config(|general| {
        general.prepared_statements = PreparedStatements::Full;
    });

    let mut client = Client::new_test(Stream::dev_null(), Parameters::default());

    let messages = run_test(
        &mut client,
        &[Query::new("PREPARE __test_1 AS SELECT $1, $2, $3").into()],
    )
    .await;

    assert!(
        matches!(messages[0].clone(), ProtocolMessage::Query(query) if query.query() == "PREPARE __pgdog_1 AS SELECT $1, $2, $3"),
        "expected rewritten prepared statement"
    );

    let messages = run_test(
        &mut client,
        &[Query::new("EXECUTE __test_1(1, 2, 3)").into()],
    )
    .await;

    assert!(
        matches!(messages[0].clone(), ProtocolMessage::Prepare { name, statement } if name == "__pgdog_1" && statement == "SELECT $1, $2, $3")
    );

    assert!(
        matches!(messages[1].clone(), ProtocolMessage::Query(query) if query.query() == "EXECUTE __pgdog_1(1, 2, 3)"),
        "expected rewritten prepared statement"
    );
}
