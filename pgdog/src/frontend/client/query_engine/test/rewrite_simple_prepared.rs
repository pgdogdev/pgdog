use pgdog_config::PreparedStatementsLevel;

use crate::config::load_test;

use super::change_config;
use super::prelude::*;

async fn run_test(client: &mut Client, messages: &[ProtocolMessage]) -> Vec<ProtocolMessage> {
    client.client_request = ClientRequest::from(messages.to_vec());
    let mut engine = QueryEngine::from_client(client).unwrap();
    let mut context = QueryEngineContext::new(client);

    assert!(engine.parse_and_rewrite(&mut context).unwrap().is_some());

    client.client_request.messages.clone()
}

#[tokio::test]
async fn test_rewrite_prepare() {
    load_test();

    change_config(|general| {
        general.prepared_statements = PreparedStatementsLevel::Full;
    });

    let mut client = Client::new_test(Stream::dev_null(), Parameters::default());

    let messages = run_test(
        &mut client,
        &[Query::new("PREPARE __test_1 AS SELECT $1, $2, $3").into()],
    )
    .await;

    assert!(
        matches!(messages[0].clone(), ProtocolMessage::PrepareFromClient(prepare) if prepare.query() == "PREPARE __pgdog_template_name AS SELECT $1, $2, $3"),
        "expected rewritten prepared statement: {:#?}",
        messages,
    );

    let messages = run_test(
        &mut client,
        &[Query::new("EXECUTE __test_1(1, 2, 3)").into()],
    )
    .await;

    assert!(
        matches!(messages[0].clone(), ProtocolMessage::EnsurePrepared(prepare) if prepare.name() == "__pgdog_1" && prepare.query() == "PREPARE __pgdog_template_name AS SELECT $1, $2, $3")
    );

    assert!(
        matches!(messages[1].clone(), ProtocolMessage::Query(query) if query.query() == "EXECUTE __pgdog_1(1, 2, 3)"),
        "expected rewritten prepared statement"
    );
}

fn rewritten_query(messages: &[ProtocolMessage]) -> String {
    match &messages[0] {
        ProtocolMessage::PrepareFromClient(prepare) => prepare.query().to_string(),
        other => panic!("expected Query, got {other:#?}"),
    }
}

#[tokio::test]
async fn test_reprepare_releases_previous_statement() {
    load_test();

    change_config(|general| {
        general.prepared_statements = PreparedStatementsLevel::Full;
    });

    let global = crate::frontend::PreparedStatements::global();
    let mut client = Client::new_test(Stream::dev_null(), Parameters::default());

    assert_eq!(global.read().len(), 0);

    let first = rewritten_query(
        &run_test(
            &mut client,
            &[Query::new("PREPARE reused AS SELECT $1::bigint").into()],
        )
        .await,
    );

    // The client name is replaced, the statement is not.
    assert!(first.starts_with("PREPARE __pgdog_"));
    assert!(first.ends_with(" AS SELECT $1::bigint"));

    assert_eq!(global.read().len(), 1);

    let second = rewritten_query(
        &run_test(
            &mut client,
            &[Query::new("PREPARE reused AS SELECT $1::bigint + 1").into()],
        )
        .await,
    );

    assert!(second.starts_with("PREPARE __pgdog_"));
    assert!(second.ends_with(" AS SELECT $1::bigint + 1"));

    // PREPARE-ing a different statement with the same name creates a new global name.
    assert_eq!(global.read().len(), 2);

    // Only the statement the client replaced is evictable.
    assert_eq!(global.write().close_unused(0), 1);
    assert_eq!(global.read().len(), 1);
}
