use pgdog_config::PreparedStatementsLevel;

use crate::config::load_test;
use crate::frontend::PreparedOperation;

use super::change_config;
use super::prelude::*;

async fn run_test(client: &mut Client, messages: &[ProtocolMessage]) -> Vec<ProtocolMessage> {
    client.client_request = ClientRequest::from(messages.to_vec());
    let engine = QueryEngine::from_client(client).unwrap();
    let (mut context, client_request) = QueryEngineContext::new(client);

    assert!(
        engine
            .parse_and_rewrite(&mut context, client_request)
            .await
            .unwrap()
            .is_some()
    );

    client.client_request.messages.clone()
}

async fn run_extended_test(
    client: &mut Client,
    messages: &[ProtocolMessage],
) -> Vec<ProtocolMessage> {
    client.client_request = ClientRequest::from(messages.to_vec());
    let mut engine = QueryEngine::from_client(client).unwrap();
    let (mut context, client_request) = QueryEngineContext::new(client);

    engine
        .rewrite_extended(&mut context, &mut client_request.messages)
        .unwrap();
    assert!(
        engine
            .parse_and_rewrite(&mut context, client_request)
            .await
            .unwrap()
            .is_some()
    );

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

#[tokio::test]
async fn test_rewrite_prepare_and_execute_preserves_extended_protocol() {
    load_test();

    change_config(|general| {
        general.prepared_statements = PreparedStatementsLevel::Full;
    });

    let mut client = Client::new_test(Stream::dev_null(), Parameters::default());
    let messages = run_extended_test(
        &mut client,
        &[
            Parse::named("outer_prepare", "PREPARE stmt1 AS SELECT $1").into(),
            Bind::new_statement("outer_prepare").into(),
            Execute::new().into(),
            Sync.into(),
        ],
    )
    .await;

    let ProtocolMessage::Parse(parse) = &messages[0] else {
        panic!("expected Parse, got {:#?}", messages[0]);
    };
    let outer_prepare_name = parse.name().to_owned();
    assert!(parse.query().starts_with("PREPARE __pgdog_"));
    assert!(parse.query().ends_with(" AS SELECT $1"));

    assert!(
        matches!(&messages[1], ProtocolMessage::Bind(bind) if bind.statement() == outer_prepare_name)
    );
    let Some(PreparedOperation::PrepareOnExecute(prepare)) =
        client.client_request.prepared_operation.as_deref()
    else {
        panic!("expected SQL PREPARE execution metadata");
    };
    let sql_prepared_name = prepare.name().to_owned();
    assert!(
        parse
            .query()
            .starts_with(&format!("PREPARE {sql_prepared_name} AS"))
    );
    assert!(matches!(&messages[2], ProtocolMessage::Execute(_)));
    assert!(matches!(&messages[3], ProtocolMessage::Sync(_)));

    let messages = run_extended_test(
        &mut client,
        &[
            Parse::named("outer_execute", "EXECUTE stmt1($1)").into(),
            Bind::new_params("outer_execute", &[Parameter::new(b"5")]).into(),
            Execute::new().into(),
            Sync.into(),
        ],
    )
    .await;

    assert!(
        matches!(&messages[0], ProtocolMessage::EnsurePrepared(prepare) if prepare.name() == sql_prepared_name)
    );
    assert!(
        matches!(&messages[1], ProtocolMessage::Parse(parse) if parse.query() == format!("EXECUTE {sql_prepared_name}($1)"))
    );
    assert!(matches!(&messages[2], ProtocolMessage::Bind(bind)
            if bind.statement() == sql_prepared_name
                && bind.params_raw() == [Parameter::new(b"5")]));
    assert!(matches!(&messages[3], ProtocolMessage::Execute(_)));
    assert!(matches!(&messages[4], ProtocolMessage::Sync(_)));
    assert!(client.client_request.prepared_operation.is_none());
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
