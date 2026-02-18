use crate::frontend::router::parser::rewrite::statement::{
    offset::OffsetPlan, plan::RewriteResult,
};
use crate::frontend::router::parser::Limit;

use super::prelude::*;
use super::{test_client, test_sharded_client};

async fn run_test(messages: Vec<ProtocolMessage>) -> Option<OffsetPlan> {
    let mut client = test_sharded_client();
    client.client_request = ClientRequest::from(messages);

    let mut engine = QueryEngine::from_client(&client).unwrap();
    let mut context = QueryEngineContext::new(&mut client);

    engine.parse_and_rewrite(&mut context).await.unwrap();

    match context.rewrite_result {
        Some(RewriteResult::InPlace { offset }) => offset,
        other => panic!("expected InPlace, got {:?}", other),
    }
}

#[tokio::test]
async fn test_offset_limit_literals() {
    let offset = run_test(vec![ProtocolMessage::Query(Query::new(
        "SELECT * FROM test LIMIT 10 OFFSET 5",
    ))])
    .await;

    let offset = offset.expect("expected OffsetPlan");
    assert_eq!(
        offset.limit,
        Limit {
            limit: Some(10),
            offset: Some(5)
        }
    );
}

#[tokio::test]
async fn test_offset_limit_params() {
    let offset = run_test(vec![
        ProtocolMessage::Parse(Parse::new_anonymous(
            "SELECT * FROM test LIMIT $1 OFFSET $2",
        )),
        ProtocolMessage::Bind(Bind::new_params(
            "",
            &[Parameter::new(b"10"), Parameter::new(b"5")],
        )),
        ProtocolMessage::Execute(Execute::new()),
        ProtocolMessage::Sync(Sync),
    ])
    .await;

    let offset = offset.expect("expected OffsetPlan");
    assert_eq!(offset.limit.limit, None);
    assert_eq!(offset.limit.offset, None);
    assert_eq!(offset.limit_param, 1);
    assert_eq!(offset.offset_param, 2);
}

#[tokio::test]
async fn test_offset_limit_no_offset_no_plan() {
    let offset = run_test(vec![ProtocolMessage::Query(Query::new(
        "SELECT * FROM test LIMIT 10",
    ))])
    .await;

    assert!(offset.is_none());
}

#[tokio::test]
async fn test_offset_limit_no_limit_no_plan() {
    let offset = run_test(vec![ProtocolMessage::Query(Query::new(
        "SELECT * FROM test OFFSET 5",
    ))])
    .await;

    assert!(offset.is_none());
}

#[tokio::test]
async fn test_offset_limit_not_sharded() {
    let mut client = test_client();
    client.client_request = ClientRequest::from(vec![ProtocolMessage::Query(Query::new(
        "SELECT * FROM test LIMIT 10 OFFSET 5",
    ))]);

    let mut engine = QueryEngine::from_client(&client).unwrap();
    let mut context = QueryEngineContext::new(&mut client);

    engine.parse_and_rewrite(&mut context).await.unwrap();

    assert!(context.rewrite_result.is_none());
}

#[tokio::test]
async fn test_offset_limit_no_select() {
    let offset = run_test(vec![ProtocolMessage::Query(Query::new(
        "INSERT INTO test (id) VALUES (1)",
    ))])
    .await;

    assert!(offset.is_none());
}
