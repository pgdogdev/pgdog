use crate::frontend::router::parser::rewrite::statement::{
    offset::OffsetPlan, plan::RewriteResult,
};
use crate::frontend::router::parser::route::{Route, Shard, ShardWithPriority};
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

fn cross_shard_route() -> Route {
    Route::select(
        ShardWithPriority::new_table(Shard::All),
        vec![],
        Default::default(),
        Limit::default(),
        None,
    )
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

#[tokio::test]
async fn test_offset_with_unique_id_simple() {
    unsafe {
        std::env::set_var("NODE_ID", "pgdog-1");
    }
    let sql = "SELECT pgdog.unique_id() FROM test LIMIT 10 OFFSET 5";
    let mut client = test_sharded_client();
    client.client_request = ClientRequest::from(vec![ProtocolMessage::Query(Query::new(sql))]);

    let mut engine = QueryEngine::from_client(&client).unwrap();
    let mut context = QueryEngineContext::new(&mut client);

    engine.parse_and_rewrite(&mut context).await.unwrap();

    // After parse_and_rewrite, the Query message should have unique_id replaced.
    let rewritten_sql = match &context.client_request.messages[0] {
        ProtocolMessage::Query(q) => q.query().to_owned(),
        _ => panic!("expected Query"),
    };
    assert!(
        !rewritten_sql.contains("pgdog.unique_id"),
        "unique_id should be replaced: {rewritten_sql}"
    );
    assert!(
        rewritten_sql.contains("::bigint"),
        "should have bigint cast: {rewritten_sql}"
    );

    // apply_after_parser with a cross-shard route.
    context.client_request.route = Some(cross_shard_route());
    context
        .rewrite_result
        .as_ref()
        .unwrap()
        .apply_after_parser(context.client_request)
        .unwrap();

    let final_sql = match &context.client_request.messages[0] {
        ProtocolMessage::Query(q) => q.query().to_owned(),
        _ => panic!("expected Query"),
    };

    // unique_id rewrite must survive.
    assert!(
        !final_sql.contains("pgdog.unique_id"),
        "unique_id rewrite must survive apply_after_parser: {final_sql}"
    );
    assert!(
        final_sql.contains("::bigint"),
        "bigint cast must survive: {final_sql}"
    );
    // LIMIT/OFFSET must be rewritten for cross-shard.
    assert!(
        final_sql.contains("LIMIT 15"),
        "LIMIT should be 10+5=15: {final_sql}"
    );
    assert!(
        final_sql.contains("OFFSET 0"),
        "OFFSET should be 0: {final_sql}"
    );
}

#[tokio::test]
async fn test_offset_with_unique_id_extended() {
    unsafe {
        std::env::set_var("NODE_ID", "pgdog-1");
    }
    let sql = "SELECT pgdog.unique_id(), $1 FROM test LIMIT $2 OFFSET $3";
    let mut client = test_sharded_client();
    client.client_request = ClientRequest::from(vec![
        ProtocolMessage::Parse(Parse::new_anonymous(sql)),
        ProtocolMessage::Bind(Bind::new_params(
            "",
            &[
                Parameter::new(b"hello"),
                Parameter::new(b"10"),
                Parameter::new(b"5"),
            ],
        )),
        ProtocolMessage::Execute(Execute::new()),
        ProtocolMessage::Sync(Sync),
    ]);

    let mut engine = QueryEngine::from_client(&client).unwrap();
    let mut context = QueryEngineContext::new(&mut client);

    engine.parse_and_rewrite(&mut context).await.unwrap();

    // After parse_and_rewrite, Parse should have unique_id rewritten to $4::bigint.
    let rewritten_sql = match &context.client_request.messages[0] {
        ProtocolMessage::Parse(p) => p.query().to_owned(),
        _ => panic!("expected Parse"),
    };
    assert_eq!(
        rewritten_sql,
        "SELECT $4::bigint, $1 FROM test LIMIT $2 OFFSET $3"
    );

    // apply_after_parser with cross-shard route should only rewrite Bind params.
    context.client_request.route = Some(cross_shard_route());
    context
        .rewrite_result
        .as_ref()
        .unwrap()
        .apply_after_parser(context.client_request)
        .unwrap();

    // SQL unchanged (all limit/offset are params).
    let final_sql = match &context.client_request.messages[0] {
        ProtocolMessage::Parse(p) => p.query().to_owned(),
        _ => panic!("expected Parse"),
    };
    assert_eq!(
        final_sql, "SELECT $4::bigint, $1 FROM test LIMIT $2 OFFSET $3",
        "SQL must be unchanged for all-param case"
    );

    // Bind params: $1=hello unchanged, $2=limit rewritten to 15, $3=offset rewritten to 0.
    if let ProtocolMessage::Bind(bind) = &context.client_request.messages[1] {
        assert_eq!(bind.params_raw()[0].data.as_ref(), b"hello");
        assert_eq!(bind.params_raw()[1].data.as_ref(), b"15");
        assert_eq!(bind.params_raw()[2].data.as_ref(), b"0");
    } else {
        panic!("expected Bind");
    }
}
