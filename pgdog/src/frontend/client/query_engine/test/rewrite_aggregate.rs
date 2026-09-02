use crate::frontend::router::parser::Shard;

use super::prelude::*;
use super::test_sharded_client;

async fn route_and_rewrite_request(messages: Vec<ProtocolMessage>) -> (Shard, String) {
    let mut client = test_sharded_client();
    client.client_request = ClientRequest::from(messages);

    let mut engine = QueryEngine::from_client(&client).unwrap();
    let mut context = QueryEngineContext::new(&mut client);

    let rewrite = engine.parse_and_rewrite(&mut context).unwrap();
    engine
        .route_query(&mut context, rewrite.as_ref())
        .await
        .unwrap();

    let route = context.client_request.route().shard().clone();
    let sql = context
        .client_request
        .iter()
        .find_map(|message| match message {
            ProtocolMessage::Query(query) => Some(query.query().to_owned()),
            ProtocolMessage::Parse(parse) => Some(parse.query().to_owned()),
            _ => None,
        })
        .unwrap();

    (route, sql)
}

async fn route_and_rewrite(sql: &str) -> (Shard, String) {
    route_and_rewrite_request(vec![Query::new(sql).into()]).await
}

#[tokio::test]
async fn test_direct_aggregates_do_not_include_cross_shard_helpers() {
    for function in ["avg", "stddev", "variance"] {
        let (route, sql) = route_and_rewrite(&format!(
            "SELECT {function}(region_id) FROM sharded WHERE id = 1"
        ))
        .await;

        assert!(matches!(route, Shard::Direct(_)));
        assert!(
            !sql.contains("__pgdog_"),
            "direct {function} query included helper columns: {sql}"
        );
    }
}

#[tokio::test]
async fn test_cross_shard_aggregate_keeps_helpers() {
    let (route, sql) = route_and_rewrite("SELECT stddev(region_id) FROM sharded").await;

    assert!(route.is_all());
    assert!(sql.contains("__pgdog_count_col0"));
    assert!(sql.contains("__pgdog_sum_col0"));
    assert!(sql.contains("__pgdog_sumsq_col0"));
}

#[tokio::test]
async fn test_direct_extended_aggregate_does_not_include_helpers() {
    let (route, sql) = route_and_rewrite_request(vec![
        Parse::new_anonymous("SELECT avg(region_id) FROM sharded WHERE id = $1").into(),
        Bind::new_params("", &[Parameter::new(b"1")]).into(),
        Execute::new().into(),
        Sync.into(),
    ])
    .await;

    assert!(matches!(route, Shard::Direct(_)));
    assert!(!sql.contains("__pgdog_"));
}

#[tokio::test]
async fn test_cross_shard_extended_aggregate_keeps_helpers() {
    let (route, sql) = route_and_rewrite_request(vec![
        Parse::new_anonymous("SELECT avg(region_id) FROM sharded").into(),
        Bind::new_params("", &[]).into(),
        Execute::new().into(),
        Sync.into(),
    ])
    .await;

    assert!(route.is_all());
    assert!(sql.contains("__pgdog_count_col0"));
}
