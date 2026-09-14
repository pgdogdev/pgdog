use crate::backend::schema::Schema;
use crate::frontend::router::parser::rewrite::statement::plan::RewriteResult;
use crate::frontend::router::parser::rewrite::statement::projection;
use crate::frontend::router::parser::route::{Route, Shard, ShardWithPriority};
use crate::frontend::{
    PreparedStatements,
    router::parser::{Limit, OrderBy},
};

use super::prelude::*;
use super::test_sharded_client;

fn route(shard: Shard) -> Route {
    Route::select(
        ShardWithPriority::new_table(shard),
        vec![],
        Default::default(),
        Limit::default(),
        None,
    )
}

#[tokio::test]
async fn direct_aggregate_keeps_base_sql() {
    let sql = "SELECT AVG(price) FROM products";
    let mut client = test_sharded_client();
    client.client_request = ClientRequest::from(vec![ProtocolMessage::Query(Query::new(sql))]);

    let mut engine = QueryEngine::from_client(&client).unwrap();
    let mut context = QueryEngineContext::new(&mut client);
    let result = engine.parse_and_rewrite(&mut context).await.unwrap();

    let query = match &context.client_request.messages[0] {
        ProtocolMessage::Query(query) => query,
        _ => panic!("expected Query"),
    };
    assert_eq!(query.query(), sql, "pre-route phase must not add helpers");

    context.client_request.route = Some(route(Shard::Direct(0)));
    projection::finalize_after_route(
        context.client_request,
        &Schema::default(),
        result.as_ref().and_then(RewriteResult::offset_plan),
    )
    .unwrap();

    let query = match &context.client_request.messages[0] {
        ProtocolMessage::Query(query) => query,
        _ => panic!("expected Query"),
    };
    assert_eq!(query.query(), sql);
    assert!(
        context
            .client_request
            .route()
            .projection_rewrite_plan()
            .is_noop()
    );
}

#[tokio::test]
async fn cross_shard_aggregate_adds_and_tracks_helpers() {
    let mut client = test_sharded_client();
    client.client_request = ClientRequest::from(vec![ProtocolMessage::Query(Query::new(
        "SELECT AVG(price) FROM products",
    ))]);

    let mut engine = QueryEngine::from_client(&client).unwrap();
    let mut context = QueryEngineContext::new(&mut client);
    let result = engine.parse_and_rewrite(&mut context).await.unwrap();
    context.client_request.route = Some(route(Shard::All));

    projection::finalize_after_route(
        context.client_request,
        &Schema::default(),
        result.as_ref().and_then(RewriteResult::offset_plan),
    )
    .unwrap();

    let query = match &context.client_request.messages[0] {
        ProtocolMessage::Query(query) => query,
        _ => panic!("expected Query"),
    };
    assert!(query.query().contains("__pgdog_count_col0"));
    assert_eq!(
        context
            .client_request
            .route()
            .projection_rewrite_plan()
            .aggregate_helpers()
            .len(),
        1
    );
}

#[tokio::test]
async fn named_prepared_aggregate_uses_cross_shard_variant() {
    let mut client = test_sharded_client();
    client.client_request = ClientRequest::from(vec![
        ProtocolMessage::Parse(Parse::named(
            "avg_measurement",
            "SELECT AVG(value) FROM measurements",
        )),
        ProtocolMessage::Bind(Bind::new_params("avg_measurement", &[])),
        ProtocolMessage::Execute(Execute::new()),
        ProtocolMessage::Sync(Sync),
    ]);

    let mut engine = QueryEngine::from_client(&client).unwrap();
    let mut context = QueryEngineContext::new(&mut client);
    engine.rewrite_extended(&mut context).unwrap();
    let base = match &context.client_request.messages[1] {
        ProtocolMessage::Bind(bind) => bind.statement().to_owned(),
        _ => panic!("expected Bind"),
    };
    let result = engine.parse_and_rewrite(&mut context).await.unwrap();
    context.client_request.route = Some(route(Shard::All));

    projection::finalize_after_route(
        context.client_request,
        &Schema::default(),
        result.as_ref().and_then(RewriteResult::offset_plan),
    )
    .unwrap();

    let variant = format!("{base}_cross_shard");
    match &context.client_request.messages[0] {
        ProtocolMessage::Parse(parse) => {
            assert_eq!(parse.name(), variant);
            assert!(parse.query().contains("__pgdog_count_col0"));
        }
        _ => panic!("expected Parse"),
    }
    match &context.client_request.messages[1] {
        ProtocolMessage::Bind(bind) => assert_eq!(bind.statement(), variant),
        _ => panic!("expected Bind"),
    }

    let cache = PreparedStatements::global();
    let cache = cache.read();
    assert!(
        !cache
            .rewritten_parse(&base)
            .unwrap()
            .query()
            .contains("__pgdog_")
    );
    assert!(
        cache
            .rewritten_parse(&variant)
            .unwrap()
            .query()
            .contains("__pgdog_count_col0")
    );
}

#[tokio::test]
async fn named_prepared_direct_aggregate_keeps_base_variant() {
    let mut client = test_sharded_client();
    client.client_request = ClientRequest::from(vec![
        ProtocolMessage::Parse(Parse::named(
            "direct_avg",
            "SELECT AVG(value) FROM direct_measurements",
        )),
        ProtocolMessage::Bind(Bind::new_params("direct_avg", &[])),
        ProtocolMessage::Execute(Execute::new()),
        ProtocolMessage::Sync(Sync),
    ]);

    let mut engine = QueryEngine::from_client(&client).unwrap();
    let mut context = QueryEngineContext::new(&mut client);
    engine.rewrite_extended(&mut context).unwrap();
    let base = match &context.client_request.messages[1] {
        ProtocolMessage::Bind(bind) => bind.statement().to_owned(),
        _ => panic!("expected Bind"),
    };
    let result = engine.parse_and_rewrite(&mut context).await.unwrap();
    context.client_request.route = Some(route(Shard::Direct(0)));

    projection::finalize_after_route(
        context.client_request,
        &Schema::default(),
        result.as_ref().and_then(RewriteResult::offset_plan),
    )
    .unwrap();

    match &context.client_request.messages[0] {
        ProtocolMessage::Parse(parse) => {
            assert_eq!(parse.name(), base);
            assert!(!parse.query().contains("__pgdog_"));
        }
        _ => panic!("expected Parse"),
    }
    match &context.client_request.messages[1] {
        ProtocolMessage::Bind(bind) => assert_eq!(bind.statement(), base),
        _ => panic!("expected Bind"),
    }
    assert!(
        PreparedStatements::global()
            .read()
            .rewritten_parse(&format!("{base}_cross_shard"))
            .is_none()
    );
}

#[tokio::test]
async fn cross_shard_order_by_projects_missing_sort_column() {
    let sql = "SELECT id FROM products ORDER BY price";
    let mut client = test_sharded_client();
    client.client_request = ClientRequest::from(vec![ProtocolMessage::Query(Query::new(sql))]);

    let mut engine = QueryEngine::from_client(&client).unwrap();
    let mut context = QueryEngineContext::new(&mut client);
    let result = engine.parse_and_rewrite(&mut context).await.unwrap();
    context.client_request.route = Some(Route::select(
        ShardWithPriority::new_table(Shard::All),
        vec![OrderBy::AscColumn("price".into())],
        Default::default(),
        Limit::default(),
        None,
    ));

    projection::finalize_after_route(
        context.client_request,
        &Schema::default(),
        result.as_ref().and_then(RewriteResult::offset_plan),
    )
    .unwrap();

    let query = match &context.client_request.messages[0] {
        ProtocolMessage::Query(query) => query,
        _ => panic!("expected Query"),
    };
    assert!(query.query().contains("price AS __pgdog_order_col0"));
    assert_eq!(
        context.client_request.route().order_by(),
        &[OrderBy::Asc(2)]
    );
    assert_eq!(
        context
            .client_request
            .route()
            .projection_rewrite_plan()
            .order_by_helpers()
            .len(),
        1
    );
}

#[tokio::test]
async fn aggregate_order_by_and_offset_compose_after_route() {
    let mut client = test_sharded_client();
    client.client_request = ClientRequest::from(vec![ProtocolMessage::Query(Query::new(
        "SELECT AVG(value) FROM measurements ORDER BY created_at LIMIT 10 OFFSET 5",
    ))]);

    let mut engine = QueryEngine::from_client(&client).unwrap();
    let mut context = QueryEngineContext::new(&mut client);
    let result = engine.parse_and_rewrite(&mut context).await.unwrap();
    context.client_request.route = Some(Route::select(
        ShardWithPriority::new_table(Shard::All),
        vec![OrderBy::AscColumn("created_at".into())],
        Default::default(),
        Limit::default(),
        None,
    ));

    projection::finalize_after_route(
        context.client_request,
        &Schema::default(),
        result.as_ref().and_then(RewriteResult::offset_plan),
    )
    .unwrap();
    result
        .as_ref()
        .unwrap()
        .apply_after_route(context.client_request)
        .unwrap();

    let query = match &context.client_request.messages[0] {
        ProtocolMessage::Query(query) => query,
        _ => panic!("expected Query"),
    };
    assert!(query.query().contains("__pgdog_count_col0"));
    assert!(query.query().contains("created_at AS __pgdog_order_col0"));
    assert!(query.query().contains("LIMIT 10::bigint + 5::bigint"));
    assert!(!query.query().contains("OFFSET"));

    let route = context.client_request.route();
    assert_eq!(route.order_by(), &[OrderBy::Asc(3)]);
    assert_eq!(
        route
            .projection_rewrite_plan()
            .drop_columns()
            .collect::<Vec<_>>(),
        [1, 2]
    );
    assert_eq!(
        route.limit(),
        &Limit {
            limit: Some(10),
            offset: Some(5),
        }
    );
}

#[tokio::test]
async fn split_anonymous_prepare_finalizes_saved_parse_on_execute() {
    let mut client = test_sharded_client();
    client.client_request = ClientRequest::default();
    client
        .client_request
        .push(ProtocolMessage::Parse(Parse::new_anonymous(
            "SELECT AVG(value) FROM split_measurements",
        )));
    client
        .client_request
        .push(ProtocolMessage::Describe(Describe::new_statement("")));
    client.client_request.push(Flush.into());
    client.client_request.clear();
    client
        .client_request
        .push(ProtocolMessage::Bind(Bind::new_params("", &[])));
    client
        .client_request
        .push(ProtocolMessage::Execute(Execute::new()));
    client.client_request.push(ProtocolMessage::Sync(Sync));

    let mut engine = QueryEngine::from_client(&client).unwrap();
    let mut context = QueryEngineContext::new(&mut client);
    let result = engine.parse_and_rewrite(&mut context).await.unwrap();
    context.client_request.route = Some(route(Shard::All));

    projection::finalize_after_route(
        context.client_request,
        &Schema::default(),
        result.as_ref().and_then(RewriteResult::offset_plan),
    )
    .unwrap();

    assert!(
        context
            .client_request
            .last_parse
            .as_ref()
            .unwrap()
            .query()
            .contains("__pgdog_count_col0")
    );
    assert!(
        context
            .client_request
            .messages
            .iter()
            .all(|message| !matches!(message, ProtocolMessage::Parse(_)))
    );
}

#[tokio::test]
async fn named_statement_can_switch_from_direct_to_cross_shard_variant() {
    let mut client = test_sharded_client();
    client.client_request = ClientRequest::from(vec![
        ProtocolMessage::Parse(Parse::named(
            "route_switch",
            "SELECT AVG(value) FROM route_switch_measurements",
        )),
        ProtocolMessage::Sync(Sync),
    ]);

    let base = {
        let mut engine = QueryEngine::from_client(&client).unwrap();
        let mut context = QueryEngineContext::new(&mut client);
        engine.rewrite_extended(&mut context).unwrap();
        let base = match &context.client_request.messages[0] {
            ProtocolMessage::Parse(parse) => parse.name().to_owned(),
            _ => panic!("expected Parse"),
        };
        let result = engine.parse_and_rewrite(&mut context).await.unwrap();
        context.client_request.route = Some(route(Shard::Direct(0)));
        projection::finalize_after_route(
            context.client_request,
            &Schema::default(),
            result.as_ref().and_then(RewriteResult::offset_plan),
        )
        .unwrap();
        base
    };

    client.client_request.clear();
    client
        .client_request
        .push(ProtocolMessage::Bind(Bind::new_params("route_switch", &[])));
    client
        .client_request
        .push(ProtocolMessage::Execute(Execute::new()));
    client.client_request.push(ProtocolMessage::Sync(Sync));

    let mut engine = QueryEngine::from_client(&client).unwrap();
    let mut context = QueryEngineContext::new(&mut client);
    engine.rewrite_extended(&mut context).unwrap();
    let result = engine.parse_and_rewrite(&mut context).await.unwrap();
    context.client_request.route = Some(route(Shard::All));
    projection::finalize_after_route(
        context.client_request,
        &Schema::default(),
        result.as_ref().and_then(RewriteResult::offset_plan),
    )
    .unwrap();

    match &context.client_request.messages[0] {
        ProtocolMessage::Bind(bind) => {
            assert_eq!(bind.statement(), format!("{base}_cross_shard"));
        }
        _ => panic!("expected Bind"),
    }
}
