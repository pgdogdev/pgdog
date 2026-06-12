use crate::{
    frontend::{
        ClientRequest,
        client::{query_engine::QueryEngineContext, test::TestClient},
    },
    net::{Parameters, Query},
};

#[tokio::test]
async fn test_same_shard_insert_uses_direct_route() {
    crate::logger();

    let mut client = TestClient::new_rewrites(Parameters::default()).await;
    let id1 = client.random_id_for_shard(0);
    let id2 = client.random_id_for_shard(0);

    client.client.client_request = ClientRequest::from(vec![
        Query::new(format!(
            "INSERT INTO sharded (id, value) VALUES ({}, 'a'), ({}, 'b')",
            id1, id2
        ))
        .into(),
    ]);

    let mut context = QueryEngineContext::new(&mut client.client);
    client.engine.parse_and_rewrite(&mut context).await.unwrap();
    client.engine.route_query(&mut context).await.unwrap();
    client.engine.execute(&mut context).await.unwrap();

    assert!(
        context.client_request.route().shard().is_direct(),
        "same-shard INSERT should bypass the split and use direct-to-shard routing"
    );
}

#[tokio::test]
async fn test_cross_shard_insert_uses_all_shards() {
    crate::logger();

    let mut client = TestClient::new_rewrites(Parameters::default()).await;
    let id0 = client.random_id_for_shard(0);
    let id1 = client.random_id_for_shard(1);

    client.client.client_request = ClientRequest::from(vec![
        Query::new(format!(
            "INSERT INTO sharded (id, value) VALUES ({}, 'a'), ({}, 'b')",
            id0, id1
        ))
        .into(),
    ]);

    let mut context = QueryEngineContext::new(&mut client.client);
    client.engine.parse_and_rewrite(&mut context).await.unwrap();
    client.engine.route_query(&mut context).await.unwrap();
    client.engine.execute(&mut context).await.unwrap();

    assert!(
        !context.client_request.route().shard().is_direct(),
        "cross-shard INSERT must go through the split path, not the direct route"
    );
}
