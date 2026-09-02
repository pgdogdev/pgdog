use crate::{
    expect_message,
    net::{CommandComplete, Parameters, ParseComplete, ReadyForQuery},
};

use super::prelude::*;

#[tokio::test]
async fn test_discard_clears_prepared_statement_cache() {
    let mut client = TestClient::new_replicas(Parameters::default())
        .await
        .with_full_prepared_statements();

    client.send(Parse::named("test_stmt", "SELECT $1")).await;
    client.send(Sync).await;
    client.try_process().await.unwrap();

    expect_message!(client.read().await, ParseComplete);
    expect_message!(client.read().await, ReadyForQuery);

    assert_eq!(client.client().prepared_statements.num_statements(), 1);
    let global = client.client().prepared_statements.global.clone();
    assert_eq!(global.read().statements().iter().next().unwrap().1.used, 1);

    client.send_simple(Query::new("DISCARD ALL")).await;

    expect_message!(client.read().await, CommandComplete);
    expect_message!(client.read().await, ReadyForQuery);

    assert_eq!(client.client().prepared_statements.num_statements(), 0);
    assert_eq!(global.read().statements().iter().next().unwrap().1.used, 0);
}

#[tokio::test]
async fn test_non_all_discard_keeps_prepared_statement_cache() {
    for query in ["DISCARD PLANS", "DISCARD SEQUENCES", "DISCARD TEMP"] {
        let mut client = TestClient::new_replicas(Parameters::default())
            .await
            .with_full_prepared_statements();

        client.send(Parse::named("test_stmt", "SELECT $1")).await;
        client.send(Sync).await;
        client.try_process().await.unwrap();

        expect_message!(client.read().await, ParseComplete);
        expect_message!(client.read().await, ReadyForQuery);

        let global = client.client().prepared_statements.global.clone();
        assert_eq!(client.client().prepared_statements.num_statements(), 1);
        assert_eq!(global.read().statements().iter().next().unwrap().1.used, 1);

        client.send_simple(Query::new(query)).await;

        expect_message!(client.read().await, CommandComplete);
        expect_message!(client.read().await, ReadyForQuery);

        assert_eq!(
            client.client().prepared_statements.num_statements(),
            1,
            "{query} should not clear prepared statements",
        );
        assert_eq!(global.read().statements().iter().next().unwrap().1.used, 1);
    }
}
