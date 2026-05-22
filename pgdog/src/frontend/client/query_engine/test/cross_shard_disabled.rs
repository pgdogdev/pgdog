use super::prelude::*;

#[tokio::test]
async fn test_cross_shard_ddl() {
    let mut client = TestClient::new_cross_shard_disabled_replicas(Parameters::default()).await;

    client.send_simple(Query::new("BEGIN")).await;

    // Force Postgres connection.
    client.send_simple(Query::new("SELECT 1")).await;

    client
        .send_simple(Query::new("SET lock_timeout TO '1s'"))
        .await;

    client.send_simple(Query::new("ROLLBACK")).await;
}
