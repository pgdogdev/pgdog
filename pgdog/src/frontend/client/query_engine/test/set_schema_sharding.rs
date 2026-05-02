use super::prelude::*;

use crate::net::{DataRow, Parameter};

#[tokio::test]
async fn test_set_works_cross_shard_disabled() {
    let mut client = TestClient::new_cross_shard_disabled(Parameters::from(vec![Parameter {
        name: "search_path".into(),
        value: "bcustomer".into(),
    }]))
    .await;

    client.send_simple(Query::new("BEGIN")).await;
    let reply = client.read_until('Z').await.unwrap();
    assert_eq!(reply.len(), 2);

    client.send_simple(Query::new("SELECT 1")).await;
    let reply = client.read_until('Z').await.unwrap();
    assert_eq!(reply.len(), 4);

    client
        .send_simple(Query::new("SET statement_timeout TO '12345s'"))
        .await;
    let reply = client.read_until('Z').await.unwrap();
    assert_eq!(reply.len(), 2);

    client
        .send_simple(Query::new("SHOW statement_timeout"))
        .await;
    let reply = client.read_until('Z').await.unwrap();
    assert_eq!(reply.len(), 4);

    let row = DataRow::try_from(reply[1].clone()).unwrap();
    assert_eq!(row.get_text(0).unwrap(), "12345s");

    client.send_simple(Query::new("COMMIT")).await;
    let reply = client.read_until('Z').await.unwrap();
    assert_eq!(reply.len(), 2);
}

#[tokio::test]
async fn test_ambiguous_schema_sharded_query_errors_when_cross_shard_disabled() {
    let table = "schema_shard_ambiguous_test";

    let mut setup = TestClient::new_sharded(Parameters::default()).await;
    for stmt in [
        "CREATE SCHEMA IF NOT EXISTS acustomer".to_string(),
        "CREATE SCHEMA IF NOT EXISTS bcustomer".to_string(),
        format!("CREATE TABLE IF NOT EXISTS acustomer.{table} (id INT)"),
        format!("CREATE TABLE IF NOT EXISTS bcustomer.{table} (id INT)"),
    ] {
        setup.send_simple(Query::new(&stmt)).await;
        setup.read_until('Z').await.unwrap();
    }

    let mut client = TestClient::new_cross_shard_disabled(Parameters::default()).await;
    client
        .send_simple(Query::new(&format!("SELECT * FROM {table}")))
        .await;
    let err = client.read_until('Z').await.unwrap_err();
    assert_eq!(err.code, "58000");
    assert_eq!(err.message, "cross-shard queries are disabled");
}
