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
