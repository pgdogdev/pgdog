use pgdog_config::QueryParserLevel;

use super::prelude::*;
use crate::{
    config::load_test,
    expect_message,
    net::{CommandComplete, ErrorResponse, ReadyForQuery},
};

#[tokio::test]
async fn empty_query_then_rollback() {
    load_test();
    super::change_config(|general| general.query_parser = QueryParserLevel::On);
    let mut client = TestClient::new(Parameters::default()).await;

    client.send_simple(Query::new("BEGIN")).await;
    client.read_until('Z').await.expect("begin transaction");
    client.send_simple(Query::new("SELECT 1/0")).await;
    assert_eq!(
        expect_message!(client.read().await, ErrorResponse).code,
        "22012"
    );
    assert_eq!(
        expect_message!(client.read().await, ReadyForQuery).status,
        'E'
    );

    client.send_simple(Query::new(";")).await;
    assert_eq!(client.read().await.code(), 'I');
    assert_eq!(
        expect_message!(client.read().await, ReadyForQuery).status,
        'E'
    );

    client.send_simple(Query::new("ROLLBACK")).await;
    expect_message!(client.read().await, CommandComplete);
    assert_eq!(
        expect_message!(client.read().await, ReadyForQuery).status,
        'I'
    );
}
