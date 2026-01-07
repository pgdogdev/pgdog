use crate::{
    expect_message,
    net::{parameter::ParameterValue, CommandComplete, ReadyForQuery},
};

use super::prelude::*;

#[tokio::test]
async fn test_set() {
    let mut test_client = TestClient::new_sharded(Parameters::default()).await;

    test_client
        .send_simple(Query::new("SET application_name TO 'test_set'"))
        .await;

    assert_eq!(
        expect_message!(test_client.read().await, CommandComplete).command(),
        "SET"
    );
    assert_eq!(
        expect_message!(test_client.read().await, ReadyForQuery).status,
        'I'
    );

    assert_eq!(
        test_client.client().params.get("application_name").unwrap(),
        &ParameterValue::String("test_set".into()),
    );
}

#[tokio::test]
async fn test_set_search_path() {
    let mut test_client = TestClient::new_sharded(Parameters::default()).await;

    test_client
        .send_simple(Query::new(
            "SET search_path TO \"$user\", public, acustomer",
        ))
        .await;

    assert_eq!(
        expect_message!(test_client.read().await, CommandComplete).command(),
        "SET"
    );
    assert_eq!(
        expect_message!(test_client.read().await, ReadyForQuery).status,
        'I'
    );

    assert_eq!(
        test_client.client().params.get("search_path").unwrap(),
        &ParameterValue::Tuple(vec!["$user".into(), "public".into(), "acustomer".into()]),
    );
}

#[tokio::test]
async fn test_set_inside_transaction() {
    let mut test_client = TestClient::new_sharded(Parameters::default()).await;

    test_client.send_simple(Query::new("BEGIN")).await;

    assert_eq!(
        expect_message!(test_client.read().await, CommandComplete).command(),
        "BEGIN"
    );
    assert_eq!(
        expect_message!(test_client.read().await, ReadyForQuery).status,
        'T'
    );

    test_client
        .send_simple(Query::new("SET search_path TO acustomer, public"))
        .await;

    assert_eq!(
        expect_message!(test_client.read().await, CommandComplete).command(),
        "SET"
    );
    assert_eq!(
        expect_message!(test_client.read().await, ReadyForQuery).status,
        'T'
    );

    test_client.send_simple(Query::new("COMMIT")).await;

    assert_eq!(
        expect_message!(test_client.read().await, CommandComplete).command(),
        "COMMIT"
    );
    assert_eq!(
        expect_message!(test_client.read().await, ReadyForQuery).status,
        'I'
    );

    assert_eq!(
        test_client.client().params.get("search_path").unwrap(),
        &ParameterValue::Tuple(vec!["acustomer".into(), "public".into()]),
    );
}

#[tokio::test]
async fn test_set_inside_transaction_rollback() {
    let mut test_client = TestClient::new_sharded(Parameters::default()).await;

    test_client.send_simple(Query::new("BEGIN")).await;

    assert_eq!(
        expect_message!(test_client.read().await, CommandComplete).command(),
        "BEGIN"
    );
    assert_eq!(
        expect_message!(test_client.read().await, ReadyForQuery).status,
        'T'
    );

    test_client
        .send_simple(Query::new("SET search_path TO acustomer, public"))
        .await;

    assert_eq!(
        expect_message!(test_client.read().await, CommandComplete).command(),
        "SET"
    );
    assert_eq!(
        expect_message!(test_client.read().await, ReadyForQuery).status,
        'T'
    );

    test_client.send_simple(Query::new("ROLLBACK")).await;

    assert_eq!(
        expect_message!(test_client.read().await, CommandComplete).command(),
        "ROLLBACK"
    );
    assert_eq!(
        expect_message!(test_client.read().await, ReadyForQuery).status,
        'I'
    );

    assert!(
        test_client.client().params.get("search_path").is_none(),
        "search_path should not be set",
    );
}
