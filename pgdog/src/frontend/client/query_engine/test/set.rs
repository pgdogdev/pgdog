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

    assert!(!test_client.backend_locked());
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

    assert!(!test_client.backend_locked());

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

    assert!(!test_client.backend_locked());
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

#[tokio::test]
async fn test_reset() {
    let mut test_client = TestClient::new_sharded(Parameters::default()).await;

    // First set a parameter
    test_client
        .send_simple(Query::new("SET application_name TO 'test_reset'"))
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
        &ParameterValue::String("test_reset".into()),
    );

    // Now reset it
    test_client
        .send_simple(Query::new("RESET application_name"))
        .await;

    assert_eq!(
        expect_message!(test_client.read().await, CommandComplete).command(),
        "RESET"
    );
    assert_eq!(
        expect_message!(test_client.read().await, ReadyForQuery).status,
        'I'
    );

    assert!(
        test_client
            .client()
            .params
            .get("application_name")
            .is_none(),
        "application_name should be reset"
    );
}

#[tokio::test]
async fn test_reset_all() {
    let mut test_client = TestClient::new_sharded(Parameters::default()).await;

    // Set multiple parameters
    test_client
        .send_simple(Query::new("SET application_name TO 'test_reset_all'"))
        .await;
    expect_message!(test_client.read().await, CommandComplete);
    expect_message!(test_client.read().await, ReadyForQuery);

    test_client
        .send_simple(Query::new("SET statement_timeout TO 5000"))
        .await;
    expect_message!(test_client.read().await, CommandComplete);
    expect_message!(test_client.read().await, ReadyForQuery);

    assert!(test_client
        .client()
        .params
        .get("application_name")
        .is_some());
    assert!(test_client
        .client()
        .params
        .get("statement_timeout")
        .is_some());

    // Reset all
    test_client.send_simple(Query::new("RESET ALL")).await;

    assert_eq!(
        expect_message!(test_client.read().await, CommandComplete).command(),
        "RESET"
    );
    assert_eq!(
        expect_message!(test_client.read().await, ReadyForQuery).status,
        'I'
    );

    assert!(
        test_client
            .client()
            .params
            .get("application_name")
            .is_none(),
        "application_name should be reset"
    );
    assert!(
        test_client
            .client()
            .params
            .get("statement_timeout")
            .is_none(),
        "statement_timeout should be reset"
    );
}

#[tokio::test]
async fn test_reset_inside_transaction_commit() {
    let mut test_client = TestClient::new_sharded(Parameters::default()).await;

    // Set a parameter outside transaction
    test_client
        .send_simple(Query::new("SET application_name TO 'before_reset'"))
        .await;
    expect_message!(test_client.read().await, CommandComplete);
    expect_message!(test_client.read().await, ReadyForQuery);

    assert_eq!(
        test_client.client().params.get("application_name").unwrap(),
        &ParameterValue::String("before_reset".into()),
    );

    // Begin transaction
    test_client.send_simple(Query::new("BEGIN")).await;
    expect_message!(test_client.read().await, CommandComplete);
    assert_eq!(
        expect_message!(test_client.read().await, ReadyForQuery).status,
        'T'
    );

    // Reset inside transaction
    test_client
        .send_simple(Query::new("RESET application_name"))
        .await;
    assert_eq!(
        expect_message!(test_client.read().await, CommandComplete).command(),
        "RESET"
    );
    assert_eq!(
        expect_message!(test_client.read().await, ReadyForQuery).status,
        'T'
    );

    // Commit
    test_client.send_simple(Query::new("COMMIT")).await;
    expect_message!(test_client.read().await, CommandComplete);
    assert_eq!(
        expect_message!(test_client.read().await, ReadyForQuery).status,
        'I'
    );

    // Parameter should be reset after commit
    assert!(
        test_client
            .client()
            .params
            .get("application_name")
            .is_none(),
        "application_name should be reset after commit"
    );
}

#[tokio::test]
async fn test_reset_inside_transaction_rollback() {
    let mut test_client = TestClient::new_sharded(Parameters::default()).await;

    // Set a parameter outside transaction
    test_client
        .send_simple(Query::new("SET application_name TO 'before_reset'"))
        .await;
    expect_message!(test_client.read().await, CommandComplete);
    expect_message!(test_client.read().await, ReadyForQuery);

    assert_eq!(
        test_client.client().params.get("application_name").unwrap(),
        &ParameterValue::String("before_reset".into()),
    );

    // Begin transaction
    test_client.send_simple(Query::new("BEGIN")).await;
    expect_message!(test_client.read().await, CommandComplete);
    assert_eq!(
        expect_message!(test_client.read().await, ReadyForQuery).status,
        'T'
    );

    // Reset inside transaction
    test_client
        .send_simple(Query::new("RESET application_name"))
        .await;
    assert_eq!(
        expect_message!(test_client.read().await, CommandComplete).command(),
        "RESET"
    );
    assert_eq!(
        expect_message!(test_client.read().await, ReadyForQuery).status,
        'T'
    );

    // Rollback
    test_client.send_simple(Query::new("ROLLBACK")).await;
    expect_message!(test_client.read().await, CommandComplete);
    assert_eq!(
        expect_message!(test_client.read().await, ReadyForQuery).status,
        'I'
    );

    // Parameter should be restored after rollback
    assert_eq!(
        test_client.client().params.get("application_name").unwrap(),
        &ParameterValue::String("before_reset".into()),
        "application_name should be restored after rollback"
    );
}
