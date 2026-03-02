use super::prelude::*;

#[tokio::test]
async fn test_lock_session_advisory_lock() {
    let mut test_client = TestClient::new_sharded(Parameters::default()).await;

    // Regular query should not lock the backend
    test_client.send_simple(Query::new("SELECT 1")).await;
    test_client.read_until('Z').await.unwrap();

    assert!(!test_client.backend_locked());

    // Advisory lock should lock the backend
    test_client
        .send_simple(Query::new("SELECT pg_advisory_lock(12345)"))
        .await;
    test_client.read_until('Z').await.unwrap();

    assert!(test_client.backend_locked());
}

#[tokio::test]
async fn test_regular_query_not_locked() {
    let mut test_client = TestClient::new_sharded(Parameters::default()).await;

    test_client.send_simple(Query::new("SELECT 1")).await;
    test_client.read_until('Z').await.unwrap();

    assert!(!test_client.backend_locked());
}

#[tokio::test]
async fn test_transaction_not_locked() {
    let mut test_client = TestClient::new_sharded(Parameters::default()).await;

    test_client.send_simple(Query::new("BEGIN")).await;
    test_client.read_until('Z').await.unwrap();

    assert!(!test_client.backend_locked());

    test_client.send_simple(Query::new("SELECT 1")).await;
    test_client.read_until('Z').await.unwrap();

    assert!(!test_client.backend_locked());

    test_client.send_simple(Query::new("COMMIT")).await;
    test_client.read_until('Z').await.unwrap();

    assert!(!test_client.backend_locked());
}
