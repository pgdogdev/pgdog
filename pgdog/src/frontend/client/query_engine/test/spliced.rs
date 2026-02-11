use super::{test_client, test_sharded_client};
use crate::{
    expect_message,
    net::{BindComplete, CommandComplete, DataRow, Parameters, ParseComplete, ReadyForQuery},
};

use super::prelude::*;

#[tokio::test]
async fn test_intercept_incomplete_sync_only_not_connected() {
    for mut client in [test_client(), test_sharded_client()] {
        let mut engine = QueryEngine::from_client(&client).unwrap();

        // Backend should not be connected initially
        assert!(!engine.backend().connected());

        // Set up a Sync-only request
        client.client_request = vec![Sync.into()].into();
        let mut context = QueryEngineContext::new(&mut client);

        // When backend is NOT connected, intercept_incomplete should return true
        // (it handles the Sync locally without connecting to backend)
        let intercepted = engine.intercept_incomplete(&mut context).await.unwrap();
        assert!(
            intercepted,
            "intercept_incomplete should return true when backend not connected"
        );
    }
}

#[tokio::test]
async fn test_intercept_incomplete_sync_only_when_connected() {
    let mut test_client = TestClient::new_sharded(Parameters::default()).await;

    // First, establish a backend connection with Execute+Flush
    test_client.send(Parse::named("stmt", "SELECT 1")).await;
    test_client.send(Bind::new_statement("stmt")).await;
    test_client.send(Execute::new()).await;
    test_client.send(Flush).await;

    test_client.try_process().await.unwrap();

    // Backend should be connected
    assert!(test_client.backend_connected());

    // Drain responses
    expect_message!(test_client.read().await, ParseComplete);
    expect_message!(test_client.read().await, BindComplete);
    expect_message!(test_client.read().await, DataRow);
    expect_message!(test_client.read().await, CommandComplete);

    // Now manually test intercept_incomplete with Sync-only request
    // Access client and engine separately to avoid borrow conflict
    test_client.client.client_request = vec![Sync.into()].into();

    let mut context = QueryEngineContext::new(&mut test_client.client);

    // When backend IS connected, intercept_incomplete should return false
    // (Sync should be forwarded to the backend, not intercepted)
    let intercepted = test_client
        .engine
        .intercept_incomplete(&mut context)
        .await
        .unwrap();
    assert!(
        !intercepted,
        "intercept_incomplete should return false when backend is connected"
    );
}

#[tokio::test]
async fn test_sync_forwarded_when_backend_connected() {
    let mut client = TestClient::new_sharded(Parameters::default()).await;

    // First, establish a connection with Execute+Flush
    client.send(Parse::named("stmt", "SELECT 1")).await;
    client.send(Bind::new_statement("stmt")).await;
    client.send(Execute::new()).await;
    client.send(Flush).await;

    client.try_process().await.unwrap();

    // Backend should be connected
    assert!(client.backend_connected());

    // Read the responses from first request
    expect_message!(client.read().await, ParseComplete);
    expect_message!(client.read().await, BindComplete);
    let row = expect_message!(client.read().await, DataRow);
    assert_eq!(row.get_int(0, true), Some(1));
    expect_message!(client.read().await, CommandComplete);

    // Now send only Sync - this should be forwarded to backend (not intercepted)
    // because intercept_incomplete returns false when backend is connected
    client.send(Sync).await;

    client.try_process().await.unwrap();

    // Backend should be released after Sync
    assert!(!client.backend_connected());

    // We should receive ReadyForQuery from the actual backend
    let rfq = expect_message!(client.read().await, ReadyForQuery);
    // Transaction status should be idle ('I') since we're not in a transaction
    assert_eq!(rfq.status, 'I');
}

#[tokio::test]
async fn test_spliced_pipelined_executes() {
    let mut client = TestClient::new_sharded(Parameters::default()).await;

    // Before any query, backend should not be connected
    assert!(!client.backend_connected());

    // Send two pipelined Parse/Bind/Execute sequences without Sync.
    client.send(Parse::named("stmt1", "SELECT 1")).await;
    client.send(Bind::new_statement("stmt1")).await;
    client.send(Execute::new()).await;
    client.send(Parse::named("stmt2", "SELECT 2")).await;
    client.send(Bind::new_statement("stmt2")).await;
    client.send(Execute::new()).await;
    client.send(Flush).await;

    client.try_process().await.unwrap();

    // Backend stays connected after Execute+Flush (no Sync yet)
    assert!(client.backend_connected());

    // First statement responses
    expect_message!(client.read().await, ParseComplete);
    expect_message!(client.read().await, BindComplete);
    let row = expect_message!(client.read().await, DataRow);
    assert_eq!(row.get_int(0, true), Some(1));
    expect_message!(client.read().await, CommandComplete);

    // Second statement responses
    expect_message!(client.read().await, ParseComplete);
    expect_message!(client.read().await, BindComplete);
    let row = expect_message!(client.read().await, DataRow);
    assert_eq!(row.get_int(0, true), Some(2));
    expect_message!(client.read().await, CommandComplete);

    // Still connected before Sync
    assert!(client.backend_connected());

    // Now send Sync
    client.send(Sync).await;
    client.try_process().await.unwrap();

    // Released after Sync
    assert!(!client.backend_connected());

    // Sync response
    expect_message!(client.read().await, ReadyForQuery);
}

#[tokio::test]
async fn test_spliced_with_flush_mid_pipeline() {
    let mut client = TestClient::new_sharded(Parameters::default()).await;

    assert!(!client.backend_connected());

    // Parse with Flush, then Bind, Execute, Sync in separate request.
    // In transaction mode, connection is released after each complete request.
    client.send(Parse::named("stmt", "SELECT 1")).await;
    client.send(Flush).await;

    client.try_process().await.unwrap();

    // After Flush in transaction mode, backend is released (no pending splices)
    assert!(!client.backend_connected());

    expect_message!(client.read().await, ParseComplete);

    // Now send the rest - this gets a new connection
    client.send(Bind::new_statement("stmt")).await;
    client.send(Execute::new()).await;
    client.send(Sync).await;

    client.try_process().await.unwrap();

    // After Sync, backend is released
    assert!(!client.backend_connected());

    expect_message!(client.read().await, BindComplete);
    let row = expect_message!(client.read().await, DataRow);
    assert_eq!(row.get_int(0, true), Some(1));
    expect_message!(client.read().await, CommandComplete);
    expect_message!(client.read().await, ReadyForQuery);
}

#[tokio::test]
async fn test_spliced_single_execute_no_splice() {
    let mut client = TestClient::new_sharded(Parameters::default()).await;

    assert!(!client.backend_connected());

    // Single Execute should not be spliced (optimization)
    // Send everything except Sync first
    client.send(Parse::named("stmt", "SELECT 42")).await;
    client.send(Bind::new_statement("stmt")).await;
    client.send(Execute::new()).await;
    client.send(Flush).await;

    client.try_process().await.unwrap();

    // Backend connected after Execute+Flush (no Sync yet)
    assert!(client.backend_connected());

    expect_message!(client.read().await, ParseComplete);
    expect_message!(client.read().await, BindComplete);
    let row = expect_message!(client.read().await, DataRow);
    assert_eq!(row.get_int(0, true), Some(42));
    expect_message!(client.read().await, CommandComplete);

    // Still connected before Sync
    assert!(client.backend_connected());

    // Now send Sync
    client.send(Sync).await;
    client.try_process().await.unwrap();

    // Released after Sync
    assert!(!client.backend_connected());

    expect_message!(client.read().await, ReadyForQuery);
}

#[tokio::test]
async fn test_spliced_reuses_named_statement() {
    let mut client = TestClient::new_sharded(Parameters::default()).await;

    assert!(!client.backend_connected());

    // Parse once, Bind and Execute multiple times without Sync.
    // Second Bind references statement from first splice.
    client.send(Parse::named("reuse", "SELECT 100")).await;
    client.send(Bind::new_statement("reuse")).await;
    client.send(Execute::new()).await;
    client.send(Bind::new_statement("reuse")).await;
    client.send(Execute::new()).await;
    client.send(Flush).await;

    client.try_process().await.unwrap();

    // Backend stays connected (no Sync yet)
    assert!(client.backend_connected());

    // First execution
    expect_message!(client.read().await, ParseComplete);
    expect_message!(client.read().await, BindComplete);
    let row = expect_message!(client.read().await, DataRow);
    assert_eq!(row.get_int(0, true), Some(100));
    expect_message!(client.read().await, CommandComplete);

    // Second execution (reuses the named statement, same result)
    expect_message!(client.read().await, BindComplete);
    let row = expect_message!(client.read().await, DataRow);
    assert_eq!(row.get_int(0, true), Some(100));
    expect_message!(client.read().await, CommandComplete);

    // Still connected before Sync
    assert!(client.backend_connected());

    // Now send Sync
    client.send(Sync).await;
    client.try_process().await.unwrap();

    // Released after Sync
    assert!(!client.backend_connected());

    expect_message!(client.read().await, ReadyForQuery);
}
