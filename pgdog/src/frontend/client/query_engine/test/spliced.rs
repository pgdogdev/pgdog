use super::{test_client, test_sharded_client};
use crate::{
    expect_message,
    net::{
        BindComplete, CommandComplete, DataRow, Describe, ErrorResponse, Parameters, ParseComplete,
        ReadyForQuery,
    },
};

use super::prelude::*;

#[tokio::test]
async fn test_intercept_incomplete_sync_only_not_connected() {
    for mut client in [test_client(), test_sharded_client()] {
        let mut engine = QueryEngine::from_client(&client).unwrap();

        assert!(!engine.backend().connected());

        client.client_request = vec![Sync.into()].into();
        let mut context = QueryEngineContext::new(&mut client);

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

    test_client.send(Parse::named("stmt", "SELECT 1")).await;
    test_client.send(Bind::new_statement("stmt")).await;
    test_client.send(Execute::new()).await;
    test_client.send(Flush).await;

    test_client.try_process().await.unwrap();
    assert!(test_client.backend_connected());

    expect_message!(test_client.read().await, ParseComplete);
    expect_message!(test_client.read().await, BindComplete);
    expect_message!(test_client.read().await, DataRow);
    expect_message!(test_client.read().await, CommandComplete);

    test_client.client.client_request = vec![Sync.into()].into();

    let mut context = QueryEngineContext::new(&mut test_client.client);

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

    client.send(Parse::named("stmt", "SELECT 1")).await;
    client.send(Bind::new_statement("stmt")).await;
    client.send(Execute::new()).await;
    client.send(Flush).await;

    client.try_process().await.unwrap();
    assert!(client.backend_connected());

    expect_message!(client.read().await, ParseComplete);
    expect_message!(client.read().await, BindComplete);
    let row = expect_message!(client.read().await, DataRow);
    assert_eq!(row.get_int(0, true), Some(1));
    expect_message!(client.read().await, CommandComplete);

    client.send(Sync).await;
    client.try_process().await.unwrap();

    assert!(!client.backend_connected());

    let rfq = expect_message!(client.read().await, ReadyForQuery);
    assert_eq!(rfq.status, 'I');
}

#[tokio::test]
async fn test_spliced_pipelined_executes() {
    let mut client = TestClient::new_sharded(Parameters::default()).await;

    assert!(!client.backend_connected());

    client.send(Parse::named("stmt1", "SELECT 1")).await;
    client.send(Bind::new_statement("stmt1")).await;
    client.send(Execute::new()).await;
    client.send(Parse::named("stmt2", "SELECT 2")).await;
    client.send(Bind::new_statement("stmt2")).await;
    client.send(Execute::new()).await;
    client.send(Flush).await;

    client.try_process().await.unwrap();
    assert!(client.backend_connected());

    expect_message!(client.read().await, ParseComplete);
    expect_message!(client.read().await, BindComplete);
    let row = expect_message!(client.read().await, DataRow);
    assert_eq!(row.get_int(0, true), Some(1));
    expect_message!(client.read().await, CommandComplete);

    expect_message!(client.read().await, ParseComplete);
    expect_message!(client.read().await, BindComplete);
    let row = expect_message!(client.read().await, DataRow);
    assert_eq!(row.get_int(0, true), Some(2));
    expect_message!(client.read().await, CommandComplete);

    assert!(client.backend_connected());

    client.send(Sync).await;
    client.try_process().await.unwrap();

    assert!(!client.backend_connected());
    expect_message!(client.read().await, ReadyForQuery);
}

#[tokio::test]
async fn test_spliced_with_flush_mid_pipeline() {
    let mut client = TestClient::new_sharded(Parameters::default()).await;

    assert!(!client.backend_connected());

    client.send(Parse::named("stmt", "SELECT 1")).await;
    client.send(Flush).await;

    client.try_process().await.unwrap();
    assert!(!client.backend_connected());

    expect_message!(client.read().await, ParseComplete);

    client.send(Bind::new_statement("stmt")).await;
    client.send(Execute::new()).await;
    client.send(Sync).await;

    client.try_process().await.unwrap();
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

    client.send(Parse::named("stmt", "SELECT 42")).await;
    client.send(Bind::new_statement("stmt")).await;
    client.send(Execute::new()).await;
    client.send(Flush).await;

    client.try_process().await.unwrap();
    assert!(client.backend_connected());

    expect_message!(client.read().await, ParseComplete);
    expect_message!(client.read().await, BindComplete);
    let row = expect_message!(client.read().await, DataRow);
    assert_eq!(row.get_int(0, true), Some(42));
    expect_message!(client.read().await, CommandComplete);

    assert!(client.backend_connected());

    client.send(Sync).await;
    client.try_process().await.unwrap();

    assert!(!client.backend_connected());
    expect_message!(client.read().await, ReadyForQuery);
}

#[tokio::test]
async fn test_spliced_reuses_named_statement() {
    let mut client = TestClient::new_sharded(Parameters::default()).await;

    assert!(!client.backend_connected());

    client.send(Parse::named("reuse", "SELECT 100")).await;
    client.send(Bind::new_statement("reuse")).await;
    client.send(Execute::new()).await;
    client.send(Bind::new_statement("reuse")).await;
    client.send(Execute::new()).await;
    client.send(Flush).await;

    client.try_process().await.unwrap();
    assert!(client.backend_connected());

    expect_message!(client.read().await, ParseComplete);
    expect_message!(client.read().await, BindComplete);
    let row = expect_message!(client.read().await, DataRow);
    assert_eq!(row.get_int(0, true), Some(100));
    expect_message!(client.read().await, CommandComplete);

    expect_message!(client.read().await, BindComplete);
    let row = expect_message!(client.read().await, DataRow);
    assert_eq!(row.get_int(0, true), Some(100));
    expect_message!(client.read().await, CommandComplete);

    assert!(client.backend_connected());

    client.send(Sync).await;
    client.try_process().await.unwrap();

    assert!(!client.backend_connected());
    expect_message!(client.read().await, ReadyForQuery);
}

/// Test JDBC transaction pattern: BEGIN + SELECT with Describe pipelined.
/// The request is spliced into 3 parts: BEGIN, SELECT, Sync.
#[tokio::test]
async fn test_jdbc_transaction_pattern_sharded() {
    let mut client = TestClient::new_sharded(Parameters::default()).await;

    assert!(!client.backend_connected());

    client.send(Parse::named("", "BEGIN")).await;
    client.send(Bind::new_statement("")).await;
    client.send(Execute::new()).await;
    client
        .send(Parse::named("", "SELECT COUNT(*) as count FROM sharded"))
        .await;
    client.send(Bind::new_statement("")).await;
    client.send(Describe::new_portal("")).await;
    client.send(Execute::new()).await;
    client.send(Sync).await;

    client.try_process().await.unwrap();

    let messages = client.read_until('Z').await.unwrap();

    // Expected: ParseComplete, BindComplete, CommandComplete (BEGIN),
    //           ParseComplete, BindComplete, RowDescription, DataRow, CommandComplete (SELECT),
    //           ReadyForQuery
    assert_eq!(messages.len(), 9, "Expected 9 messages");
    assert_eq!(messages[0].code(), '1'); // ParseComplete
    assert_eq!(messages[1].code(), '2'); // BindComplete
    assert_eq!(messages[2].code(), 'C'); // CommandComplete (BEGIN)
    assert_eq!(messages[3].code(), '1'); // ParseComplete
    assert_eq!(messages[4].code(), '2'); // BindComplete
    assert_eq!(messages[5].code(), 'T'); // RowDescription
    assert_eq!(messages[6].code(), 'D'); // DataRow
    assert_eq!(messages[7].code(), 'C'); // CommandComplete (SELECT)
    assert_eq!(messages[8].code(), 'Z'); // ReadyForQuery
}

/// Test JDBC transaction with INSERT followed by SELECT on sharded database.
/// This reproduces the bug where multi-shard state got confused after
/// spliced BEGIN + INSERT going to different shard counts.
#[tokio::test]
async fn test_jdbc_transaction_insert_then_select_sharded() {
    let mut client = TestClient::new_sharded(Parameters::default()).await;

    client.send(Query::new("TRUNCATE TABLE sharded")).await;
    client.try_process().await.unwrap();
    client.read_until('Z').await.unwrap();

    // BEGIN + SELECT pipelined (goes to all shards)
    client.send(Parse::named("", "BEGIN")).await;
    client.send(Bind::new_statement("")).await;
    client.send(Execute::new()).await;
    client
        .send(Parse::named("", "SELECT COUNT(*) as count FROM sharded"))
        .await;
    client.send(Bind::new_statement("")).await;
    client.send(Describe::new_portal("")).await;
    client.send(Execute::new()).await;
    client.send(Sync).await;
    client.try_process().await.unwrap();

    let messages = client.read_until('Z').await.unwrap();
    assert_eq!(messages.len(), 9, "Expected 9 messages for BEGIN+SELECT");

    // ROLLBACK
    client.send(Query::new("ROLLBACK")).await;
    client.try_process().await.unwrap();
    client.read_until('Z').await.unwrap();

    // BEGIN + INSERT pipelined (BEGIN goes to all shards, INSERT goes to one)
    client.send(Parse::named("", "BEGIN")).await;
    client.send(Bind::new_statement("")).await;
    client.send(Execute::new()).await;
    client
        .send(Parse::named(
            "",
            "INSERT INTO sharded (id, value) VALUES (1, 'test1')",
        ))
        .await;
    client.send(Bind::new_statement("")).await;
    client.send(Describe::new_portal("")).await;
    client.send(Execute::new()).await;
    client.send(Sync).await;
    client.try_process().await.unwrap();
    client.read_until('Z').await.unwrap();

    // Second INSERT (goes to potentially different shard)
    client
        .send(Parse::named(
            "",
            "INSERT INTO sharded (id, value) VALUES (2, 'test2')",
        ))
        .await;
    client.send(Bind::new_statement("")).await;
    client.send(Describe::new_portal("")).await;
    client.send(Execute::new()).await;
    client.send(Sync).await;
    client.try_process().await.unwrap();
    client.read_until('Z').await.unwrap();

    // SELECT COUNT(*) (goes to all shards)
    client
        .send(Parse::named("", "SELECT COUNT(*) as count FROM sharded"))
        .await;
    client.send(Bind::new_statement("")).await;
    client.send(Describe::new_portal("")).await;
    client.send(Execute::new()).await;
    client.send(Sync).await;
    client.try_process().await.unwrap();

    let messages = client.read_until('Z').await.unwrap();

    // Expected: ParseComplete, BindComplete, RowDescription, DataRow, CommandComplete, ReadyForQuery
    assert_eq!(messages.len(), 6, "Expected 6 messages for SELECT");
    assert_eq!(messages[0].code(), '1'); // ParseComplete
    assert_eq!(messages[1].code(), '2'); // BindComplete
    assert_eq!(messages[2].code(), 'T'); // RowDescription
    assert_eq!(messages[3].code(), 'D'); // DataRow
    assert_eq!(messages[4].code(), 'C'); // CommandComplete
    assert_eq!(messages[5].code(), 'Z'); // ReadyForQuery

    client.send(Query::new("ROLLBACK")).await;
    client.try_process().await.unwrap();
    client.read_until('Z').await.unwrap();
}

/// Test simple Query protocol on sharded database in a transaction.
#[tokio::test]
async fn test_simple_query_protocol_sharded_transaction() {
    let mut client = TestClient::new_sharded(Parameters::default()).await;

    client.send(Query::new("TRUNCATE TABLE sharded")).await;
    client.try_process().await.unwrap();
    client.read_until('Z').await.unwrap();

    client.send(Query::new("BEGIN")).await;
    client.try_process().await.unwrap();
    client.read_until('Z').await.unwrap();

    client
        .send(Query::new("SELECT COUNT(*) as count FROM sharded"))
        .await;
    client.try_process().await.unwrap();
    client.read_until('Z').await.unwrap();

    client.send(Query::new("ROLLBACK")).await;
    client.try_process().await.unwrap();
    client.read_until('Z').await.unwrap();

    client
        .send(Query::new(
            "INSERT INTO sharded (id, value) VALUES (1, 'test1')",
        ))
        .await;
    client.try_process().await.unwrap();
    client.read_until('Z').await.unwrap();

    client
        .send(Query::new(
            "INSERT INTO sharded (id, value) VALUES (2, 'test2')",
        ))
        .await;
    client.try_process().await.unwrap();
    client.read_until('Z').await.unwrap();

    client
        .send(Query::new("SELECT COUNT(*) as count FROM sharded"))
        .await;
    client.try_process().await.unwrap();

    let messages = client.read_until('Z').await.unwrap();

    // Simple query returns: RowDescription, DataRow, CommandComplete, ReadyForQuery
    assert!(messages.len() >= 3, "Expected at least 3 messages");
    assert!(
        messages.iter().any(|m| m.code() == 'T'),
        "Should have RowDescription"
    );
    assert!(
        messages.iter().any(|m| m.code() == 'D'),
        "Should have DataRow"
    );
    assert!(
        messages.iter().any(|m| m.code() == 'C'),
        "Should have CommandComplete"
    );
    assert_eq!(
        messages.last().unwrap().code(),
        'Z',
        "Last message should be ReadyForQuery"
    );

    client.send(Query::new("ROLLBACK")).await;
    client.try_process().await.unwrap();
    client.read_until('Z').await.unwrap();
}

/// Test that pipeline errors skip remaining queries and jump to Sync.
/// When Postgres receives an error in pipeline mode, it enters "aborted" state
/// and ignores all commands until Sync. pgdog must detect this and skip
/// remaining spliced requests to avoid timeout.
#[tokio::test]
async fn test_spliced_pipeline_error_skips_to_sync() {
    let mut client = TestClient::new_sharded(Parameters::default()).await;

    assert!(!client.backend_connected());

    // Send 3 queries in pipeline mode, first one will error (non-existent table)
    client
        .send(Parse::named("", "SELECT * FROM nonexistent_table_12345"))
        .await;
    client.send(Bind::new_statement("")).await;
    client.send(Execute::new()).await;

    client.send(Parse::named("", "SELECT 2")).await;
    client.send(Bind::new_statement("")).await;
    client.send(Execute::new()).await;

    client.send(Parse::named("", "SELECT 3")).await;
    client.send(Bind::new_statement("")).await;
    client.send(Execute::new()).await;

    client.send(Sync).await;

    // Process should complete without timeout
    client.try_process().await.unwrap();

    // Read messages manually since read_until returns Err on ErrorResponse
    let mut messages = vec![];
    loop {
        let message = client.read().await;
        let code = message.code();
        messages.push(message);
        if code == 'Z' {
            break;
        }
    }

    // Must have at least ErrorResponse and ReadyForQuery
    assert!(messages.len() >= 2, "Expected at least 2 messages");

    // First message should be ErrorResponse (42P01 = undefined table)
    let error =
        ErrorResponse::try_from(messages[0].clone()).expect("first message should be error");
    assert_eq!(error.code, "42P01", "Expected undefined table error");

    // Last message should be ReadyForQuery
    let rfq = expect_message!(messages.last().unwrap().clone(), ReadyForQuery);
    assert_eq!(rfq.status, 'I', "Should be idle after pipeline error");

    // Connection should be released back to pool
    assert!(!client.backend_connected());
}
