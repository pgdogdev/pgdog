use super::prelude::*;

#[tokio::test]
async fn test_session_lock_tracked_outside_transaction() {
    let mut client = TestClient::new_sharded(Parameters::default()).await;

    client
        .send_simple(Query::new("SELECT pg_advisory_lock(101)"))
        .await;
    client.read_until('Z').await.unwrap();

    {
        let locks = client.engine.advisory_locks();
        assert!(locks.contains(101));
        assert_eq!(locks.len(), 1);
    }

    assert!(client.backend_connected());
    assert!(client.backend_locked());

    // A follow-up query must not release the pinned backend — otherwise the
    // session-scoped lock would be invisible on a different connection.
    client.send_simple(Query::new("SELECT 1")).await;
    client.read_until('Z').await.unwrap();

    assert!(client.backend_connected());
    assert!(client.backend_locked());
    assert!(client.engine.advisory_locks().contains(101));
}

#[tokio::test]
async fn test_session_lock_inside_transaction_survives_commit() {
    // A plain pg_advisory_lock taken inside a transaction lives past COMMIT
    // because it's session-scoped — we record it in `locks` right away.
    let mut client = TestClient::new_sharded(Parameters::default()).await;

    client.send_simple(Query::new("BEGIN")).await;
    client.read_until('Z').await.unwrap();

    client
        .send_simple(Query::new("SELECT pg_advisory_lock(202)"))
        .await;
    client.read_until('Z').await.unwrap();

    assert!(client.engine.advisory_locks().contains(202));
    assert!(client.backend_connected());
    assert!(client.backend_locked());

    client.send_simple(Query::new("COMMIT")).await;
    client.read_until('Z').await.unwrap();

    assert!(
        client.engine.advisory_locks().contains(202),
        "session-scoped lock must survive COMMIT"
    );
    assert!(client.backend_connected());
    assert!(
        client.backend_locked(),
        "backend must stay pinned while the session lock is held"
    );
}

#[tokio::test]
async fn test_session_lock_inside_transaction_survives_rollback() {
    // Session-scoped locks aren't unwound by ROLLBACK — only xact locks are.
    let mut client = TestClient::new_sharded(Parameters::default()).await;

    client.send_simple(Query::new("BEGIN")).await;
    client.read_until('Z').await.unwrap();

    client
        .send_simple(Query::new("SELECT pg_advisory_lock(303)"))
        .await;
    client.read_until('Z').await.unwrap();

    assert!(client.engine.advisory_locks().contains(303));
    assert!(client.backend_connected());
    assert!(client.backend_locked());

    client.send_simple(Query::new("ROLLBACK")).await;
    client.read_until('Z').await.unwrap();

    assert!(
        client.engine.advisory_locks().contains(303),
        "session-scoped lock must survive ROLLBACK"
    );
    assert!(client.backend_connected());
    assert!(client.backend_locked());
}

#[tokio::test]
async fn test_unlock_removes_session_lock() {
    let mut client = TestClient::new_sharded(Parameters::default()).await;

    client
        .send_simple(Query::new("SELECT pg_advisory_lock(404)"))
        .await;
    client.read_until('Z').await.unwrap();

    assert!(client.engine.advisory_locks().contains(404));
    assert!(client.backend_connected());
    assert!(client.backend_locked());

    client
        .send_simple(Query::new("SELECT pg_advisory_unlock(404)"))
        .await;
    client.read_until('Z').await.unwrap();

    let locks = client.engine.advisory_locks();
    assert!(!locks.contains(404));
    assert_eq!(locks.len(), 0);
    assert!(
        !client.backend_locked(),
        "backend must be released once the last session lock is dropped"
    );
}

#[tokio::test]
async fn test_unlock_all_clears_session_locks() {
    let mut client = TestClient::new_sharded(Parameters::default()).await;

    client
        .send_simple(Query::new("SELECT pg_advisory_lock(1)"))
        .await;
    client.read_until('Z').await.unwrap();

    client
        .send_simple(Query::new("SELECT pg_advisory_lock(2)"))
        .await;
    client.read_until('Z').await.unwrap();

    assert_eq!(client.engine.advisory_locks().len(), 2);
    assert!(client.backend_connected());
    assert!(client.backend_locked());

    client
        .send_simple(Query::new("SELECT pg_advisory_unlock_all()"))
        .await;
    client.read_until('Z').await.unwrap();

    let locks = client.engine.advisory_locks();
    assert_eq!(locks.len(), 0);
    assert!(
        !client.backend_locked(),
        "backend must be released after pg_advisory_unlock_all()"
    );
}

#[tokio::test]
async fn test_xact_lock_does_not_pin_backend_and_releases_on_commit() {
    // pg_advisory_xact_lock isn't tracked.
    let mut client = TestClient::new_sharded(Parameters::default()).await;

    client.send_simple(Query::new("BEGIN")).await;
    client.read_until('Z').await.unwrap();

    client
        .send_simple(Query::new("SELECT pg_advisory_xact_lock(999)"))
        .await;
    client.read_until('Z').await.unwrap();

    let locks = client.engine.advisory_locks();
    assert_eq!(locks.len(), 0);
    assert!(client.backend_connected());

    client.send_simple(Query::new("COMMIT")).await;
    client.read_until('Z').await.unwrap();

    let locks = client.engine.advisory_locks();
    assert_eq!(locks.len(), 0);
    assert!(
        !client.backend_locked(),
        "backend must be released after xact lock is dropped"
    );
}

#[tokio::test]
async fn test_xact_lock_released_on_rollback() {
    let mut client = TestClient::new_sharded(Parameters::default()).await;

    client.send_simple(Query::new("BEGIN")).await;
    client.read_until('Z').await.unwrap();

    client
        .send_simple(Query::new("SELECT pg_advisory_xact_lock(777)"))
        .await;
    client.read_until('Z').await.unwrap();

    assert_eq!(client.engine.advisory_locks().len(), 0);
    assert!(client.backend_connected());

    client.send_simple(Query::new("ROLLBACK")).await;
    client.read_until('Z').await.unwrap();

    let locks = client.engine.advisory_locks();
    assert_eq!(locks.len(), 0);
    assert!(!client.backend_locked());
}
