use super::prelude::*;
use crate::{
    expect_message,
    net::{BindComplete, DataRow, Format, ParseComplete, ReadyForQuery},
};

const TRY_LOCK_KEY: i64 = 8_871_234_567;

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
async fn test_failed_try_session_lock_does_not_pin_backend() {
    let mut holder = TestClient::new_replicas(Parameters::default())
        .await
        .leak_pool();
    let mut contender = TestClient::new(Parameters::default()).await;

    holder
        .send_simple(Query::new(format!(
            "SELECT pg_advisory_lock({TRY_LOCK_KEY})"
        )))
        .await;
    holder.read_until('Z').await.unwrap();

    contender
        .send_simple(Query::new(format!(
            "SELECT pg_try_advisory_lock({TRY_LOCK_KEY})"
        )))
        .await;
    let messages = contender.read_until('Z').await.unwrap();
    let row = messages
        .into_iter()
        .find(|message| message.code() == 'D')
        .and_then(|message| DataRow::try_from(message).ok())
        .expect("try-lock should return one row");

    assert_eq!(row.get::<bool>(0, Format::Text), Some(false));
    assert!(!contender.backend_locked());
    assert!(!contender.backend_connected());
    assert_eq!(contender.engine.advisory_locks().len(), 0);
}

#[tokio::test]
async fn test_successful_try_session_lock_pins_backend() {
    let mut client = TestClient::new_sharded(Parameters::default()).await;

    client
        .send_simple(Query::new(format!(
            "SELECT false, pg_try_advisory_lock({TRY_LOCK_KEY}) AS acquired"
        )))
        .await;
    let messages = client.read_until('Z').await.unwrap();
    let row = messages
        .into_iter()
        .find(|message| message.code() == 'D')
        .and_then(|message| DataRow::try_from(message).ok())
        .expect("try-lock should return one row");

    assert_eq!(row.get::<bool>(0, Format::Text), Some(false));
    assert_eq!(row.get::<bool>(1, Format::Text), Some(true));
    assert!(client.backend_connected());
    assert!(client.backend_locked());
}

#[tokio::test]
async fn test_try_session_locks_follow_rows_and_columns() {
    let mut holder = TestClient::new_replicas(Parameters::default())
        .await
        .leak_pool();
    let mut contender = TestClient::new(Parameters::default()).await;

    holder
        .send_simple(Query::new(format!(
            "SELECT pg_advisory_lock({TRY_LOCK_KEY})"
        )))
        .await;
    holder.read_until('Z').await.unwrap();

    contender
        .send_simple(Query::new(format!(
            "SELECT false, pg_try_advisory_lock(value) \
             FROM (VALUES ({TRY_LOCK_KEY}), ({})) AS locks(value)",
            TRY_LOCK_KEY + 1
        )))
        .await;
    contender.read_until('Z').await.unwrap();

    assert!(contender.engine.advisory_locks().contains(TRY_LOCK_KEY + 1));
    assert!(!contender.engine.advisory_locks().contains(TRY_LOCK_KEY));
    assert!(contender.backend_locked());

    contender
        .send_simple(Query::new(format!(
            "SELECT pg_advisory_unlock({}::bigint)",
            TRY_LOCK_KEY + 1
        )))
        .await;
    contender.read_until('Z').await.unwrap();

    assert!(!contender.backend_locked());
    assert!(!contender.backend_connected());
}

#[tokio::test]
async fn test_failed_try_session_lock_extended_binary_does_not_pin_backend() {
    let mut holder = TestClient::new_replicas(Parameters::default())
        .await
        .leak_pool();
    let mut contender = TestClient::new(Parameters::default()).await;

    holder
        .send_simple(Query::new(format!(
            "SELECT pg_advisory_lock({TRY_LOCK_KEY})"
        )))
        .await;
    holder.read_until('Z').await.unwrap();

    contender
        .send(Parse::named(
            "try_lock",
            "SELECT pg_try_advisory_lock($1::bigint)",
        ))
        .await;
    contender
        .send(Bind::new_params_codes_results(
            "try_lock",
            &[Parameter::new(&TRY_LOCK_KEY.to_be_bytes())],
            &[Format::Binary],
            &[1],
        ))
        .await;
    contender.send(Execute::new()).await;
    contender.send(Sync).await;
    contender.try_process().await.unwrap();

    expect_message!(contender.read().await, ParseComplete);
    expect_message!(contender.read().await, BindComplete);
    let row = expect_message!(contender.read().await, DataRow);
    assert_eq!(row.get::<bool>(0, Format::Binary), Some(false));
    expect_message!(contender.read().await, crate::net::CommandComplete);
    expect_message!(contender.read().await, ReadyForQuery);

    assert!(!contender.backend_locked());
    assert!(!contender.backend_connected());
}

#[tokio::test]
async fn test_failed_try_session_lock_keeps_binary_format_across_flush() {
    let mut holder = TestClient::new_replicas(Parameters::default())
        .await
        .leak_pool();
    let mut contender = TestClient::new(Parameters::default()).await;

    holder
        .send_simple(Query::new(format!(
            "SELECT pg_advisory_lock({TRY_LOCK_KEY})"
        )))
        .await;
    holder.read_until('Z').await.unwrap();

    contender
        .send(Parse::named(
            "try_lock_flush",
            "SELECT pg_try_advisory_lock($1::bigint)",
        ))
        .await;
    contender
        .send(Bind::new_params_codes_results(
            "try_lock_flush",
            &[Parameter::new(&TRY_LOCK_KEY.to_be_bytes())],
            &[Format::Binary],
            &[1],
        ))
        .await;
    contender.send(Flush).await;
    contender.try_process().await.unwrap();

    expect_message!(contender.read().await, ParseComplete);
    expect_message!(contender.read().await, BindComplete);

    contender.send(Execute::new()).await;
    contender.send(Sync).await;
    contender.try_process().await.unwrap();

    let row = expect_message!(contender.read().await, DataRow);
    assert_eq!(row.get::<bool>(0, Format::Binary), Some(false));
    expect_message!(contender.read().await, crate::net::CommandComplete);
    expect_message!(contender.read().await, ReadyForQuery);

    assert!(!contender.backend_locked());
    assert!(!contender.backend_connected());
}

#[tokio::test]
async fn test_failed_try_session_lock_in_transaction_unpins_at_commit() {
    let mut holder = TestClient::new_replicas(Parameters::default())
        .await
        .leak_pool();
    let mut contender = TestClient::new(Parameters::default()).await;

    holder
        .send_simple(Query::new(format!(
            "SELECT pg_advisory_lock({TRY_LOCK_KEY})"
        )))
        .await;
    holder.read_until('Z').await.unwrap();

    contender.send_simple(Query::new("BEGIN")).await;
    contender.read_until('Z').await.unwrap();
    contender
        .send_simple(Query::new(format!(
            "SELECT pg_try_advisory_lock({TRY_LOCK_KEY})"
        )))
        .await;
    contender.read_until('Z').await.unwrap();

    assert!(contender.backend_connected());

    contender.send_simple(Query::new("COMMIT")).await;
    contender.read_until('Z').await.unwrap();

    assert!(!contender.backend_locked());
    assert!(!contender.backend_connected());
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
async fn test_discard_all_clears_session_locks() {
    let mut client = TestClient::new_sharded(Parameters::default()).await;

    client
        .send_simple(Query::new("SELECT pg_advisory_lock(1)"))
        .await;
    client.read_until('Z').await.unwrap();

    assert!(client.engine.advisory_locks().contains(1));
    assert!(client.backend_locked());

    client.send_simple(Query::new("DISCARD ALL")).await;
    client.read_until('Z').await.unwrap();

    assert_eq!(client.engine.advisory_locks().len(), 0);
    assert!(
        !client.backend_locked(),
        "backend must be released after DISCARD ALL"
    );
}

#[tokio::test]
async fn test_non_all_discard_keeps_session_locks() {
    let mut client = TestClient::new_sharded(Parameters::default()).await;

    client
        .send_simple(Query::new("SELECT pg_advisory_lock(1)"))
        .await;
    client.read_until('Z').await.unwrap();

    for query in ["DISCARD PLANS", "DISCARD SEQUENCES", "DISCARD TEMP"] {
        client.send_simple(Query::new(query)).await;
        client.read_until('Z').await.unwrap();

        assert!(
            client.engine.advisory_locks().contains(1),
            "{query} must not release advisory locks",
        );
        assert!(client.backend_locked());
    }
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
