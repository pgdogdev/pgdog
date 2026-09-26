use crate::{
    backend::databases::reload_from_existing,
    config::{config, load_test, set},
    expect_message,
    net::{CommandComplete, DataRow, ReadyForQuery, RowDescription},
};

use super::prelude::*;
use crate::{frontend::router::parser::statement::AdvisoryLockId, net::DataRow};

#[tokio::test]
async fn test_pg_catalog_advisory_lock_pins_until_qualified_unlock() {
    let mut client = TestClient::new_sharded(Parameters::default()).await;
    client
        .send_simple(Query::new("SELECT pg_catalog.pg_advisory_lock(2026092101)"))
        .await;
    client.read_until('Z').await.expect("acquire advisory lock");
    assert!(client.backend_locked());

    client.send_simple(Query::new("SELECT 1")).await;
    client.read_until('Z').await.expect("follow-up query");
    assert!(client.backend_locked());

    client
        .send_simple(Query::new(
            "SELECT pg_catalog.pg_advisory_unlock(2026092101)",
        ))
        .await;
    client.read_until('Z').await.expect("release advisory lock");
    assert!(!client.backend_locked());
    assert_eq!(client.engine.advisory_locks().len(), 0);
}

fn load_single_connection_test_pool() {
    load_test();

    let mut config = (*config()).clone();
    config.config.general.default_pool_size = 1;
    config.config.general.min_pool_size = 0;
    set(config).unwrap();
    reload_from_existing().unwrap();
}

async fn identity(client: &mut TestClient) -> (i64, String, String) {
    client
        .send_simple(Query::new(
            "SELECT pg_backend_pid(), session_user, current_user",
        ))
        .await;
    expect_message!(client.read().await, RowDescription);
    let row = expect_message!(client.read().await, DataRow);
    expect_message!(client.read().await, CommandComplete);
    expect_message!(client.read().await, ReadyForQuery);

    (
        row.get_int(0, true).expect("backend pid"),
        row.get_text(1).expect("session_user"),
        row.get_text(2).expect("current_user"),
    )
}

#[tokio::test]
async fn test_session_lock_tracked_outside_transaction() {
    let mut client = TestClient::new_sharded(Parameters::default()).await;

    client
        .send_simple(Query::new("SELECT pg_advisory_lock(101)"))
        .await;
    client.read_until('Z').await.unwrap();

    {
        let locks = client.engine.advisory_locks();
        assert!(locks.contains(AdvisoryLockId::OneParameter(101)));
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
    assert!(
        client
            .engine
            .advisory_locks()
            .contains(AdvisoryLockId::OneParameter(101))
    );
}

#[tokio::test]
async fn test_session_lock_connects_to_all_shards() {
    let mut client = TestClient::new_sharded(Parameters::default()).await;

    client
        .send_simple(Query::new("SELECT pg_advisory_lock(707)"))
        .await;
    client.read_until('Z').await.unwrap();

    // If we use a pg_advisory_lock, we must be connected to all shards,
    // as we may need to use any particular shard based on how the ID is hashed.
    let connected_to_all_shards = client.engine.backend().connected_servers() == 2;
    assert!(connected_to_all_shards);
}

// We want a pg_advisory_lock(ID), and related functions, to deterministically resolve to the same Shard.
#[tokio::test]
async fn test_session_lock_resolves_to_same_shard() {
    let mut client = TestClient::new_sharded(Parameters::default()).await;

    // I use ::new instead of ::new_sharded to not create a new Config
    let mut client2 = TestClient::new(Parameters::default()).await;

    // These numbers are based on the hashing function (as it is of this commit)
    let lock_on_shard_0 = 606;
    let lock_on_shard_1 = 505;

    // Acquire the locks on the first client.
    for lock in [lock_on_shard_1, lock_on_shard_0] {
        client
            .send_simple(Query::new(format!("SELECT pg_advisory_lock({lock})")))
            .await;
        client.read_until('Z').await.unwrap();
    }

    // We should not be allowed to take the same locks we took on the
    // first client, as the first client still is holding them.
    for lock in [lock_on_shard_0, lock_on_shard_1] {
        client2
            .send_simple(Query::new(format!("SELECT pg_try_advisory_lock({lock})")))
            .await;
        let messages = client2.read_until('Z').await.unwrap();
        let row = messages
            .iter()
            .find(|m| m.code() == 'D')
            .map(|m| DataRow::try_from(m.clone()).unwrap())
            .unwrap();

        // returns false! not allowed to get the lock
        let returns_false = row.get_text(0).as_deref() == Some("f");
        assert!(returns_false);
    }
}

#[tokio::test]
async fn test_xact_lock_outside_transaction_releases_backend() {
    let mut client = TestClient::new_sharded(Parameters::default()).await;

    client
        .send_simple(Query::new("SELECT pg_advisory_xact_lock(808)"))
        .await;
    client.read_until('Z').await.unwrap();

    let backend_released = !client.backend_locked() && !client.backend_connected();
    assert!(backend_released);
}

/// Test a case where:
/// - We have a SESSION LEVEL lock held on shard 1 (so our connection is pinned)
/// - We start a transaction and obtain a TRANSACTION LEVEL lock that hashes to shard 0
/// - We try obtaining that same lock on a separate connection
/// It should resolve to shard 0 and fail (initially),
/// and then succeed after we COMMIT that transaction.
#[tokio::test]
async fn test_xact_lock_resolves_to_same_shard_while_session_lock_held() {
    let mut client = TestClient::new_sharded(Parameters::default()).await;

    // This re-uses the `client` config (see `new`)
    let mut client2 = TestClient::new(Parameters::default()).await;

    // These numbers are based on the hashing function (as it is of this commit)
    let lock_on_shard_0 = 606;
    let lock_on_shard_1 = 505;

    client
        .send_simple(Query::new(format!(
            "SELECT pg_advisory_lock({lock_on_shard_1})"
        )))
        .await;
    client.read_until('Z').await.unwrap();

    client.send_simple(Query::new("BEGIN")).await;
    client.read_until('Z').await.unwrap();

    client
        .send_simple(Query::new(format!(
            "SELECT pg_advisory_xact_lock({lock_on_shard_0})"
        )))
        .await;
    client.read_until('Z').await.unwrap();

    // Try, and fail, to acquire the lock held by the transaction.
    client2
        .send_simple(Query::new(format!(
            "SELECT pg_try_advisory_xact_lock({lock_on_shard_0})"
        )))
        .await;
    let messages = client2.read_until('Z').await.unwrap();
    let row = messages
        .iter()
        .find(|m| m.code() == 'D')
        .map(|m| DataRow::try_from(m.clone()).unwrap())
        .unwrap();
    let returns_false = row.get_text(0).as_deref() == Some("f");
    assert!(returns_false);

    // COMMIT will release the lock! Lets try again.
    client.send_simple(Query::new("COMMIT")).await;
    client.read_until('Z').await.unwrap();

    client2
        .send_simple(Query::new(format!(
            "SELECT pg_try_advisory_xact_lock({lock_on_shard_0})"
        )))
        .await;
    let messages = client2.read_until('Z').await.unwrap();
    let row = messages
        .iter()
        .find(|m| m.code() == 'D')
        .map(|m| DataRow::try_from(m.clone()).unwrap())
        .unwrap();

    // Bingo!
    let returns_true = row.get_text(0).as_deref() == Some("t");
    assert!(returns_true);
}

#[tokio::test]
async fn test_session_lock_cleanup_resets_role_before_backend_reuse() {
    load_single_connection_test_pool();
    let mut source = TestClient::new(Parameters::default()).await;
    let role = format!("pgdog_cleanup_role_{}_advisory", std::process::id());

    source
        .send_simple(Query::new(format!("CREATE ROLE {role}")))
        .await;
    source.read_until('Z').await.unwrap();
    source
        .send_simple(Query::new("SELECT pg_advisory_lock(707519)"))
        .await;
    source.read_until('Z').await.unwrap();
    source
        .send_simple(Query::new(format!("SET ROLE {role}")))
        .await;
    source.read_until('Z').await.unwrap();

    let before = identity(&mut source).await;
    assert_eq!(
        (before.1.as_str(), before.2.as_str()),
        ("pgdog", role.as_str())
    );
    drop(source.leak_pool());

    let mut peer = TestClient::new(Parameters::default()).await;
    let after = identity(&mut peer).await;
    assert_eq!(after.0, before.0, "physical backend should be reused");
    assert_eq!((after.1.as_str(), after.2.as_str()), ("pgdog", "pgdog"));

    peer.send_simple(Query::new(format!("DROP ROLE {role}")))
        .await;
    peer.read_until('Z').await.unwrap();
}

#[tokio::test]
async fn test_deferred_role_cleanup_before_backend_reuse() {
    load_single_connection_test_pool();
    let mut source = TestClient::new(Parameters::default()).await;
    let role = format!("pgdog_cleanup_role_{}_deferred", std::process::id());

    source
        .send_simple(Query::new(format!("CREATE ROLE {role}")))
        .await;
    source.read_until('Z').await.unwrap();
    source
        .send_simple(Query::new(format!("SET ROLE {role}")))
        .await;
    source.read_until('Z').await.unwrap();
    assert!(
        !source.backend_connected(),
        "SET ROLE should be deferred until a query checks out a backend"
    );

    let before = identity(&mut source).await;
    assert_eq!(
        (before.1.as_str(), before.2.as_str()),
        ("pgdog", role.as_str())
    );
    assert!(!source.backend_connected());
    drop(source.leak_pool());

    let mut peer = TestClient::new(Parameters::default()).await;
    let after = identity(&mut peer).await;
    assert_eq!(after.0, before.0, "physical backend should be reused");
    assert_eq!((after.1.as_str(), after.2.as_str()), ("pgdog", "pgdog"));

    peer.send_simple(Query::new(format!("DROP ROLE {role}")))
        .await;
    peer.read_until('Z').await.unwrap();
}

#[tokio::test]
async fn test_deferred_transaction_role_cleanup_before_backend_reuse() {
    load_single_connection_test_pool();
    let mut source = TestClient::new(Parameters::default()).await;
    let role = format!("pgdog_cleanup_role_{}_transaction", std::process::id());

    source
        .send_simple(Query::new(format!("CREATE ROLE {role}")))
        .await;
    source.read_until('Z').await.unwrap();
    source.send_simple(Query::new("BEGIN")).await;
    source.read_until('Z').await.unwrap();
    source
        .send_simple(Query::new(format!("SET ROLE {role}")))
        .await;
    source.read_until('Z').await.unwrap();
    assert!(
        !source.backend_connected(),
        "BEGIN and SET ROLE should remain deferred until the first query"
    );

    let before = identity(&mut source).await;
    assert_eq!(
        (before.1.as_str(), before.2.as_str()),
        ("pgdog", role.as_str())
    );
    assert!(source.backend_connected());

    source.send_simple(Query::new("COMMIT")).await;
    source.read_until('Z').await.unwrap();
    assert!(!source.backend_connected());
    drop(source.leak_pool());

    let mut peer = TestClient::new(Parameters::default()).await;
    let after = identity(&mut peer).await;
    assert_eq!(after.0, before.0, "physical backend should be reused");
    assert_eq!((after.1.as_str(), after.2.as_str()), ("pgdog", "pgdog"));

    peer.send_simple(Query::new(format!("DROP ROLE {role}")))
        .await;
    peer.read_until('Z').await.unwrap();
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

    assert!(
        client
            .engine
            .advisory_locks()
            .contains(AdvisoryLockId::OneParameter(202))
    );
    assert!(client.backend_connected());
    assert!(client.backend_locked());

    client.send_simple(Query::new("COMMIT")).await;
    client.read_until('Z').await.unwrap();

    assert!(
        client
            .engine
            .advisory_locks()
            .contains(AdvisoryLockId::OneParameter(202)),
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

    assert!(
        client
            .engine
            .advisory_locks()
            .contains(AdvisoryLockId::OneParameter(303))
    );
    assert!(client.backend_connected());
    assert!(client.backend_locked());

    client.send_simple(Query::new("ROLLBACK")).await;
    client.read_until('Z').await.unwrap();

    assert!(
        client
            .engine
            .advisory_locks()
            .contains(AdvisoryLockId::OneParameter(303)),
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

    assert!(
        client
            .engine
            .advisory_locks()
            .contains(AdvisoryLockId::OneParameter(404))
    );
    assert!(client.backend_connected());
    assert!(client.backend_locked());

    client
        .send_simple(Query::new("SELECT pg_advisory_unlock(404)"))
        .await;
    client.read_until('Z').await.unwrap();

    let locks = client.engine.advisory_locks();
    assert!(!locks.contains(AdvisoryLockId::OneParameter(404)));
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

    assert!(
        client
            .engine
            .advisory_locks()
            .contains(AdvisoryLockId::OneParameter(1))
    );
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
            client
                .engine
                .advisory_locks()
                .contains(AdvisoryLockId::OneParameter(1)),
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
