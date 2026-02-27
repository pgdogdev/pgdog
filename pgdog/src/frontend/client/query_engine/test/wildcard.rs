use crate::frontend::client::test::test_client::TestClient;
use crate::net::{Parameters, Query};

/// Wildcard database: connecting to an unmapped database name triggers
/// dynamic pool creation from the "*" template. The pool should forward
/// queries to the real Postgres database whose name matches the
/// client-requested name.
#[tokio::test]
async fn test_wildcard_database_simple_query() {
    let mut params = Parameters::default();
    params.insert("user", "pgdog");
    params.insert("database", "pgdog");

    let mut client = TestClient::new_wildcard(params).await;

    // The explicit pool for (pgdog, pgdog) already exists, so this goes
    // through the explicit path. Verify basic connectivity.
    client.send_simple(Query::new("SELECT 1 AS result")).await;
    let messages = client.read_until('Z').await.unwrap();
    assert!(
        messages.len() >= 3,
        "expected DataRow + CommandComplete + ReadyForQuery"
    );
}

/// When a wildcard template is configured, `exists_or_wildcard` should
/// return true for database names that don't have an explicit pool but
/// match the wildcard pattern.
#[tokio::test]
async fn test_wildcard_exists_or_wildcard() {
    use crate::backend::databases::databases;
    use crate::config::load_test_wildcard;

    load_test_wildcard();

    let dbs = databases();

    // Explicit pool exists:
    assert!(dbs.exists(("pgdog", "pgdog")));
    assert!(dbs.exists_or_wildcard(("pgdog", "pgdog")));

    // No explicit pool, but wildcard matches:
    assert!(!dbs.exists(("pgdog", "some_other_db")));
    assert!(dbs.exists_or_wildcard(("pgdog", "some_other_db")));

    // Fully unknown user + database — wildcard user+db template covers it:
    assert!(!dbs.exists(("unknown_user", "unknown_db")));
    assert!(dbs.exists_or_wildcard(("unknown_user", "unknown_db")));
}

/// Dynamic pool creation via `add_wildcard_pool` for a database that has
/// no explicit pool but matches the wildcard template.
#[tokio::test]
async fn test_wildcard_add_pool_dynamic() {
    use crate::backend::databases::{add_wildcard_pool, databases};
    use crate::config::load_test_wildcard;

    load_test_wildcard();

    let target_db = "pgdog"; // must exist in Postgres

    // Before: no explicit pool for ("pgdog", target_db) via wildcard user.
    // The explicit pool is under user "pgdog" / database "pgdog", so let's
    // test a wildcard user scenario.
    let dbs = databases();
    assert!(!dbs.exists(("wildcard_user", target_db)));
    drop(dbs);

    // Create pool dynamically.
    let result = add_wildcard_pool("wildcard_user", target_db, None);
    assert!(result.is_ok(), "add_wildcard_pool should succeed");
    let cluster = result.unwrap();
    assert!(cluster.is_some(), "wildcard match should produce a cluster");

    // After: pool exists.
    let dbs = databases();
    assert!(dbs.exists(("wildcard_user", target_db)));
}

/// Requesting a database that doesn't exist in Postgres should still
/// create a wildcard pool — the error only surfaces when a connection
/// attempt is actually made.
#[tokio::test]
async fn test_wildcard_nonexistent_pg_database() {
    use crate::backend::databases::{add_wildcard_pool, databases};
    use crate::config::load_test_wildcard;

    load_test_wildcard();

    let fake_db = "totally_fake_db_12345";

    let dbs = databases();
    assert!(!dbs.exists(("pgdog", fake_db)));
    assert!(dbs.exists_or_wildcard(("pgdog", fake_db)));
    drop(dbs);

    // Pool creation succeeds (it only creates the config, doesn't connect yet).
    let result = add_wildcard_pool("pgdog", fake_db, None);
    assert!(result.is_ok());
    assert!(result.unwrap().is_some());

    let dbs = databases();
    assert!(dbs.exists(("pgdog", fake_db)));
}
