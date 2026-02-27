use crate::backend::databases::{add_wildcard_pool, databases};
use crate::config::load_test_wildcard_with_limit;
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

/// When `max_wildcard_pools` is set, pools beyond the limit are rejected
/// (returning `Ok(None)`) without panicking or erroring.
#[tokio::test]
async fn test_max_wildcard_pools_limit_enforced() {
    load_test_wildcard_with_limit(2);

    // First two pools succeed.
    let r1 = add_wildcard_pool("user_a", "db_one", None);
    assert!(r1.is_ok());
    assert!(
        r1.unwrap().is_some(),
        "first pool within limit should be created"
    );

    let r2 = add_wildcard_pool("user_b", "db_two", None);
    assert!(r2.is_ok());
    assert!(
        r2.unwrap().is_some(),
        "second pool within limit should be created"
    );

    // Third pool must be rejected.
    let r3 = add_wildcard_pool("user_c", "db_three", None);
    assert!(r3.is_ok(), "should not error, just reject gracefully");
    assert!(
        r3.unwrap().is_none(),
        "pool creation beyond max_wildcard_pools must return None"
    );

    let dbs = databases();
    assert!(
        !dbs.exists(("user_c", "db_three")),
        "rejected pool must not be registered"
    );
}

/// `max_wildcard_pools = 0` means unlimited: any number of pools may be
/// created without triggering the limit.
#[tokio::test]
async fn test_max_wildcard_pools_zero_means_unlimited() {
    load_test_wildcard_with_limit(0);

    for i in 0..5usize {
        let db = format!("unlimited_db_{i}");
        let user = format!("unlimited_user_{i}");
        let result = add_wildcard_pool(&user, &db, None);
        assert!(result.is_ok());
        assert!(
            result.unwrap().is_some(),
            "pool {i} should be created when limit is 0"
        );
    }
}

/// After a config reload (simulated by calling `load_test_wildcard_with_limit`
/// again) the wildcard pool counter is reset to zero, so pools that were
/// previously rejected can now be created.
#[tokio::test]
async fn test_max_wildcard_pools_counter_resets_on_reload() {
    load_test_wildcard_with_limit(1);

    // Fill the single slot.
    let r1 = add_wildcard_pool("reload_user", "first_db", None);
    assert!(r1.unwrap().is_some(), "slot 1 should be filled");

    // Next pool is rejected.
    let r2 = add_wildcard_pool("reload_user", "second_db", None);
    assert!(r2.unwrap().is_none(), "should be rejected at limit");

    // Simulate SIGHUP / config reload — resets the counter and the pool map.
    load_test_wildcard_with_limit(1);

    // The previously rejected database can now be created again.
    let r3 = add_wildcard_pool("reload_user", "second_db", None);
    assert!(
        r3.unwrap().is_some(),
        "should succeed after reload resets the counter"
    );
}
