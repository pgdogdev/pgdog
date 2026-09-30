use std::process;

use integration_tests_rust::setup::{admin_sqlx, connections_sqlx};
use sqlx::{Connection, Executor, PgConnection, Pool, Postgres, Row, postgres::PgConnectOptions};

// Same test as `advisory_locks_working_generally` but with an inner hashtext() & hashtextextended func.
// We previously weren't parsing out and resolving hash funcs that can be used inside advisory locks.
// In fact, these previously weren't tracked at all (id resolved to None in `AdvisoryLock`)
// This just tests that it works generally speaking. I have a unit test also that tests what shards they resolve to
#[tokio::test]
pub async fn advisory_locks_with_functions() {
    let sharded_conn = connections_sqlx().await;
    let sharded_conn = sharded_conn.get(1).unwrap();

    let sharded_conn_2 = connections_sqlx().await;
    let sharded_conn_2 = sharded_conn_2.get(1).unwrap();
    let funcs = [
        "hashtext('super_cool_resource')",
        "hashtextextended('lock++', 123)",
    ];

    for func_to_try in funcs {
        sqlx::raw_sql(format!("SELECT pg_advisory_lock({func_to_try})").as_str())
            .execute(sharded_conn)
            .await
            .unwrap();

        let lock_acquired: bool =
            sqlx::query_scalar(format!("SELECT pg_try_advisory_lock({func_to_try})").as_str())
                .fetch_one(sharded_conn_2)
                .await
                .unwrap();
        assert!(!lock_acquired);
    }
}

// Same test as `advisory_locks_working_generally`, but now testing 2 inner params
#[tokio::test]
pub async fn advisory_locks_with_2_params() {
    let sharded_conn = connections_sqlx().await;
    let sharded_conn = sharded_conn.get(1).unwrap();

    let sharded_conn_2 = connections_sqlx().await;
    let sharded_conn_2 = sharded_conn_2.get(1).unwrap();
    let funcs = ["1, 2", "2, 1"];

    for func_to_try in funcs {
        sqlx::raw_sql(format!("SELECT pg_advisory_lock({func_to_try})").as_str())
            .execute(sharded_conn)
            .await
            .unwrap();

        let lock_acquired: bool =
            sqlx::query_scalar(format!("SELECT pg_try_advisory_lock({func_to_try})").as_str())
                .fetch_one(sharded_conn_2)
                .await
                .unwrap();
        assert!(!lock_acquired);
    }
}
// Test a general case where:
// - We have 2 shards.
// - We obtain a lock on one connection
// - We try obtaining that same lock on another connection
// It should resolve to the same shard and fail.
#[tokio::test]
pub async fn advisory_locks_working_generally() {
    // Connect to PgDog.
    let sharded_conn = connections_sqlx().await;
    let sharded_conn = sharded_conn.get(1).unwrap();

    // Obtain lock.
    sqlx::raw_sql("SELECT pg_advisory_lock(1)")
        .execute(sharded_conn)
        .await
        .unwrap();

    let sharded_conn_2 = connections_sqlx().await;
    let sharded_conn_2 = sharded_conn_2.get(1).unwrap();

    // Try obtaining lock using the method that returns instantly
    let lock_acquired: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock(1)")
        .fetch_one(sharded_conn_2)
        .await
        .unwrap();

    // This config has two shards; this would've failed before.
    // Now it's routed to same shard.
    assert!(!lock_acquired);
}

// Test a case where:
// - We have SESSION LEVEL advisory lock within a transaction
// - We try obtaining that lock on a separate connection.
#[tokio::test]
pub async fn advisory_locks_with_transactions() {
    let sharded_conn = connections_sqlx().await;
    let sharded_conn = sharded_conn.get(1).unwrap();

    // Obtain session lock.
    let mut transaction = sharded_conn.begin().await.unwrap();
    sqlx::raw_sql("SELECT pg_advisory_lock(1)")
        .execute(&mut *transaction)
        .await
        .unwrap();

    let sharded_conn_2 = connections_sqlx().await;
    let sharded_conn_2 = sharded_conn_2.get(1).unwrap();
    let mut transaction_2 = sharded_conn_2.begin().await.unwrap();
    sqlx::raw_sql("SELECT pg_advisory_lock(2)")
        .execute(&mut *transaction_2)
        .await
        .unwrap();

    // Now we'll try to acquire on a separate connection for both
    // (1) explicit transaction and (2) "implicit" (where we haven't dropped the conn)
    // It should fail on both of these
    let sharded_conn_3 = connections_sqlx().await;
    let sharded_conn_3 = sharded_conn_3.get(1).unwrap();

    for lock in [1, 2] {
        let lock_acquired: bool =
            sqlx::query_scalar(format!("SELECT pg_try_advisory_lock({lock})").as_str())
                .fetch_one(sharded_conn_3)
                .await
                .unwrap();
        assert!(!lock_acquired);
    }
}

// Test a case where we acquire 2 locks on different shards, then try unlock all.
#[tokio::test]
pub async fn advisory_locks_unlock_all() {
    let sharded_conn = connections_sqlx().await;
    let sharded_conn = sharded_conn.get(1).unwrap();
    for lock in [1, 2] {
        sqlx::raw_sql(format!("SELECT pg_advisory_lock({lock})").as_str())
            .execute(sharded_conn)
            .await
            .unwrap();
    }

    let unlock_all_rows = sqlx::raw_sql("SELECT pg_advisory_unlock_all()")
        .fetch_all(sharded_conn)
        .await
        .unwrap();
    assert_eq!(unlock_all_rows.len(), 2);

    let sharded_conn_2 = connections_sqlx().await;
    let sharded_conn_2 = sharded_conn_2.get(1).unwrap();

    for lock in [1, 2] {
        let lock_acquired: bool =
            sqlx::query_scalar(format!("SELECT pg_try_advisory_lock({lock})").as_str())
                .fetch_one(sharded_conn_2)
                .await
                .unwrap();
        assert!(lock_acquired);
    }
}

// Create two locks on diff shards.
// Query the catalog table.
// Verify they're present on both shards.
// TODO: I think this can vary depending on what setting is used for catalog
#[tokio::test]
pub async fn advisory_locks_catalog_table() {
    let fetch_all_active_advisory_locks_query =
        "SELECT objid FROM pg_catalog.pg_locks WHERE locktype = 'advisory'";

    let sharded_conn = connections_sqlx().await;
    let sharded_conn = sharded_conn.get(1).unwrap();
    for lock in [1, 2] {
        sqlx::raw_sql(format!("SELECT pg_advisory_lock({lock})").as_str())
            .execute(sharded_conn)
            .await
            .unwrap();
    }

    let advisory_lock_rows = sqlx::raw_sql(fetch_all_active_advisory_locks_query)
        .fetch_all(sharded_conn)
        .await
        .unwrap();

    // despite being on different shards, both are present!
    assert_eq!(advisory_lock_rows.len(), 2);
}

// Try obtaining multiple advisory locks which resolve to multiple shards.
// Should get an error.
// There's some discussion on this in select.rs
#[tokio::test]
pub async fn advisory_locks_multiple_shards() {
    // Try connecting to pgdog
    let sharded_conn = connections_sqlx().await;
    let sharded_conn = sharded_conn.get(1).unwrap();

    // Obtain 2 locks in one SELECT; diff shards.
    let err = sqlx::raw_sql("SELECT pg_advisory_lock(1), pg_advisory_lock(2)")
        .execute(sharded_conn)
        .await
        .err()
        .unwrap();

    assert!(
        err.as_database_error()
            .unwrap()
            .message()
            .contains("the advisory locks in this query resolve to different shards")
    );
}

#[tokio::test]
async fn advisory_unlock_null_parameter_keeps_session_lock()
-> Result<(), Box<dyn std::error::Error>> {
    let application = format!("advisory_unlock_scope_{}", process::id());
    let options: PgConnectOptions =
        "postgres://pgdog:pgdog@127.0.0.1:6432/pgdog_sharded".parse()?;
    let mut owner = PgConnection::connect_with(&options.application_name(&application)).await?;
    let admin = admin_sqlx().await;

    owner.execute("SELECT pg_advisory_lock(2026092109)").await?;
    assert!(advisory_client_locked(&mut owner, &admin, &application).await?);

    let result: Option<bool> = sqlx::query_scalar("SELECT pg_advisory_unlock($1::bigint)")
        .bind(None::<i64>)
        .fetch_one(&mut owner)
        .await?;
    assert_eq!(result, None);
    assert!(advisory_client_locked(&mut owner, &admin, &application).await?);

    owner.execute("SELECT pg_advisory_unlock_all()").await?;
    assert!(!advisory_client_locked(&mut owner, &admin, &application).await?);
    owner.close().await?;
    Ok(())
}

async fn advisory_client_locked(
    owner: &mut PgConnection,
    admin: &Pool<Postgres>,
    application: &str,
) -> Result<bool, sqlx::Error> {
    // Finish another request so SHOW CLIENTS observes the preceding unlock.
    owner.execute("SELECT 1").await?;
    let clients = admin.fetch_all("SHOW CLIENTS").await?;
    let client = clients
        .iter()
        .find(|row| row.get::<String, _>("application_name") == application)
        .expect("owner appears in SHOW CLIENTS");
    client.try_get("locked")
}
