use crate::setup::{admin_sqlx, connections_sqlx};

/// <https://github.com/pgdogdev/pgdog/issues/1472>
/// Test the implementation of the command FORCE_RELOAD, which is a normal RELOAD + terminates all in-flight transactions.
#[tokio::test]
async fn admin_reload_test() {
    let admin = admin_sqlx().await;
    let connections = connections_sqlx().await; // [pgdog, pgdog_sharded]

    {
        let mut transaction = connections.get(1).unwrap().begin().await.unwrap();

        // Isn't strictly needed for the functionaltiy of the test; but why not?
        sqlx::raw_sql("SELECT * FROM sharded")
            .fetch_all(&mut *transaction)
            .await
            .unwrap();

        // After we force reload, existing transactions (i.e. this one) are terminated.
        sqlx::raw_sql("FORCE_RELOAD").execute(&admin).await.unwrap();

        let err = sqlx::raw_sql("SELECT * FROM sharded")
            .fetch_all(&mut *transaction)
            .await
            .err()
            .unwrap();

        // Standard Postgres error pertaining to pg_terminate_backend; <https://www.postgresql.org/docs/current/functions-admin.html>
        assert!(
            err.as_database_error()
                .unwrap()
                .message()
                .contains("terminating connection due to administrator command")
        );

        // The transaction drops (allowing another connection in sqlx `Pool`)
    }

    // Does it work with a new Pool?
    let test = connections_sqlx().await;
    let test = test.get(1).unwrap();
    sqlx::raw_sql("SELECT 1234").fetch_all(test).await.unwrap();

    // Does it still with the old Pool?
    let conn = connections.get(1).unwrap();
    sqlx::raw_sql("SELECT 1000").fetch_all(conn).await.unwrap();
}
