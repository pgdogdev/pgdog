use sqlx::Row;
use std::time::Duration;

use tokio::time::{Instant, sleep};

use crate::setup::{admin_sqlx, connections_sqlx};

/// <https://github.com/pgdogdev/pgdog/issues/1472>
/// Test the implementation of the command FORCE_RELOAD, which is a normal RELOAD + terminates all in-flight transactions.
#[tokio::test]
async fn admin_force_reload_test() {
    let admin = admin_sqlx().await;
    let connections = connections_sqlx().await; // [pgdog, pgdog_sharded]

    // Test with an idle transaction.
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

        // Identical to Postgres error pertaining to normal backend termination.
        // <https://www.postgresql.org/docs/current/functions-admin.html>
        assert!(
            err.as_database_error()
                .unwrap()
                .message()
                .contains("terminating connection due to administrator command")
        );

        // Should be overwritten from the generic error code to the same one as Postgres.
        assert_eq!(err.as_database_error().unwrap().code().unwrap(), "57P01");

        // The transaction drops (allowing another connection in sqlx `Pool`)
    }

    // Does it work with a new Pool?
    let test = connections_sqlx().await;
    let test = test.get(1).unwrap();
    sqlx::raw_sql("SELECT 1234").fetch_all(test).await.unwrap();

    // Does it still with the old Pool?
    let conn = connections.get(1).unwrap();
    sqlx::raw_sql("SELECT 1000").fetch_all(conn).await.unwrap();

    // Additionally try when we're in an active query (as opposed to an idle transaction)
    {
        let query = async {
            let connections = connections_sqlx().await;
            let mut transaction = connections.get(1).unwrap().begin().await.unwrap();

            let start_ts = Instant::now();

            // Arbitrary amount of time; it should be cancelled long before; lets us be sure that
            // FORCE_RELOAD is actually cancelling it mid-query
            let err = sqlx::raw_sql("SELECT pg_sleep(5)")
                .execute(&mut *transaction)
                .await
                .err()
                .unwrap();

            assert!(
                err.as_database_error()
                    .unwrap()
                    .message()
                    .contains("terminating connection due to administrator command")
            );

            // We're sleeping for 5s. Waiting for 500ms on `reload` async block.
            // So it should be <1s until we get the admin termination error.
            assert!(start_ts.elapsed() < Duration::from_secs(1));
        };

        let reload = async {
            sleep(Duration::from_millis(500)).await;

            // After we force reload, existing transactions (i.e. this one) are terminated.
            sqlx::raw_sql("FORCE_RELOAD").execute(&admin).await.unwrap();
        };

        // Runs both the query and reload concurrently
        // Returns after both have finished.
        tokio::join!(query, reload);

        // The transaction was terminated (through PgDog), however, is Postgres still running it?
        let rows = sqlx::raw_sql(
            "SELECT * FROM pg_stat_activity WHERE state = 'active' AND query NOT LIKE '%pg_stat_activity%'",
        )
        .fetch_all(conn)
        .await
        .unwrap();

        // I did it this way to prevent flaky tests if the health-check were to run in parallel
        // Usually there's no rows.
        for row in rows {
            assert!(!row.get::<&str, &str>("query").eq("SELECT pg_sleep(5)"));
        }
    }
}
