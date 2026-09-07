use sqlx::{Connection, Row};
use std::time::Duration;
use tokio::time::sleep;

/// In addition to the unit test, `ensure_prepared_re_prepares_after_ttl_expire`,
/// which proves that it actually does a Close -> Prepare,
/// ensure that nothing breaks Client-side by the internal re-Prepare operation it conducts.
#[tokio::test]
async fn test_simple_prepared_ttl() {
    let mut conn =
        sqlx::PgConnection::connect("postgres://pgdog:pgdog@127.0.0.1:6432/pgdog_sharded")
            .await
            .unwrap();

    let mut transaction = conn.begin().await.unwrap();

    sqlx::raw_sql("PREPARE __pgdog_test (int) AS SELECT $1")
        .execute(&mut *transaction)
        .await
        .unwrap();

    integration_tests_rust::utils::assert_setting_str("prepared_statements_ttl", "300").await;
    integration_tests_rust::utils::assert_setting_str("prepared_statements_ttl_jitter", "100")
        .await;
    integration_tests_rust::utils::assert_setting_str("prepared_statements", "full").await;
    sleep(Duration::from_millis(500)).await;

    let test_return = sqlx::raw_sql("EXECUTE __pgdog_test (1)")
        .fetch_one(&mut *transaction)
        .await
        .unwrap();

    assert_eq!(test_return.try_get::<i32, &str>("?column?").unwrap(), 1);
}

/// <https://github.com/pgdogdev/pgdog/issues/1383>
/// TODO: will need to support extended-protocol `Bind`s later for the re-write
#[tokio::test]
async fn test_simple_prepared_limit() {
    let mut conn =
        sqlx::PgConnection::connect("postgres://pgdog:pgdog@127.0.0.1:6432/pgdog_sharded")
            .await
            .unwrap();

    // Clear out the table first.
    sqlx::raw_sql("TRUNCATE sharded")
        .execute(&mut conn)
        .await
        .unwrap();

    // Insert 55 rows.
    for i in 1..=55 {
        sqlx::query("INSERT INTO sharded(id) VALUES ($1)")
            .bind(i)
            .execute(&mut conn)
            .await
            .unwrap();
    }

    // Test limit 5 offset 10 (one ParamRef)
    {
        // Write the PREPARE / EXECUTE
        sqlx::raw_sql("PREPARE stmt AS SELECT * FROM sharded ORDER BY id DESC LIMIT 5 OFFSET $1")
            .execute(&mut conn)
            .await
            .unwrap();

        let rows = sqlx::raw_sql("EXECUTE stmt(10)")
            .fetch_all(&mut conn)
            .await
            .unwrap();

        // There's 55 rows. We're offsetting by 10 with a limit of 5 in reverse (DESC).
        // Therefore, we start at 45, and go down from there.
        assert_eq!(
            rows.iter()
                .map(|row| row.get::<i64, &str>("id"))
                .collect::<Vec<_>>(),
            vec![45, 44, 43, 42, 41]
        );
    }

    // Test limit 10 offset 5 (two ParamRefs);
    // order Limit ref before Offset (out-of-order left-to-right refs)
    {
        // Write the PREPARE / EXECUTE
        // This also resolves to the same cached entry $2, $1 as the last one.
        // However, since the Simple cache key now also takes an Option<OffsetPlan>, they should resolve differently.
        // If we didn't also have Option<OffsetPlan>, it would re-use the LIMIT 5 from last time
        // (regardless of local stmt name differing)
        sqlx::raw_sql("PREPARE stmt2 AS SELECT * FROM sharded ORDER BY id DESC LIMIT $2 OFFSET $1")
            .execute(&mut conn)
            .await
            .unwrap();

        let rows = sqlx::raw_sql("EXECUTE stmt2(5, 10)")
            .fetch_all(&mut conn)
            .await
            .unwrap();

        // There's 55 rows. We're offsetting by 5 with a limit of 10 in reverse (DESC).
        // Therefore, we start at 50, and go down from there.
        assert_eq!(
            rows.iter()
                .map(|row| row.get::<i64, &str>("id"))
                .collect::<Vec<_>>(),
            vec![50, 49, 48, 47, 46, 45, 44, 43, 42, 41]
        );
    }

    // Try normal order $1, $2
    {
        // Write the PREPARE / EXECUTE
        sqlx::raw_sql("PREPARE stmt2 AS SELECT * FROM sharded ORDER BY id DESC LIMIT $1 OFFSET $2")
            .execute(&mut conn)
            .await
            .unwrap();

        let rows = sqlx::raw_sql("EXECUTE stmt2(5, 10)")
            .fetch_all(&mut conn)
            .await
            .unwrap();

        // There's 55 rows. We're offsetting by 10 with a limit of 5 in reverse (DESC).
        // Therefore, we start at 45, and go down from there.
        assert_eq!(
            rows.iter()
                .map(|row| row.get::<i64, &str>("id"))
                .collect::<Vec<_>>(),
            vec![45, 44, 43, 42, 41]
        );
    }

    // Test limit 10 offset 5 (no ParamRefs; all A_Const nodes)
    {
        // Write the PREPARE / EXECUTE
        sqlx::raw_sql("PREPARE stmt3 AS SELECT * FROM sharded ORDER BY id DESC LIMIT 10 OFFSET 5")
            .execute(&mut conn)
            .await
            .unwrap();

        let rows = sqlx::raw_sql("EXECUTE stmt3")
            .fetch_all(&mut conn)
            .await
            .unwrap();

        // There's 55 rows. We're offsetting by 5 with a limit of 10 in reverse (DESC).
        // Therefore, we start at 50, and go down from there.
        assert_eq!(
            rows.iter()
                .map(|row| row.get::<i64, &str>("id"))
                .collect::<Vec<_>>(),
            vec![50, 49, 48, 47, 46, 45, 44, 43, 42, 41]
        );
    }

    // Lets also test with an un-related param (WHERE id < $2)
    {
        // Write the PREPARE / EXECUTE
        sqlx::raw_sql("PREPARE stmt4 AS SELECT * FROM sharded WHERE id < $2 ORDER BY id DESC LIMIT $3 OFFSET $1")
            .execute(&mut conn)
            .await
            .unwrap();

        // [offset, WHERE id <, limit]
        let rows = sqlx::raw_sql("EXECUTE stmt4(5, 25, 10)")
            .fetch_all(&mut conn)
            .await
            .unwrap();

        // There's 55 rows. We're offsetting by 5 with a limit of 10 in reverse (DESC).
        // We also filter out any >= $2 (25)
        // Therefore, we start at 19 (24 - 5), and go down from there.
        assert_eq!(
            rows.iter()
                .map(|row| row.get::<i64, &str>("id"))
                .collect::<Vec<_>>(),
            vec![19, 18, 17, 16, 15, 14, 13, 12, 11, 10]
        );
    }

    // Clean-up (clear again)
    sqlx::raw_sql("TRUNCATE sharded")
        .execute(&mut conn)
        .await
        .unwrap();
}
