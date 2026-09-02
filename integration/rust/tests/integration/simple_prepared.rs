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

    integration_tests_rust::utils::assert_setting_str("prepared_statements_ttl", "5000").await;
    integration_tests_rust::utils::assert_setting_str("prepared_statements_ttl_jitter", "1000")
        .await;
    sleep(Duration::from_secs(8)).await;

    let test_return = sqlx::raw_sql("EXECUTE __pgdog_test (1)")
        .fetch_one(&mut *transaction)
        .await
        .unwrap();

    assert_eq!(test_return.try_get::<i32, &str>("?column?").unwrap(), 1);
}
