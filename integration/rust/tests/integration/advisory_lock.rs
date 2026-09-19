use crate::setup::{admin_sqlx, connection_sqlx_direct};
use serial_test::serial;
use sqlx::{Executor, Pool, Postgres, Row, postgres::PgPoolOptions};

const APP_NAME: &str = "test_try_advisory_lock_pinning";
const LOCK_KEY: i64 = 8_871_234_567;

#[tokio::test]
#[serial]
async fn test_failed_try_advisory_lock_does_not_pin_client() {
    let holder = connection_sqlx_direct().await;
    let contender = PgPoolOptions::new()
        .max_connections(1)
        .connect(&format!(
            "postgres://pgdog:pgdog@127.0.0.1:6432/pgdog?application_name={APP_NAME}"
        ))
        .await
        .unwrap();
    let admin = admin_sqlx().await;

    sqlx::query("SELECT pg_advisory_lock($1::bigint)")
        .bind(LOCK_KEY)
        .execute(&holder)
        .await
        .unwrap();

    let acquired = sqlx::query_scalar::<_, bool>("SELECT pg_try_advisory_lock($1::bigint)")
        .bind(LOCK_KEY)
        .fetch_one(&contender)
        .await
        .unwrap();

    assert!(!acquired);
    assert!(!client_locked(&admin, APP_NAME).await);

    let acquired = sqlx::query_scalar::<_, bool>("SELECT pg_try_advisory_lock($1::bigint)")
        .bind(LOCK_KEY + 1)
        .fetch_one(&contender)
        .await
        .unwrap();

    assert!(acquired);
    assert!(client_locked(&admin, APP_NAME).await);

    let unlocked = sqlx::query_scalar::<_, bool>("SELECT pg_advisory_unlock($1::bigint)")
        .bind(LOCK_KEY + 1)
        .fetch_one(&contender)
        .await
        .unwrap();

    assert!(unlocked);
    assert!(!client_locked(&admin, APP_NAME).await);

    sqlx::query("SELECT pg_advisory_unlock($1::bigint)")
        .bind(LOCK_KEY)
        .execute(&holder)
        .await
        .unwrap();

    admin.close().await;
    contender.close().await;
    holder.close().await;
}

async fn client_locked(admin: &Pool<Postgres>, application_name: &str) -> bool {
    admin
        .fetch_all("SHOW CLIENTS application_name, locked")
        .await
        .unwrap()
        .into_iter()
        .find(|row| row.get::<String, _>("application_name") == application_name)
        .map(|row| row.get::<bool, _>("locked"))
        .unwrap_or_else(|| panic!("client {application_name:?} not found in SHOW CLIENTS"))
}
