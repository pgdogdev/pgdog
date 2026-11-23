use std::time::Duration;

use serial_test::serial;
use sqlx::{Connection, Executor, PgConnection};
use tokio::time::sleep;
use uuid::Uuid;

use crate::setup::{admin_sqlx, connection_sqlx_direct};
use crate::utils::assert_setting_str;

#[tokio::test]
#[serial]
async fn test_passthrough_debounce_retries_same_credentials_after_expiry() {
    let admin = admin_sqlx().await;
    let direct = connection_sqlx_direct().await;
    admin.execute("RELOAD").await.expect("reset config");
    admin
        .execute("SET passthrough_auth TO 'enabled_plain'")
        .await
        .expect("enable passthrough auth");
    // Failed validation waits for checkout to time out.
    admin
        .execute("SET checkout_timeout TO 250")
        .await
        .expect("shorten failed validation wait");
    admin
        .execute("SET connect_timeout TO 250")
        .await
        .expect("shorten validation connection timeout");
    admin
        .execute("SET passthrough_auth_debounce_delay TO 100")
        .await
        .expect("shorten debounce delay");
    assert_setting_str("passthrough_auth_debounce_delay", "100ms").await;

    let user = format!("debounce_{}", Uuid::new_v4().simple());
    // NOLOGIN makes the initial validation fail even when local PostgreSQL
    // uses trust authentication. No shared test user's credentials are changed.
    direct
        .execute(format!("CREATE ROLE {user} NOLOGIN PASSWORD 'pgdog'").as_str())
        .await
        .expect("create temporarily disabled role");
    let url = format!("postgres://{user}:pgdog@127.0.0.1:6432/pgdog");

    // Clean up the role and config even if a connection or query fails.
    let result: Result<i32, sqlx::Error> = async {
        let err = match PgConnection::connect(&url).await {
            Err(err) => err,
            Ok(conn) => {
                conn.close().await?;
                return Err(sqlx::Error::Protocol(
                    "disabled role unexpectedly authenticated".into(),
                ));
            }
        };
        if err
            .as_database_error()
            .and_then(|error| error.code())
            .as_deref()
            != Some("28000")
        {
            return Err(err);
        }

        direct
            .execute(format!("ALTER ROLE {user} LOGIN").as_str())
            .await?;
        // Neither reload nor unban: only expiry should allow fresh validation
        // of exactly the credentials whose previous check failed.
        sleep(Duration::from_millis(150)).await;
        let mut conn = PgConnection::connect(&url).await?;
        let value = sqlx::query_scalar("SELECT 1").fetch_one(&mut conn).await?;
        conn.close().await?;
        Ok(value)
    }
    .await;

    admin.execute("RELOAD").await.expect("restore config");
    direct
        .execute(format!("DROP ROLE {user}").as_str())
        .await
        .expect("remove test role");
    assert_eq!(result.expect("same credentials must work after expiry"), 1);
}
