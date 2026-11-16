use std::time::Duration;

use anyhow::{Context, ensure};
use futures_util::future::join_all;
use serial_test::serial;
use sqlx::{Connection, Executor, PgConnection};
use tokio::{sync::Barrier, time::timeout};

use crate::setup::{admin_sqlx, connection_sqlx_direct};

#[tokio::test]
#[serial]
async fn test_passthrough_concurrent_first_login() -> anyhow::Result<()> {
    const CLIENTS: usize = 32;
    const ROUNDS: usize = 16;
    const ROUND_TIMEOUT: Duration = Duration::from_secs(10);

    let admin = admin_sqlx().await;
    let direct = connection_sqlx_direct().await;
    let mut roles = Vec::new();

    let result: anyhow::Result<()> = async {
        admin.execute("RELOAD").await?;
        admin
            .execute("SET passthrough_auth TO 'enabled_plain'")
            .await?;

        for round in 0..ROUNDS {
            // Reserved, predictable fixture names also allow cleanup after a
            // forcibly interrupted run that could not reach the cleanup below.
            let user = format!("pgdog_passthrough_race_{round:02}");
            direct
                .execute(format!("DROP ROLE IF EXISTS {user}").as_str())
                .await?;
            direct
                .execute(format!("CREATE ROLE {user} LOGIN PASSWORD 'pgdog'").as_str())
                .await?;
            roles.push(user.clone());
            let url = format!("postgres://{user}:pgdog@127.0.0.1:6432/pgdog");
            let barrier = Barrier::new(CLIENTS);

            // No warm-up: every round races the first publication of this user's
            // credentials and pools. Previously a concurrent login could see the
            // credentials before the pool existed and fail after authentication.
            // Use individual connections so a client pool cannot retry failures.
            let clients = (0..CLIENTS).map(|client| {
                let barrier = &barrier;
                let url = &url;
                let user = &user;
                async move {
                    barrier.wait().await;
                    let result: anyhow::Result<()> = async {
                        let mut conn = PgConnection::connect(url).await.context("connect")?;
                        let row: (String, String, i32) = sqlx::query_as(
                            "SELECT current_user::text, current_database()::text, 1",
                        )
                        .fetch_one(&mut conn)
                        .await
                        .context("query immediately after connecting")?;
                        ensure!(
                            row == (user.clone(), "pgdog".to_owned(), 1),
                            "unexpected result: {row:?}"
                        );
                        conn.close().await.context("close connection")?;
                        Ok(())
                    }
                    .await;
                    result.with_context(|| format!("round {round}, client {client}, role {user}"))
                }
            });

            // Finish every client before inspecting errors; dropping a timed-out
            // round also drops its connection futures, leaving no detached tasks.
            let results = timeout(ROUND_TIMEOUT, join_all(clients))
                .await
                .with_context(|| format!("round {round} timed out"))?;
            for result in results {
                result?;
            }
        }
        Ok(())
    }
    .await;

    // Attempt all cleanup even when a login, query, or earlier cleanup fails.
    let mut cleanup_errors = Vec::new();
    if let Err(error) = admin.execute("RELOAD").await {
        cleanup_errors.push(format!("restore config: {error}"));
    }
    for user in roles {
        if let Err(error) = direct.execute(format!("DROP ROLE {user}").as_str()).await {
            cleanup_errors.push(format!("drop {user}: {error}"));
        }
    }
    ensure!(
        cleanup_errors.is_empty(),
        "cleanup failed: {cleanup_errors:?}; test result: {result:?}"
    );
    result
}
