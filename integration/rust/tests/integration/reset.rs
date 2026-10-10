use crate::setup::{admin_sqlx, connections_sqlx};
use sqlx::{Executor, Row};

#[tokio::test]
async fn set_local_default_outside_transaction_preserves_timeout()
-> Result<(), Box<dyn std::error::Error>> {
    use futures_util::future::poll_fn;
    use tokio_postgres::{AsyncMessage, Config, NoTls};

    let postgres_port = std::env::var("PGPORT")
        .unwrap_or_else(|_| "5432".into())
        .parse::<u16>()?;
    for (port, database) in [
        (postgres_port, "pgdog"),
        (6432, "pgdog"),
        (6432, "pgdog_sharded"),
    ] {
        for extended in [false, true] {
            let (client, mut connection) = Config::new()
                .host("127.0.0.1")
                .port(port)
                .user("pgdog")
                .password("pgdog")
                .dbname(database)
                .connect(NoTls)
                .await?;
            let (send_notice, mut notices) = tokio::sync::mpsc::unbounded_channel();
            let task = tokio::spawn(async move {
                while let Some(message) = poll_fn(|cx| connection.poll_message(cx)).await {
                    if let AsyncMessage::Notice(notice) = message? {
                        let _ = send_notice.send(notice);
                    }
                }
                Ok::<_, tokio_postgres::Error>(())
            });

            // Use separate requests so LOCAL is genuinely outside a transaction.
            client
                .batch_execute("SET statement_timeout TO '5s'")
                .await?;
            if extended {
                client
                    .execute("SET LOCAL statement_timeout TO DEFAULT", &[])
                    .await?;
            } else {
                client
                    .batch_execute("SET LOCAL statement_timeout TO DEFAULT")
                    .await?;
            }
            let row = client.query_one("SHOW statement_timeout", &[]).await?;
            assert_eq!(
                row.get::<_, String>(0),
                "5s",
                "port={port}, database={database}, extended={extended}"
            );
            let notice = tokio::time::timeout(std::time::Duration::from_secs(2), notices.recv())
                .await?
                .expect("warning received");
            assert_eq!(notice.severity(), "WARNING");
            assert_eq!(notice.code().code(), "25P01");
            assert_eq!(
                notice.message(),
                "SET LOCAL can only be used in transaction blocks"
            );

            // Session-scoped resets must still take effect in both protocols.
            for reset in [
                "SET statement_timeout TO DEFAULT",
                "SET SESSION statement_timeout = DEFAULT",
                "RESET statement_timeout",
            ] {
                client
                    .batch_execute("SET statement_timeout TO '5s'")
                    .await?;
                if extended {
                    client.execute(reset, &[]).await?;
                } else {
                    client.batch_execute(reset).await?;
                }
                let row = client.query_one("SHOW statement_timeout", &[]).await?;
                assert_eq!(
                    row.get::<_, String>(0),
                    "0",
                    "port={port}, database={database}, extended={extended}, reset={reset}"
                );
                assert!(notices.try_recv().is_err(), "session reset must not warn");
            }

            drop(client);
            task.await??;
        }
    }
    Ok(())
}

#[tokio::test]
async fn test_npgsql_reset_batch() -> Result<(), sqlx::Error> {
    let pools = connections_sqlx().await;
    let mut conn = pools[1].acquire().await?;

    conn.execute("SET statement_timeout TO '5s'; SET lock_timeout TO '3s'")
        .await?;
    let row = conn.fetch_one("SHOW statement_timeout").await?;
    assert_eq!(row.get::<String, _>(0), "5s");
    let row = conn.fetch_one("SHOW lock_timeout").await?;
    assert_eq!(row.get::<String, _>(0), "3s");

    conn.execute(
        "SET SESSION AUTHORIZATION DEFAULT;RESET ALL;CLOSE ALL;UNLISTEN *;SELECT pg_advisory_unlock_all();DISCARD SEQUENCES;DISCARD TEMP",
    ).await?;

    let row = conn.fetch_one("SHOW statement_timeout").await?;
    assert_eq!(row.get::<String, _>(0), "0");
    let row = conn.fetch_one("SHOW lock_timeout").await?;
    assert_eq!(row.get::<String, _>(0), "0");
    Ok(())
}

async fn run_reset_single_param() {
    let pools = connections_sqlx().await;
    let sharded = &pools[1];

    let mut conn = sharded.acquire().await.unwrap();

    // Set a parameter
    conn.execute("SET statement_timeout TO '5000'")
        .await
        .unwrap();

    // Verify it's set
    let timeout: String = sqlx::query_scalar("SHOW statement_timeout")
        .fetch_one(&mut *conn)
        .await
        .unwrap();
    assert_eq!(timeout, "5s", "statement_timeout should be 5s after SET");

    // Reset the parameter
    conn.execute("RESET statement_timeout").await.unwrap();

    // Verify it's reset to default
    let timeout: String = sqlx::query_scalar("SHOW statement_timeout")
        .fetch_one(&mut *conn)
        .await
        .unwrap();
    assert_eq!(
        timeout, "0",
        "statement_timeout should be 0 (default) after RESET"
    );
}

#[tokio::test]
async fn test_reset_single_param() {
    let mut handles = Vec::new();
    for _ in 0..10 {
        handles.push(tokio::spawn(run_reset_single_param()));
    }
    for handle in handles {
        handle.await.unwrap();
    }
}

async fn run_reset_all() {
    let pools = connections_sqlx().await;
    let sharded = &pools[1];

    let mut conn = sharded.acquire().await.unwrap();

    // Set multiple parameters
    conn.execute("SET statement_timeout TO '5000'")
        .await
        .unwrap();
    conn.execute("SET lock_timeout TO '3000'").await.unwrap();

    // Verify they're set
    let statement_timeout: String = sqlx::query_scalar("SHOW statement_timeout")
        .fetch_one(&mut *conn)
        .await
        .unwrap();
    assert_eq!(statement_timeout, "5s");

    let lock_timeout: String = sqlx::query_scalar("SHOW lock_timeout")
        .fetch_one(&mut *conn)
        .await
        .unwrap();
    assert_eq!(lock_timeout, "3s");

    // Reset all parameters
    conn.execute("RESET ALL").await.unwrap();

    // Verify they're reset to defaults
    let statement_timeout: String = sqlx::query_scalar("SHOW statement_timeout")
        .fetch_one(&mut *conn)
        .await
        .unwrap();
    assert_eq!(
        statement_timeout, "0",
        "statement_timeout should be 0 after RESET ALL"
    );

    let lock_timeout: String = sqlx::query_scalar("SHOW lock_timeout")
        .fetch_one(&mut *conn)
        .await
        .unwrap();
    assert_eq!(
        lock_timeout, "0",
        "lock_timeout should be 0 after RESET ALL"
    );
}

#[tokio::test]
async fn test_reset_all() {
    let mut handles = Vec::new();
    for _ in 0..10 {
        handles.push(tokio::spawn(run_reset_all()));
    }
    for handle in handles {
        handle.await.unwrap();
    }
}

async fn run_reset_in_transaction_commit() {
    let pools = connections_sqlx().await;
    let sharded = &pools[1];

    let mut conn = sharded.acquire().await.unwrap();

    // Set a parameter outside transaction
    conn.execute("SET statement_timeout TO '5000'")
        .await
        .unwrap();

    let timeout: String = sqlx::query_scalar("SHOW statement_timeout")
        .fetch_one(&mut *conn)
        .await
        .unwrap();
    assert_eq!(timeout, "5s");

    // Begin transaction and reset
    conn.execute("BEGIN").await.unwrap();
    conn.execute("RESET statement_timeout").await.unwrap();

    // Verify it's reset inside transaction
    let timeout: String = sqlx::query_scalar("SHOW statement_timeout")
        .fetch_one(&mut *conn)
        .await
        .unwrap();
    assert_eq!(timeout, "0", "should be reset inside transaction");

    // Commit
    conn.execute("COMMIT").await.unwrap();

    // Verify it stays reset after commit
    let timeout: String = sqlx::query_scalar("SHOW statement_timeout")
        .fetch_one(&mut *conn)
        .await
        .unwrap();
    assert_eq!(timeout, "0", "should stay reset after COMMIT");
}

#[tokio::test]
async fn test_reset_in_transaction_commit() {
    let admin = admin_sqlx().await;
    admin
        .execute("SET cross_shard_disabled TO true")
        .await
        .unwrap();

    let mut handles = Vec::new();
    for _ in 0..10 {
        handles.push(tokio::spawn(run_reset_in_transaction_commit()));
    }
    for handle in handles {
        handle.await.unwrap();
    }

    admin
        .execute("SET cross_shard_disabled TO false")
        .await
        .unwrap();
}

async fn run_reset_in_transaction_rollback() {
    let pools = connections_sqlx().await;
    let sharded = &pools[1];

    let mut conn = sharded.acquire().await.unwrap();

    // Set a parameter outside transaction
    conn.execute("SET statement_timeout TO '5000'")
        .await
        .unwrap();

    let timeout: String = sqlx::query_scalar("SHOW statement_timeout")
        .fetch_one(&mut *conn)
        .await
        .unwrap();
    assert_eq!(timeout, "5s");

    // Begin transaction and reset
    conn.execute("BEGIN").await.unwrap();
    conn.execute("RESET statement_timeout").await.unwrap();

    // Verify it's reset inside transaction
    let timeout: String = sqlx::query_scalar("SHOW statement_timeout")
        .fetch_one(&mut *conn)
        .await
        .unwrap();
    assert_eq!(timeout, "0", "should be reset inside transaction");

    // Rollback
    conn.execute("ROLLBACK").await.unwrap();

    // Verify it's restored after rollback
    let timeout: String = sqlx::query_scalar("SHOW statement_timeout")
        .fetch_one(&mut *conn)
        .await
        .unwrap();
    assert_eq!(timeout, "5s", "should be restored after ROLLBACK");
}

#[tokio::test]
async fn test_reset_in_transaction_rollback() {
    let admin = admin_sqlx().await;
    admin
        .execute("SET cross_shard_disabled TO true")
        .await
        .unwrap();

    let mut handles = Vec::new();
    for _ in 0..10 {
        handles.push(tokio::spawn(run_reset_in_transaction_rollback()));
    }
    for handle in handles {
        handle.await.unwrap();
    }

    admin
        .execute("SET cross_shard_disabled TO false")
        .await
        .unwrap();
}

#[tokio::test]
async fn test_reset_all_startup_parameters() -> Result<(), tokio_postgres::Error> {
    for port in [5432, 6432] {
        for extended in [false, true] {
            let mut config = tokio_postgres::Config::new();
            config
                .host("127.0.0.1")
                .port(port)
                .user("pgdog")
                .password("pgdog")
                .dbname("pgdog")
                .options("-c search_path=s1 -c timezone=Asia/Tokyo");
            let (client, connection) = config.connect(tokio_postgres::NoTls).await?;
            let task = tokio::spawn(connection);

            for changed in [false, true] {
                if changed {
                    client.batch_execute(
                        "SET search_path TO runtime; SET timezone TO 'Europe/Paris'; SET statement_timeout TO '5s'",
                    ).await?;
                }
                if extended {
                    client.execute("RESET ALL", &[]).await?;
                } else {
                    client.batch_execute("RESET ALL").await?;
                }
                let row = client.query_one(
                    "SELECT current_setting('search_path'), current_setting('TimeZone'), current_setting('statement_timeout')",
                    &[],
                ).await?;
                assert_eq!(
                    (
                        row.get::<_, String>(0),
                        row.get::<_, String>(1),
                        row.get::<_, String>(2)
                    ),
                    ("s1".into(), "Asia/Tokyo".into(), "0".into()),
                    "port={port}, extended={extended}, changed={changed}"
                );
            }
            drop(client);
            task.await.expect("connection task completed")?;
        }
    }
    Ok(())
}
