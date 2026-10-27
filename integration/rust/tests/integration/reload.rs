use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;

use crate::setup::{
    Backend, admin_sqlx, admin_tokio, backends, connection_sqlx_direct, connections_sqlx,
};
use rust_decimal::Decimal;
use serial_test::serial;
use sqlx::{Executor, Pool, Postgres, Row, postgres::PgPoolOptions};
use tokio::time::{Instant, sleep};

async fn total_query_count(admin: &Pool<Postgres>) -> i64 {
    admin
        .fetch_all("SHOW STATS")
        .await
        .unwrap()
        .iter()
        .filter(|row| row.get::<String, _>("database") == "pgdog")
        .map(|row| {
            i64::try_from(row.get::<Decimal, _>("total_query_count"))
                .expect("total_query_count should fit in i64")
        })
        .sum()
}

async fn setting(admin: &Pool<Postgres>, name: &str) -> String {
    admin
        .fetch_all("SHOW CONFIG")
        .await
        .unwrap()
        .iter()
        .find(|row| row.get::<String, _>(0) == name)
        .map(|row| row.get::<String, _>(1))
        .unwrap_or_else(|| panic!("{name} not found in SHOW CONFIG"))
}

async fn pgdog_server_connections(direct: &Pool<Postgres>) -> i64 {
    direct
        .fetch_one("SELECT COUNT(*)::BIGINT FROM pg_stat_activity WHERE application_name = 'PgDog'")
        .await
        .unwrap()
        .get(0)
}

async fn failover_with_backends(app_name: &str) -> (Pool<Postgres>, Vec<Backend>) {
    let conn = PgPoolOptions::new()
        .max_connections(5)
        .connect(&format!(
            "postgres://pgdog:pgdog@127.0.0.1:6432/failover?application_name={app_name}"
        ))
        .await
        .unwrap();

    conn.execute("SELECT 1").await.unwrap();

    let backends = backends(app_name, &conn).await;
    assert!(!backends.is_empty());

    (conn, backends)
}

async fn override_query_timeout(admin: &Pool<Postgres>) -> String {
    let from_file = setting(admin, "query_timeout").await;

    admin.execute("SET query_timeout TO 2500").await.unwrap();
    assert_ne!(setting(admin, "query_timeout").await, from_file);

    from_file
}

async fn assert_query_timeout_reloaded(admin: &Pool<Postgres>, from_file: &str) {
    let mut current = String::new();
    for _ in 0..20 {
        current = setting(admin, "query_timeout").await;
        if current == from_file {
            break;
        }
        sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(
        current, from_file,
        "reload should restore query_timeout from the config file"
    );
}

async fn assert_reload(app_name: &str, reload: impl AsyncFn(&Pool<Postgres>)) {
    let admin = admin_sqlx().await;
    // SET recreates the pools, so it must run before the backends are recorded.
    let from_file = override_query_timeout(&admin).await;
    let (conn, backends_before) = failover_with_backends(app_name).await;

    for _ in 0..5 {
        conn.execute("SELECT 1").await.unwrap();
        reload(&admin).await;
        sleep(Duration::from_millis(50)).await;
    }

    assert_query_timeout_reloaded(&admin, &from_file).await;

    let backends_after = backends(app_name, &conn).await;
    let some_survived = backends_after.iter().any(|b| backends_before.contains(b));
    assert!(some_survived);

    conn.close().await;
    admin.close().await;
}

fn assert_admin_termination(err: &sqlx::Error) {
    let err = err.as_database_error().unwrap();
    assert!(
        err.message()
            .contains("terminating connection due to administrator command")
    );
    assert_eq!(err.code().unwrap(), "57P01");
}

#[cfg(unix)]
fn send_sighup() {
    let pid_path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../pgdog.pid");
    let pid: libc::pid_t = std::fs::read_to_string(pid_path)
        .expect("pgdog.pid should exist")
        .trim()
        .parse()
        .expect("pgdog.pid should contain a pid");
    let res = unsafe { libc::kill(pid, libc::SIGHUP) };
    assert_eq!(res, 0, "kill failed: {}", std::io::Error::last_os_error());
}

#[tokio::test]
#[serial]
async fn test_reload() {
    sleep(Duration::from_secs(1)).await;

    assert_reload("test_reload", async |admin| {
        admin.execute("RELOAD").await.unwrap();
    })
    .await;
}

#[cfg(unix)]
#[tokio::test]
#[serial]
async fn test_reload_sighup() {
    assert_reload("test_reload_sighup", async |_| send_sighup()).await;
}

#[tokio::test]
#[serial]
async fn test_reload_connection_count_stable() {
    let admin = admin_tokio().await;
    let direct = connection_sqlx_direct().await;

    // Perform a single reload to get a stable baseline
    admin.simple_query("RELOAD").await.unwrap();
    sleep(Duration::from_millis(250)).await;
    // Count PgDog connections on Postgres before reload.
    let before = pgdog_server_connections(&direct).await;

    assert!(before > 0, "expected pgdog connections before reload");

    admin.simple_query("RELOAD").await.unwrap();
    sleep(Duration::from_millis(250)).await;
    let after = pgdog_server_connections(&direct).await;

    assert!(
        after as f64 / before as f64 <= 1.1,
        "connection count changed by more than 10% after RELOAD: before={before}, after={after}"
    );

    direct.close().await;
}

#[tokio::test]
#[ignore]
async fn test_reload_pool_size_not_exceeded() {
    let pool_size: i64 = 50; // default_pool_size
    let num_clients = 10;
    let app_name = "test_reload_pool_limit";

    sleep(Duration::from_secs(1)).await;
    let admin = admin_tokio().await;
    let direct = connection_sqlx_direct().await;

    // Spin up clients that continuously run queries.
    let stop = Arc::new(AtomicBool::new(false));
    let mut handles = vec![];

    for _ in 0..num_clients {
        let stop = stop.clone();
        handles.push(tokio::spawn(async move {
            let pool = PgPoolOptions::new()
                .max_connections(1)
                .connect(&format!(
                    "postgres://pgdog:pgdog@127.0.0.1:6432/pgdog?application_name={app_name}"
                ))
                .await
                .unwrap();
            while !stop.load(Ordering::Relaxed) {
                let _ = pool.execute("SELECT 1").await;
                sleep(Duration::from_millis(10)).await;
            }
            pool.close().await;
        }));
    }

    // Let clients warm up.
    sleep(Duration::from_millis(500)).await;

    // Issue multiple reloads while clients are active and check pool size each time.
    for i in 0..10 {
        admin.simple_query("RELOAD").await.unwrap();
        sleep(Duration::from_millis(100)).await;

        let query = format!(
            "SELECT COUNT(*)::BIGINT FROM pg_stat_activity WHERE application_name = '{app_name}'"
        );
        let count: i64 = direct.fetch_one(query.as_str()).await.unwrap().get(0);

        eprintln!("reload {i}: {count} connections (pool_size={pool_size})");
        assert!(
            count <= pool_size,
            "pool size exceeded after reload {i}: {count} > {pool_size}"
        );
    }

    stop.store(true, Ordering::Relaxed);
    for h in handles {
        h.await.unwrap();
    }

    direct.close().await;
}

#[tokio::test]
#[serial]
async fn test_reconnect() {
    let admin = admin_tokio().await;
    let (conn, backends_before) = failover_with_backends("test_reconnect").await;

    conn.execute("SELECT 1").await.unwrap();
    admin.simple_query("RECONNECT").await.unwrap();
    sleep(Duration::from_millis(50)).await;

    let backends_after = backends("test_reconnect", &conn).await;
    let some_survived = backends_after.iter().any(|b| backends_before.contains(b));
    assert!(!some_survived);

    conn.close().await;
}

#[tokio::test]
#[serial]
async fn test_reload_preserves_stats_until_reset() {
    let admin = admin_sqlx().await;
    let pools = connections_sqlx().await;
    let conn = &pools[0];

    admin.execute("RESET STATS").await.unwrap();

    const QUERIES: i64 = 50;
    for _ in 0..QUERIES {
        conn.execute("SELECT 1").await.unwrap();
    }

    let before_reload = total_query_count(&admin).await;
    assert!(
        before_reload >= QUERIES,
        "expected at least {QUERIES} queries before reload, got {before_reload}"
    );

    admin.execute("RELOAD").await.unwrap();

    let after_reload = total_query_count(&admin).await;
    assert!(
        after_reload >= before_reload,
        "RELOAD zeroed pool stats: before={before_reload}, after={after_reload}"
    );

    admin.execute("RESET STATS").await.unwrap();

    let after_reset = total_query_count(&admin).await;
    assert!(
        after_reset < before_reload,
        "RESET STATS left accumulated counters: before={before_reload}, after={after_reset}"
    );

    admin.close().await;
}

/// <https://github.com/pgdogdev/pgdog/issues/1472>
/// FORCE_RELOAD is a normal RELOAD that also terminates all in-flight transactions.
#[tokio::test]
#[serial]
async fn test_force_reload() {
    let admin = admin_sqlx().await;
    let connections = connections_sqlx().await; // [pgdog, pgdog_sharded]
    let conn = &connections[1];

    // Idle transaction.
    {
        let from_file = override_query_timeout(&admin).await;
        let mut transaction = conn.begin().await.unwrap();

        sqlx::raw_sql("SELECT * FROM sharded")
            .fetch_all(&mut *transaction)
            .await
            .unwrap();

        admin.execute("FORCE_RELOAD").await.unwrap();
        assert_query_timeout_reloaded(&admin, &from_file).await;

        let err = sqlx::raw_sql("SELECT * FROM sharded")
            .fetch_all(&mut *transaction)
            .await
            .unwrap_err();

        // Same error as Postgres returns for pg_terminate_backend.
        // <https://www.postgresql.org/docs/current/functions-admin.html>
        assert_admin_termination(&err);
    }

    // A new pool works.
    let new_connections = connections_sqlx().await;
    sqlx::raw_sql("SELECT 1234")
        .fetch_all(&new_connections[1])
        .await
        .unwrap();

    // The old pool still works.
    sqlx::raw_sql("SELECT 1000").fetch_all(conn).await.unwrap();

    // Active query.
    {
        let from_file = override_query_timeout(&admin).await;

        let query = async {
            let connections = connections_sqlx().await;
            let mut transaction = connections[1].begin().await.unwrap();

            let start_ts = Instant::now();

            // FORCE_RELOAD runs after 500ms, so the query must fail long before 5s.
            let err = sqlx::raw_sql("SELECT pg_sleep(5)")
                .execute(&mut *transaction)
                .await
                .unwrap_err();

            assert_admin_termination(&err);
            assert!(start_ts.elapsed() < Duration::from_secs(1));
        };

        let reload = async {
            sleep(Duration::from_millis(500)).await;
            admin.execute("FORCE_RELOAD").await.unwrap();
        };

        tokio::join!(query, reload);
        assert_query_timeout_reloaded(&admin, &from_file).await;

        // PgDog terminated the client. Postgres must not run the query anymore.
        // The filter ignores other active queries, for example a health check.
        let rows = sqlx::raw_sql(
            "SELECT * FROM pg_stat_activity WHERE state = 'active' AND query NOT LIKE '%pg_stat_activity%'",
        )
        .fetch_all(conn)
        .await
        .unwrap();

        for row in rows {
            assert_ne!(row.get::<&str, _>("query"), "SELECT pg_sleep(5)");
        }
    }
}
