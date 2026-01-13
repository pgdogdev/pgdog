//! Pool tests.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use rand::Rng;
use tokio::spawn;
use tokio::task::yield_now;
use tokio::time::{sleep, timeout, Instant};
use tokio_util::task::TaskTracker;

use crate::net::ProtocolMessage;
use crate::net::{Parse, Protocol, Query, Sync};
use crate::state::State;

use super::*;

pub fn pool() -> Pool {
    let config = Config {
        max: 1,
        min: 1,
        ..Default::default()
    };

    let pool = Pool::new(&PoolConfig {
        address: Address {
            host: "127.0.0.1".into(),
            port: 5432,
            database_name: "pgdog".into(),
            user: "pgdog".into(),
            password: "pgdog".into(),
            ..Default::default()
        },
        config,
    });
    pool.launch();
    pool
}

pub fn pool_with_prepared_capacity(capacity: usize) -> Pool {
    let config = Config {
        max: 1,
        min: 1,
        prepared_statements_limit: capacity,
        ..Default::default()
    };

    let pool = Pool::new(&PoolConfig {
        address: Address {
            host: "127.0.0.1".into(),
            port: 5432,
            database_name: "pgdog".into(),
            user: "pgdog".into(),
            password: "pgdog".into(),
            ..Default::default()
        },
        config,
    });
    pool.launch();
    pool
}

#[tokio::test(flavor = "current_thread")]
async fn test_pool_checkout() {
    crate::logger();

    let pool = pool();
    let conn = pool.get(&Request::default()).await.unwrap();
    let id = *(conn.id());

    assert!(conn.done());
    assert!(conn.done());
    assert!(!conn.in_transaction());
    assert!(!conn.error());

    assert_eq!(pool.lock().idle(), 0);
    assert_eq!(pool.lock().total(), 1);
    assert_eq!(pool.lock().should_create(), inner::ShouldCreate::No);

    let err = timeout(Duration::from_millis(100), pool.get(&Request::default())).await;

    assert_eq!(pool.lock().total(), 1);
    assert_eq!(pool.lock().idle(), 0);
    assert!(err.is_err());

    drop(conn); // Return conn to the pool.
    let conn = pool.get(&Request::default()).await.unwrap();
    assert_eq!(conn.id(), &id);
}

#[tokio::test]
async fn test_concurrency() {
    let pool = pool();
    let tracker = TaskTracker::new();

    for _ in 0..1000 {
        let pool = pool.clone();
        tracker.spawn(async move {
            let _conn = pool.get(&Request::default()).await.unwrap();
            let duration = rand::rng().random_range(0..10);
            sleep(Duration::from_millis(duration)).await;
        });
    }

    tracker.close();
    tracker.wait().await;

    // This may be flakey,
    // we're waiting for Guard to check the connection
    // back in.
    sleep(Duration::from_millis(100)).await;
    yield_now().await;

    assert_eq!(pool.lock().total(), 1);
    assert_eq!(pool.lock().idle(), 1);
}

#[tokio::test]
async fn test_concurrency_with_gas() {
    let pool = pool();
    let tracker = TaskTracker::new();

    let config = Config {
        max: 10,
        ..Default::default()
    };
    pool.update_config(config);

    for _ in 0..10000 {
        let pool = pool.clone();
        tracker.spawn(async move {
            let _conn = pool.get(&Request::default()).await.unwrap();
            let duration = rand::rng().random_range(0..10);
            assert!(pool.lock().checked_out() > 0);
            assert!(pool.lock().total() <= 10);
            sleep(Duration::from_millis(duration)).await;
        });
    }

    tracker.close();
    tracker.wait().await;

    assert_eq!(pool.lock().total(), 10);
}

#[tokio::test]
async fn test_offline() {
    let pool = pool();
    assert!(pool.lock().online);

    pool.shutdown();
    assert!(!pool.lock().online);

    // Cannot get a connection from the pool.
    let err = pool.get(&Request::default()).await;
    err.expect_err("pool is shut down");
}

#[tokio::test]
async fn test_pause() {
    let pool = pool();
    let tracker = TaskTracker::new();
    let config = Config {
        checkout_timeout: Duration::from_millis(1_000),
        max: 1,
        ..Default::default()
    };
    pool.update_config(config);

    let hold = pool.get(&Request::default()).await.unwrap();
    pool.get(&Request::default())
        .await
        .expect_err("checkout timeout");
    drop(hold);
    // Make sure we're not blocked still.
    drop(pool.get(&Request::default()).await.unwrap());

    pool.pause();

    // We'll hit the timeout now because we're waiting forever.
    let pause = Duration::from_millis(2_000);
    assert!(timeout(pause, pool.get(&Request::default())).await.is_err());

    // Spin up a bunch of clients and make them wait for
    // a connection while the pool is paused.
    for _ in 0..1000 {
        let pool = pool.clone();
        tracker.spawn(async move {
            let _conn = pool.get(&Request::default()).await.unwrap();
        });
    }

    pool.resume();
    tracker.close();
    tracker.wait().await;

    assert!(pool.get(&Request::default()).await.is_ok());

    // Shutdown the pool while clients wait.
    // Makes sure they get woken up and kicked out of
    // the pool.
    pool.pause();
    let tracker = TaskTracker::new();
    let didnt_work = Arc::new(AtomicBool::new(false));
    for _ in 0..1000 {
        let didnt_work = didnt_work.clone();
        let pool = pool.clone();
        tracker.spawn(async move {
            if !pool
                .get(&Request::default())
                .await
                .is_err_and(|err| err == Error::Offline)
            {
                didnt_work.store(true, Ordering::Relaxed);
            }
        });
    }

    sleep(Duration::from_millis(100)).await;
    pool.shutdown();
    tracker.close();
    tracker.wait().await;
    assert!(!didnt_work.load(Ordering::Relaxed));
}

// Proof that the mutex is working well.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_benchmark_pool() {
    let counts = 500_000;
    let workers = 4;

    let pool = pool();

    // Prewarm
    let request = Request::default();
    drop(pool.get(&request).await.unwrap());

    let mut handles = Vec::with_capacity(2);
    let start = Instant::now();

    for _ in 0..workers {
        let pool = pool.clone();
        let handle = tokio::spawn(async move {
            for _ in 0..counts {
                let conn = pool.get(&request).await.unwrap();
                conn.addr();
                drop(conn);
            }
        });
        handles.push(handle);
    }
    for handle in handles {
        handle.await.unwrap();
    }
    let duration = start.elapsed();
    println!("bench: {}ms", duration.as_millis());
}

#[tokio::test]
async fn test_incomplete_request_recovery() {
    crate::logger();

    let pool = pool();

    for query in ["SELECT 1", "BEGIN"] {
        let mut conn = pool.get(&Request::default()).await.unwrap();
        let conn_id = *(conn.id());

        conn.send(&vec![ProtocolMessage::from(Query::new(query))].into())
            .await
            .unwrap();
        drop(conn); // Drop the connection to simulating client dying.

        sleep(Duration::from_millis(500)).await;
        let state = pool.state();
        let out_of_sync = state.out_of_sync;
        assert_eq!(out_of_sync, 0);
        assert_eq!(state.idle, 1);
        if query == "BEGIN" {
            assert_eq!(state.stats.counts.rollbacks, 1);
        } else {
            assert_eq!(state.stats.counts.rollbacks, 0);
        }

        // Verify the same connection is reused
        let conn = pool.get(&Request::default()).await.unwrap();
        assert_eq!(conn.id(), &conn_id);
    }
}

#[tokio::test]
async fn test_force_close() {
    let pool = pool();
    let mut conn = pool.get(&Request::default()).await.unwrap();
    conn.execute("BEGIN").await.unwrap();
    assert!(conn.in_transaction());
    conn.stats_mut().state(State::ForceClose);
    drop(conn);
    assert_eq!(pool.lock().force_close, 1);
}

#[tokio::test]
async fn test_server_force_close_discards_connection() {
    crate::logger();

    let config = Config {
        max: 1,
        min: 0,
        ..Default::default()
    };

    let pool = Pool::new(&PoolConfig {
        address: Address {
            host: "127.0.0.1".into(),
            port: 5432,
            database_name: "pgdog".into(),
            user: "pgdog".into(),
            password: "pgdog".into(),
            ..Default::default()
        },
        config,
    });
    pool.launch();

    let mut conn = pool.get(&Request::default()).await.unwrap();
    conn.execute("BEGIN").await.unwrap();
    assert!(conn.in_transaction());

    assert_eq!(pool.lock().total(), 1);
    assert_eq!(pool.lock().idle(), 0);
    assert_eq!(pool.lock().checked_out(), 1);

    conn.stats_mut().state(State::ForceClose);
    drop(conn);

    sleep(Duration::from_millis(100)).await;

    let state = pool.state();
    assert_eq!(state.force_close, 1);
    assert_eq!(state.idle, 0);
    assert_eq!(state.total, 0);
}

#[tokio::test]
async fn test_query_stats() {
    let pool = pool();
    let before = pool.state();

    let mut tasks = vec![];
    for _ in 0..25 {
        let pool = pool.clone();
        let handle = spawn(async move {
            let mut conn = pool.get(&Request::default()).await.unwrap();

            for _ in 0..25 {
                conn.execute("BEGIN").await.unwrap();
                conn.execute("SELECT 1").await.unwrap();
                conn.execute("COMMIT").await.unwrap();
            }

            drop(conn);

            let mut conn = pool.get(&Request::default()).await.unwrap();
            conn.execute("SELECT 2").await.unwrap();
        });
        tasks.push(handle);
    }

    for task in tasks {
        task.await.unwrap();
    }

    let after = pool.state();

    assert_eq!(before.stats.counts.query_count, 0);
    assert_eq!(before.stats.counts.xact_count, 0);
    assert_eq!(
        after.stats.counts.query_count,
        25 * 25 * 3 + 25 + after.stats.counts.healthchecks
    );
    assert_eq!(
        after.stats.counts.xact_count,
        25 * 26 + after.stats.counts.healthchecks
    );
    assert_eq!(after.stats.counts.healthchecks, 1)
}

#[tokio::test]
async fn test_prepared_statements_limit() {
    crate::logger();
    let pool = pool_with_prepared_capacity(2);

    // Let's churn them like crazy
    for id in 0..100 {
        let mut guard = pool.get(&Request::default()).await.unwrap();
        guard
            .send(
                &vec![
                    Parse::named(&format!("__pgdog_{}", id), "SELECT $1::bigint").into(),
                    Sync.into(),
                ]
                .into(),
            )
            .await
            .unwrap();

        for c in ['1', 'Z'] {
            let msg = guard.read().await.unwrap();
            assert_eq!(msg.code(), c);
        }
        drop(guard); // Cleanup happens now.
    }

    let mut guard = pool.get(&Request::default()).await.unwrap();
    // It's random!
    assert!(
        guard.prepared_statements_mut().contains("__pgdog_99")
            || guard.prepared_statements_mut().contains("__pgdog_98")
    );
    assert_eq!(guard.prepared_statements_mut().len(), 2);

    // Let's make sure Postgres agreees.
    guard.sync_prepared_statements().await.unwrap();

    // It's random!
    assert!(
        guard.prepared_statements_mut().contains("__pgdog_99")
            || guard.prepared_statements_mut().contains("__pgdog_98")
    );
    assert_eq!(guard.prepared_statements_mut().len(), 2);
    assert_eq!(guard.stats().total.prepared_statements, 2); // stats are accurate.

    let pool = pool_with_prepared_capacity(100);

    // Won't churn any.
    for id in 0..100 {
        let mut guard = pool.get(&Request::default()).await.unwrap();
        guard
            .send(
                &vec![
                    Parse::named(&format!("__pgdog_{}", id), "SELECT $1::bigint").into(),
                    Sync.into(),
                ]
                .into(),
            )
            .await
            .unwrap();

        for c in ['1', 'Z'] {
            let msg = guard.read().await.unwrap();
            assert_eq!(msg.code(), c);
        }
        drop(guard); // Cleanup happens now.
    }

    let mut guard = pool.get(&Request::default()).await.unwrap();
    assert!(guard.prepared_statements_mut().contains("__pgdog_99"));
    assert_eq!(guard.prepared_statements_mut().len(), 100);
    assert_eq!(guard.stats().total.prepared_statements, 100); // stats are accurate.

    // Let's make sure Postgres agreees.
    guard.sync_prepared_statements().await.unwrap();

    assert!(guard.prepared_statements_mut().contains("__pgdog_99"));
    assert_eq!(guard.prepared_statements_mut().len(), 100);
    assert_eq!(guard.stats().total.prepared_statements, 100); // stats are accurate.
}

#[tokio::test]
async fn test_idle_healthcheck_loop() {
    crate::logger();

    let config = Config {
        max: 1,
        min: 1,
        idle_healthcheck_interval: Duration::from_millis(100),
        idle_healthcheck_delay: Duration::from_millis(10),
        ..Default::default()
    };

    let pool = Pool::new(&PoolConfig {
        address: Address {
            host: "127.0.0.1".into(),
            port: 5432,
            database_name: "pgdog".into(),
            user: "pgdog".into(),
            password: "pgdog".into(),
            ..Default::default()
        },
        config,
    });
    pool.launch();

    let initial_healthchecks = pool.state().stats.counts.healthchecks;

    sleep(Duration::from_millis(350)).await;

    let after_healthchecks = pool.state().stats.counts.healthchecks;

    assert!(
        after_healthchecks > initial_healthchecks,
        "Expected healthchecks to increase from {} but got {}",
        initial_healthchecks,
        after_healthchecks
    );
    assert!(
        after_healthchecks >= initial_healthchecks + 2,
        "Expected at least 2 healthchecks to run in 350ms with 100ms interval, got {} (increase of {})",
        after_healthchecks,
        after_healthchecks - initial_healthchecks
    );
}

#[tokio::test]
async fn test_checkout_timeout() {
    crate::logger();

    let config = Config {
        max: 1,
        min: 1,
        checkout_timeout: Duration::from_millis(100),
        ..Default::default()
    };

    let pool = Pool::new(&PoolConfig {
        address: Address::new_test(),
        config,
    });
    pool.launch();

    // Hold the only connection
    let _conn = pool.get(&Request::default()).await.unwrap();

    // Try to get another connection - should timeout
    let result = pool.get(&Request::default()).await;
    assert!(result.is_err());
    assert_eq!(result.unwrap_err(), Error::CheckoutTimeout);
    assert!(pool.lock().waiting.is_empty());
}

#[tokio::test]
async fn test_move_conns_to() {
    crate::logger();

    let config = Config {
        max: 3,
        min: 0,
        ..Default::default()
    };

    let source = Pool::new(&PoolConfig {
        address: Address {
            host: "127.0.0.1".into(),
            port: 5432,
            database_name: "pgdog".into(),
            user: "pgdog".into(),
            password: "pgdog".into(),
            ..Default::default()
        },
        config,
    });
    source.launch();

    let destination = Pool::new(&PoolConfig {
        address: Address {
            host: "127.0.0.1".into(),
            port: 5432,
            database_name: "pgdog".into(),
            user: "pgdog".into(),
            password: "pgdog".into(),
            ..Default::default()
        },
        config,
    });

    let conn1 = source.get(&Request::default()).await.unwrap();
    let conn2 = source.get(&Request::default()).await.unwrap();

    drop(conn1);

    sleep(Duration::from_millis(50)).await;

    assert_eq!(source.lock().idle(), 1);
    assert_eq!(source.lock().checked_out(), 1);
    assert_eq!(source.lock().total(), 2);
    assert_eq!(destination.lock().total(), 0);
    assert!(!destination.lock().online);

    source.move_conns_to(&destination).unwrap();

    assert!(!source.lock().online);
    assert!(destination.lock().online);
    assert_eq!(destination.lock().total(), 2);
    assert_eq!(source.lock().total(), 0);
    let new_pool_id = destination.id();
    for conn in destination.lock().idle_conns() {
        assert_eq!(conn.stats().pool_id, new_pool_id);
    }

    drop(conn2);

    sleep(Duration::from_millis(50)).await;

    assert_eq!(destination.lock().idle(), 2);
    assert_eq!(destination.lock().checked_out(), 0);
}

#[tokio::test]
async fn test_lsn_monitor() {
    crate::logger();

    let config = Config {
        max: 1,
        min: 1,
        lsn_check_delay: Duration::from_millis(10),
        lsn_check_interval: Duration::from_millis(50),
        lsn_check_timeout: Duration::from_millis(5_000),
        ..Default::default()
    };

    let pool = Pool::new(&PoolConfig {
        address: Address::new_test(),
        config,
    });

    let initial_stats = pool.lsn_stats();
    assert!(!initial_stats.valid());

    pool.launch();

    sleep(Duration::from_millis(200)).await;

    let stats = pool.lsn_stats();
    assert!(
        stats.valid(),
        "LSN stats should be valid after monitor runs"
    );
    assert!(!stats.replica, "Local PostgreSQL should not be a replica");
    assert!(stats.lsn.lsn > 0, "LSN should be greater than 0");
    assert!(
        stats.offset_bytes > 0,
        "Offset bytes should be greater than 0"
    );

    let age = stats.lsn_age(Instant::now());
    assert!(
        age < Duration::from_millis(300),
        "LSN stats age should be recent, got {:?}",
        age
    );

    pool.shutdown();
}
