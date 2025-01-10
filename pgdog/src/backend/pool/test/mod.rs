//! Pool tests.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use rand::Rng;
use tokio::task::yield_now;
use tokio::time::{sleep, timeout};
use tokio_util::task::TaskTracker;

use crate::net::messages::BackendKeyData;

use super::*;

mod replica;

fn pool() -> Pool {
    let mut config = Config::default();
    config.max = 1;
    config.min = 1;

    Pool::new(PoolConfig {
        address: Address {
            host: "127.0.0.1".into(),
            port: 5432,
            database_name: "pgdog".into(),
            user: "pgdog".into(),
            password: "pgdog".into(),
        },
        config,
    })
}

#[tokio::test(flavor = "current_thread")]
async fn test_pool_checkout() {
    crate::logger();

    let pool = pool();
    let conn = pool.get(&BackendKeyData::new()).await.unwrap();
    let id = *(conn.id());

    assert!(conn.in_sync());
    assert!(conn.done());
    assert!(!conn.in_transaction());
    assert!(!conn.error());

    assert_eq!(pool.lock().idle(), 0);
    assert_eq!(pool.lock().total(), 1);
    assert!(!pool.lock().can_create());
    assert!(!pool.lock().should_create());

    let err = timeout(Duration::from_millis(100), pool.get(&BackendKeyData::new())).await;

    assert_eq!(pool.lock().total(), 1);
    assert_eq!(pool.lock().idle(), 0);
    assert!(err.is_err());

    drop(conn); // Return conn to the pool.
    let conn = pool.get(&BackendKeyData::new()).await.unwrap();
    assert_eq!(conn.id(), &id);
}

#[tokio::test]
async fn test_concurrency() {
    let pool = pool();
    let tracker = TaskTracker::new();

    for _ in 0..1000 {
        let pool = pool.clone();
        tracker.spawn(async move {
            let _conn = pool.get(&BackendKeyData::new()).await.unwrap();
            let duration = rand::thread_rng().gen_range(0..10);
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

    let mut config = Config::default();
    config.max = 10;
    pool.update_config(config);

    for _ in 0..10000 {
        let pool = pool.clone();
        tracker.spawn(async move {
            let _conn = pool.get(&BackendKeyData::new()).await.unwrap();
            let duration = rand::thread_rng().gen_range(0..10);
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
async fn test_bans() {
    let pool = pool();

    pool.ban(Error::CheckoutTimeout);
    assert!(pool.banned());

    // Can still get a connection from the pool.
    let _conn = pool.get(&BackendKeyData::new()).await.unwrap();
}

#[tokio::test]
async fn test_offline() {
    let pool = pool();
    assert!(pool.lock().online);

    pool.shutdown();
    assert!(!pool.lock().online);
    assert!(!pool.banned());

    // Cannot get a connection from the pool.
    let err = pool.get(&BackendKeyData::new()).await;
    err.expect_err("pool is shut down");
}

#[tokio::test]
async fn test_pause() {
    let pool = pool();
    let tracker = TaskTracker::new();
    let mut config = Config::default();
    config.checkout_timeout = 50;
    config.max = 1;
    pool.update_config(config);

    let hold = pool.get(&BackendKeyData::new()).await.unwrap();
    pool.get(&BackendKeyData::new())
        .await
        .expect_err("checkout timeout");
    drop(hold);
    // Make sure we're not blocked still.
    drop(pool.get(&BackendKeyData::new()).await.unwrap());

    pool.pause();

    // We'll hit the timeout now because we're waiting forever.
    let pause = Duration::from_millis(500);
    assert!(timeout(pause, pool.get(&BackendKeyData::new()))
        .await
        .is_err());

    // Spin up a bunch of clients and make them wait for
    // a connection while the pool is paused.
    for _ in 0..1000 {
        let pool = pool.clone();
        tracker.spawn(async move {
            let _conn = pool.get(&BackendKeyData::new()).await.unwrap();
        });
    }

    pool.resume();
    tracker.close();
    tracker.wait().await;

    assert!(pool.get(&BackendKeyData::new()).await.is_ok());

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
                .get(&BackendKeyData::new())
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
