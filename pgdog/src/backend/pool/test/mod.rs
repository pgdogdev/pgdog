//! Pool tests.

use std::time::Duration;

use tokio::time::timeout;

use crate::net::messages::BackendKeyData;

use super::*;

fn pool() -> Pool {
    let mut config = Config::default();
    config.max = 1;
    config.min = 1;

    let pool = Pool::new(PoolConfig {
        address: Address {
            host: "127.0.0.1".into(),
            port: 5432,
            database_name: "pgdog".into(),
            user: "pgdog".into(),
            password: "pgdog".into(),
        },
        config,
    });

    pool
}

#[tokio::test(flavor = "current_thread")]
async fn test_pool_checkout() {
    crate::logger();

    let pool = pool();
    println!("{:?}", pool.lock().config);
    println!("checking out");
    let conn = pool.get(&BackendKeyData::new()).await.unwrap();
    println!("checked out");

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
    assert!(err.is_err());
}
