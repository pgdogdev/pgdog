use std::{sync::Arc, time::Duration};

use pgdog_config::{ConfigAndUsers, User};
use tokio::{sync::oneshot, task::yield_now, time::advance};

use super::{Key, THROTTLE, check_db};

const DEBOUNCE: Duration = Duration::from_millis(500);

fn setup() {
    let mut config = ConfigAndUsers::default();
    config.config.general.passthrough_auth_debounce_delay = DEBOUNCE.as_millis() as u64;
    crate::config::set(config).expect("set debounce config");
}

// Seed the result of database validation so these tests exercise the actual
// cache and expiration path without requiring PostgreSQL. With no database in
// the config, a fresh check returns false rather than the seeded success.
#[tokio::test(start_paused = true)]
async fn debounce_shares_in_flight_success_and_failure() {
    setup();

    for result in [true, false] {
        let user = User::new(&format!("concurrent_{result}"), "secret", "db");
        let key = Key::new(&user);
        let entry = THROTTLE.entry(key.clone()).or_default().clone();
        let (started, waiting) = oneshot::channel();
        let (release, released) = oneshot::channel();
        let validation = tokio::spawn(async move {
            *entry
                .get_or_init(async || {
                    started.send(()).expect("signal validation started");
                    released.await.expect("release validation");
                    result
                })
                .await
        });
        waiting.await.expect("wait for validation to start");

        let mut clients = Vec::new();
        for _ in 0..16 {
            let user = user.clone();
            clients.push(tokio::spawn(async move { check_db(&user).await }));
        }
        yield_now().await;
        assert!(clients.iter().all(|client| !client.is_finished()));

        release.send(()).expect("finish validation");
        assert_eq!(validation.await.expect("join validation"), result);
        for client in clients {
            assert_eq!(client.await.expect("join client"), result);
        }
        assert_eq!(check_db(&user).await, result);
        assert!(THROTTLE.contains_key(&key));
    }
}

#[tokio::test(start_paused = true)]
async fn debounce_expires_success_and_failure() {
    setup();

    for result in [true, false] {
        let user = User::new(&format!("expiry_{result}"), "secret", "db");
        let key = Key::new(&user);
        let entry = THROTTLE.entry(key.clone()).or_default().clone();
        entry.set(result).expect("seed validation result");

        assert_eq!(check_db(&user).await, result);
        // Let the cleanup task start its timer before advancing virtual time.
        yield_now().await;
        advance(DEBOUNCE / 2).await;
        assert_eq!(check_db(&user).await, result);
        assert!(Arc::ptr_eq(
            &entry,
            &THROTTLE.get(&key).expect("cached check")
        ));

        advance(DEBOUNCE / 2 + Duration::from_millis(1)).await;
        yield_now().await;
        assert!(!THROTTLE.contains_key(&key));

        // A repeated hit must not keep this password suppressed forever.
        assert!(!check_db(&user).await);
        assert!(!Arc::ptr_eq(
            &entry,
            &THROTTLE.get(&key).expect("fresh check")
        ));
    }
}

#[tokio::test(start_paused = true)]
async fn debounce_is_scoped_to_user_database_and_password() {
    setup();
    let user = User::new("scoped", "secret", "db");
    THROTTLE
        .entry(Key::new(&user))
        .or_default()
        .set(true)
        .expect("seed successful validation");

    for candidate in [
        User::new("other", "secret", "db"),
        User::new("scoped", "secret", "other_db"),
        User::new("scoped", "different", "db"),
    ] {
        assert!(!check_db(&candidate).await);
    }
    assert!(check_db(&user).await);
}

#[tokio::test(start_paused = true)]
async fn debounce_cancelled_waiter_allows_fresh_validation_after_expiry() {
    setup();
    let user = User::new("cancelled", "secret", "db");
    let key = Key::new(&user);
    let entry = THROTTLE.entry(key.clone()).or_default().clone();
    let (started, waiting) = oneshot::channel();
    let validation = tokio::spawn(async move {
        entry
            .get_or_init(async || {
                started.send(()).expect("signal validation started");
                std::future::pending::<bool>().await
            })
            .await;
    });
    waiting.await.expect("wait for validation to start");

    let waiter = tokio::spawn({
        let user = user.clone();
        async move { check_db(&user).await }
    });
    yield_now().await;
    assert!(!waiter.is_finished());
    waiter.abort();
    assert!(waiter.await.expect_err("waiter cancelled").is_cancelled());
    yield_now().await;

    advance(DEBOUNCE + Duration::from_millis(1)).await;
    yield_now().await;
    assert!(!THROTTLE.contains_key(&key));
    assert!(!validation.is_finished());
    // Expiration intentionally permits another check while the old one hangs.
    assert!(!check_db(&user).await);
    validation.abort();
    assert!(
        validation
            .await
            .expect_err("validation cancelled")
            .is_cancelled()
    );
}
