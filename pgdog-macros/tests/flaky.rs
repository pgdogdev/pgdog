//! Tests for the `#[flaky]` retry attribute macro.
//!
//! Each test uses an atomic counter so that it panics on its first few
//! attempts and only succeeds once `#[flaky]` has retried it enough times.
//! If retries did not happen, these tests would fail.

use std::sync::atomic::{AtomicUsize, Ordering};

use pgdog_macros::flaky;

/// Panic on the first `fail_until - 1` calls, succeed afterwards. Returns the
/// 1-based attempt number that was just executed.
fn flake(counter: &AtomicUsize, fail_until: usize) -> usize {
    let attempt = counter.fetch_add(1, Ordering::SeqCst) + 1;
    assert!(
        attempt >= fail_until,
        "induced flake on attempt {attempt} (need {fail_until})"
    );
    attempt
}

static SYNC_DEFAULT: AtomicUsize = AtomicUsize::new(0);

#[flaky]
#[test]
fn sync_default_retries() {
    // Fails twice, passes on the 3rd (default) attempt.
    let attempt = flake(&SYNC_DEFAULT, 3);
    assert_eq!(attempt, 3);
}

static SYNC_EXPLICIT: AtomicUsize = AtomicUsize::new(0);

#[flaky(5)]
#[test]
fn sync_explicit_count() {
    let attempt = flake(&SYNC_EXPLICIT, 5);
    assert_eq!(attempt, 5);
}

static ASYNC_DEFAULT: AtomicUsize = AtomicUsize::new(0);

#[flaky]
#[tokio::test]
async fn async_default_retries() {
    tokio::task::yield_now().await;
    let attempt = flake(&ASYNC_DEFAULT, 3);
    tokio::task::yield_now().await;
    assert_eq!(attempt, 3);
}

static ASYNC_EXPLICIT: AtomicUsize = AtomicUsize::new(0);

#[flaky(4)]
#[tokio::test]
async fn async_explicit_count() {
    tokio::task::yield_now().await;
    let attempt = flake(&ASYNC_EXPLICIT, 4);
    assert_eq!(attempt, 4);
}

// A test that never flakes must still pass and only run once.
static SYNC_STABLE: AtomicUsize = AtomicUsize::new(0);

#[flaky(3)]
#[test]
fn sync_stable_runs_once() {
    SYNC_STABLE.fetch_add(1, Ordering::SeqCst);
    assert_eq!(SYNC_STABLE.load(Ordering::SeqCst), 1);
}

// `#[flaky]` should preserve a `Result`-returning async test signature.
#[flaky]
#[tokio::test]
async fn async_returns_result() -> Result<(), String> {
    tokio::task::yield_now().await;
    Ok(())
}

// When every attempt fails, the final (uncaught) attempt must propagate the
// original panic message. `#[should_panic(expected = ...)]` proves both that
// the budget is exhausted and that the original message survives.
#[flaky(2)]
#[test]
#[should_panic(expected = "boom")]
fn exhausts_budget_then_panics() {
    panic!("boom");
}

#[flaky(2)]
#[tokio::test]
#[should_panic(expected = "async boom")]
async fn async_exhausts_budget_then_panics() {
    tokio::task::yield_now().await;
    panic!("async boom");
}
