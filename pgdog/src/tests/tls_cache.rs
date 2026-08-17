use std::{
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};

// Timing tests are flaky by their nature.
#[pgdog_macros::flaky]
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn tls_acceptor_reuse_is_fast() {
    crate::logger();

    crate::net::tls::test_reset_acceptor();

    let base = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/tls");
    let cert = base.join("cert.pem");
    let key = base.join("key.pem");

    let mut cfg = crate::config::ConfigAndUsers::default();
    cfg.config.general.tls_certificate = Some(cert.clone());
    cfg.config.general.tls_private_key = Some(key.clone());
    crate::config::set(cfg).unwrap();

    let build_start = Instant::now();
    crate::net::tls::reload().unwrap();
    let _build_time = build_start.elapsed();

    let snapshot = crate::net::tls::acceptor().expect("acceptor initialized");

    let reuse_start = Instant::now();
    for _ in 0..10 {
        let candidate = crate::net::tls::acceptor().expect("acceptor initialized");
        assert!(Arc::ptr_eq(&snapshot, &candidate));
    }
    let reuse_time = reuse_start.elapsed();

    assert!(
        reuse_time < Duration::from_millis(5),
        "cached acceptor clones should be fast: {:?}",
        reuse_time
    );

    assert_eq!(crate::net::tls::test_acceptor_build_count(), 1);

    crate::net::tls::test_reset_acceptor();
    crate::config::set(crate::config::ConfigAndUsers::default()).unwrap();
}
