//! Listen for OS signals that request a graceful shutdown.
//!
//! PgDog drains client connections on `Ctrl-C` / `SIGINT` *and* on `SIGTERM`.
//! `SIGTERM` is what Kubernetes' kubelet, systemd and `docker stop` send by
//! default, so handling it is what lets pods terminate cleanly instead of
//! being abruptly killed.

#[cfg(target_family = "unix")]
use tokio::signal::unix::{Signal, SignalKind, signal};

#[cfg(not(target_family = "unix"))]
use tokio::signal::ctrl_c;

#[cfg(target_family = "unix")]
use tokio::select;

/// Listener for shutdown signals (`SIGINT` and `SIGTERM`).
///
/// Construct this once and hold it for the lifetime of the loop that awaits
/// [`Shutdown::listen`]. The underlying signal streams are kept registered the
/// whole time, so a signal delivered between `listen` calls is still observed
/// on the next poll — recreating the streams on every iteration could drop a
/// `SIGTERM`, which is never redelivered by the kernel.
pub struct Shutdown {
    #[cfg(target_family = "unix")]
    interrupt: Signal,
    #[cfg(target_family = "unix")]
    terminate: Signal,
}

impl Shutdown {
    #[cfg(target_family = "unix")]
    pub(crate) fn new() -> std::io::Result<Self> {
        Ok(Self {
            interrupt: signal(SignalKind::interrupt())?,
            terminate: signal(SignalKind::terminate())?,
        })
    }

    #[cfg(not(target_family = "unix"))]
    pub(crate) fn new() -> std::io::Result<Self> {
        Ok(Self {})
    }

    /// Wait for the next shutdown signal (`SIGINT` or `SIGTERM`).
    pub(crate) async fn listen(&mut self) {
        #[cfg(target_family = "unix")]
        select! {
            _ = self.interrupt.recv() => {},
            _ = self.terminate.recv() => {},
        }

        #[cfg(not(target_family = "unix"))]
        let _ = ctrl_c().await;
    }
}

#[cfg(all(test, target_family = "unix"))]
mod test {
    use super::*;
    use std::time::Duration;
    use tokio::time::timeout;

    // Each test runs in its own process under nextest, so raising a real
    // signal here is isolated and safe.
    async fn assert_resolves_on(kind: libc::c_int) {
        let mut shutdown = Shutdown::new().unwrap();

        // SAFETY: sending a signal to our own process; a handler is already
        // installed by `Shutdown::new` above.
        unsafe {
            assert_eq!(libc::raise(kind), 0);
        }

        timeout(Duration::from_secs(5), shutdown.listen())
            .await
            .expect("shutdown.listen() should resolve after the signal");
    }

    #[tokio::test]
    async fn resolves_on_sigterm() {
        assert_resolves_on(libc::SIGTERM).await;
    }

    #[tokio::test]
    async fn resolves_on_sigint() {
        assert_resolves_on(libc::SIGINT).await;
    }
}
