use std::{
    ops::Deref,
    sync::{
        atomic::{AtomicBool, AtomicU16, Ordering},
        Arc,
    },
};

use crate::{backend::databases::databases, config::config};

use super::{Error, PostgresProcess};
use once_cell::sync::Lazy;
use tokio::{
    select, spawn,
    sync::watch,
    time::{sleep, Duration},
};
use tracing::{error, info, warn};

static LAUNCHER: Lazy<PostgresLauncher> = Lazy::new(PostgresLauncher::new);

#[derive(Debug, Clone)]
pub struct PostgresLauncher {
    inner: Arc<Inner>,
}

impl Deref for PostgresLauncher {
    type Target = Inner;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[derive(Debug)]
pub struct Inner {
    /// Incremented to trigger restart/shutdown check in the spawn loop.
    restart_trigger: watch::Sender<u64>,
    /// Whether the launcher should be running.
    online: AtomicBool,
    /// True when shutdown has been requested (used to detect early shutdown).
    shutdown_requested: AtomicBool,
    /// Current blue/green port.
    port: AtomicU16,
    /// True when postgres is ready to accept connections.
    ready: watch::Sender<bool>,
    /// True when shutdown is complete.
    shutdown_complete: watch::Sender<bool>,
}

impl PostgresLauncher {
    fn new() -> Self {
        let fdw = config().config.fdw;
        let port = AtomicU16::new(fdw.blue_port);

        let (restart_trigger, _) = watch::channel(0u64);
        let (ready, _) = watch::channel(false);
        let (shutdown_complete, _) = watch::channel(false);

        let launcher = Self {
            inner: Arc::new(Inner {
                restart_trigger,
                online: AtomicBool::new(false),
                shutdown_requested: AtomicBool::new(false),
                port,
                ready,
                shutdown_complete,
            }),
        };

        launcher.spawn();
        launcher
    }

    /// Get the launcher singleton instance.
    pub(crate) fn get() -> Self {
        LAUNCHER.clone()
    }

    /// Get current blue/green port.
    pub(crate) fn port(&self) -> u16 {
        self.port.load(Ordering::Relaxed)
    }

    /// Start the launcher.
    ///
    /// Idempontent.
    pub(crate) fn launch(&self) {
        if self.online.load(Ordering::Relaxed) {
            return;
        }

        self.launch_blue_green(false);
    }

    fn spawn(&self) {
        let launcher = self.clone();

        spawn(async move {
            let mut restart_receiver = launcher.restart_trigger.subscribe();

            loop {
                let online = launcher.online.load(Ordering::Relaxed);

                if !online {
                    // Check if shutdown was already requested before we even started.
                    if launcher.shutdown_requested.load(Ordering::Acquire) {
                        launcher.ready.send_modify(|v| *v = true);
                        launcher.mark_shutdown();
                        return;
                    }

                    // Wait for trigger (launch or shutdown).
                    let _ = restart_receiver.changed().await;

                    // Re-check if this was a shutdown request.
                    if launcher.shutdown_requested.load(Ordering::Acquire) {
                        launcher.ready.send_modify(|v| *v = true);
                        launcher.mark_shutdown();
                        return;
                    }
                }

                info!(
                    "[fdw] launching fdw backend on 0.0.0.0:{}",
                    launcher.port.load(Ordering::Relaxed),
                );

                let had_error = launcher.run(&mut restart_receiver).await.is_err();
                if had_error {
                    error!("[fdw] launcher exited with error");
                }

                let online = launcher.online.load(Ordering::Relaxed);
                if !online {
                    break;
                }

                // Delay retry only on error to prevent tight loops.
                if had_error {
                    sleep(Duration::from_millis(1000)).await;
                }
            }

            // Signal shutdown complete when exiting the loop.
            launcher.mark_shutdown();
        });
    }

    pub(crate) fn shutdown(&self) {
        self.shutdown_requested.store(true, Ordering::Release);
        self.online.store(false, Ordering::Relaxed);
        self.ready.send_modify(|v| *v = false);
        self.shutdown_complete.send_modify(|v| *v = false);
        self.restart_trigger.send_modify(|v| *v = v.wrapping_add(1));
    }

    pub(crate) async fn shutdown_wait(&self) {
        self.shutdown();
        self.wait_shutdown().await;
    }

    /// Trigger blue/green deployment.
    pub(crate) fn launch_blue_green(&self, failover: bool) {
        let fdw = config().config.fdw;
        let port = self.port.load(Ordering::Relaxed);

        let port = if failover {
            if port == fdw.blue_port {
                fdw.green_port
            } else {
                fdw.blue_port
            }
        } else {
            port
        };

        warn!("[fdw] relaunching fdw backend on 0.0.0.0:{}", port);

        self.port.store(port, Ordering::Relaxed);
        // Use send_modify to ensure value is updated even without receivers.
        self.ready.send_modify(|v| *v = false);
        self.shutdown_complete.send_modify(|v| *v = false);
        self.online.store(true, Ordering::Relaxed);
        self.restart_trigger.send_modify(|v| *v = v.wrapping_add(1));
    }

    /// Wait for Postgres to be ready.
    pub(crate) async fn wait_ready(&self) {
        let mut receiver = self.ready.subscribe();

        // Check current state first.
        if *receiver.borrow() {
            return;
        }

        // Wait for ready to become true.
        while receiver.changed().await.is_ok() {
            if *receiver.borrow() {
                return;
            }
        }
    }

    fn mark_ready(&self) {
        self.ready.send_modify(|v| *v = true);
    }

    async fn run(&self, restart_receiver: &mut watch::Receiver<u64>) -> Result<(), Error> {
        let port = self.port();
        let mut process = PostgresProcess::new(None, port).await?;
        let mut shutdown_receiver = process.shutdown_receiver();

        // Use a closure to ensure process is stopped on any exit path.
        let result = async {
            process.launch().await?;
            process.wait_ready().await;

            for cluster in databases().all().values() {
                if cluster.shards().len() > 1 {
                    process.configure(cluster).await?;
                }
            }

            self.mark_ready();

            select! {
                _ = restart_receiver.changed() => {
                    let online = self.online.load(Ordering::Relaxed);
                    if online {
                        process.request_stop();
                    } else {
                        process.stop_wait().await;
                    }
                }

                _ = shutdown_receiver.changed() => {
                    // Process exited (possibly unexpectedly).
                    // Clear pid to prevent dirty shutdown warning since process already exited.
                    process.clear_pid();
                }
            }

            Ok::<(), Error>(())
        }
        .await;

        // Ensure process is stopped if we're exiting due to an error.
        if result.is_err() {
            process.stop_wait().await;
        }

        self.mark_shutdown();

        result
    }

    fn mark_shutdown(&self) {
        self.shutdown_complete.send_modify(|v| *v = true);
    }

    async fn wait_shutdown(&self) {
        let mut receiver = self.shutdown_complete.subscribe();

        // Check current state first.
        if *receiver.borrow() {
            return;
        }

        // Wait for shutdown_complete to become true.
        while receiver.changed().await.is_ok() {
            if *receiver.borrow() {
                return;
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::backend::{pool::Address, ConnectReason, Server, ServerOptions};
    use crate::config::config;

    #[tokio::test]
    async fn test_postgres_blue_green() {
        use tokio::time::timeout;

        crate::logger();
        let fdw = config().config.fdw;

        let mut address = Address {
            host: "127.0.0.1".into(),
            port: fdw.blue_port,
            user: "postgres".into(),
            database_name: "postgres".into(),
            ..Default::default()
        };

        let launcher = PostgresLauncher::get();
        launcher.launch_blue_green(false);

        timeout(Duration::from_secs(10), launcher.wait_ready())
            .await
            .expect("timeout waiting for first ready");

        let mut conn =
            Server::connect(&address, ServerOptions::default(), ConnectReason::default())
                .await
                .unwrap();
        conn.execute("SELECT 1").await.unwrap();
        drop(conn);

        launcher.launch_blue_green(true);

        timeout(Duration::from_secs(10), launcher.wait_ready())
            .await
            .expect("timeout waiting for second ready");

        address.port = fdw.green_port;
        let mut conn =
            Server::connect(&address, ServerOptions::default(), ConnectReason::default())
                .await
                .unwrap();
        conn.execute("SELECT 1").await.unwrap();

        timeout(Duration::from_secs(10), launcher.shutdown_wait())
            .await
            .expect("timeout waiting for shutdown");
    }

    #[tokio::test]
    async fn test_shutdown_without_start() {
        use tokio::time::timeout;

        // Test that shutdown_wait() works even if FDW was never started.
        // This creates a new launcher directly (not the singleton) to avoid
        // interference from other tests.
        let (restart_trigger, _) = watch::channel(0u64);
        let (ready, _) = watch::channel(false);
        let (shutdown_complete, _) = watch::channel(false);

        let launcher = PostgresLauncher {
            inner: Arc::new(Inner {
                restart_trigger,
                online: AtomicBool::new(false),
                shutdown_requested: AtomicBool::new(false),
                port: AtomicU16::new(6433),
                ready,
                shutdown_complete,
            }),
        };

        launcher.spawn();

        // Give spawn task time to start waiting
        sleep(Duration::from_millis(10)).await;

        // Shutdown without ever starting - should not hang
        timeout(Duration::from_secs(5), launcher.shutdown_wait())
            .await
            .expect("shutdown_wait() hung when FDW was never started");
    }
}
