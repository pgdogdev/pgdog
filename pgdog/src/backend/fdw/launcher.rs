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
    sync::Notify,
    time::{sleep, Duration},
};
use tracing::{error, info};

const LAUNCHER: Lazy<PostgresLauncher> = Lazy::new(PostgresLauncher::new);

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

#[derive(Debug, Default)]
pub struct Inner {
    restart: Notify,
    online: AtomicBool,
    port: AtomicU16,
    ready_signal: Notify,
    ready: AtomicBool,
}

impl PostgresLauncher {
    fn new() -> Self {
        let fdw = config().config.fdw;
        let port = AtomicU16::new(fdw.blue_port);

        let laucher = Self {
            inner: Arc::new(Inner {
                port,
                ..Default::default()
            }),
        };

        laucher.spawn();

        laucher
    }

    /// Get the launcher singleton instance.
    pub(crate) fn get() -> Self {
        LAUNCHER.clone()
    }

    /// Start the launcher.
    ///
    /// Idempontent.
    pub(crate) fn launch(&self) {
        let online = self.online.load(Ordering::Relaxed);

        if online {
            return;
        }

        self.launch_blue_green();
    }

    fn spawn(&self) {
        let launcher = self.clone();

        spawn(async move {
            let online = launcher.online.load(Ordering::Relaxed);

            if !online {
                launcher.restart.notified().await;
            }

            let online = launcher.online.load(Ordering::Relaxed);

            if !online {
                launcher.ready_signal.notify_waiters();
                return;
            }

            loop {
                info!(
                    "[fdw] launching fdw backend on 0.0.0.0:{}",
                    launcher.port.load(Ordering::Relaxed),
                );

                if let Err(err) = launcher.run().await {
                    error!("[fdw] launcher exited with error: {}", err);
                }

                let online = launcher.online.load(Ordering::Relaxed);
                if !online {
                    break;
                } else {
                    sleep(Duration::from_millis(1000)).await;
                }
            }
        });
    }

    pub(crate) fn shutdown(&self) {
        self.online.store(false, Ordering::Relaxed);
        self.ready.store(false, Ordering::Relaxed);
        self.restart.notify_waiters();
    }

    pub(crate) async fn shutdown_wait(&self) {
        self.shutdown();
        self.wait_ready().await
    }

    /// Trigger blue/green deployment.
    pub(crate) fn launch_blue_green(&self) {
        let fdw = config().config.fdw;
        let port = self.port.load(Ordering::Relaxed);
        let port = if port == fdw.blue_port {
            fdw.green_port
        } else {
            fdw.blue_port
        };

        self.port.store(port, Ordering::Relaxed);
        self.ready.store(false, Ordering::Relaxed);
        self.online.store(true, Ordering::Relaxed);
        self.restart.notify_waiters();
    }

    /// Wait for Postgres to be ready.
    pub(crate) async fn wait_ready(&self) {
        let ready = self.ready.load(Ordering::Relaxed);

        if ready {
            return;
        }

        let waiter = self.ready_signal.notified();
        let ready = self.ready.load(Ordering::Relaxed);

        if ready {
            return;
        }

        waiter.await;
    }

    fn mark_ready(&self) {
        self.ready.store(true, Ordering::Relaxed);
        self.ready_signal.notify_waiters();
    }

    async fn run(&self) -> Result<(), Error> {
        let port = self.port.load(Ordering::Relaxed);
        let mut process = PostgresProcess::new(None, port).await?;
        let waiter = process.notify();

        process.launch().await?;
        process.wait_ready().await;

        for cluster in databases().all().values() {
            if cluster.shards().len() > 1 {
                process.configure(cluster).await?;
            }
        }

        self.mark_ready();

        select! {
            _ = self.restart.notified() => {
                let online = self.online.load(Ordering::Relaxed);
                if online {
                    process.request_stop();
                } else {
                    process.stop_wait().await;
                    self.mark_ready();
                }
            }

            _ = waiter.notified() => {
                // Unexpected exit.
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::backend::{pool::Address, ConnectReason, Server, ServerOptions};

    #[tokio::test]
    async fn test_postgres_blue_green() {
        crate::logger();
        let mut address = Address {
            host: "127.0.0.1".into(),
            port: 6433,
            user: "postgres".into(),
            database_name: "postgres".into(),
            ..Default::default()
        };

        let launcher = PostgresLauncher::get();
        launcher.launch_blue_green();
        launcher.wait_ready().await;
        let mut conn =
            Server::connect(&address, ServerOptions::default(), ConnectReason::default())
                .await
                .unwrap();
        conn.execute("SELECT 1").await.unwrap();
        drop(conn);
        launcher.launch_blue_green();
        launcher.wait_ready().await;

        address.port = 6434;
        let mut conn =
            Server::connect(&address, ServerOptions::default(), ConnectReason::default())
                .await
                .unwrap();
        conn.execute("SELECT 1").await.unwrap();
        launcher.shutdown();
        launcher.wait_ready().await;
    }
}
