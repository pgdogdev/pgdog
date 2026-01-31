use std::{
    ops::Deref,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use crate::{backend::databases::databases, config::config};

use super::{Error, PostgresProcess};
use once_cell::sync::Lazy;
use tokio::{
    select, spawn,
    sync::broadcast,
    time::{sleep, Duration},
};
use tracing::{error, info, warn};

static LAUNCHER: Lazy<PostgresLauncher> = Lazy::new(PostgresLauncher::new);

#[derive(Clone, Debug, PartialEq)]
pub enum LauncherEvent {
    // Commands
    Start,
    Shutdown,
    Reconfigure,

    // Status
    Ready,
    ShutdownComplete,
}

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
    events: broadcast::Sender<LauncherEvent>,
    ready: AtomicBool,
}

impl PostgresLauncher {
    fn new() -> Self {
        let (events, _) = broadcast::channel(16);

        let launcher = Self {
            inner: Arc::new(Inner {
                events,
                ready: AtomicBool::new(false),
            }),
        };

        // Subscribe before spawning to avoid race condition.
        let receiver = launcher.events.subscribe();
        launcher.spawn(receiver);
        launcher
    }

    /// Get the launcher singleton instance.
    pub(crate) fn get() -> Self {
        LAUNCHER.clone()
    }

    /// Get configured port.
    pub(crate) fn port(&self) -> u16 {
        config().config.fdw.port
    }

    /// Start the launcher. Idempotent.
    pub(crate) fn launch(&self) -> Result<(), Error> {
        self.events
            .send(LauncherEvent::Start)
            .map_err(|_| Error::ChannelClosed)?;
        Ok(())
    }

    pub(crate) fn shutdown(&self) -> Result<(), Error> {
        self.events
            .send(LauncherEvent::Shutdown)
            .map_err(|_| Error::ChannelClosed)?;
        Ok(())
    }

    /// Request reconfiguration.
    pub(crate) fn reconfigure(&self) -> Result<(), Error> {
        self.events
            .send(LauncherEvent::Reconfigure)
            .map_err(|_| Error::ChannelClosed)?;
        Ok(())
    }

    /// Shutdown and wait for completion.
    pub(crate) async fn shutdown_wait(&self) {
        // Subscribe before sending to avoid race condition.
        let receiver = self.events.subscribe();
        if self.events.send(LauncherEvent::Shutdown).is_err() {
            warn!("[fdw] shutdown channel closed, no receivers");
            return;
        }
        Self::wait_for(receiver, LauncherEvent::ShutdownComplete).await;
    }

    /// Wait for Postgres to be ready.
    pub(crate) async fn wait_ready(&self) {
        // Subscribe first to avoid race with Ready event.
        let receiver = self.events.subscribe();
        if self.ready.load(Ordering::Acquire) {
            return;
        }
        Self::wait_for(receiver, LauncherEvent::Ready).await;
    }

    async fn wait_for(mut receiver: broadcast::Receiver<LauncherEvent>, target: LauncherEvent) {
        loop {
            match receiver.recv().await {
                Ok(event) if event == target => return,
                Ok(_) => continue,
                Err(broadcast::error::RecvError::Closed) => return,
                Err(broadcast::error::RecvError::Lagged(_)) => continue,
            }
        }
    }

    fn send(&self, event: LauncherEvent) {
        match &event {
            LauncherEvent::Ready => self.ready.store(true, Ordering::Release),
            LauncherEvent::Start | LauncherEvent::Shutdown => {
                self.ready.store(false, Ordering::Release)
            }
            _ => {}
        }
        if self.events.send(event).is_err() {
            warn!("[fdw] failed to send event, no receivers");
        }
    }

    fn spawn(&self, receiver: broadcast::Receiver<LauncherEvent>) {
        let launcher = self.clone();

        spawn(async move {
            let mut receiver = receiver;

            loop {
                // Wait for Start or Shutdown.
                match receiver.recv().await {
                    Ok(LauncherEvent::Start) => {}
                    Ok(LauncherEvent::Shutdown) => {
                        launcher.send(LauncherEvent::ShutdownComplete);
                        continue;
                    }
                    Ok(_) => continue,
                    Err(broadcast::error::RecvError::Closed) => return,
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                }

                // Auto-restart on fatal errors (process crash, init failure).
                // Only a clean shutdown (Shutdown command) breaks out.
                loop {
                    info!("[fdw] launching fdw backend on 0.0.0.0:{}", launcher.port());

                    match launcher.run(&mut receiver).await {
                        Ok(()) => break,
                        Err(err) => {
                            error!("[fdw] launcher error: {}", err);
                            sleep(Duration::from_millis(1000)).await;

                            // Honor any Shutdown that arrived during recovery.
                            let mut shutdown = false;
                            while let Ok(event) = receiver.try_recv() {
                                if event == LauncherEvent::Shutdown {
                                    shutdown = true;
                                }
                            }
                            if shutdown {
                                launcher.send(LauncherEvent::ShutdownComplete);
                                break;
                            }
                        }
                    }
                }
            }
        });
    }

    async fn run(&self, receiver: &mut broadcast::Receiver<LauncherEvent>) -> Result<(), Error> {
        let port = self.port();
        let mut process = PostgresProcess::new(None, port).await?;
        let mut shutdown_receiver = process.shutdown_receiver();

        process.launch().await?;
        process.wait_ready().await;

        for cluster in databases().all().values() {
            if cluster.shards().len() > 1 {
                process.configure(cluster).await?;
            }
        }

        process.configuration_complete();

        self.send(LauncherEvent::Ready);

        loop {
            select! {
                event = receiver.recv() => {
                    match event {
                        Ok(LauncherEvent::Shutdown) => {
                            process.stop_wait().await;
                            self.send(LauncherEvent::ShutdownComplete);
                            return Ok(());
                        }

                        Ok(LauncherEvent::Reconfigure) => {
                            for cluster in databases().all().values() {
                                if cluster.shards().len() > 1 {
                                    if let Err(err) = process.configure(cluster).await {
                                        error!("[fdw] reconfigure error: {}", err);
                                    }
                                }
                            }

                            process.configuration_complete();
                        }

                        Ok(_) => continue,
                        Err(broadcast::error::RecvError::Closed) => {
                            process.stop_wait().await;
                            return Ok(());
                        }
                        Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    }
                }

                _ = shutdown_receiver.changed() => {
                    process.clear_pid();
                    self.send(LauncherEvent::ShutdownComplete);
                    return Err(Error::ProcessExited);
                }
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::backend::{pool::Address, ConnectReason, Server, ServerOptions};
    use crate::config::config;
    use tokio::time::timeout;

    fn test_launcher() -> PostgresLauncher {
        let (events, _) = broadcast::channel(16);
        let launcher = PostgresLauncher {
            inner: Arc::new(Inner {
                events,
                ready: AtomicBool::new(false),
            }),
        };
        let receiver = launcher.events.subscribe();
        launcher.spawn(receiver);
        launcher
    }

    #[tokio::test]
    async fn test_postgres_launcher() {
        crate::logger();
        let fdw = config().config.fdw;

        let address = Address {
            host: "127.0.0.1".into(),
            port: fdw.port,
            user: "postgres".into(),
            database_name: "postgres".into(),
            ..Default::default()
        };

        let launcher = PostgresLauncher::get();
        launcher.launch().unwrap();

        timeout(Duration::from_secs(10), launcher.wait_ready())
            .await
            .expect("timeout waiting for ready");

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
        let launcher = test_launcher();

        // Give spawn task time to start waiting.
        sleep(Duration::from_millis(10)).await;

        // Shutdown without ever starting - should not hang.
        timeout(Duration::from_secs(5), launcher.shutdown_wait())
            .await
            .expect("shutdown_wait() hung when FDW was never started");
    }

    #[tokio::test]
    async fn test_wait_ready_no_race() {
        // Test that wait_ready doesn't miss Ready event due to race condition.
        // Run multiple iterations to increase chance of hitting race window.
        for _ in 0..100 {
            let launcher = test_launcher();

            // Spawn task that sends Ready immediately.
            let launcher_clone = launcher.clone();
            spawn(async move {
                launcher_clone.send(LauncherEvent::Ready);
            });

            // wait_ready should not hang even if Ready is sent
            // between subscribe and wait.
            timeout(Duration::from_millis(100), launcher.wait_ready())
                .await
                .expect("wait_ready() missed Ready event - race condition");
        }
    }

    #[tokio::test]
    async fn test_shutdown_wait_no_race() {
        // Test that shutdown_wait doesn't miss ShutdownComplete due to race.
        for _ in 0..100 {
            let launcher = test_launcher();

            // Give spawn task time to start.
            sleep(Duration::from_millis(1)).await;

            // shutdown_wait sends Shutdown and waits for ShutdownComplete.
            // The spawn loop should receive Shutdown and send ShutdownComplete.
            // This should not hang even with tight timing.
            timeout(Duration::from_millis(100), launcher.shutdown_wait())
                .await
                .expect("shutdown_wait() missed ShutdownComplete - race condition");
        }
    }

    #[tokio::test]
    async fn test_start_after_shutdown() {
        let launcher = test_launcher();
        sleep(Duration::from_millis(10)).await;

        // Shutdown while not running.
        timeout(Duration::from_secs(5), launcher.shutdown_wait())
            .await
            .expect("shutdown_wait timed out");

        // The spawn loop should still be alive and accept a new Ready event.
        let launcher_clone = launcher.clone();
        spawn(async move {
            sleep(Duration::from_millis(5)).await;
            launcher_clone.send(LauncherEvent::Ready);
        });

        timeout(Duration::from_millis(100), launcher.wait_ready())
            .await
            .expect("spawn loop died after shutdown - Start would be lost");
    }

    #[tokio::test]
    async fn test_concurrent_wait_ready() {
        // Multiple tasks waiting for Ready concurrently.
        let launcher = test_launcher();

        let mut handles = vec![];
        for _ in 0..10 {
            let l = launcher.clone();
            handles.push(spawn(async move {
                timeout(Duration::from_millis(100), l.wait_ready())
                    .await
                    .expect("concurrent wait_ready timed out");
            }));
        }

        // Small delay then send Ready.
        sleep(Duration::from_millis(5)).await;
        launcher.send(LauncherEvent::Ready);

        for handle in handles {
            handle.await.unwrap();
        }
    }
}
