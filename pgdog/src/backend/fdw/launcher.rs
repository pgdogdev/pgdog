use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use super::{Error, PostgresProcess};
use tokio::{
    select, spawn,
    sync::Notify,
    time::{sleep, Duration},
};
use tracing::error;

#[derive(Debug, Clone)]
pub(crate) struct PostgresLauncher {
    shutdown: Arc<Notify>,
    online: Arc<AtomicBool>,
}

impl PostgresLauncher {
    pub(crate) async fn spawn(&self) {
        let launcher = self.clone();

        spawn(async move {
            loop {
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
        self.shutdown.notify_waiters();
    }

    async fn run(&self) -> Result<(), Error> {
        let mut process = PostgresProcess::new(None, 45000)?;
        let waiter = process.notify();

        process.launch().await?;

        select! {
            _ = self.shutdown.notified() => {
                process.stop().await;
            }

            _ = waiter.notified() => {
                // Unexpected exit.
            }
        }

        Ok(())
    }
}
