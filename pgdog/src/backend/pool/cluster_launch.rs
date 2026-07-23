//! Cluster startup and shutdown primitives.
//!
//! Launching and shutting down the connection pools, and gating
//! traffic until boot-time maintenance has completed.

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use tokio::{select, time::sleep};
use tokio_util::sync::CancellationToken;
use tracing::error;

use crate::backend::pool::ee::schema_changed_hook;
use crate::tasks;

use super::Cluster;

/// Cluster readiness state.
#[derive(Default, Debug)]
pub(super) struct Readiness {
    online: AtomicBool,
    launch_waiter: CancellationToken,
}

impl Readiness {
    fn set_online(&self, online: bool) {
        self.online.store(online, Ordering::Relaxed);
    }

    fn online(&self) -> bool {
        self.online.load(Ordering::Relaxed)
    }

    fn mark_ready(&self) {
        self.launch_waiter.cancel();
    }

    #[cfg(test)]
    fn ready(&self) -> bool {
        self.launch_waiter.is_cancelled()
    }

    async fn wait_ready(&self) {
        self.launch_waiter.cancelled().await;
    }
}

impl Cluster {
    /// Launch the connection pools.
    pub(crate) fn launch(&self) {
        for shard in self.shards() {
            shard.launch();
        }

        self.readiness.set_online(true);

        self.launch_schema_sync();
        self.launch_readiness_monitor();
    }

    /// Shutdown the connection pools.
    pub(crate) fn shutdown(&self) {
        for shard in self.shards() {
            shard.shutdown();
        }

        self.readiness.set_online(false);

        // Release readiness waiters, the cluster is going away.
        self.mark_ready();
    }

    /// Is the cluster online?
    pub(crate) fn online(&self) -> bool {
        self.readiness.online()
    }

    /// Wait until boot-time maintenance is done and the cluster can serve traffic.
    pub(crate) async fn wait_ready(&self) {
        self.readiness.wait_ready().await;
    }

    /// Boot-time maintenance is done and the cluster can serve traffic.
    #[cfg(test)]
    pub(crate) fn ready(&self) -> bool {
        self.readiness.ready()
    }

    /// Mark the cluster as ready to serve traffic.
    pub(crate) fn mark_ready(&self) {
        self.readiness.mark_ready();
    }

    /// Mark the cluster ready schema loading is done.
    fn launch_readiness_monitor(&self) {
        let cluster = self.clone();
        tasks::spawn("cluster readiness monitor", async move {
            let shutdown = tasks::shutdown_signal();
            select! {
                _ = cluster.wait_schema_loaded() => {}
                _ = shutdown.cancelled() => {}
            }
            cluster.mark_ready();
        });
    }

    /// Wait for the schema to load on all shards.
    async fn wait_schema_loaded(&self) {
        if !self.load_schema() {
            return;
        }

        for shard in self.shards() {
            shard.wait_schema_loaded().await;
        }
    }

    /// Load database schema in the background, retrying
    /// until success or shutdown.
    fn launch_schema_sync(&self) {
        if !self.load_schema() {
            for shard in self.shards() {
                shard.schema_not_needed();
            }
            return;
        }

        for shard in self.shards() {
            let identifier = self.identifier();
            let shard = shard.clone();
            let shutdown = tasks::shutdown_signal();

            tasks::spawn("shard schema sync", async move {
                loop {
                    let loader = shard.load_schema();
                    let result = select! {
                        _ = shutdown.cancelled() => break,
                        result = loader => { result },
                    };

                    match result {
                        Ok(true) => {
                            schema_changed_hook(&shard.schema(), &identifier, &shard);
                            return;
                        }
                        Ok(false) => return,
                        Err(err) => {
                            if shard.online() {
                                error!(
                                    "error loading schema for shard {}: {}",
                                    shard.number(),
                                    err
                                );
                                sleep(Duration::from_millis(100)).await;
                            } else {
                                // Cluster is shutting down: unblock any
                                // wait_schema_loaded callers.
                                shard.schema_not_needed();
                                return;
                            }
                        }
                    }
                }
            });
        }
    }
}
