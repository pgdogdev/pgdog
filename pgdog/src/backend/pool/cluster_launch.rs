//! Cluster startup and shutdown primitives.
//!
//! Launching and shutting down the connection pools, and gating
//! traffic until boot-time maintenance (two-phase commit cleanup,
//! schema sync) has completed.

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use tokio::time::{sleep, timeout};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::backend::pool::ee::schema_changed_hook;
use crate::frontend::client::query_engine::two_pc::Manager;
use crate::tasks;

use super::Cluster;

/// Cluster readiness state.
///
/// `online` is set once the pools are launched and cleared on shutdown.
/// `launch_waiter` is cancelled once boot-time maintenance — two-phase
/// commit cleanup and schema loading — is done and the cluster can
/// serve traffic.
#[derive(Default, Debug)]
pub(super) struct Readiness {
    online: AtomicBool,
    launch_waiter: CancellationToken,
    two_pc_waiter: CancellationToken,
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

    fn mark_two_pc_cleaned_up(&self) {
        self.two_pc_waiter.cancel();
    }

    async fn wait_two_pc_cleaned_up(&self) {
        self.two_pc_waiter.cancelled().await;
    }
}

impl Cluster {
    /// Launch the connection pools.
    pub(crate) fn launch(&self) {
        for shard in self.shards() {
            shard.launch();
        }

        self.readiness.set_online(true);

        self.launch_two_pc_cleanup();
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

    /// Wait until boot-time maintenance — two-phase commit cleanup
    /// and schema loading — is done and the cluster can serve traffic.
    ///
    /// Two-phase commit cleanup is bounded by
    /// `two_phase_commit_rollback_abandoned_timeout`; schema loading
    /// retries until success, so callers should apply their own timeout.
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

    /// Rollback abandoned two-phase commit transactions before
    /// serving traffic. Queries wait on [`Cluster::wait_ready`]
    /// until this is done.
    fn launch_two_pc_cleanup(&self) {
        if !self.two_pc_rollback_abandoned() {
            self.readiness.mark_two_pc_cleaned_up();
            return;
        }

        let cluster = self.clone();
        tasks::spawn("two-pc abandoned transactions cleanup", async move {
            cluster.rollback_abandoned_two_pc().await;
            cluster.readiness.mark_two_pc_cleaned_up();
        });
    }

    /// Mark the cluster ready once two-phase commit cleanup
    /// and schema loading are done.
    fn launch_readiness_monitor(&self) {
        let cluster = self.clone();
        tasks::spawn("cluster readiness monitor", async move {
            cluster.readiness.wait_two_pc_cleaned_up().await;
            cluster.wait_schema_loaded().await;
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

            tasks::spawn("cluster schema sync", async move {
                loop {
                    match shard.load_schema().await {
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

    /// Rollback abandoned two-phase commit transactions on all shards.
    ///
    /// Bounded by `two_phase_commit_rollback_abandoned_timeout`.
    async fn rollback_abandoned_two_pc(&self) {
        let result = timeout(
            self.two_pc_rollback_abandoned_timeout(),
            Manager::get().cleanup_abandoned_for_cluster(self),
        )
        .await;

        let identifier = self.identifier();

        match result {
            Ok(Ok(cleaned_up)) => {
                if cleaned_up > 0 {
                    warn!(
                        "[2pc] rolled back {} abandoned transactions [{}]",
                        cleaned_up, identifier,
                    );
                } else {
                    info!("[2pc] no abandoned transactions found [{}]", identifier,);
                }
            }
            Ok(Err(err)) => {
                error!(
                    "[2pc] abandoned transactions cleanup error: {} [{}]",
                    err, identifier
                );
            }
            Err(_) => {
                error!(
                    "[2pc] abandoned transactions cleanup timed out after {}ms [{}]",
                    self.two_pc_rollback_abandoned_timeout().as_millis(),
                    identifier,
                );
            }
        }
    }
}
