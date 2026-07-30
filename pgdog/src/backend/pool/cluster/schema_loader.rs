use super::Cluster;
use crate::backend::pool::ee::schema_changed_hook;
use crate::tasks;
use crate::util::safe_sleep;
use dyn_clone::DynClone;
use std::time::Duration;
use tokio::select;
use tracing::error;

pub(crate) trait SchemaLoader: Send + Sync + DynClone {
    fn launch_schema_sync(&self, cluster: &Cluster);
}

dyn_clone::clone_trait_object!(SchemaLoader);

#[derive(Clone, Copy)]
pub(super) struct FromServer;

impl SchemaLoader for FromServer {
    fn launch_schema_sync(&self, cluster: &Cluster) {
        if !cluster.load_schema() {
            for shard in cluster.shards() {
                shard.schema_not_needed();
            }
            return;
        }

        for shard in cluster.shards() {
            let identifier = cluster.identifier();
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
                                safe_sleep(Duration::from_millis(100)).await;
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

#[cfg(test)]
#[derive(Clone, Copy)]
pub(crate) struct Dummy;

#[cfg(test)]
impl SchemaLoader for Dummy {
    fn launch_schema_sync(&self, cluster: &Cluster) {
        for shard in cluster.shards() {
            shard.schema_not_needed();
        }
    }
}

#[cfg(test)]
impl Default for Box<dyn SchemaLoader> {
    fn default() -> Self {
        Box::new(Dummy)
    }
}
