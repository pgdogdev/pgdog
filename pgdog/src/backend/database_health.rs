use std::sync::Arc;

use super::Error;
use super::databases::*;
use crate::tasks::{shutdown_signal, spawn};

use once_cell::sync::Lazy;
use tokio::{select, sync::Notify};
use tracing::{error, warn};

pub(in crate::backend) static DATABASE_HEALTH: Lazy<DatabaseHealth> = Lazy::new(|| {
    let health = DatabaseHealth::default();
    let background = health.clone();

    spawn("database health", async move {
        let shutdown = shutdown_signal();
        select! {
            _ = shutdown.cancelled() => return,
            _ = background.listen_loop() => unreachable!("listen loop never resolves"),
        }
    });

    health
});

#[derive(Debug, Clone, Default)]
pub(in crate::backend) struct DatabaseHealth {
    notify: Arc<Notify>,
}

impl DatabaseHealth {
    /// Notify the health monitor that we found a bad user.
    pub(in crate::backend) fn notify_bad_auth(&self) {
        self.notify.notify_one();
    }

    async fn listen_loop(&self) {
        loop {
            self.notify.notified().await;

            if let Err(err) = Self::remove_bad_auth() {
                error!("error removing user: {}", err);
            }
        }
    }

    fn remove_bad_auth() -> Result<(), Error> {
        for cluster in databases().all().values() {
            let all_bad_auth = cluster
                .shards()
                .iter()
                .all(|shard| shard.pool_iter().all(|pool| !pool.auth_ok()));

            if all_bad_auth {
                warn!(
                    r#"removing user \"{}\" for database \"{}\" due to bad credentials"#,
                    cluster.user(),
                    cluster.name()
                );

                remove(cluster.user(), cluster.name())?;
            }
        }

        Ok(())
    }
}
