use std::time::Instant;

use super::*;

use tokio::{select, spawn, time::interval};
use tracing::debug;

static MAINTENANCE: Duration = Duration::from_millis(333);

#[derive(Clone, Debug)]
pub(super) struct Monitor {
    replicas: Replicas,
}

impl Monitor {
    /// Create new replica targets monitor.
    pub(super) fn new(replicas: &Replicas) {
        let monitor = Self {
            replicas: replicas.clone(),
        };

        spawn(async move {
            monitor.run().await;
        });
    }

    async fn run(&self) {
        let mut interval = interval(MAINTENANCE);

        let mut bans: Vec<Ban> = self
            .replicas
            .replicas
            .iter()
            .map(|target| target.ban.clone())
            .collect();

        if let Some(ref primary) = self.replicas.primary {
            bans.push(primary.ban.clone());
        }

        debug!("replicas monitor running");

        loop {
            select! {
                _ = interval.tick() => {
                    let now = Instant::now();

                    for ban in &bans {
                        // Clear expired bans.
                        ban.unban_if_expired(now);
                    }
                }

                _ = self.replicas.shutdown.notified() => { break; }
            }
        }

        debug!("replicas monitor shut down");
    }
}
