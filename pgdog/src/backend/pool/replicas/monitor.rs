use std::time::Instant;

use super::*;

use tokio::{select, spawn, time::interval};
use tracing::{debug, info};

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

        let mut bans = self.replicas.replica_bans.clone();
        if let Some(primary_ban) = self.replicas.primary_read_ban.clone() {
            bans.push(primary_ban);
        }

        debug!("replicas monitor running");

        loop {
            select! {
                _ = interval.tick() => {
                    let now = Instant::now();
                    for ban in &bans {
                        if ban.unban_if_expired(now) {
                            info!("pool ban expired, resuming read queries [{}]", ban.pool().addr());
                        }
                    }
                }

                _ = self.replicas.shutdown.notified() => { break; }
            }
        }

        debug!("replicas monitor shut down");
    }
}
