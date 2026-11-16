use std::time::Duration;

use tokio_util::task::AbortOnDropHandle;
use tracing::warn;

use super::super::resharding_state::ReshardingState;
use crate::tasks;
use crate::util::safe_timeout;

/// The heartbeat write also checks the state connection.
pub(crate) const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);
const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(10);

pub(crate) struct ActiveRun {
    _watchdog: Option<AbortOnDropHandle<()>>,
}

impl ActiveRun {
    pub(crate) fn start(state: &ReshardingState, on_lost: impl FnOnce() + Send + 'static) -> Self {
        let watchdog = state.run_store().map(|store| {
            AbortOnDropHandle::new(tasks::spawn("resharding lock watchdog", async move {
                loop {
                    tokio::time::sleep(HEARTBEAT_INTERVAL).await;
                    let Some(store) = store.upgrade() else {
                        return;
                    };
                    if !matches!(
                        safe_timeout(HEARTBEAT_TIMEOUT, store.heartbeat()).await,
                        Ok(Ok(()))
                    ) {
                        warn!(
                            "resharding {}: state connection lost, stopping",
                            store.key()
                        );
                        on_lost();
                        return;
                    }
                }
            }))
        });

        Self {
            _watchdog: watchdog,
        }
    }
}
