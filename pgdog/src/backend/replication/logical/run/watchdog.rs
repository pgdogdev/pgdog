use std::time::Duration;

use pgdog_stats::TaskId;
use tokio_util::task::AbortOnDropHandle;
use tracing::warn;

use super::super::resharding_state::ReshardingState;
use crate::api::tasks_storage;
use crate::tasks;
use crate::util::safe_timeout;

/// The heartbeat query checks the state connection.
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);
const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(10);
const HEARTBEAT_TIMEOUTS: u32 = 5;

/// Checks the state connection that holds the run lock while it lives,
/// and stops the run's root task when the connection, and with it the lock, is lost.
pub(crate) struct LockWatchdog {
    _heartbeat: Option<AbortOnDropHandle<()>>,
}

impl LockWatchdog {
    pub(crate) fn start(state: &ReshardingState, root_task: TaskId) -> Self {
        let heartbeat = state.run_store().map(|store| {
            AbortOnDropHandle::new(tasks::spawn("resharding lock watchdog", async move {
                loop {
                    tokio::time::sleep(HEARTBEAT_INTERVAL).await;
                    let Some(store) = store.upgrade() else {
                        return;
                    };
                    let mut heartbeat = std::pin::pin!(store.heartbeat());
                    let mut timeouts = 0;
                    let failure = loop {
                        match safe_timeout(HEARTBEAT_TIMEOUT, &mut heartbeat).await {
                            Ok(Ok(())) => break None,
                            Ok(Err(err)) => break Some(err.to_string()),
                            Err(_) => {
                                timeouts += 1;
                                if timeouts == HEARTBEAT_TIMEOUTS {
                                    break Some(format!(
                                        "no reply in {:?}",
                                        HEARTBEAT_TIMEOUT * HEARTBEAT_TIMEOUTS
                                    ));
                                }
                                warn!(
                                    "resharding {}: heartbeat is slow, no reply in {:?}",
                                    store.key(),
                                    HEARTBEAT_TIMEOUT * timeouts
                                );
                            }
                        }
                    };
                    if let Some(failure) = failure {
                        warn!(
                            "resharding {}: heartbeat failed ({failure}), stopping task {root_task}",
                            store.key()
                        );
                        tasks_storage().cancel_task(root_task);
                        return;
                    }
                }
            }))
        });

        Self {
            _heartbeat: heartbeat,
        }
    }
}
