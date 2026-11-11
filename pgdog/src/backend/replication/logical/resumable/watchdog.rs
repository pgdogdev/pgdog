use std::time::Duration;

use tokio_util::task::AbortOnDropHandle;
use tracing::warn;

use super::ResumeState;
use crate::tasks;
use crate::util::safe_timeout;

const LOCK_CHECK_INTERVAL: Duration = Duration::from_secs(5);
const LOCK_CHECK_TIMEOUT: Duration = Duration::from_secs(10);

pub(crate) struct ActiveRun {
    _watchdog: Option<AbortOnDropHandle<()>>,
}

impl ActiveRun {
    pub(crate) fn start(resumable: &ResumeState, on_lost: impl FnOnce() + Send + 'static) -> Self {
        let watchdog = resumable.run_store().map(|store| {
            AbortOnDropHandle::new(tasks::spawn("resharding lock watchdog", async move {
                loop {
                    tokio::time::sleep(LOCK_CHECK_INTERVAL).await;
                    let Some(store) = store.upgrade() else {
                        return;
                    };
                    if !matches!(
                        safe_timeout(LOCK_CHECK_TIMEOUT, store.ping()).await,
                        Ok(Ok(()))
                    ) {
                        warn!("resharding {}: state connection lost, pausing", store.key());
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
