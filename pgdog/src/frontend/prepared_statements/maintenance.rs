use super::{PreparedStatements, prelude::*};
use crate::tasks::{shutdown_signal, spawn};

use std::time::Duration;

/// Run prepared statements maintenance task every second.
///
/// Public because it's used in main.rs.
pub fn start_maintenance() {
    spawn("prepared statements cache", async move {
        debug!("prepared statements cache maintenance started");
        let shutdown = shutdown_signal();
        loop {
            tokio::select! {
                _ = safe_sleep(Duration::from_secs(1)) => {}
                _ = shutdown.cancelled() => break,
            }
            run_maintenance();
        }
    });
}

/// Check prepared statements cache for overflows
/// and remove any unused statements exceeding the limit.
fn run_maintenance() {
    let capacity = config().config.general.prepared_statements_limit;
    PreparedStatements::global().write().close_unused(capacity);
}
