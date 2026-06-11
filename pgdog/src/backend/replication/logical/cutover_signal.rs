//! Cutover signal for the logical replication task.
//!
//! Standalone channel between the `CUTOVER` admin command and the
//! running replication task: the command [`request`]s the cutover
//! from anywhere, the replication task [`requested`] waits for it.
//!
//! One request is buffered, so a request arriving before the task
//! starts waiting is not lost. There is at most one replication
//! task per process, matching the single buffered permit.

use std::sync::LazyLock;

use tokio::sync::Notify;

static CUTOVER: LazyLock<Notify> = LazyLock::new(Notify::new);

/// Request a cutover from the running replication task.
pub fn request() {
    CUTOVER.notify_one();
}

/// Wait until a cutover is requested. Only the replication task
/// waits on this.
pub async fn requested() {
    CUTOVER.notified().await;
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[tokio::test]
    async fn test_request_is_buffered() {
        // Request lands before anyone waits: still delivered.
        request();

        tokio::time::timeout(Duration::from_secs(1), requested())
            .await
            .unwrap();
    }
}
