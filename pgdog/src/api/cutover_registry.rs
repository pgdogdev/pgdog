//! Registry of tasks awaiting an operator `CUTOVER`.
//!
//! A task that parks at "caught up, ready to cut over" registers here
//! under its root task id, with the database (and, for ADD SHARD, the
//! shard) it works on. The admin `CUTOVER` command signals it through
//! [`trigger_cutover`], targeted by task id, by database and shard, or
//! bare (the first registered task). The cutover token is *separate*
//! from the task's `STOP_TASK` cancellation token — signalling it
//! means "cut over", not "abandon".

use std::collections::HashMap;
use std::sync::LazyLock;

use parking_lot::Mutex;
use tokio_util::sync::CancellationToken;

use crate::api::async_task::AsyncTaskId;

struct Registration {
    database: String,
    shard: Option<usize>,
    token: CancellationToken,
}

static CUTOVERS: LazyLock<Mutex<HashMap<AsyncTaskId, Registration>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// What a `CUTOVER` command points at.
#[derive(Clone, Debug)]
pub(crate) enum CutoverTarget {
    /// Bare `CUTOVER`: the first (lowest-id) registered task.
    First,
    /// `CUTOVER <task_id>`.
    Id(AsyncTaskId),
    /// `CUTOVER SHARD <database> <shard>`: the ADD SHARD task working
    /// on that shard.
    Shard { database: String, shard: usize },
}

/// Guard held by a running task: removes its cutover registration on
/// drop. Awaiting [`CutoverWaiter::requested`] resolves when an
/// operator `CUTOVER` targets the task.
pub(crate) struct CutoverWaiter {
    root_id: AsyncTaskId,
    token: CancellationToken,
}

impl CutoverWaiter {
    /// Wait until a cutover is requested for this task. The token
    /// latches, so a cutover that arrived earlier is delivered
    /// immediately.
    pub(crate) async fn requested(&self) {
        self.token.cancelled().await;
    }
}

impl Drop for CutoverWaiter {
    fn drop(&mut self) {
        CUTOVERS.lock().remove(&self.root_id);
    }
}

/// Register a task (by its `root_id`) to receive operator cutovers for
/// as long as the returned guard is held. `shard` identifies ADD SHARD
/// tasks; replication tasks register without one.
pub(crate) fn register_cutover(
    root_id: AsyncTaskId,
    database: &str,
    shard: Option<usize>,
) -> CutoverWaiter {
    let token = CancellationToken::new();
    CUTOVERS.lock().insert(
        root_id,
        Registration {
            database: database.to_string(),
            shard,
            token: token.clone(),
        },
    );
    CutoverWaiter { root_id, token }
}

/// Trigger a cutover on a running task.
pub(crate) fn trigger_cutover(target: CutoverTarget) -> bool {
    let registrations = CUTOVERS.lock();

    let token = match target {
        CutoverTarget::Id(id) => registrations.get(&id).map(|r| &r.token),
        CutoverTarget::First => registrations
            .keys()
            .min()
            .and_then(|id| registrations.get(id))
            .map(|r| &r.token),
        CutoverTarget::Shard { database, shard } => registrations
            .values()
            .find(|r| r.database == database && r.shard == Some(shard))
            .map(|r| &r.token),
    };

    match token {
        Some(token) => {
            token.cancel();
            true
        }
        None => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    // Serialize tests that touch the process-global `CUTOVERS` map so they
    // never observe each other's registrations under a multi-threaded harness.
    static CUTOVER_TEST_LOCK: std::sync::LazyLock<tokio::sync::Mutex<()>> =
        std::sync::LazyLock::new(|| tokio::sync::Mutex::new(()));

    fn register(id: u64) -> CutoverWaiter {
        register_cutover(AsyncTaskId::from(id), "testdb", None)
    }

    #[tokio::test]
    async fn cutover_delivers_even_when_buffered() {
        let _guard = CUTOVER_TEST_LOCK.lock().await;
        // Cutover lands before the task awaits: still delivered (latches).
        let waiter = register(1);
        assert!(
            trigger_cutover(CutoverTarget::Id(AsyncTaskId::from(1))),
            "the named task must receive the cutover"
        );

        tokio::time::timeout(Duration::from_secs(1), waiter.requested())
            .await
            .expect("buffered cutover was not delivered");
    }

    #[tokio::test]
    async fn cutover_targets_only_the_named_task() {
        let _guard = CUTOVER_TEST_LOCK.lock().await;
        // A cutover for one id must never disturb a task registered under a
        // different id — the whole point of keying by task id.
        let waiter = register(7);

        assert!(
            !trigger_cutover(CutoverTarget::Id(AsyncTaskId::from(8))),
            "no task is registered under id 8"
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(200), waiter.requested())
                .await
                .is_err(),
            "a cutover for a different id leaked to this task"
        );

        assert!(trigger_cutover(CutoverTarget::Id(AsyncTaskId::from(7))));
        tokio::time::timeout(Duration::from_secs(1), waiter.requested())
            .await
            .expect("targeted cutover was not delivered");
    }

    #[tokio::test]
    async fn cutover_by_database_and_shard() {
        let _guard = CUTOVER_TEST_LOCK.lock().await;
        let prod = register_cutover(AsyncTaskId::from(1), "prod", Some(2));
        let other = register_cutover(AsyncTaskId::from(2), "analytics", Some(2));

        // Wrong database or shard: nothing fires.
        assert!(!trigger_cutover(CutoverTarget::Shard {
            database: "prod".into(),
            shard: 3,
        }));
        assert!(!trigger_cutover(CutoverTarget::Shard {
            database: "staging".into(),
            shard: 2,
        }));

        // The right pair fires only its task, even with another
        // database's task registered under the same shard number.
        assert!(trigger_cutover(CutoverTarget::Shard {
            database: "prod".into(),
            shard: 2,
        }));
        tokio::time::timeout(Duration::from_secs(1), prod.requested())
            .await
            .expect("the named database's task was not cut over");
        assert!(
            tokio::time::timeout(Duration::from_millis(200), other.requested())
                .await
                .is_err(),
            "a cutover leaked across databases"
        );

        // Replication tasks register without a shard and can't be hit
        // by the shard form.
        drop(prod);
        drop(other);
        let replication = register_cutover(AsyncTaskId::from(3), "prod", None);
        assert!(!trigger_cutover(CutoverTarget::Shard {
            database: "prod".into(),
            shard: 2,
        }));
        assert!(
            tokio::time::timeout(Duration::from_millis(200), replication.requested())
                .await
                .is_err(),
            "the shard form must not target a replication task"
        );
    }

    #[tokio::test]
    async fn cutover_without_id_targets_the_first_task() {
        let _guard = CUTOVER_TEST_LOCK.lock().await;
        // No id: the lowest-id (first) registered task is cut over, and only
        // it.
        let first = register(3);
        let second = register(9);

        assert!(
            trigger_cutover(CutoverTarget::First),
            "the first registered task must be cut over"
        );

        tokio::time::timeout(Duration::from_secs(1), first.requested())
            .await
            .expect("the first task was not cut over");
        assert!(
            tokio::time::timeout(Duration::from_millis(200), second.requested())
                .await
                .is_err(),
            "cutover(First) disturbed a task other than the first"
        );
    }

    #[tokio::test]
    async fn cutover_does_not_leak_to_the_next_task() {
        let _guard = CUTOVER_TEST_LOCK.lock().await;
        // A cutover to a task that never consumes it must die with that task,
        // never reaching the next one. Regression guard for the signal leak.
        {
            let first = register(1);
            assert!(trigger_cutover(CutoverTarget::Id(AsyncTaskId::from(1))));
            drop(first); // ends without ever awaiting `requested()`
        }

        let next = register(2);
        assert!(
            tokio::time::timeout(Duration::from_millis(200), next.requested())
                .await
                .is_err(),
            "stale cutover leaked into the next replication task"
        );
    }

    #[tokio::test]
    async fn cutover_with_no_task_is_rejected() {
        let _guard = CUTOVER_TEST_LOCK.lock().await;
        // Nothing registered: `CUTOVER` (in any form) is rejected.
        assert!(!trigger_cutover(CutoverTarget::First));
        assert!(!trigger_cutover(CutoverTarget::Id(AsyncTaskId::from(404))));
        assert!(!trigger_cutover(CutoverTarget::Shard {
            database: "prod".into(),
            shard: 0,
        }));
    }
}
