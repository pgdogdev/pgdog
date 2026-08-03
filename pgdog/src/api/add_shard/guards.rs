//! The Validating phase: everything the task refuses on, and
//! everything it holds until it ends.

use std::collections::HashSet;
use std::sync::LazyLock;

use parking_lot::Mutex;

use super::AddShardTask;
use crate::api::MigrationError;
use crate::backend::Cluster;
use crate::backend::databases::{databases, provisioning_cluster};
use crate::backend::pool;
use crate::backend::replication::logical::Error;
use crate::backend::replication::logical::add_shard::{
    destination_is_empty, placement_stable, provisioning_lock,
};
use crate::config::config;

/// Databases with a topology change (ADD SHARD) in flight. A second
/// concurrent change to the same database is refused.
static TOPOLOGY_TASKS: LazyLock<Mutex<HashSet<String>>> = LazyLock::new(Default::default);

/// Removes the database from the in-flight topology registry on drop.
struct TopologyGuard {
    database: String,
}

impl TopologyGuard {
    fn acquire(database: &str) -> Result<Self, Error> {
        let mut tasks = TOPOLOGY_TASKS.lock();
        if !tasks.insert(database.to_string()) {
            return Err(Error::TopologyChangeInProgress(database.to_string()));
        }
        Ok(Self {
            database: database.to_string(),
        })
    }
}

impl Drop for TopologyGuard {
    fn drop(&mut self) {
        TOPOLOGY_TASKS.lock().remove(&self.database);
    }
}

/// Shuts the caller-owned provisioning cluster down on drop.
struct DestinationGuard(Cluster);

impl Drop for DestinationGuard {
    fn drop(&mut self) {
        self.0.shutdown();
    }
}

/// Everything `run()` holds for the task's lifetime, acquired in the
/// Validating phase. Dropping it returns the cross-instance advisory
/// lock's connection, shuts the provisioning cluster down (closing
/// that lock's session even if this future was hard-aborted by a
/// cancellation timeout), and frees the in-flight-topology slot —
/// field order is drop order.
pub(super) struct Preflight {
    /// The serving cluster the new shard joins.
    pub(super) source: Cluster,
    /// The database's omnisharded tables; empty means the schema-only
    /// path.
    pub(super) omni_tables: Vec<String>,
    /// Session-scoped `pg_try_advisory_lock` on the new shard: which
    /// pgdog instance runs this ADD SHARD.
    _lock: pool::Guard,
    /// The caller-owned, launched, non-serving one-shard cluster for
    /// the new shard. The activated shard gets fresh pools from the
    /// registry reload.
    destination: DestinationGuard,
    _topology: TopologyGuard,
}

impl Preflight {
    pub(super) fn destination(&self) -> &Cluster {
        &self.destination.0
    }
}

/// Entry: nothing held. Exit: the cluster is placement-stable, the
/// named shard is the next one, this instance holds both the local
/// topology slot and the cross-instance advisory lock, and the new
/// shard is empty. Failure: everything acquired so far is released by
/// drop, and the task fails with the guard's error.
pub(super) async fn preflight(task: &AddShardTask) -> Result<Preflight, MigrationError> {
    let topology = TopologyGuard::acquire(&task.database)?;

    let source = databases()
        .schema_owner(&task.database)
        .map_err(Error::from)?;

    placement_stable(&source)?;

    // Declared shards are added in order: pure config math, so it runs
    // before anything touches the new shard.
    if task.shard != source.shards().len() {
        return Err(Error::ProvisioningShardNotNext {
            declared: task.shard,
            expected: source.shards().len(),
        }
        .into());
    }

    let destination =
        DestinationGuard(provisioning_cluster(&task.database, task.shard).map_err(Error::from)?);

    // Cross-instance mutex, arbitrated by the new shard itself.
    // Session-scoped: held (checked out) in the Preflight until the
    // task ends; the destination shutdown that follows it in drop
    // order closes the session even if the connection was returned to
    // the pool.
    let lock = provisioning_lock(&destination.0).await?;

    destination_is_empty(&destination.0).await?;

    let omni_tables = config()
        .config
        .omnisharded_tables
        .iter()
        .find(|tables| tables.database == task.database)
        .map(|tables| tables.tables.clone())
        .unwrap_or_default();

    Ok(Preflight {
        source,
        omni_tables,
        _lock: lock,
        destination,
        _topology: topology,
    })
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_topology_guard_excludes_concurrent_changes() {
        let guard = TopologyGuard::acquire("guard_test_db").unwrap();
        assert!(TopologyGuard::acquire("guard_test_db").is_err());
        // A different database is unaffected.
        let other = TopologyGuard::acquire("guard_test_other").unwrap();
        drop(guard);
        // Released: can acquire again.
        let _again = TopologyGuard::acquire("guard_test_db").unwrap();
        drop(other);
    }
}
