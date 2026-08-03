//! Guards for adding a shard to a live cluster.
//!
//! Growing a cluster in place is only safe when no table's placement
//! depends on the shard count: hash routing moves every row when the
//! count changes, which is what resharding is for. Fixed lookups
//! (`lookup_result = "shard"`) and explicit mappings are stable.

use pgdog_config::LookupResult;

use super::Error;
use crate::backend::{
    Cluster,
    pool::{Guard, Request},
};

/// Advisory lock key taken on the new shard (ASCII "pgdog_ad"):
/// arbitrates which pgdog instance runs `ADD SHARD` when several share
/// the same config.
pub(crate) const ADD_SHARD_LOCK: i64 = 0x7067646f675f6164;

/// Every sharded table's placement must survive a shard-count change:
/// routed by a lookup that returns the shard number, or by an explicit
/// mapping. Hash-routed tables (including vector/centroids) make
/// grow-in-place silently corrupting and are refused.
pub(crate) fn placement_stable(cluster: &Cluster) -> Result<(), Error> {
    for table in cluster.sharded_tables() {
        let stable = table.lookup_result == LookupResult::Shard || table.mapping.is_some();
        if !stable {
            return Err(Error::PlacementNotStable(
                table
                    .name
                    .clone()
                    .unwrap_or_else(|| format!("column \"{}\"", table.column)),
            ));
        }
        if !table.centroids.is_empty() {
            return Err(Error::PlacementNotStable(
                table
                    .name
                    .clone()
                    .unwrap_or_else(|| format!("column \"{}\"", table.column)),
            ));
        }
    }

    Ok(())
}

/// Take the cross-instance provisioning lock on the new shard. The
/// new shard itself is the mutex: every instance that could run this
/// task connects to it, it serves no other traffic, and a lock held
/// there needs no healthy existing shard. The lock is session-scoped,
/// so the caller must keep the returned connection checked out for as
/// long as the task runs; a crashed holder releases it with its
/// connection.
pub(crate) async fn provisioning_lock(cluster: &Cluster) -> Result<Guard, Error> {
    let mut server = cluster
        .shards()
        .first()
        .ok_or(crate::backend::pool::Error::NoShard(0))?
        .primary(&Request::default())
        .await?;

    let locked: Vec<String> = server
        .fetch_all(format!("SELECT pg_try_advisory_lock({})", ADD_SHARD_LOCK).as_str())
        .await?;

    if locked.first().map(|l| l == "t").unwrap_or(false) {
        Ok(server)
    } else {
        Err(Error::ProvisioningLocked)
    }
}

/// The new shard must be empty: the schema sync and data copy assume
/// a blank destination, and `ADD SHARD` refuses to guess at leftovers
/// from a previous attempt.
pub(crate) async fn destination_is_empty(cluster: &Cluster) -> Result<(), Error> {
    let mut server = cluster
        .shards()
        .first()
        .ok_or(crate::backend::pool::Error::NoShard(0))?
        .primary(&Request::default())
        .await?;

    let tables: Vec<String> = server
        .fetch_all(
            "SELECT table_schema || '.' || table_name FROM information_schema.tables \
             WHERE table_schema NOT IN ('pg_catalog', 'information_schema', 'pgdog') \
             AND table_type = 'BASE TABLE'",
        )
        .await?;

    if tables.is_empty() {
        Ok(())
    } else {
        Err(Error::DestinationNotEmpty(tables.len()))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::frontend::router::sharding::ShardedTable;
    use pgdog_config::{ConfigAndUsers, DataType};

    fn cluster_with(tables: Vec<ShardedTable>) -> Cluster {
        let mut cluster = Cluster::new_test(&ConfigAndUsers::default());
        cluster.set_sharded_tables(crate::backend::ShardedTables::new(
            tables,
            vec![],
            false,
            pgdog_config::SystemCatalogsBehavior::default(),
        ));
        cluster
    }

    fn table() -> ShardedTable {
        ShardedTable {
            database: "pgdog".into(),
            column: "org_id".into(),
            data_type: DataType::Varchar,
            ..Default::default()
        }
    }

    #[test]
    fn test_placement_stable() {
        // Fixed lookup: stable.
        let stable = ShardedTable {
            lookup_query: Some("SELECT shard_id FROM orgs WHERE id = $1".into()),
            lookup_result: LookupResult::Shard,
            ..table()
        };
        placement_stable(&cluster_with(vec![stable.clone()])).unwrap();

        // Hash routing (default): refused.
        assert!(placement_stable(&cluster_with(vec![table()])).is_err());

        // A value-mode lookup still hashes: refused.
        let value_lookup = ShardedTable {
            lookup_query: Some("SELECT parent FROM orgs WHERE id = $1".into()),
            lookup_result: LookupResult::Value,
            ..table()
        };
        assert!(placement_stable(&cluster_with(vec![value_lookup])).is_err());

        // One stable, one hashed: refused.
        assert!(placement_stable(&cluster_with(vec![stable, table()])).is_err());

        // No sharded tables at all: nothing to destabilize.
        placement_stable(&cluster_with(vec![])).unwrap();
    }
}
