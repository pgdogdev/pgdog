//! Instance heartbeat registry.
//!
//! Every pgdog instance heartbeats a row in `pgdog.instances` on
//! shard 0 of each database that has a `schema_admin` user, when the
//! table exists (it's installed by `SETUP SCHEMA` and the topology
//! tasks). A row with a recent heartbeat is a live instance: the
//! ADD SHARD cutover reads this to learn the fleet it must coordinate
//! with, and to refuse when an instance hasn't seen the new config.

use std::time::Duration;

use once_cell::sync::Lazy;
use tokio::time::sleep;
use tracing::debug;

use crate::backend::databases::databases;
use crate::backend::pool::Request;
use crate::backend::{Cluster, Error};
use crate::config::config;
use crate::net::messages::{DataRow, Format};
use crate::util::{hostname, pgdog_version};

pub(crate) const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);

/// Bounds one heartbeat: a single slow primary must not stall the
/// loop past the liveness window and drop this instance from the
/// fleet on every other database.
const BEAT_TIMEOUT: Duration = Duration::from_secs(5);

/// A row whose heartbeat is younger than this is a live instance.
pub(crate) const LIVENESS_WINDOW: Duration = Duration::from_secs(15);

/// Rows older than this are dead instances; any heartbeat removes them.
const EXPIRY: Duration = Duration::from_secs(3600);

/// This instance's identity, for the lifetime of the process.
pub(crate) fn node_id() -> i64 {
    static NODE_ID: Lazy<i64> = Lazy::new(|| (rand::random::<u64>() >> 1) as i64);
    *NODE_ID
}

/// A live instance registered in `pgdog.instances`.
#[allow(dead_code)] // Consumed by the fleet coordination protocol.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Instance {
    pub(crate) node_id: i64,
    pub(crate) hostname: String,
}

impl From<DataRow> for Instance {
    fn from(value: DataRow) -> Self {
        Self {
            node_id: value.get(0, Format::Text).unwrap_or_default(),
            hostname: value.get(1, Format::Text).unwrap_or_default(),
        }
    }
}

/// Start the heartbeat loop. Called once at startup.
pub fn start() {
    crate::tasks::spawn("instance heartbeat", async {
        loop {
            for database in schema_admin_databases() {
                match tokio::time::timeout(BEAT_TIMEOUT, beat(&database)).await {
                    Ok(Ok(())) => {}
                    Ok(Err(err)) => {
                        debug!(r#"instance heartbeat for "{}" failed: {}"#, database, err)
                    }
                    Err(_) => debug!(r#"instance heartbeat for "{}" timed out"#, database),
                }
            }
            sleep(HEARTBEAT_INTERVAL).await;
        }
    });
}

/// Distinct database names with a `schema_admin` user: the databases
/// the topology tooling can operate on.
pub(crate) fn schema_admin_databases() -> Vec<String> {
    let config = config();
    let mut names = config
        .users
        .users
        .iter()
        .filter(|user| user.schema_admin)
        .map(|user| user.database.clone())
        .collect::<Vec<_>>();
    names.sort_unstable();
    names.dedup();
    names
}

/// Heartbeat this instance's row on shard 0 of `database`. A no-op
/// when `pgdog.instances` isn't installed there.
async fn beat(database: &str) -> Result<(), Error> {
    let cluster = databases().schema_owner(database)?;
    register(&cluster, 0).await
}

/// Upsert this instance's row on one shard of `cluster`, and garbage
/// collect rows of instances dead for longer than the expiry.
pub(crate) async fn register(cluster: &Cluster, shard: usize) -> Result<(), Error> {
    let mut server = cluster
        .shards()
        .get(shard)
        .ok_or(crate::backend::pool::Error::NoShard(shard))?
        .primary(&Request::default())
        .await?;

    server
        .execute(
            format!(
                "DO $$ BEGIN
                    IF to_regclass('pgdog.instances') IS NOT NULL THEN
                        INSERT INTO pgdog.instances (node_id, hostname, version)
                        VALUES ({}, '{}', '{}')
                        ON CONFLICT (node_id) DO UPDATE
                        SET heartbeat_at = NOW(),
                            hostname = EXCLUDED.hostname,
                            version = EXCLUDED.version;
                        DELETE FROM pgdog.instances
                        WHERE heartbeat_at < NOW() - INTERVAL '{} seconds';
                    END IF;
                END $$;",
                node_id(),
                escape_literal(hostname()),
                escape_literal(&pgdog_version()),
                EXPIRY.as_secs(),
            )
            .as_str(),
        )
        .await?;

    Ok(())
}

/// Live instances registered on one shard of `cluster`. An empty list
/// also covers `pgdog.instances` not being installed.
#[allow(dead_code)] // Consumed by the fleet coordination protocol.
pub(crate) async fn live_instances(
    cluster: &Cluster,
    shard: usize,
) -> Result<Vec<Instance>, Error> {
    let mut server = cluster
        .shards()
        .get(shard)
        .ok_or(crate::backend::pool::Error::NoShard(shard))?
        .primary(&Request::default())
        .await?;

    let installed: Vec<String> = server
        .fetch_all("SELECT COALESCE(to_regclass('pgdog.instances')::text, '')")
        .await?;
    if installed.first().map(|s| s.is_empty()).unwrap_or(true) {
        return Ok(vec![]);
    }

    let instances = server
        .fetch_all(
            format!(
                "SELECT node_id, hostname FROM pgdog.instances
                 WHERE heartbeat_at > NOW() - INTERVAL '{} seconds'
                 ORDER BY node_id",
                LIVENESS_WINDOW.as_secs()
            )
            .as_str(),
        )
        .await?;

    Ok(instances)
}

/// Remove this instance's fleet rows. Called on shutdown so a
/// restart doesn't leave a ghost "live" row that blocks a cutover for
/// the length of the liveness window. Best effort: a killed process
/// leaves its row, and the liveness window handles it.
pub async fn deregister() {
    for database in schema_admin_databases() {
        let result: Result<(), Error> = async {
            let cluster = databases().schema_owner(&database)?;
            let mut server = cluster
                .shards()
                .first()
                .ok_or(crate::backend::pool::Error::NoShard(0))?
                .primary(&Request::default())
                .await?;
            server
                .execute(
                    format!(
                        "DO $$ BEGIN
                            IF to_regclass('pgdog.instances') IS NOT NULL THEN
                                DELETE FROM pgdog.instances WHERE node_id = {};
                            END IF;
                        END $$;",
                        node_id()
                    )
                    .as_str(),
                )
                .await?;
            Ok(())
        }
        .await;
        if let Err(err) = result {
            debug!(
                r#"instance deregistration for "{}" failed: {}"#,
                database, err
            );
        }
    }
}

/// Make an arbitrary string safe inside a single-quoted literal in a
/// dollar-quoted DO block: quotes are doubled, and characters that
/// could terminate either quoting layer (`$`, `\`) are dropped. The
/// inputs are hostnames and version strings; fidelity loses to safety.
/// A registry row as `SHOW INSTANCES` reports it.
#[derive(Debug, Clone)]
pub(crate) struct InstanceDetail {
    pub(crate) node_id: i64,
    pub(crate) hostname: String,
    pub(crate) version: String,
    pub(crate) started_at: String,
    pub(crate) heartbeat_at: String,
    pub(crate) live: bool,
}

impl From<DataRow> for InstanceDetail {
    fn from(value: DataRow) -> Self {
        Self {
            node_id: value.get(0, Format::Text).unwrap_or_default(),
            hostname: value.get(1, Format::Text).unwrap_or_default(),
            version: value.get(2, Format::Text).unwrap_or_default(),
            started_at: value.get(3, Format::Text).unwrap_or_default(),
            heartbeat_at: value.get(4, Format::Text).unwrap_or_default(),
            live: value
                .get::<String>(5, Format::Text)
                .map(|live| live == "t")
                .unwrap_or_default(),
        }
    }
}

/// Every registered instance on one shard of `cluster`, dead rows
/// included (`live` says which is which). Empty when the registry
/// isn't installed there.
pub(crate) async fn list(cluster: &Cluster, shard: usize) -> Result<Vec<InstanceDetail>, Error> {
    let mut server = cluster
        .shards()
        .get(shard)
        .ok_or(crate::backend::pool::Error::NoShard(shard))?
        .primary(&Request::default())
        .await?;

    let installed: Vec<String> = server
        .fetch_all("SELECT COALESCE(to_regclass('pgdog.instances')::text, '')")
        .await?;
    if installed.first().map(|s| s.is_empty()).unwrap_or(true) {
        return Ok(vec![]);
    }

    let instances = server
        .fetch_all(
            format!(
                "SELECT node_id, hostname, version, started_at::text, heartbeat_at::text,
                        heartbeat_at > NOW() - INTERVAL '{} seconds'
                 FROM pgdog.instances ORDER BY node_id",
                LIVENESS_WINDOW.as_secs()
            )
            .as_str(),
        )
        .await?;

    Ok(instances)
}

fn escape_literal(s: &str) -> String {
    s.chars()
        .filter(|c| *c != '$' && *c != '\\')
        .collect::<String>()
        .replace('\'', "''")
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_node_id_stable() {
        assert_eq!(node_id(), node_id());
        assert!(node_id() >= 0);
    }

    #[test]
    fn test_escape_literal() {
        assert_eq!(escape_literal("it's"), "it''s");
        assert_eq!(escape_literal("plain"), "plain");
        // Dollar-quote and backslash escapes can't survive: a hostname
        // like `x$$; DROP TABLE t; --` must not terminate the DO block.
        assert_eq!(escape_literal("x$$; DROP"), "x; DROP");
        assert_eq!(escape_literal("a\\'b"), "a''b");
    }
}
