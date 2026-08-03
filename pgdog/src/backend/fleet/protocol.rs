//! Shared coordination state on a "medium" database: a coordinator
//! publishes topic-scoped states with NOTIFY riding each write, and
//! followers ack them. The tables live in the `pgdog` schema of
//! whichever database a consumer designates; topics keep consumers
//! from colliding on them.

use std::collections::HashSet;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::backend::fleet::registry;
use crate::backend::pool::Request;
use crate::backend::{Cluster, Error};
use crate::net::messages::{DataRow, Format};

/// How often a coordinator refreshes a state it holds followers in.
pub(crate) const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(5);

/// A held state older than this means its coordinator died: followers
/// must not stay held forever. Several multiples of
/// [`KEEPALIVE_INTERVAL`].
pub(crate) const COORDINATOR_SILENCE: Duration = Duration::from_secs(30);

/// Follower fallback poll. State changes normally arrive over NOTIFY
/// within milliseconds; the poll covers missed notifications (e.g.
/// across a listener reconnect) and paces the registration heartbeat.
pub(crate) const FALLBACK_POLL: Duration = Duration::from_secs(5);

/// Namespaces one consumer's coordination on the shared tables and its
/// NOTIFY channel. The name is embedded in SQL literals and a channel
/// identifier: lowercase alphanumerics and underscores only.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Topic(&'static str);

impl Topic {
    pub(crate) const fn new(name: &'static str) -> Self {
        // Const-evaluated when used in a const: a bad name fails the
        // build, not the runtime.
        let bytes = name.as_bytes();
        let mut i = 0;
        while i < bytes.len() {
            let b = bytes[i];
            assert!(
                b == b'_' || b.is_ascii_lowercase() || b.is_ascii_digit(),
                "topic names are lowercase alphanumerics and underscores"
            );
            i += 1;
        }
        Self(name)
    }

    pub(crate) fn as_str(&self) -> &'static str {
        self.0
    }

    /// The NOTIFY channel this topic's state writes signal.
    pub(crate) fn channel(&self) -> String {
        format!("__pgdog_fleet_{}", self.0)
    }
}

/// A coordinator's published state, as followers read it.
#[derive(Debug, Clone)]
pub(crate) struct StateRow {
    pub(crate) state: String,
    pub(crate) epoch: i64,
    pub(crate) coordinator: i64,
    pub(crate) age_secs: i64,
}

impl StateRow {
    /// A live coordinator refreshes a held state every
    /// [`KEEPALIVE_INTERVAL`]; a row this stale means it died.
    pub(crate) fn coordinator_silent(&self) -> bool {
        self.age_secs > COORDINATOR_SILENCE.as_secs() as i64
    }
}

impl From<DataRow> for StateRow {
    fn from(value: DataRow) -> Self {
        Self {
            state: value.get(0, Format::Text).unwrap_or_default(),
            epoch: value.get(1, Format::Text).unwrap_or_default(),
            coordinator: value.get(2, Format::Text).unwrap_or_default(),
            age_secs: value.get(3, Format::Text).unwrap_or_default(),
        }
    }
}

/// A unique epoch per coordination attempt.
pub(crate) fn epoch() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as i64
}

/// Create the fleet's tables on a medium. Idempotent. The registry
/// table rides along so a follower heals a medium that was wiped.
pub(crate) async fn ensure_tables(cluster: &Cluster) -> Result<(), Error> {
    let mut server = shard_primary(cluster).await?;
    server
        .execute(
            "CREATE SCHEMA IF NOT EXISTS pgdog;
             CREATE TABLE IF NOT EXISTS pgdog.instances (
                 node_id BIGINT PRIMARY KEY,
                 hostname TEXT NOT NULL DEFAULT '',
                 version TEXT NOT NULL DEFAULT '',
                 started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                 heartbeat_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
             );
             CREATE TABLE IF NOT EXISTS pgdog.fleet_state (
                 topic TEXT PRIMARY KEY,
                 state TEXT NOT NULL,
                 epoch BIGINT NOT NULL,
                 coordinator BIGINT NOT NULL,
                 updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
             );
             CREATE TABLE IF NOT EXISTS pgdog.fleet_acks (
                 topic TEXT NOT NULL,
                 node_id BIGINT NOT NULL,
                 epoch BIGINT NOT NULL,
                 state TEXT NOT NULL,
                 updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                 PRIMARY KEY (topic, node_id, epoch)
             );",
        )
        .await?;
    Ok(())
}

/// Tear the state and ack tables down. For dedicated media only: this
/// removes every topic's rows, which is what a consumer that owns the
/// medium outright wants once everyone is done.
pub(crate) async fn drop_tables(cluster: &Cluster) -> Result<(), Error> {
    let mut server = shard_primary(cluster).await?;
    server
        .execute("DROP TABLE IF EXISTS pgdog.fleet_state, pgdog.fleet_acks")
        .await?;
    Ok(())
}

/// Publish the coordinator's state. NOTIFY rides the same statement,
/// so it fires with the commit.
pub(crate) async fn write_state(
    cluster: &Cluster,
    topic: Topic,
    state: &str,
    epoch: i64,
    coordinator: i64,
) -> Result<(), Error> {
    let mut server = shard_primary(cluster).await?;
    server
        .execute(
            format!(
                "INSERT INTO pgdog.fleet_state (topic, state, epoch, coordinator)
                 VALUES ('{}', '{}', {}, {})
                 ON CONFLICT (topic) DO UPDATE
                 SET state = EXCLUDED.state,
                     epoch = EXCLUDED.epoch,
                     coordinator = EXCLUDED.coordinator,
                     updated_at = NOW();
                 NOTIFY \"{}\"",
                topic.as_str(),
                state,
                epoch,
                coordinator,
                topic.channel()
            )
            .as_str(),
        )
        .await?;
    Ok(())
}

/// Read the coordinator's state for a topic; `None` when there is none.
pub(crate) async fn read_state(cluster: &Cluster, topic: Topic) -> Result<Option<StateRow>, Error> {
    let mut server = shard_primary(cluster).await?;

    let installed: Vec<String> = server
        .fetch_all("SELECT COALESCE(to_regclass('pgdog.fleet_state')::text, '')")
        .await?;
    if installed.first().map(|s| s.is_empty()).unwrap_or(true) {
        return Ok(None);
    }

    let rows: Vec<StateRow> = server
        .fetch_all(
            format!(
                "SELECT state, epoch, coordinator,
                        EXTRACT(EPOCH FROM NOW() - updated_at)::bigint
                 FROM pgdog.fleet_state WHERE topic = '{}'",
                topic.as_str()
            )
            .as_str(),
        )
        .await?;
    Ok(rows.into_iter().next())
}

/// Record this instance's ack for a state.
pub(crate) async fn ack(
    cluster: &Cluster,
    topic: Topic,
    epoch: i64,
    state: &str,
) -> Result<(), Error> {
    let mut server = shard_primary(cluster).await?;
    server
        .execute(
            format!(
                "INSERT INTO pgdog.fleet_acks (topic, node_id, epoch, state)
                 VALUES ('{}', {}, {}, '{}')
                 ON CONFLICT (topic, node_id, epoch) DO UPDATE
                 SET state = EXCLUDED.state, updated_at = NOW()",
                topic.as_str(),
                registry::node_id(),
                epoch,
                state
            )
            .as_str(),
        )
        .await?;
    Ok(())
}

/// Instances that acked a state for an epoch.
pub(crate) async fn acked(
    cluster: &Cluster,
    topic: Topic,
    epoch: i64,
    state: &str,
) -> Result<HashSet<i64>, Error> {
    let mut server = shard_primary(cluster).await?;
    let ids: Vec<i64> = server
        .fetch_all(
            format!(
                "SELECT node_id FROM pgdog.fleet_acks
                 WHERE topic = '{}' AND epoch = {} AND state = '{}'",
                topic.as_str(),
                epoch,
                state
            )
            .as_str(),
        )
        .await?;
    Ok(ids.into_iter().collect())
}

pub(crate) async fn shard_primary(cluster: &Cluster) -> Result<crate::backend::pool::Guard, Error> {
    Ok(cluster
        .shards()
        .first()
        .ok_or(crate::backend::pool::Error::NoShard(0))?
        .primary(&Request::default())
        .await?)
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_epochs_unique() {
        assert_ne!(epoch(), epoch());
    }

    #[test]
    fn test_topic_channel() {
        const TOPIC: Topic = Topic::new("add_shard");
        assert_eq!(TOPIC.channel(), "__pgdog_fleet_add_shard");
        assert_eq!(TOPIC.as_str(), "add_shard");
    }
}
