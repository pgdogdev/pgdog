//! Provisioning shards: convergence and multi-instance coordination.
//!
//! The config is declarative: a `provisioning = true` entry describes
//! the shard's final shape and the flag only keeps it out of the
//! serving topology. That leaves two problems this module solves,
//! both using the new shard itself as the shared medium — every
//! instance that has the entry can reach it, and it serves nothing
//! else.
//!
//! **Convergence**: activation flips the flag in memory, so a restart
//! or reload with the flag still present (a manifest that wasn't
//! updated yet) would regress to the old topology while the new shard
//! already holds data. The cutover writes `(shard = N, shards = N+1)`
//! into the new shard's `pgdog.config`; at startup and after every
//! reload we ask the new shard and re-activate when it reports itself
//! live.
//!
//! **Coordination**: with several pgdog instances, the cutover must
//! pause omni writes on all of them and activate the shard on all of
//! them. Each instance that sees a provisioning entry runs an agent
//! that registers on the new shard and polls a state row
//! (`pgdog.topology_state`); the instance running `ADD SHARD` drives
//! the state machine (armed -> activated, or released on abort) and
//! collects acks (`pgdog.topology_acks`).

use std::collections::HashSet;
use std::time::Duration;

use once_cell::sync::Lazy;
use parking_lot::Mutex;
use pgdog_config::ConfigAndUsers;
use tokio::time::sleep;
use tracing::{debug, info, warn};

use crate::backend::Cluster;
use crate::backend::Error;
use crate::backend::databases::{
    activate_provisioning_shard, databases, persist_config, provisioning_cluster,
};
use crate::backend::fleet::protocol::StateRow;
use crate::backend::fleet::{Follower, Topic, barrier};
use crate::backend::pool::Request;
use crate::config::config;

/// Convergence attempts before giving up; a reload retries.
const ATTEMPTS: usize = 5;
const RETRY_DELAY: Duration = Duration::from_secs(3);

/// Bounds each activation-marker check: convergence runs before the
/// listener opens at startup, and a hanging new shard must not stall
/// boot (the background retries pick it up).
const MARKER_TIMEOUT: Duration = Duration::from_secs(5);

/// ADD SHARD's coordination, on the new shard as the medium.
pub(crate) const TOPIC: Topic = Topic::new("add_shard");

/// How long the coordinator waits for every peer to arm its barrier.
pub(crate) const ARM_ACK_TIMEOUT: Duration = Duration::from_secs(10);

/// How long the coordinator waits for every peer to activate. The
/// activation stands either way; stragglers converge on their own.
pub(crate) const ACTIVATE_ACK_TIMEOUT: Duration = Duration::from_secs(15);

pub(crate) const STATE_ARMED: &str = "armed";
pub(crate) const STATE_ACTIVATED: &str = "activated";
pub(crate) const STATE_RELEASED: &str = "released";

/// Called at startup and after every reload: converge provisioning
/// entries and make sure each has an agent.
pub fn on_config_change() {
    converge_in_background();
    ensure_agents();
}

/// The actionable provisioning entry per database: the lowest-numbered
/// one. Several future shards can be declared at once, but only the
/// next shard can be added (and so only it can have been activated),
/// so the lowest declared number is the only one worth watching.
fn candidates(config: &ConfigAndUsers) -> Vec<(String, usize)> {
    let mut names = config
        .config
        .databases
        .iter()
        .filter(|database| database.provisioning)
        .map(|database| database.name.as_str())
        .collect::<Vec<_>>();
    names.sort_unstable();
    names.dedup();

    names
        .into_iter()
        .filter_map(|name| {
            config
                .config
                .provisioning_shards(name)
                .iter()
                .map(|entry| entry.shard)
                .min()
                .map(|shard| (name.to_string(), shard))
        })
        .collect()
}

//
// Convergence.
//

/// Bounded convergence passes, awaited before the listener opens: a
/// crashed instance whose manifest still carries flags re-joins the
/// new topology before serving a single query. Bounded by
/// [`MARKER_TIMEOUT`] per check, inside `converge`. Loops because with
/// several declared shards, each activation can expose the next one.
pub async fn converge_at_startup() {
    loop {
        let mut progressed = false;
        for (database, shard) in candidates(&config()) {
            match converge(&database, shard).await {
                Ok(activated) => progressed = progressed || activated,
                Err(err) => debug!(
                    r#"startup convergence for "{}" failed: {}; retrying in the background"#,
                    database, err
                ),
            }
        }
        if !progressed {
            break;
        }
    }
}

/// Converge all provisioning entries in the config against what their
/// new shards report. A no-op without provisioning entries.
fn converge_in_background() {
    for (database, shard) in candidates(&config()) {
        crate::tasks::spawn("provisioning convergence", async move {
            for attempt in 1..=ATTEMPTS {
                match converge(&database, shard).await {
                    Ok(_) => return,
                    Err(err) => {
                        if attempt == ATTEMPTS {
                            warn!(
                                r#"could not check whether shard {} of "{}" was already activated: {}; reload to retry"#,
                                shard, database, err
                            );
                        } else {
                            sleep(RETRY_DELAY).await;
                        }
                    }
                }
            }
        });
    }
}

/// Activate `declared` for `database` if the new shard reports itself
/// live.
async fn converge(database: &str, declared: usize) -> Result<bool, Error> {
    let serving = databases().schema_owner(database)?.shards().len();
    if declared != serving {
        // Not the next shard; ADD SHARD would refuse it too.
        return Ok(false);
    }

    // The cluster is always shut down before the timeout propagates:
    // a dropped-at-await future would leak its launched pools.
    let cluster = provisioning_cluster(database, declared)?;
    let marker = tokio::time::timeout(MARKER_TIMEOUT, activation_marker(&cluster)).await;
    cluster.shutdown();
    let marker = match marker {
        Ok(marker) => marker,
        Err(_) => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "activation marker check timed out",
            )
            .into());
        }
    };

    let Some((marker_shard, marker_shards)) = marker? else {
        debug!(
            r#"shard {} of "{}" doesn't report itself active; leaving it provisioning"#,
            declared, database
        );
        return Ok(false);
    };

    if marker_shard != declared || marker_shards != serving + 1 {
        warn!(
            r#"the provisioning shard of "{}" reports itself as shard {} of {}, but the config declares it as shard {} of {}; not converging"#,
            database,
            marker_shard,
            marker_shards,
            declared,
            serving + 1
        );
        return Ok(false);
    }

    info!(
        r#"shard {} of "{}" was already activated (the shard reports itself live); converging"#,
        declared, database
    );
    let new_config = activate_provisioning_shard(database, declared).await?;
    if config().config.general.cutover_save_config {
        persist_config(&new_config).await?;
    }

    Ok(true)
}

/// Read `(shard, shards)` from the new shard's `pgdog.config`, written
/// there by the cutover. `None` when the marker table or row doesn't
/// exist: the shard was never activated.
async fn activation_marker(cluster: &Cluster) -> Result<Option<(usize, usize)>, Error> {
    let mut server = cluster
        .shards()
        .first()
        .ok_or(crate::backend::pool::Error::NoShard(0))?
        .primary(&Request::default())
        .await?;

    let installed: Vec<String> = server
        .fetch_all("SELECT COALESCE(to_regclass('pgdog.config')::text, '')")
        .await?;
    if installed.first().map(|s| s.is_empty()).unwrap_or(true) {
        return Ok(None);
    }

    let shard: Vec<i32> = server.fetch_all("SELECT shard FROM pgdog.config").await?;
    let shards: Vec<i32> = server.fetch_all("SELECT shards FROM pgdog.config").await?;

    Ok(match (shard.first(), shards.first()) {
        (Some(&shard), Some(&shards)) if shard >= 0 && shards > 0 => {
            Some((shard as usize, shards as usize))
        }
        _ => None,
    })
}

//
// The per-instance agent.
//

/// Shards with a running agent on this instance.
static AGENTS: Lazy<Mutex<HashSet<(String, usize)>>> = Lazy::new(Default::default);

struct AgentSlot(String, usize);

impl Drop for AgentSlot {
    fn drop(&mut self) {
        AGENTS.lock().remove(&(self.0.clone(), self.1));
    }
}

/// Spawn an agent for every actionable provisioning entry that
/// doesn't have one.
fn ensure_agents() {
    for (database, shard) in candidates(&config()) {
        if AGENTS.lock().insert((database.clone(), shard)) {
            info!(
                r#"provisioning agent started for shard {} of "{}""#,
                shard, database
            );
            crate::tasks::spawn("provisioning agent", agent_loop(database, shard));
        }
    }
}

/// Register on the new shard and follow the coordinator's state until
/// the entry stops being a provisioning entry (activated here, or
/// removed from the config).
async fn agent_loop(database: String, declared: usize) {
    let _slot = AgentSlot(database.clone(), declared);
    let mut follower: Option<Follower> = None;
    let mut armed = false;

    loop {
        let still_provisioning = candidates(&config())
            .iter()
            .any(|(name, shard)| name == &database && *shard == declared);
        if !still_provisioning {
            break;
        }

        match agent_tick(&database, declared, &mut follower, &mut armed).await {
            Ok(()) => {
                if let Some(follower) = follower.as_mut() {
                    follower.wake().await;
                }
            }
            Err(err) => {
                debug!(r#"provisioning agent for "{}": {}"#, database, err);
                if let Some(follower) = follower.take() {
                    follower.shutdown();
                }
                sleep(Duration::from_secs(1)).await;
            }
        }
    }

    if armed {
        barrier::stop(&database);
    }
    if let Some(follower) = follower.take() {
        follower.shutdown();
    }
    info!(r#"provisioning agent stopped for "{}""#, database);
}

async fn agent_tick(
    database: &str,
    declared: usize,
    follower_slot: &mut Option<Follower>,
    armed: &mut bool,
) -> Result<(), Error> {
    if follower_slot.is_none() {
        let cluster = provisioning_cluster(database, declared)?;
        *follower_slot = Some(Follower::connect(cluster, TOPIC).await?);
    }
    let follower = follower_slot.as_mut().expect("follower just ensured");

    // Registration doubles as the heartbeat the coordinator's
    // completeness check reads, and heals a new shard that was wiped
    // for a rerun.
    follower.heartbeat().await?;
    let follower = follower_slot.as_ref().expect("follower just ensured");

    let me = crate::backend::fleet::registry::node_id();
    match follower.state().await? {
        // Our own task drives the local barrier directly.
        Some(row) if row.coordinator == me => {}
        Some(row) if row.state == STATE_ARMED => {
            follow_arm(database, &row, follower, armed).await?
        }
        Some(row) if row.state == STATE_ACTIVATED => {
            follow_activation(database, declared, &row, follower, armed).await?;
        }
        Some(row) if row.state == STATE_RELEASED => {
            if disarm(database, armed) {
                info!(
                    r#"cutover released: resuming omni writes for "{}""#,
                    database
                );
            }
            follower.ack(row.epoch, STATE_RELEASED).await?;
        }
        _ => {
            disarm(database, armed);
        }
    }

    Ok(())
}

/// Release the barrier if this agent armed it. Returns whether it did,
/// so callers log with their own context.
fn disarm(database: &str, armed: &mut bool) -> bool {
    if *armed {
        barrier::stop(database);
        *armed = false;
        true
    } else {
        false
    }
}

/// Park omni writes for an armed cutover and ack. The barrier is
/// re-checked every tick: an `OMNI_WRITES ON` or a local task's guard
/// can drop it underneath us, and the coordinator trusts the ack.
async fn follow_arm(
    database: &str,
    row: &StateRow,
    follower: &Follower,
    armed: &mut bool,
) -> Result<(), Error> {
    // Failsafe: a live coordinator keeps the armed row fresh; one that
    // died mid-drain must not park omni writes forever.
    if row.coordinator_silent() {
        if disarm(database, armed) {
            warn!(
                r#"cutover coordinator for "{}" went silent; resuming omni writes"#,
                database
            );
        }
        return Ok(());
    }

    if !barrier::is_on(database) {
        info!(
            r#"cutover armed by another instance: pausing omni writes for "{}""#,
            database
        );
        barrier::start(database);
    }
    *armed = true;
    follower.ack(row.epoch, STATE_ARMED).await
}

/// Activate the shard this instance was told is live, release the
/// barrier, and ack.
async fn follow_activation(
    database: &str,
    declared: usize,
    row: &StateRow,
    follower: &Follower,
    armed: &mut bool,
) -> Result<(), Error> {
    match activate_provisioning_shard(database, declared).await {
        Ok(new_config) => {
            info!(
                r#"shard {} of "{}" activated by another instance; following"#,
                declared, database
            );
            if config().config.general.cutover_save_config
                && let Err(err) = persist_config(&new_config).await
            {
                warn!("failed to persist config after activation: {}", err);
            }
        }
        // Already flipped here (e.g. convergence raced us).
        Err(err) => debug!(r#"activation on "{}" already applied: {}"#, database, err),
    }
    disarm(database, armed);
    follower.ack(row.epoch, STATE_ACTIVATED).await
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_candidates() {
        let source = r#"
[[databases]]
name = "prod"
host = "10.0.0.1"
shard = 0

[[databases]]
name = "prod"
host = "10.0.0.2"
shard = 2
provisioning = true

[[databases]]
name = "other"
host = "10.0.0.3"
shard = 0
"#;
        let config = ConfigAndUsers {
            config: toml::from_str(source).unwrap(),
            ..Default::default()
        };
        assert_eq!(candidates(&config), vec![("prod".to_string(), 2)]);

        // Several future shards declared at once: the candidate is the
        // lowest-numbered one, the only shard that can be added (or
        // have been activated) next.
        let source = source.to_string()
            + r#"
[[databases]]
name = "prod"
host = "10.0.0.4"
shard = 3
provisioning = true
"#;
        let config = ConfigAndUsers {
            config: toml::from_str(&source).unwrap(),
            ..Default::default()
        };
        assert_eq!(candidates(&config), vec![("prod".to_string(), 2)]);
    }
}
