use std::collections::BTreeMap;
use std::time::Duration;

use derive_more::Display;
use futures::future::{join_all, try_join_all};

use super::super::{Error, subscriber::connect_address};
use super::queries::{CountOrigins, CountSlots, Query};
use super::snapshot::ConfigMismatch;
use super::{ConfigSnapshot, ReshardingKey, ReshardingRecord, ReshardingStore, state_address};
use crate::backend::Cluster;
use crate::backend::databases::databases;
use crate::backend::pool::{Address, Request};
use crate::config::config;
use crate::util::safe_timeout;

const SCAN_TIMEOUT: Duration = Duration::from_secs(10);

/// One entry of the scan: what `SHOW RESHARDING` and the startup warning
/// report for each state table and its rows.
#[derive(Debug)]
pub(crate) enum ReshardingScan {
    /// A row this pgdog version can read, with the live checks of its objects.
    Found(Box<ReshardingReport>),
    /// A row written by a newer or unknown format; only its key is known.
    Unreadable { key: ReshardingKey, error: String },
    /// The state table of these databases could not be read at all.
    Failed { database: String, error: String },
}

/// An unfinished resharding and what is true about it right now.
#[derive(Debug)]
pub(crate) struct ReshardingReport {
    /// The stored row, as the last attempt left it.
    pub(crate) record: ReshardingRecord,
    /// Whether a session holds the resharding lock at the time of the scan.
    pub(crate) state: RunState,
    /// Forward replication slots on the source shards. `None` when they could
    /// not be counted: the source is not configured, or a query failed.
    pub(crate) slots: Option<ObjectCount>,
    /// Replication origins on the destination shards. `None` as for `slots`.
    pub(crate) origins: Option<ObjectCount>,
    /// The stored sharding config compared with the live one.
    pub(crate) config: ConfigCheck,
}

/// The lock decides the state: the database is the source of truth.
#[derive(Debug, Clone, PartialEq, Eq, Display)]
pub(crate) enum RunState {
    /// A session holds the lock; `owner` is its pgdog instance id.
    #[display("running")]
    Running { owner: String },
    /// No session holds the lock: the run stopped or crashed and kept its
    /// slots and origins.
    #[display("paused")]
    Paused,
}

impl RunState {
    pub(crate) fn owner(&self) -> Option<&str> {
        match self {
            Self::Running { owner } => Some(owner),
            Self::Paused => None,
        }
    }
}

/// How many objects of one kind exist, and how many the resharding needs.
/// One slot per source shard; one origin per source shard on each destination
/// shard.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Display)]
#[display("{present}/{expected}")]
pub(crate) struct ObjectCount {
    pub(crate) present: usize,
    pub(crate) expected: usize,
}

/// Result of comparing the config snapshot in the row with the live config.
/// A resume (Stage 2) refuses `Changed`; Stage 1 only reports it.
#[derive(Debug, Display)]
pub(crate) enum ConfigCheck {
    /// Every stored sharding setting matches the live config.
    #[display("ok")]
    Unchanged,
    /// These settings differ from the live config.
    #[display(
        "changed: {}",
        _0.iter().map(ToString::to_string).collect::<Vec<_>>().join(", ")
    )]
    Changed(Vec<ConfigMismatch>),
    /// The live config cannot be compared, for example because the source or
    /// destination is no longer configured.
    #[display("unknown: {_0}")]
    Unknown(String),
}

pub(crate) async fn scan() -> Vec<ReshardingScan> {
    let clusters = databases().schema_owners();
    let addresses = join_all(clusters.iter().map(state_address)).await;

    let mut results = vec![];
    let mut tables: BTreeMap<String, (Address, Vec<String>)> = BTreeMap::new();
    for (cluster, address) in clusters.iter().zip(addresses) {
        let database = cluster.identifier().database.clone();
        match address {
            Ok(address) => tables
                .entry(address.to_string())
                .or_insert_with(|| (address, vec![]))
                .1
                .push(database),
            Err(err) => results.push(ReshardingScan::Failed {
                database,
                error: err.to_string(),
            }),
        }
    }

    let scans = join_all(tables.into_values().map(|(address, mut names)| async move {
        names.sort();
        let database = names.join(", ");
        match safe_timeout(SCAN_TIMEOUT, scan_table(&address)).await {
            Ok(Ok(found)) => found,
            Ok(Err(err)) => vec![ReshardingScan::Failed {
                database,
                error: err.to_string(),
            }],
            Err(_) => vec![ReshardingScan::Failed {
                database,
                error: format!("timed out after {}s", SCAN_TIMEOUT.as_secs()),
            }],
        }
    }))
    .await;
    results.extend(scans.into_iter().flatten());
    results
}

async fn scan_table(address: &Address) -> Result<Vec<ReshardingScan>, Error> {
    let mut server = connect_address(address).await?;
    let mut found = vec![];
    let mut unreadable = vec![];
    for row in ReshardingStore::list(&mut server).await? {
        match row {
            Ok(record) => {
                let owner = ReshardingStore::lock_owner(&mut server, &record.key).await?;
                found.push((record, owner));
            }
            Err(row) => unreadable.push(ReshardingScan::Unreadable {
                key: row.key,
                error: row.error.to_string(),
            }),
        }
    }

    let mut results: Vec<ReshardingScan> = join_all(
        found
            .into_iter()
            .map(|(record, owner)| async move { report(record, owner).await }),
    )
    .await
    .into_iter()
    .map(|report| ReshardingScan::Found(Box::new(report)))
    .collect();
    results.extend(unreadable);
    Ok(results)
}

async fn report(record: ReshardingRecord, owner: Option<String>) -> ReshardingReport {
    let names = &record.key.databases;
    let source = databases().schema_owner(&names.source).ok();
    let destination = databases().schema_owner(&names.destination).ok();

    let (slots, origins, config) = match (&source, &destination) {
        (Some(source), Some(destination)) => {
            let (slots, origins) = tokio::join!(
                count_slots(source, &record.slot_name),
                count_origins(source, destination, &record.slot_name),
            );
            (
                slots.ok(),
                origins.ok(),
                check_config(&record, source, destination),
            )
        }
        (Some(source), None) => (
            count_slots(source, &record.slot_name).await.ok(),
            None,
            not_in_config(&names.destination),
        ),
        (None, _) => (None, None, not_in_config(&names.source)),
    };

    ReshardingReport {
        record,
        state: match owner {
            Some(owner) => RunState::Running { owner },
            None => RunState::Paused,
        },
        slots,
        origins,
        config,
    }
}

fn not_in_config(database: &str) -> ConfigCheck {
    ConfigCheck::Unknown(format!("\"{database}\" is not in the config"))
}

fn check_config(record: &ReshardingRecord, source: &Cluster, destination: &Cluster) -> ConfigCheck {
    match ConfigSnapshot::capture(source, destination, &config().config) {
        Ok(current) => match record.config.validate_equal(&current) {
            Ok(()) => ConfigCheck::Unchanged,
            Err(changed) => ConfigCheck::Changed(changed),
        },
        Err(err) => ConfigCheck::Unknown(err.to_string()),
    }
}

async fn count_slots(source: &Cluster, slot_name: &str) -> Result<ObjectCount, Error> {
    let counts = try_join_all(source.shards().iter().enumerate().map(
        |(number, shard)| async move {
            let mut server = shard.primary(&Request::default()).await?;
            let counts: Vec<i64> = server
                .fetch_all(CountSlots::request(&format!("{slot_name}_{number}")))
                .await?;
            Ok::<_, Error>(counts.first().copied().unwrap_or(0) as usize)
        },
    ))
    .await?;
    Ok(ObjectCount {
        present: counts.iter().sum(),
        expected: source.shards().len(),
    })
}

async fn count_origins(
    source: &Cluster,
    destination: &Cluster,
    slot_name: &str,
) -> Result<ObjectCount, Error> {
    let sources = source.shards().len();
    let counts = try_join_all(destination.shards().iter().enumerate().map(
        |(shard, destination_shard)| async move {
            let names: Vec<String> = (0..sources)
                .map(|number| format!("__pgdog_origin_{slot_name}_{number}_{shard}"))
                .collect();
            let mut server = destination_shard.primary(&Request::default()).await?;
            let counts: Vec<i64> = server.fetch_all(CountOrigins::request(&names)).await?;
            Ok::<_, Error>(counts.first().copied().unwrap_or(0) as usize)
        },
    ))
    .await?;
    Ok(ObjectCount {
        present: counts.iter().sum(),
        expected: sources * destination.shards().len(),
    })
}
