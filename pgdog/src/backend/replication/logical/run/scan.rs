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
use crate::util::{human_duration_display, safe_timeout};
use tracing::{info, warn};

const SCAN_TIMEOUT: Duration = Duration::from_secs(10);
/// Rows of running reshardings are refreshed every few seconds by the heartbeat.
const STALE_AFTER: Duration = Duration::from_secs(5 * 60);

#[derive(Debug)]
pub(crate) enum ReshardingScan {
    Found(Box<ReshardingReport>),
    Unreadable { key: ReshardingKey, error: String },
    Failed { database: String, error: String },
}

#[derive(Debug)]
pub(crate) struct ReshardingReport {
    pub(crate) record: ReshardingRecord,
    pub(crate) state: RunState,
    /// `None` when the objects could not be counted.
    pub(crate) slots: Option<ObjectCount>,
    pub(crate) origins: Option<ObjectCount>,
    pub(crate) config: ConfigCheck,
}

#[derive(Debug, Clone, PartialEq, Eq, Display)]
pub(crate) enum RunState {
    #[display("running")]
    Running { owner: String },
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Display)]
#[display("{present}/{expected}")]
pub(crate) struct ObjectCount {
    pub(crate) present: usize,
    pub(crate) expected: usize,
}

#[derive(Debug, Display)]
pub(crate) enum ConfigCheck {
    #[display("ok")]
    Unchanged,
    #[display(
        "changed: {}",
        _0.iter().map(ToString::to_string).collect::<Vec<_>>().join(", ")
    )]
    Changed(Vec<ConfigMismatch>),
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

/// Never fails the run: other reshardings on the destination are only logged.
pub(crate) async fn check_other_runs(address: &Address, own: &ReshardingKey) {
    info!("resharding {own}: checking {address} for other unfinished reshardings");
    let found = match safe_timeout(SCAN_TIMEOUT, scan_table(address)).await {
        Ok(Ok(found)) => found,
        Ok(Err(err)) => {
            warn!("resharding {own}: checking other reshardings on {address} failed: {err}");
            return;
        }
        Err(_) => {
            warn!(
                "resharding {own}: checking other reshardings on {address} timed out after {}s",
                SCAN_TIMEOUT.as_secs()
            );
            return;
        }
    };

    let mut others = 0;
    for entry in found {
        match entry {
            ReshardingScan::Found(report) if report.record.key != *own => {
                others += 1;
                log_other_run(&report);
            }
            ReshardingScan::Unreadable { key, error } if key != *own => {
                others += 1;
                warn!("unfinished resharding {key} cannot be read by this pgdog version: {error}")
            }
            _ => (),
        }
    }
    if others == 0 {
        info!("resharding {own}: no other unfinished reshardings on {address}");
    }
}

fn log_other_run(report: &ReshardingReport) {
    let record = &report.record;
    let state = match &report.state {
        RunState::Running { owner } => format!("running on {owner}"),
        RunState::Paused => "paused".into(),
    };
    let since_update = record.since_update();
    let since_text = human_duration_display(since_update);
    if since_update < STALE_AFTER {
        info!(
            "resharding {} is in progress ({}, stage {}, {state}, last update {since_text} ago)",
            record.key, record.command, record.stage,
        );
    } else {
        let count = |count: Option<ObjectCount>| {
            count.map_or_else(|| "unknown".into(), |count| count.to_string())
        };
        warn!(
            "unfinished resharding {} ({}, stage {}, {state}, last update {since_text} ago, slot {}, slots {}, origins {}, config {}); resume it or abort it with WITH (abort)",
            record.key,
            record.command,
            record.stage,
            record.slot_name,
            count(report.slots),
            count(report.origins),
            report.config,
        );
    }
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
