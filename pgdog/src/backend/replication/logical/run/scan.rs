use std::collections::BTreeMap;
use std::time::{Duration, SystemTime};

use derive_more::Display;
use futures::future::{join_all, try_join_all};

use super::super::publisher::permanent_slot_name;
use super::super::subscriber::replication_origin::origin_name;
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
const COUNT_TIMEOUT: Duration = Duration::from_secs(10);

// W: verify which types should be pub and hide inner types
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

#[derive(Debug, Display)]
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

#[derive(Debug, Clone, Copy, Display)]
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
    // W: why is it named tables? we store some address mapping
    let mut tables: BTreeMap<String, (Address, Vec<String>)> = BTreeMap::new();
    for (cluster, address) in clusters.iter().zip(addresses) {
        let database = cluster.identifier().database.clone();
        match address {
            Ok(address) => tables
                .entry(address.to_string())
                // W: why we store address again?
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
        match safe_timeout(SCAN_TIMEOUT, list_table(&address)).await {
            Ok(Ok(listed)) => reports(listed).await,
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
    let found = match safe_timeout(SCAN_TIMEOUT, list_table(address)).await {
        Ok(Ok(mut listed)) => {
            listed.found.retain(|(record, _)| record.key != *own);
            reports(listed).await
        }
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
    let times = match record.resumed_at {
        Some(resumed_at) => format!(
            "started {} ago, resumed {} ago",
            ago(record.started_at),
            ago(resumed_at)
        ),
        None => format!("started {} ago", ago(record.started_at)),
    };
    match &report.state {
        RunState::Running { owner } => info!(
            "resharding {} is in progress ({}, stage {}, running on {owner}, {times})",
            record.key, record.command, record.stage,
        ),
        RunState::Paused => warn!(
            "unfinished resharding {} ({}, stage {}, paused, {times}, slot {}, slots {}, origins {}, config {}); resume it or abort it with WITH (abort)",
            record.key,
            record.command,
            record.stage,
            record.slot_name,
            count_text(report.slots),
            count_text(report.origins),
            report.config,
        ),
    }
}

pub(crate) fn count_text(count: Option<ObjectCount>) -> String {
    count.map_or_else(|| "unknown".into(), |count| count.to_string())
}

fn ago(time: SystemTime) -> String {
    human_duration_display(SystemTime::now().duration_since(time).unwrap_or_default())
}

struct Listed {
    found: Vec<(ReshardingRecord, Option<String>)>,
    unreadable: Vec<ReshardingScan>,
}

async fn list_table(address: &Address) -> Result<Listed, Error> {
    let mut server = connect_address(address).await?;
    let mut listed = Listed {
        found: vec![],
        unreadable: vec![],
    };
    for row in ReshardingStore::list(&mut server).await? {
        match row {
            Ok(record) => {
                let owner = ReshardingStore::lock_owner(&mut server, &record.key).await?;
                listed.found.push((record, owner));
            }
            Err(row) => listed.unreadable.push(ReshardingScan::Unreadable {
                key: row.key,
                error: row.error.to_string(),
            }),
        }
    }
    Ok(listed)
}

async fn reports(listed: Listed) -> Vec<ReshardingScan> {
    let mut results: Vec<ReshardingScan> = join_all(
        listed
            .found
            .into_iter()
            .map(|(record, owner)| async move { report(record, owner).await }),
    )
    .await
    .into_iter()
    .map(|report| ReshardingScan::Found(Box::new(report)))
    .collect();
    results.extend(listed.unreadable);
    results
}

async fn report(record: ReshardingRecord, owner: Option<String>) -> ReshardingReport {
    let names = &record.key.databases;
    let source = databases().schema_owner(&names.source);
    let destination = databases().schema_owner(&names.destination);

    let (slots, origins, config) = match (&source, &destination) {
        (Ok(source), Ok(destination)) => {
            let (slots, origins) = tokio::join!(
                counted(count_slots(source, &record.slot_name)),
                counted(count_origins(source, destination, &record.slot_name)),
            );
            (
                slots,
                origins,
                check_config(&record, source, destination),
            )
        }
        (Ok(source), Err(err)) => (
            counted(count_slots(source, &record.slot_name)).await,
            None,
            ConfigCheck::Unknown(err.to_string()),
        ),
        (Err(err), _) => (None, None, ConfigCheck::Unknown(err.to_string())),
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

fn check_config(record: &ReshardingRecord, source: &Cluster, destination: &Cluster) -> ConfigCheck {
    match ConfigSnapshot::capture(source, destination, &config().config) {
        Ok(current) => match record.config.validate_equal(&current) {
            Ok(()) => ConfigCheck::Unchanged,
            Err(changed) => ConfigCheck::Changed(changed),
        },
        Err(err) => ConfigCheck::Unknown(err.to_string()),
    }
}

async fn counted(
    count: impl Future<Output = Result<ObjectCount, Error>>,
) -> Option<ObjectCount> {
    safe_timeout(COUNT_TIMEOUT, count).await.ok()?.ok()
}

async fn count_slots(source: &Cluster, slot_name: &str) -> Result<ObjectCount, Error> {
    let counts = try_join_all(source.shards().iter().enumerate().map(
        |(number, shard)| async move {
            let mut server = shard.primary(&Request::default()).await?;
            let counts: Vec<i64> = server
                .fetch_all(CountSlots::request(&permanent_slot_name(slot_name, number)))
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
                .map(|number| origin_name(&permanent_slot_name(slot_name, number), shard))
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
