use std::collections::BTreeMap;
use std::time::{Duration, SystemTime};

use derive_more::Display;
use futures::future::join_all;

use super::super::{Error, subscriber::connect_address};
use super::{ReshardingKey, ReshardingRecord, ReshardingStore, state_address};
use crate::backend::databases::databases;
use crate::backend::pool::Address;
use crate::util::{human_duration_display, safe_timeout};
use tracing::{info, warn};

const SCAN_TIMEOUT: Duration = Duration::from_secs(10);

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
}

#[derive(Debug, Display)]
pub(crate) enum RunState {
    #[display("running")]
    Running,
    #[display("paused")]
    Paused,
}

pub(crate) async fn scan() -> Vec<ReshardingScan> {
    let clusters = databases().schema_owners();
    let addresses = join_all(clusters.iter().map(state_address)).await;

    let mut results = vec![];
    let mut by_address: BTreeMap<String, (Address, Vec<String>)> = BTreeMap::new();
    for (cluster, address) in clusters.iter().zip(addresses) {
        let database = cluster.identifier().database.clone();
        match address {
            Ok(address) => by_address
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

    let scans = join_all(
        by_address
            .into_values()
            .map(|(address, mut databases)| async move {
                match scan_address(&address).await {
                    Ok(scans) => scans,
                    Err(error) => {
                        databases.sort();
                        vec![ReshardingScan::Failed {
                            database: databases.join(", "),
                            error,
                        }]
                    }
                }
            }),
    )
    .await;
    results.extend(scans.into_iter().flatten());
    results
}

/// Never fails the run: other reshardings on the destination are only logged.
pub(crate) async fn check_other_runs(address: &Address, own: &ReshardingKey) {
    info!("resharding {own}: checking {address} for other unfinished reshardings");
    let scans = match scan_address(address).await {
        Ok(scans) => scans,
        Err(err) => {
            warn!("resharding {own}: checking other reshardings on {address} failed: {err}");
            return;
        }
    };

    let mut others = 0;
    for entry in scans {
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
        RunState::Running => info!(
            "resharding {} is in progress ({}, stage {}, running on {}, {times})",
            record.key, record.command, record.stage, record.owner.instance_id,
        ),
        RunState::Paused => warn!(
            "unfinished resharding {} ({}, stage {}, paused, {times}, slot {}); resume it or abort it with WITH (abort)",
            record.key, record.command, record.stage, record.slot_name,
        ),
    }
}

fn ago(time: SystemTime) -> String {
    human_duration_display(SystemTime::now().duration_since(time).unwrap_or_default())
}

async fn scan_address(address: &Address) -> Result<Vec<ReshardingScan>, String> {
    match safe_timeout(SCAN_TIMEOUT, list_table(address)).await {
        Ok(scans) => scans.map_err(|err| err.to_string()),
        Err(_) => Err(format!("timed out after {}s", SCAN_TIMEOUT.as_secs())),
    }
}

async fn list_table(address: &Address) -> Result<Vec<ReshardingScan>, Error> {
    let mut server = connect_address(address).await?;
    let mut found = vec![];
    let mut unreadable = vec![];
    for row in ReshardingStore::list(&mut server).await? {
        match row {
            Ok(record) => {
                let state = match ReshardingStore::lock_owner(&mut server, &record.key).await? {
                    Some(_) => RunState::Running,
                    None => RunState::Paused,
                };
                found.push(ReshardingScan::Found(Box::new(ReshardingReport {
                    record,
                    state,
                })));
            }
            Err(row) => unreadable.push(ReshardingScan::Unreadable {
                key: row.key,
                error: row.error.to_string(),
            }),
        }
    }
    found.extend(unreadable);
    Ok(found)
}

#[cfg(test)]
mod test {
    use super::super::store::test::{key, row};
    use super::*;

    fn found<'a>(scans: &'a [ReshardingScan], key: &ReshardingKey) -> &'a ReshardingReport {
        scans
            .iter()
            .find_map(|scan| match scan {
                ReshardingScan::Found(report) if report.record.key == *key => Some(report.as_ref()),
                _ => None,
            })
            .unwrap_or_else(|| panic!("{key} not found in {scans:?}"))
    }

    #[tokio::test]
    async fn table_scan_reports_running_paused_and_unreadable_rows() {
        let addr = Address::new_test();
        let running_key = key("scan_running");
        let paused_key = key("scan_paused");
        let newer_key = key("scan_newer");

        let running = ReshardingStore::open(&addr, running_key.clone())
            .await
            .unwrap();
        let paused = ReshardingStore::open(&addr, paused_key.clone())
            .await
            .unwrap();
        let newer = ReshardingStore::open(&addr, newer_key.clone())
            .await
            .unwrap();
        for store in [&running, &paused, &newer] {
            store.clear().await.unwrap();
        }
        running.insert(&row(running_key.clone())).await.unwrap();
        paused.insert(&row(paused_key.clone())).await.unwrap();
        newer
            .insert(&ReshardingRecord {
                format_version: super::super::FORMAT_VERSION + 1,
                ..row(newer_key.clone())
            })
            .await
            .unwrap();
        paused.unlock().await.unwrap();
        newer.unlock().await.unwrap();

        let scans = list_table(&addr).await.unwrap();
        assert!(matches!(
            found(&scans, &running_key).state,
            RunState::Running
        ));
        assert!(matches!(found(&scans, &paused_key).state, RunState::Paused));
        assert!(
            scans.iter().any(
                |scan| matches!(scan, ReshardingScan::Unreadable { key, .. } if *key == newer_key)
            ),
            "{scans:?}"
        );

        for store in [&running, &paused, &newer] {
            store.clear().await.unwrap();
        }
    }
}
