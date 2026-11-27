use std::time::SystemTime;

use chrono::{DateTime, Local};

use crate::backend::replication::logical::run::scan::{
    ReshardingReport, ReshardingScan, RunState, scan,
};
use crate::util::format_time;

use super::prelude::*;

pub(crate) struct ShowResharding;

#[async_trait]
impl Command for ShowResharding {
    fn name(&self) -> String {
        "SHOW RESHARDING".into()
    }

    fn parse(_sql: &str) -> Result<Self, Error> {
        Ok(ShowResharding)
    }

    async fn execute(&self) -> Result<Vec<Message>, Error> {
        let rd = RowDescription::new(&[
            Field::text("source"),
            Field::text("destination"),
            Field::text("command"),
            Field::text("publication"),
            Field::text("stage"),
            Field::text("state"),
            Field::text("instance"),
            Field::text("task_id"),
            Field::text("replication_slot"),
            Field::text("started_at"),
            Field::text("resumed_at"),
        ]);
        let mut messages = vec![rd.message()];

        for found in scan().await {
            let row = match found {
                ReshardingScan::Found(report) => found_row(&report),
                ReshardingScan::Unreadable { key, error } => problem_row(
                    Some(&key.databases.source),
                    &key.databases.destination,
                    Some(&key.publication),
                    format!("unsupported: {error}"),
                ),
                ReshardingScan::Failed { database, error } => {
                    problem_row(None, &database, None, format!("error: {error}"))
                }
            };
            messages.push(row.message());
        }

        Ok(messages)
    }
}

fn found_row(report: &ReshardingReport) -> DataRow {
    let record = &report.record;

    let mut row = DataRow::new();
    row.add(record.key.databases.source.as_str())
        .add(record.key.databases.destination.as_str())
        .add(record.command.to_string())
        .add(record.key.publication.as_str())
        .add(record.stage.to_string())
        .add(report.state.to_string())
        .add(record.owner.instance_id.to_string())
        .add(matches!(report.state, RunState::Running).then(|| record.owner.task_id.to_string()))
        .add(record.slot_name.as_str())
        .add(local_time(record.started_at))
        .add(record.resumed_at.map(local_time));
    row
}

fn problem_row(
    source: Option<&str>,
    destination: &str,
    publication: Option<&str>,
    state: String,
) -> DataRow {
    let none: Option<&str> = None;
    let mut row = DataRow::new();
    row.add(source)
        .add(destination)
        .add(none)
        .add(publication)
        .add(none)
        .add(state)
        .add(none)
        .add(none)
        .add(none)
        .add(none)
        .add(none);
    row
}

fn local_time(time: SystemTime) -> String {
    format_time(DateTime::<Local>::from(time))
}
