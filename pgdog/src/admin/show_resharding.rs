use crate::backend::replication::logical::run::scan::{
    ObjectCount, ReshardingReport, ReshardingScan, scan,
};

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
            Field::text("owner"),
            Field::text("last_task"),
            Field::text("replication_slot"),
            Field::bool("slot_existed"),
            Field::text("slots"),
            Field::text("origins"),
            Field::text("config"),
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
        .add(report.state.owner())
        .add(record.owner.to_string())
        .add(record.slot_name.as_str())
        .add(record.slot_existed)
        .add(count(report.slots))
        .add(count(report.origins))
        .add(report.config.to_string());
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
        .add(None::<bool>)
        .add(none)
        .add(none)
        .add(none);
    row
}

fn count(count: Option<ObjectCount>) -> String {
    count.map_or_else(|| "unknown".into(), |count| count.to_string())
}
