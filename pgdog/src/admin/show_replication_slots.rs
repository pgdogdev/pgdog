use std::ops::ControlFlow;

use chrono::{DateTime, Local, Utc};
use pgdog_stats::{TaskDefinitionKind, TaskStatus};

use crate::{
    api::tasks_storage,
    util::{format_bytes, format_time},
};

use super::prelude::*;

pub(crate) struct ShowReplicationSlots;

#[async_trait]
impl Command for ShowReplicationSlots {
    fn name(&self) -> String {
        "SHOW REPLICATION_SLOTS".into()
    }

    fn parse(_sql: &str) -> Result<Self, Error> {
        Ok(ShowReplicationSlots {})
    }

    async fn execute(&self) -> Result<Vec<Message>, Error> {
        let rd = RowDescription::new(&[
            Field::bigint("task_id"),
            Field::text("host"),
            Field::bigint("port"),
            Field::text("database_name"),
            Field::text("name"),
            Field::text("lsn"),
            Field::text("lag"),
            Field::bigint("lag_bytes"),
            Field::bigint("source_shard"),
            Field::text("last_transaction"),
            Field::bigint("last_transaction_ms"),
            Field::bigint("missed_rows"),
        ]);
        let mut messages = vec![rd.message()];
        let now = Utc::now().timestamp_millis();

        tasks_storage().try_for_each(|task| {
            let state = task.state();
            if state.is_terminal() {
                return ControlFlow::Break(());
            }

            let definition = match &state.definition.kind {
                TaskDefinitionKind::Reshard(_) | TaskDefinitionKind::Replication(_) => {
                    return ControlFlow::Continue(());
                }
                TaskDefinitionKind::ReplicationSlot(definition) => definition,
                _ => return ControlFlow::Break(()),
            };
            let TaskStatus::ReplicationSlot(status) = state.status else {
                return ControlFlow::Break(());
            };

            let last_transaction_ms = status
                .last_transaction
                .and_then(|time| now.checked_sub(time))
                .filter(|elapsed| *elapsed >= 0);
            let last_transaction_str = status
                .last_transaction
                .and_then(DateTime::<Utc>::from_timestamp_millis)
                .map(|time| format_time(time.with_timezone(&Local)));

            let mut row = DataRow::new();
            row.add(task.root_id)
                .add(definition.host.as_str())
                .add(definition.port as i64)
                .add(definition.database_name.as_str())
                .add(definition.slot.as_str())
                .add(status.lsn.to_string())
                .add(status.lag_bytes.map(|lag| format_bytes(lag.max(0) as u64)))
                .add(status.lag_bytes)
                .add(definition.source_shard as i64)
                .add(last_transaction_str)
                .add(last_transaction_ms)
                .add(
                    (status.missed_rows.inserts
                        + status.missed_rows.updates
                        + status.missed_rows.deletes) as i64,
                );

            messages.push(row.message());
            ControlFlow::Break(())
        });

        Ok(messages)
    }
}
