use std::ops::ControlFlow;
use std::time::SystemTime;

use chrono::{DateTime, Local};
use pgdog_stats::TaskDefinitionKind;

use crate::{
    api::tasks_storage,
    util::{format_time, human_duration_display},
};

use super::prelude::*;

pub(crate) struct ShowSchemaSync;

#[async_trait]
impl Command for ShowSchemaSync {
    fn name(&self) -> String {
        "SHOW SCHEMA_SYNC".into()
    }

    fn parse(_sql: &str) -> Result<Self, Error> {
        Ok(ShowSchemaSync {})
    }

    async fn execute(&self) -> Result<Vec<Message>, Error> {
        let rd = RowDescription::new(&[
            Field::bigint("parent_id"),
            Field::bigint("id"),
            Field::text("source"),
            Field::text("destination"),
            Field::text("sync_state"),
            Field::bigint("shard"),
            Field::text("status"),
            Field::text("inner_status"),
            Field::text("started_at"),
            Field::text("elapsed"),
            Field::bigint("elapsed_ms"),
        ]);
        let now = SystemTime::now();
        let mut messages = vec![rd.message()];

        tasks_storage().try_for_each(|task| {
            let state = task.state();

            let (databases, sync_state, shard) = match &state.definition.kind {
                TaskDefinitionKind::SchemaSync(def) => {
                    (&def.databases, def.sync_state, None::<i64>)
                }
                TaskDefinitionKind::SchemaShard(def) => {
                    (&def.databases, def.sync_state, Some(def.shard as i64))
                }
                _ => return ControlFlow::Continue(()),
            };

            let end = if state.is_terminal() {
                state.updated_at
            } else {
                now
            };
            let elapsed = end.duration_since(state.started_at).unwrap_or_default();

            let mut row = DataRow::new();
            row.add(task.parent_id)
                .add(task.id)
                .add(databases.source.as_str())
                .add(databases.destination.as_str())
                .add(sync_state.to_string().as_str())
                .add(shard)
                .add(state.progress.to_string().as_str())
                .add(state.status.to_string().as_str())
                .add(format_time(DateTime::<Local>::from(state.started_at)).as_str())
                .add(human_duration_display(elapsed).as_str())
                .add(elapsed.as_millis() as i64);

            messages.push(row.message());

            ControlFlow::Continue(())
        });

        Ok(messages)
    }
}
