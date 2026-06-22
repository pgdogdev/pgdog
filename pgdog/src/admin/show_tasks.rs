use std::time::SystemTime;

use chrono::{DateTime, Local};

use crate::api::storage;
use crate::util::{format_time, human_duration_display};

use super::prelude::*;

pub struct ShowTasks;

#[async_trait]
impl Command for ShowTasks {
    fn name(&self) -> String {
        "SHOW TASKS".into()
    }

    fn parse(_sql: &str) -> Result<Self, Error> {
        Ok(ShowTasks)
    }

    async fn execute(&self) -> Result<Vec<Message>, Error> {
        let rd = RowDescription::new(&[
            Field::bigint("id"),
            Field::bigint("root_id"),
            Field::text("scope"),
            Field::text("type"),
            Field::text("status"),
            Field::text("started_at"),
            Field::text("updated_at"),
            Field::text("elapsed"),
            Field::bigint("elapsed_ms"),
        ]);
        let mut messages = vec![rd.message()?];
        let now = SystemTime::now();

        for task in storage().tasks() {
            // A root task plus its subtasks (e.g. the replication child of a
            // copy_data/reshard task). Each row carries its own `id` and the
            // `root_id` it belongs to — only root tasks are cancellable, so
            // STOP_TASK targets `root_id`. Terminal tasks are retained for
            // reporting but filtered out here.
            let root_id = task.id;
            let entries = std::iter::once((task.id, true, &task.state))
                .chain(task.subtasks.iter().map(|sub| (sub.id, false, &sub.state)));

            for (id, is_root, state) in entries {
                if state.is_terminal() {
                    continue;
                }

                let elapsed = now.duration_since(state.started_at).unwrap_or_default();
                let elapsed_ms = elapsed.as_millis() as i64;
                let elapsed_str = human_duration_display(elapsed);
                let started_at_str = format_time(DateTime::<Local>::from(state.started_at));
                let updated_at_str = format_time(DateTime::<Local>::from(state.updated_at));
                let status_str = state.status.to_string();
                let scope = if is_root { "root" } else { "subtask" };

                let mut row = DataRow::new();
                row.add(id)
                    .add(root_id)
                    .add(scope)
                    .add(state.name.as_str())
                    .add(status_str.as_str())
                    .add(started_at_str.as_str())
                    .add(updated_at_str.as_str())
                    .add(elapsed_str.as_str())
                    .add(elapsed_ms);
                messages.push(row.message()?);
            }
        }

        Ok(messages)
    }
}
