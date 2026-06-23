use std::time::SystemTime;

use chrono::{DateTime, Local};

use crate::api::tasks_storage;
use crate::net::data_row::Data;
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
            Field::text("scope"),
            Field::text("type"),
            Field::text("status"),
            Field::text("inner_status"),
            Field::text("started_at"),
            Field::text("updated_at"),
            Field::text("elapsed"),
            Field::bigint("elapsed_ms"),
        ]);
        let mut messages = vec![rd.message()?];
        let now = SystemTime::now();

        // Most-recent task first (highest id).
        for task in tasks_storage().tasks().into_iter().rev() {
            // A root task plus its subtasks (e.g. the replication child of a
            // copy_data/reshard task). Every row reports the root task's id as
            // `id` — that is the cancellable handle (STOP_TASK targets it); the
            // `scope` column distinguishes the root from its subtasks. Terminal
            // tasks stay listed with their final status until pruned from the map.
            let root_id = task.id;
            let entries = std::iter::once((true, &task.state))
                .chain(task.subtasks.iter().map(|sub| (false, &sub.state)));

            for (is_root, state) in entries {
                // Terminal tasks are retained after completion; measure their
                // elapsed to the final transition (`updated_at`), not `now`,
                // so the duration reflects the actual run rather than ticking
                // up until the task is pruned.
                let end = if state.is_terminal() {
                    state.updated_at
                } else {
                    now
                };
                let elapsed = end.duration_since(state.started_at).unwrap_or_default();
                let elapsed_ms = elapsed.as_millis() as i64;
                let elapsed_str = human_duration_display(elapsed);
                let started_at_str = format_time(DateTime::<Local>::from(state.started_at));
                let updated_at_str = format_time(DateTime::<Local>::from(state.updated_at));
                let status_str = state.status.to_string();
                let inner_str = state.inner_status.clone().unwrap_or_default();
                let scope = if is_root { "root" } else { "subtask" };

                let mut row = DataRow::new();
                // Subtasks share their root's id; only the root row carries it.
                if is_root {
                    row.add(root_id);
                } else {
                    row.add(Data::null());
                }
                row.add(scope)
                    .add(state.name.as_str())
                    .add(status_str.as_str())
                    .add(inner_str.as_str())
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
