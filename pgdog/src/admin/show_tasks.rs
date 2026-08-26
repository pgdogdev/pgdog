use std::ops::ControlFlow;
use std::time::SystemTime;

use chrono::{DateTime, Local};

use crate::api::tasks_storage;
use crate::util::{format_time, human_duration_display};

use super::prelude::*;

/// Show only two levels of nested tasks in order
/// to not overwhelm the cli output
const MAX_LEVEL: usize = 1;

pub(crate) struct ShowTasks;

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
            Field::bigint("parent_id"),
            Field::bigint("id"),
            Field::text("type"),
            Field::text("status"),
            Field::text("inner_status"),
            Field::text("started_at"),
            Field::text("updated_at"),
            Field::text("elapsed"),
            Field::bigint("elapsed_ms"),
        ]);
        let mut messages = vec![rd.message()];
        let now = SystemTime::now();

        tasks_storage().try_for_each(|task| {
            let state = task.state();
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
            let status_str = state.progress.to_string();
            let inner_str = state.status.to_string();
            let definition_str = state.definition.to_string();

            let mut row = DataRow::new();

            let stoppable_id = task.parent_id.is_none().then_some(task.id);

            row.add(task.parent_id)
                .add(stoppable_id)
                .add(definition_str.as_str())
                .add(status_str.as_str())
                .add(inner_str.as_str())
                .add(started_at_str.as_str())
                .add(updated_at_str.as_str())
                .add(elapsed_str.as_str())
                .add(elapsed_ms);
            messages.push(row.message());

            if task.level < MAX_LEVEL {
                ControlFlow::Continue(())
            } else {
                ControlFlow::Break(())
            }
        });

        Ok(messages)
    }
}
