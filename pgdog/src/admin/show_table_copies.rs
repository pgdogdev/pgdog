use std::ops::ControlFlow;
use std::time::SystemTime;

use pgdog_stats::{TableCopyStage, TaskDefinitionKind, TaskId, TaskStatus};

use crate::api::tasks_storage;
use crate::util::{format_bytes, human_duration_display, number_human};

use super::prelude::*;

pub(crate) struct ShowTableCopies;

fn latest_copy_data() -> Option<TaskId> {
    #[derive(PartialEq, PartialOrd)]
    struct TaskOrder {
        live: bool,
        started_at: SystemTime,
        id: TaskId,
    }

    let mut best: Option<TaskOrder> = None;

    tasks_storage().try_for_each(|task| {
        let state = task.state();

        // Copy data is the child of reshard, go deeper
        match state.definition.kind {
            TaskDefinitionKind::Reshard(_) => return ControlFlow::Continue(()),
            TaskDefinitionKind::CopyData(_) => {}
            _ => return ControlFlow::Break(()),
        }

        let candidate = TaskOrder {
            live: !state.is_terminal(),
            started_at: state.started_at,
            id: task.id,
        };

        if best.as_ref().is_none_or(|best| candidate > *best) {
            best = Some(candidate);
        }

        ControlFlow::Break(())
    });

    best.map(|best| best.id)
}

#[async_trait]
impl Command for ShowTableCopies {
    fn name(&self) -> String {
        "SHOW TABLE_COPIES".into()
    }

    fn parse(_sql: &str) -> Result<Self, Error> {
        Ok(ShowTableCopies)
    }

    async fn execute(&self) -> Result<Vec<Message>, Error> {
        let rd = RowDescription::new(&[
            Field::bigint("task_id"),
            Field::text("schema"),
            Field::text("table"),
            Field::bigint("source_shard"),
            Field::text("progress"),
            Field::text("status"),
            Field::bigint("attempt"),
            Field::bigint("rows"),
            Field::text("rows_human"),
            Field::bigint("estimated_rows"),
            Field::text("estimated_rows_human"),
            Field::bigint("rows_per_sec"),
            Field::text("rows_per_sec_human"),
            Field::bigint("bytes"),
            Field::text("bytes_human"),
            Field::bigint("estimated_bytes"),
            Field::text("estimated_bytes_human"),
            Field::bigint("bytes_per_sec"),
            Field::text("bytes_per_sec_human"),
            Field::text("elapsed"),
            Field::bigint("elapsed_ms"),
            Field::text("last_error"),
        ]);
        let mut messages = vec![rd.message()];
        let now = SystemTime::now();

        let Some(id) = latest_copy_data() else {
            return Ok(messages);
        };

        tasks_storage().try_for_each(|task| {
            let state = task.state();

            let definition = match &state.definition.kind {
                TaskDefinitionKind::Reshard(_) => return ControlFlow::Continue(()),
                TaskDefinitionKind::CopyData(_) if task.id == id => {
                    return ControlFlow::Continue(());
                }
                TaskDefinitionKind::TableCopy(definition) => definition,
                _ => return ControlFlow::Break(()),
            };

            let TaskStatus::TableCopy(status) = state.status else {
                return ControlFlow::Break(());
            };

            let (rows, bytes) = match status.stage {
                TableCopyStage::InProgress { rows, bytes } => (rows, bytes),
                _ => (0, 0),
            };
            let bytes_per_sec = status.bytes_per_sec.unwrap_or(0);
            let rows_per_sec = status.rows_per_sec.unwrap_or(0);
            let end = if state.progress.is_terminal() {
                state.updated_at
            } else {
                now
            };
            let elapsed = end.duration_since(state.started_at).unwrap_or_default();
            let progress_str = state.progress.to_string();
            let status_str = status.to_string();

            let mut row = DataRow::new();

            row.add(task.parent_id)
                .add(definition.schema.as_str())
                .add(definition.table.as_str())
                .add(definition.source_shard as i64)
                .add(progress_str.as_str())
                .add(status_str.as_str())
                .add(status.attempt as i64)
                .add(rows as i64)
                .add(number_human(rows).as_str())
                .add(status.estimated_rows.map(|rows| rows as i64))
                .add(status.estimated_rows.map(number_human))
                .add(rows_per_sec as i64)
                .add(number_human(rows_per_sec).as_str())
                .add(bytes as i64)
                .add(format_bytes(bytes).as_str())
                .add(status.estimated_bytes.map(|bytes| bytes as i64))
                .add(status.estimated_bytes.map(format_bytes))
                .add(bytes_per_sec as i64)
                .add(format_bytes(bytes_per_sec).as_str())
                .add(human_duration_display(elapsed).as_str())
                .add(elapsed.as_millis() as i64)
                .add(status.last_error);
            messages.push(row.message());

            ControlFlow::Break(())
        });

        Ok(messages)
    }
}
