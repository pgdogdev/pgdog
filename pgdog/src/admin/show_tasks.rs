use std::time::SystemTime;

use chrono::{DateTime, Local};

use crate::backend::replication::logical::admin::AsyncTasks;
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
            Field::text("type"),
            Field::text("started_at"),
            Field::text("elapsed"),
            Field::bigint("elapsed_ms"),
        ]);
        let mut messages = vec![rd.message()?];
        let now = SystemTime::now();

        for (id, task_kind, started_at) in AsyncTasks::get().iter() {
            let elapsed = now.duration_since(started_at).unwrap_or_default();
            let elapsed_ms = elapsed.as_millis() as i64;
            let elapsed_str = human_duration_display(elapsed);

            let started_at_str = format_time(DateTime::<Local>::from(started_at));

            let mut row = DataRow::new();
            row.add(id as i64)
                .add(task_kind.to_string().as_str())
                .add(started_at_str.as_str())
                .add(elapsed_str.as_str())
                .add(elapsed_ms);
            messages.push(row.message()?);
        }

        Ok(messages)
    }
}
