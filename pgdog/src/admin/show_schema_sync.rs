use std::time::SystemTime;

use chrono::{DateTime, Local};

use crate::{
    backend::replication::logical::status::SchemaStatements,
    util::{format_time, human_duration_display},
};

use super::prelude::*;

pub struct ShowSchemaSync;

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
            Field::text("database"),
            Field::text("user"),
            Field::bigint("shard"),
            Field::text("kind"),
            Field::text("sync_state"),
            Field::text("started_at"),
            Field::text("elapsed"),
            Field::bigint("elapsed_ms"),
            Field::text("table_schema"),
            Field::text("table_name"),
            Field::text("sql"),
        ]);
        let mut messages = vec![rd.message()?];
        let now = SystemTime::now();

        let mut entries: Vec<_> = SchemaStatements::get()
            .iter()
            .map(|task| task.clone())
            .collect();
        entries.sort_by_key(|k| if k.running { 0 } else { 1 });

        for entry in entries {
            let stmt = entry.statement;

            let elapsed = stmt
                .started_at
                .map(|t| now.duration_since(t).unwrap_or_default());
            let elapsed_ms: Option<i64> = elapsed.map(|e| e.as_millis() as i64);
            let elapsed_human: Option<String> = elapsed.map(human_duration_display);

            let kind = stmt.kind.to_string();
            let sync_state = stmt.sync_state.to_string();
            let started_at: Option<String> = stmt
                .started_at
                .map(|started_at| format_time(DateTime::<Local>::from(started_at)));

            let mut row = DataRow::new();
            row.add(stmt.user.database.as_str())
                .add(stmt.user.user.as_str())
                .add(stmt.shard as i64)
                .add(kind.as_str())
                .add(sync_state.as_str())
                .add(started_at)
                .add(elapsed_human)
                .add(elapsed_ms)
                .add(stmt.table_schema.as_deref().unwrap_or(""))
                .add(stmt.table_name.as_deref().unwrap_or(""))
                .add(stmt.sql.as_str());

            messages.push(row.message()?);
        }

        Ok(messages)
    }
}
