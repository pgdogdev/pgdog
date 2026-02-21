use std::time::SystemTime;

use chrono::{DateTime, Local};

use crate::{backend::replication::logical::status::SchemaStatements, util::format_time};

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
            Field::bigint("elapsed_ms"),
            Field::text("table_schema"),
            Field::text("table_name"),
            Field::text("sql"),
        ]);
        let mut messages = vec![rd.message()?];
        let now = SystemTime::now();

        for entry in SchemaStatements::get().iter() {
            let stmt = entry.key();

            let elapsed_ms = now
                .duration_since(stmt.started_at)
                .unwrap_or_default()
                .as_millis() as i64;

            let kind = stmt.kind.to_string();
            let sync_state = stmt.sync_state.to_string();
            let started_at: DateTime<Local> = stmt.started_at.into();
            let started_at = format_time(started_at);

            let mut row = DataRow::new();
            row.add(stmt.user.database.as_str())
                .add(stmt.user.user.as_str())
                .add(stmt.shard as i64)
                .add(kind.as_str())
                .add(sync_state.as_str())
                .add(started_at.as_str())
                .add(elapsed_ms)
                .add(stmt.table_schema.as_deref().unwrap_or(""))
                .add(stmt.table_name.as_deref().unwrap_or(""))
                .add(stmt.sql.as_str());

            messages.push(row.message()?);
        }

        Ok(messages)
    }
}
