use std::time::SystemTime;

use crate::backend::replication::logical::status::TableCopies;
use crate::util::{format_bytes, human_duration_display, number_human};

use super::prelude::*;

pub struct ShowTableCopies;

#[async_trait]
impl Command for ShowTableCopies {
    fn name(&self) -> String {
        "SHOW TABLE_COPIES".into()
    }

    fn parse(_sql: &str) -> Result<Self, Error> {
        Ok(ShowTableCopies {})
    }

    async fn execute(&self) -> Result<Vec<Message>, Error> {
        let rd = RowDescription::new(&[
            Field::text("schema"),
            Field::text("table"),
            Field::text("status"),
            Field::bigint("rows"),
            Field::text("rows_human"),
            Field::bigint("bytes"),
            Field::text("bytes_human"),
            Field::bigint("bytes_per_sec"),
            Field::text("bytes_per_sec_human"),
            Field::text("elapsed"),
            Field::bigint("elapsed_ms"),
            Field::text("sql"),
        ]);
        let mut messages = vec![rd.message()?];
        let now = SystemTime::now();

        let table_copies = TableCopies::get();
        let mut entries: Vec<_> = table_copies.iter().collect();
        entries.sort_by_key(|e| if e.value().bytes == 0 { 1 } else { 0 });

        for entry in entries {
            let key = entry.key();
            let state = entry.value();

            let elapsed = now.duration_since(state.last_update).unwrap_or_default();
            let elapsed_ms = elapsed.as_millis() as i64;
            let elapsed_human = human_duration_display(elapsed);

            let status = if state.bytes == 0 {
                "waiting"
            } else {
                "running"
            };

            let rows_human = number_human(state.rows as u64);
            let bytes_human = format_bytes(state.bytes as u64);
            let bytes_per_sec_human = format_bytes(state.bytes_per_sec as u64);

            let mut row = DataRow::new();
            row.add(key.schema.as_str())
                .add(key.table.as_str())
                .add(status)
                .add(state.rows as i64)
                .add(rows_human.as_str())
                .add(state.bytes as i64)
                .add(bytes_human.as_str())
                .add(state.bytes_per_sec as i64)
                .add(bytes_per_sec_human.as_str())
                .add(elapsed_human.as_str())
                .add(elapsed_ms)
                .add(state.sql.as_str());

            messages.push(row.message()?);
        }

        Ok(messages)
    }
}
