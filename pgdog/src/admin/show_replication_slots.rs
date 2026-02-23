use std::time::SystemTime;

use chrono::{DateTime, Local};

use crate::{
    backend::replication::logical::status::ReplicationSlots,
    net::{data_row::Data, ToDataRowColumn},
    util::{format_bytes, format_time},
};

use super::prelude::*;

pub struct ShowReplicationSlots;

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
            Field::text("host"),
            Field::bigint("port"),
            Field::text("database_name"),
            Field::text("name"),
            Field::text("lsn"),
            Field::text("lag"),
            Field::bigint("lag_bytes"),
            Field::bool("copy_data"),
            Field::text("last_transaction"),
            Field::bigint("last_transaction_ms"),
        ]);
        let mut messages = vec![rd.message()?];
        let now = SystemTime::now();

        for entry in ReplicationSlots::get().iter() {
            let slot = entry.value();

            let last_transaction_ms = slot
                .last_transaction
                .and_then(|t| now.duration_since(t).ok())
                .map(|d| d.as_millis() as i64);

            let last_transaction_str = slot
                .last_transaction
                .map(|t| format_time(DateTime::<Local>::from(t)));

            let mut row = DataRow::new();
            row.add(&slot.address.host)
                .add(slot.address.port as i64)
                .add(&slot.address.database_name)
                .add(slot.name.as_str())
                .add(slot.lsn.to_string().as_str())
                .add(format_bytes(slot.lag as u64).as_str())
                .add(slot.lag)
                .add(slot.copy_data)
                .add(if let Some(ref s) = last_transaction_str {
                    s.as_str().to_data_row_column()
                } else {
                    Data::null()
                })
                .add(if let Some(ms) = last_transaction_ms {
                    ms.to_data_row_column()
                } else {
                    Data::null()
                });

            messages.push(row.message()?);
        }

        Ok(messages)
    }
}
