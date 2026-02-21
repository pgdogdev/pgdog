use crate::{backend::replication::logical::status::ReplicationSlots, util::format_bytes};

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
            Field::text("name"),
            Field::text("lsn"),
            Field::text("lag"),
            Field::bigint("lag_bytes"),
            Field::bool("copy_data"),
        ]);
        let mut messages = vec![rd.message()?];

        for entry in ReplicationSlots::get().iter() {
            let slot = entry.value();

            let mut row = DataRow::new();
            row.add(slot.name.as_str())
                .add(slot.lsn.to_string().as_str())
                .add(format_bytes(slot.lag as u64).as_str())
                .add(slot.lag)
                .add(slot.copy_data);

            messages.push(row.message()?);
        }

        Ok(messages)
    }
}
