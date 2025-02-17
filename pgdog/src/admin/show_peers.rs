//! SHOW PEERS command.

use std::time::{Duration, SystemTime};

use crate::net::{
    discovery::Listener,
    messages::{DataRow, Field, Protocol, RowDescription},
};

use super::prelude::*;

use super::Command;

pub struct ShowPeers;

#[async_trait]
impl Command for ShowPeers {
    fn name(&self) -> String {
        "SHOW PEERS".into()
    }

    fn parse(_: &str) -> Result<Self, super::Error> {
        Ok(ShowPeers {})
    }

    async fn execute(&self) -> Result<Vec<Message>, Error> {
        let listener = Listener::get();
        let peers = listener.peers();

        let mut rows = vec![RowDescription::new(&[
            Field::text("addr"),
            Field::text("last_message"),
        ])
        .message()?];

        let now = SystemTime::now();

        for (adder, last_message) in peers {
            let mut row = DataRow::new();
            row.add(adder.to_string()).add(format!(
                "{:?}",
                now.duration_since(last_message)
                    .unwrap_or(Duration::from_secs(0))
            ));
            rows.push(row.message()?);
        }

        Ok(rows)
    }
}
