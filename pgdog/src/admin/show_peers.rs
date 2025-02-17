//! SHOW PEERS command.
//!
//! If there are other instances of PgDog running
//! on the same network, they will be shown here.
//!
//! See [`crate::net::discovery`] for how this works.
//!

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
            Field::text("last_seen"),
            Field::numeric("clients"),
        ])
        .message()?];

        let now = SystemTime::now();

        for (adder, state) in peers {
            let mut row = DataRow::new();
            row.add(adder.to_string())
                .add(format!(
                    "{:?}",
                    now.duration_since(state.last_message)
                        .unwrap_or(Duration::from_secs(0))
                ))
                .add(state.clients);
            rows.push(row.message()?);
        }

        Ok(rows)
    }
}
