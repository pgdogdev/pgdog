//! `SHOW CLIENTS` command implementation.

use super::prelude::*;
use crate::frontend::comms::comms;
use crate::net::messages::*;

/// Show clients command.
pub struct ShowClients;

#[async_trait]
impl Command for ShowClients {
    fn name(&self) -> String {
        "SHOW CLIENTS".into()
    }

    fn parse(_sql: &str) -> Result<Self, Error> {
        Ok(ShowClients)
    }

    async fn execute(&self) -> Result<Vec<Message>, Error> {
        let rd = RowDescription::new(&[
            Field::text("host"),
            Field::numeric("port"),
            Field::text("state"),
            Field::numeric("queries"),
            Field::numeric("transactions"),
            Field::numeric("wait_time"),
            Field::numeric("query_time"),
            Field::numeric("transaction_time"),
            Field::numeric("bytes_received"),
            Field::numeric("bytes_sent"),
            Field::numeric("errors"),
        ]);

        let mut rows = vec![];
        let clients = comms().clients();

        for client in clients.values() {
            let mut row = DataRow::new();
            row.add(client.addr.ip().to_string())
                .add(client.addr.port().to_string())
                .add(client.stats.state.to_string())
                .add(client.stats.queries)
                .add(client.stats.transactions)
                .add(client.stats.wait_time().as_secs_f64() * 1000.0)
                .add(client.stats.query_time.as_secs_f64() * 1000.0)
                .add(client.stats.transaction_time.as_secs_f64() * 1000.0)
                .add(client.stats.bytes_received)
                .add(client.stats.bytes_sent)
                .add(client.stats.errors);
            rows.push(row.message()?);
        }

        let mut messages = vec![rd.message()?];
        messages.extend(rows);

        Ok(messages)
    }
}
