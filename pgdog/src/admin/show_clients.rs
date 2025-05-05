//! `SHOW CLIENTS` command implementation.

use chrono::DateTime;

use super::prelude::*;
use crate::frontend::comms::comms;
use crate::net::messages::*;
use crate::util::format_time;

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
            Field::text("user"),
            Field::text("database"),
            Field::text("replication"),
            Field::text("state"),
            Field::text("addr"),
            Field::numeric("port"),
            Field::text("connect_time"),
            Field::text("last_request"),
            Field::numeric("queries"),
            Field::numeric("transactions"),
            Field::numeric("wait_time"),
            Field::numeric("query_time"),
            Field::numeric("transaction_time"),
            Field::numeric("bytes_received"),
            Field::numeric("bytes_sent"),
            Field::numeric("errors"),
            Field::text("application_name"),
            Field::numeric("memory_used"),
        ]);

        let mut rows = vec![];
        let clients = comms().clients();

        for client in clients.values() {
            let user = client.paramters.get_default("user", "postgres");
            let mut row = DataRow::new();
            row.add(user)
                .add(client.paramters.get_default("database", user))
                .add(if client.paramters.get("replication").is_some() {
                    "logical"
                } else {
                    "none"
                })
                .add(client.stats.state.to_string())
                .add(client.addr.ip().to_string())
                .add(client.addr.port().to_string())
                .add(format_time(client.connected_at))
                .add(format_time(DateTime::from(client.stats.last_request)))
                .add(client.stats.queries)
                .add(client.stats.transactions)
                .add(client.stats.wait_time().as_secs_f64() * 1000.0)
                .add(format!(
                    "{:.3}",
                    client.stats.query_time.as_secs_f64() * 1000.0
                ))
                .add(format!(
                    "{:.3}",
                    client.stats.transaction_time.as_secs_f64() * 1000.0
                ))
                .add(client.stats.bytes_received)
                .add(client.stats.bytes_sent)
                .add(client.stats.errors)
                .add(client.paramters.get_default("application_name", ""))
                .add(client.stats.memory_used);
            rows.push(row.message()?);
        }

        let mut messages = vec![rd.message()?];
        messages.extend(rows);

        Ok(messages)
    }
}
