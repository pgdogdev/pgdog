//! `SHOW CLIENTS` command implementation.

use std::collections::HashSet;

use chrono::DateTime;

use super::prelude::*;
use crate::frontend::comms::comms;
use crate::net::messages::*;
use crate::util::format_time;

/// Show clients command.
pub(crate) struct ShowClients {
    filter: NamedRow,
}

#[async_trait]
impl Command for ShowClients {
    fn name(&self) -> String {
        "SHOW CLIENTS".into()
    }

    fn parse(sql: &str) -> Result<Self, Error> {
        let parts = sql
            .split(|c| [' ', ','].contains(&c))
            .collect::<Vec<&str>>();

        let fields = vec![
            Field::text("user"),
            Field::text("database"),
            Field::text("addr"),
            Field::numeric("port"),
            Field::text("state"),
            Field::text("replication"),
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
            Field::bool("locked"),
            Field::numeric("prepared_statements"),
        ];

        let mut mandatory = HashSet::from([
            "user".to_string(),
            "database".into(),
            "addr".into(),
            "port".into(),
        ]);
        let filters: HashSet<String> = parts.iter().skip(2).map(|f| f.trim().to_string()).collect();
        mandatory.extend(filters);

        // All fields.
        if mandatory.len() == 4 {
            mandatory.clear();
        }

        let filter = NamedRow::new(&fields, &mandatory);

        Ok(ShowClients { filter })
    }

    async fn execute(&self) -> Result<Vec<Message>, Error> {
        let mut rows = vec![];
        let clients = comms().clients();

        for client in clients.values() {
            let user = client.paramters.get_default("user", "postgres");
            let row = self
                .filter
                .clone()
                .add("user", user)
                .add("database", client.paramters.get_default("database", user))
                .add("addr", client.addr.ip().to_string())
                .add("port", client.addr.port().to_string())
                .add("state", client.stats.state.to_string())
                .add(
                    "replication",
                    if client.paramters.get("replication").is_some() {
                        "logical"
                    } else {
                        "none"
                    },
                )
                .add("connect_time", format_time(client.connected_at))
                .add(
                    "last_request",
                    format_time(DateTime::from(client.stats.last_request)),
                )
                .add("queries", client.stats.queries)
                .add("transactions", client.stats.transactions)
                .add("wait_time", client.stats.wait_time().as_secs_f64() * 1000.0)
                .add(
                    "query_time",
                    format!("{:.3}", client.stats.query_time.as_secs_f64() * 1000.0),
                )
                .add(
                    "transaction_time",
                    format!(
                        "{:.3}",
                        client.stats.transaction_time.as_secs_f64() * 1000.0
                    ),
                )
                .add("bytes_received", client.stats.bytes_received)
                .add("bytes_sent", client.stats.bytes_sent)
                .add("errors", client.stats.errors)
                .add(
                    "application_name",
                    client.paramters.get_default("application_name", ""),
                )
                .add("memory_used", client.stats.memory_used)
                .add("locked", client.stats.locked)
                .add("prepared_statements", client.stats.prepared_statements)
                .data_row();
            rows.push(row.message()?);
        }

        let mut messages = vec![self.filter.row_description().message()?];
        messages.extend(rows);

        Ok(messages)
    }
}
