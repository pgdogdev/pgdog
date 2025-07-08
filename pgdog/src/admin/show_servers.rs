//! SHOW SERVERS command.

use std::{collections::HashSet, time::SystemTime};
use tokio::time::Instant;

use crate::{
    backend::stats::stats,
    net::messages::{Field, Protocol},
    util::format_time,
};

use super::prelude::*;

/// SHOW SERVERS command.
pub struct ShowServers {
    row: NamedRow,
}

#[async_trait]
impl Command for ShowServers {
    fn name(&self) -> String {
        "SHOW".into()
    }

    fn parse(sql: &str) -> Result<Self, Error> {
        let parts = sql
            .split(|c| [' ', ','].contains(&c))
            .into_iter()
            .collect::<Vec<&str>>();

        let mut mandatory = HashSet::from([
            "user".to_string(),
            "database".into(),
            "addr".into(),
            "port".into(),
        ]);

        let filters: HashSet<String> = parts.iter().skip(2).map(|f| f.trim().to_string()).collect();
        mandatory.extend(filters);

        if mandatory.len() == 4 {
            mandatory.clear();
        }

        Ok(Self {
            row: NamedRow::new(
                &[
                    Field::text("database"),
                    Field::text("user"),
                    Field::text("addr"),
                    Field::numeric("port"),
                    Field::text("state"),
                    Field::text("connect_time"),
                    Field::text("request_time"),
                    Field::numeric("remote_pid"),
                    Field::numeric("transactions"),
                    Field::numeric("queries"),
                    Field::numeric("rollbacks"),
                    Field::numeric("prepared_statements"),
                    Field::numeric("healthchecks"),
                    Field::numeric("errors"),
                    Field::numeric("bytes_received"),
                    Field::numeric("bytes_sent"),
                    Field::numeric("age"),
                    Field::text("application_name"),
                    Field::text("memory_used"),
                ],
                &mandatory,
            ),
        })
    }

    async fn execute(&self) -> Result<Vec<Message>, Error> {
        let mut messages = vec![self.row.row_description().message()?];

        let stats = stats();
        let now = Instant::now();
        let now_time = SystemTime::now();

        for (_, server) in stats {
            let age = now.duration_since(server.stats.created_at);
            let request_age = now.duration_since(server.stats.last_used);
            let request_time = now_time - request_age;

            let dr = self
                .row
                .clone()
                .add("database", server.addr.database_name)
                .add("user", server.addr.user)
                .add("addr", server.addr.host.as_str())
                .add("port", server.addr.port.to_string())
                .add("state", server.stats.state.to_string())
                .add(
                    "connect_time",
                    format_time(server.stats.created_at_time.into()),
                )
                .add("request_time", format_time(request_time.into()))
                .add("remote_pid", server.stats.id.pid as i64)
                .add("transactions", server.stats.total.transactions)
                .add("queries", server.stats.total.queries)
                .add("rollbacks", server.stats.total.rollbacks)
                .add(
                    "prepared_statements",
                    server.stats.total.prepared_statements,
                )
                .add("healthchecks", server.stats.total.healthchecks)
                .add("errors", server.stats.total.errors)
                .add("bytes_received", server.stats.total.bytes_received)
                .add("bytes_sent", server.stats.total.bytes_sent)
                .add("age", age.as_secs() as i64)
                .add("application_name", server.application_name.as_str())
                .add("memory_used", server.stats.total.memory_used)
                .data_row();
            messages.push(dr.message()?);
        }

        Ok(messages)
    }
}
