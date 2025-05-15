//! SHOW SERVERS command.

use std::time::SystemTime;
use tokio::time::Instant;

use crate::{
    backend::stats::stats,
    net::messages::{DataRow, Field, Protocol, RowDescription},
    util::format_time,
};

use super::prelude::*;

/// SHOW SERVERS command.
pub struct ShowServers;

#[async_trait]
impl Command for ShowServers {
    fn name(&self) -> String {
        "SHOW".into()
    }

    fn parse(_sql: &str) -> Result<Self, Error> {
        Ok(Self)
    }

    async fn execute(&self) -> Result<Vec<Message>, Error> {
        let mut messages = vec![RowDescription::new(&[
            Field::text("database"),
            Field::text("user"),
            Field::text("state"),
            Field::text("addr"),
            Field::numeric("port"),
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
        ])
        .message()?];

        let stats = stats();
        let now = Instant::now();
        let now_time = SystemTime::now();

        for (_, server) in stats {
            let age = now.duration_since(server.stats.created_at);
            let request_age = now.duration_since(server.stats.last_used);
            let request_time = now_time - request_age;
            let mut dr = DataRow::new();
            dr.add(server.addr.database_name)
                .add(server.addr.user)
                .add(server.stats.state.to_string())
                .add(server.addr.host.as_str())
                .add(server.addr.port.to_string())
                .add(format_time(server.stats.created_at_time.into()))
                .add(format_time(request_time.into()))
                .add(server.stats.id.pid as i64)
                .add(server.stats.total.transactions)
                .add(server.stats.total.queries)
                .add(server.stats.total.rollbacks)
                .add(server.stats.total.prepared_statements)
                .add(server.stats.healthchecks)
                .add(server.stats.total.errors)
                .add(server.stats.total.bytes_received)
                .add(server.stats.total.bytes_sent)
                .add(age.as_secs() as i64)
                .add(server.application_name.as_str());
            messages.push(dr.message()?);
        }

        Ok(messages)
    }
}
