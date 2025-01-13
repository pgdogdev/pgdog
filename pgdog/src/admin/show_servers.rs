//! SHOW SERVERS command.

use std::time::Instant;

use crate::{
    backend::stats::stats,
    net::messages::{DataRow, Field, Protocol, RowDescription},
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
            Field::text("host"),
            Field::numeric("port"),
            Field::text("state"),
            Field::numeric("transactions"),
            Field::numeric("queries"),
            Field::numeric("rollbacks"),
            Field::numeric("prepared_statements"),
            Field::numeric("healthchecks"),
            Field::numeric("errors"),
            Field::numeric("bytes_received"),
            Field::numeric("bytes_sent"),
            Field::numeric("age"),
        ])
        .message()?];

        let stats = stats();
        let now = Instant::now();

        for (_, server) in stats {
            let mut dr = DataRow::new();
            dr.add(server.addr.host.as_str())
                .add(server.addr.port.to_string())
                .add(server.stats.state.to_string())
                .add(server.stats.transactions)
                .add(server.stats.queries)
                .add(server.stats.rollbacks)
                .add(server.stats.prepared_statements)
                .add(server.stats.healthchecks)
                .add(server.stats.errors)
                .add(server.stats.bytes_received)
                .add(server.stats.bytes_sent)
                .add(now.duration_since(server.stats.created_at).as_millis() as i64);
            messages.push(dr.message()?);
        }

        Ok(messages)
    }
}
