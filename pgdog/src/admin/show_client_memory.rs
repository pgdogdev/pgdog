use crate::{
    frontend::comms::comms,
    net::messages::{DataRow, Field, Protocol, RowDescription},
};

use super::prelude::*;

pub struct ShowClientMemory;

#[async_trait]
impl Command for ShowClientMemory {
    fn name(&self) -> String {
        "SHOW CLIENT MEMORY".into()
    }

    fn parse(_sql: &str) -> Result<Self, Error> {
        Ok(ShowClientMemory {})
    }

    async fn execute(&self) -> Result<Vec<Message>, Error> {
        let rd = RowDescription::new(&[
            Field::bigint("client_id"),
            Field::text("database"),
            Field::text("user"),
            Field::text("addr"),
            Field::numeric("port"),
            Field::numeric("buffer_reallocs"),
            Field::numeric("buffer_reclaims"),
            Field::numeric("buffer_bytes_used"),
            Field::numeric("buffer_bytes_alloc"),
            Field::numeric("prepared_statements_bytes"),
            Field::numeric("net_buffer_bytes"),
            Field::numeric("total_bytes"),
        ]);
        let mut messages = vec![rd.message()?];

        let clients = comms().clients();
        for (_, client) in clients {
            let mut row = DataRow::new();
            let memory = &client.stats.memory_stats;

            let user = client.paramters.get_default("user", "postgres");
            let database = client.paramters.get_default("database", user);

            row.add(client.id.pid as i64)
                .add(database)
                .add(user)
                .add(client.addr.ip().to_string().as_str())
                .add(client.addr.port() as i64)
                .add(memory.buffer.reallocs as i64)
                .add(memory.buffer.reclaims as i64)
                .add(memory.buffer.bytes_used as i64)
                .add(memory.buffer.bytes_alloc as i64)
                .add(memory.prepared_statements as i64)
                .add(memory.stream as i64)
                .add((memory.total()) as i64);

            messages.push(row.message()?);
        }

        Ok(messages)
    }
}
