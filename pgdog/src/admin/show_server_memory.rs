use crate::{
    backend::stats::stats,
    net::messages::{DataRow, Field, Protocol, RowDescription},
};

use super::prelude::*;

pub struct ShowServerMemory;

#[async_trait]
impl Command for ShowServerMemory {
    fn name(&self) -> String {
        "SHOW SERVER MEMORY".into()
    }

    fn parse(_sql: &str) -> Result<Self, Error> {
        Ok(ShowServerMemory {})
    }

    async fn execute(&self) -> Result<Vec<Message>, Error> {
        let rd = RowDescription::new(&[
            Field::bigint("pool_id"),
            Field::text("database"),
            Field::text("user"),
            Field::text("addr"),
            Field::numeric("port"),
            Field::numeric("remote_pid"),
            Field::numeric("buffer_reallocs"),
            Field::numeric("buffer_frees"),
            Field::numeric("buffer_bytes_used"),
            Field::numeric("buffer_bytes_alloc"),
            Field::numeric("prepared_statements_bytes"),
            Field::numeric("net_buffer_bytes"),
            Field::numeric("total_bytes"),
        ]);
        let mut messages = vec![rd.message()?];

        let stats = stats();
        for (_, server) in stats {
            let mut row = DataRow::new();
            let stats = server.stats;
            let memory = &stats.memory;

            row.add(stats.pool_id as i64)
                .add(server.addr.database_name.as_str())
                .add(server.addr.user.as_str())
                .add(server.addr.host.as_str())
                .add(server.addr.port as i64)
                .add(stats.id.pid as i64)
                .add(memory.buffer.reallocs as i64)
                .add(memory.buffer.frees as i64)
                .add(memory.buffer.bytes_used as i64)
                .add(memory.buffer.bytes_alloc as i64)
                .add(memory.prepared_statements as i64)
                .add(memory.stream as i64)
                .add((memory.total() + memory.stream) as i64);

            messages.push(row.message()?);
        }

        Ok(messages)
    }
}
