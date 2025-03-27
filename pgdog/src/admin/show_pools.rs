use crate::{
    backend::databases::databases,
    net::messages::{DataRow, Field, Protocol, RowDescription},
};

// SHOW POOLS command.
use super::prelude::*;

pub struct ShowPools;

#[async_trait]
impl Command for ShowPools {
    fn name(&self) -> String {
        "SHOW POOLS".into()
    }

    fn parse(_sql: &str) -> Result<Self, Error> {
        Ok(ShowPools {})
    }

    async fn execute(&self) -> Result<Vec<Message>, Error> {
        let rd = RowDescription::new(&[
            Field::text("host"),
            Field::numeric("port"),
            Field::text("database"),
            Field::text("user"),
            Field::numeric("idle"),
            Field::numeric("active"),
            Field::numeric("total"),
            Field::numeric("clients_waiting"),
            Field::bool("paused"),
            Field::bool("banned"),
            Field::numeric("errors"),
            Field::numeric("out_of_sync"),
        ]);
        let mut messages = vec![rd.message()?];
        for (user, cluster) in databases().all() {
            for shard in cluster.shards() {
                for pool in shard.pools() {
                    let mut row = DataRow::new();
                    let addr = pool.addr();
                    let state = pool.state();
                    row.add(addr.host.as_str())
                        .add(addr.port.to_string().as_str())
                        .add(user.database.as_str())
                        .add(user.user.as_str())
                        .add(state.idle)
                        .add(state.checked_out)
                        .add(state.total)
                        .add(state.waiting)
                        .add(state.paused)
                        .add(state.banned)
                        .add(state.errors)
                        .add(state.out_of_sync);
                    messages.push(row.message()?);
                }
            }
        }
        Ok(messages)
    }
}
