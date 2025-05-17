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
            Field::text("database"),
            Field::text("user"),
            Field::text("addr"),
            Field::numeric("port"),
            Field::numeric("shard"),
            Field::text("role"),
            Field::numeric("cl_waiting"),
            Field::numeric("sv_idle"),
            Field::numeric("sv_active"),
            Field::numeric("sv_total"),
            Field::numeric("maxwait"),
            Field::numeric("maxwait_us"),
            Field::text("pool_mode"),
            Field::bool("paused"),
            Field::bool("banned"),
            Field::numeric("errors"),
            Field::numeric("re_synced"),
            Field::numeric("out_of_sync"),
            Field::bool("online"),
        ]);
        let mut messages = vec![rd.message()?];
        for (user, cluster) in databases().all() {
            for (shard_num, shard) in cluster.shards().iter().enumerate() {
                for (role, pool) in shard.pools_with_roles() {
                    let mut row = DataRow::new();
                    let state = pool.state();
                    let maxwait = state.maxwait.as_secs() as i64;
                    let maxwait_us = state.maxwait.subsec_micros() as i64;
                    row.add(user.database.as_str())
                        .add(user.user.as_str())
                        .add(pool.addr().host.as_str())
                        .add(pool.addr().port as i64)
                        .add(shard_num as i64)
                        .add(role.to_string())
                        .add(state.waiting)
                        .add(state.idle)
                        .add(state.checked_out)
                        .add(state.total)
                        .add(maxwait)
                        .add(maxwait_us)
                        .add(state.pooler_mode.to_string())
                        .add(state.paused)
                        .add(state.banned)
                        .add(state.errors)
                        .add(state.re_synced)
                        .add(state.out_of_sync)
                        .add(state.online);
                    messages.push(row.message()?);
                }
            }
        }
        Ok(messages)
    }
}
