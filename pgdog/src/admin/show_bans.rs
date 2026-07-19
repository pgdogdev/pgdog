use std::time::Instant;

use crate::{
    backend::databases::databases,
    net::messages::{DataRow, Field, Protocol, RowDescription},
};

// SHOW BANS command.
use super::prelude::*;

/// Show all connection pools that are currently banned, with the ban reason
/// and how much time is left before the ban expires.
pub struct ShowBans;

#[async_trait]
impl Command for ShowBans {
    fn name(&self) -> String {
        "SHOW BANS".into()
    }

    fn parse(_sql: &str) -> Result<Self, Error> {
        Ok(ShowBans {})
    }

    async fn execute(&self) -> Result<Vec<Message>, Error> {
        let rd = RowDescription::new(&[
            Field::bigint("id"),
            Field::text("database"),
            Field::text("user"),
            Field::text("addr"),
            Field::numeric("port"),
            Field::numeric("shard"),
            Field::text("role"),
            Field::text("ban_reason"),
            Field::numeric("ban_time_left"),
        ]);

        let mut messages = vec![rd.message()?];
        let now = Instant::now();

        for (user, cluster) in databases().all() {
            for (shard_num, shard) in cluster.shards().iter().enumerate() {
                for (role, ban, pool) in shard.pools_with_roles_and_bans() {
                    if !ban.banned() {
                        continue;
                    }

                    // Time left on the ban, in milliseconds. NULL for manual
                    // bans, which never expire on their own.
                    let time_left = ban
                        .time_remaining(now)
                        .map(|remaining| remaining.as_millis() as i64);

                    let mut row = DataRow::new();
                    row.add(pool.id() as i64)
                        .add(user.database.as_str())
                        .add(user.user.as_str())
                        .add(pool.addr().host.as_str())
                        .add(pool.addr().port as i64)
                        .add(shard_num as i64)
                        .add(role.to_string())
                        .add(ban.error().map(|err| err.to_string()))
                        .add(time_left);

                    messages.push(row.message()?);
                }
            }
        }

        Ok(messages)
    }
}
