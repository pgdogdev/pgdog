use crate::{
    backend::{databases::databases, stats::stats},
    config::config,
    frontend::comms::comms,
};

use super::prelude::*;

pub struct ShowLists;

#[async_trait]
impl Command for ShowLists {
    fn name(&self) -> String {
        "SHOW LISTS".into()
    }

    fn parse(_: &str) -> Result<Self, Error> {
        Ok(ShowLists)
    }

    async fn execute(&self) -> Result<Vec<Message>, Error> {
        let clients = comms().clients();
        let servers = stats();
        let config = config();
        let mut pools = 0;
        let users = config.users.users.len();
        let dbs = config.config.databases.len();

        for cluster in databases().all().values() {
            for shard in cluster.shards() {
                pools += shard.pools().len();
            }
        }

        let rd = RowDescription::new(&[
            Field::numeric("databases"),
            Field::numeric("users"),
            Field::numeric("pools"),
            Field::numeric("used_clients"),
            Field::numeric("used_clients"),
            Field::numeric("free_servers"),
            Field::numeric("used_servers"),
        ]);

        let mut dr = DataRow::new();
        dr.add(dbs as i64)
            .add(users as i64)
            .add(pools as i64)
            .add(0_i64)
            .add(clients.len() as i64)
            .add(0_i64)
            .add(servers.len() as i64);

        Ok(vec![rd.message()?, dr.message()?])
    }
}
