use tokio::time::Instant;

use crate::{
    backend::databases::databases,
    net::{
        data_row::Data,
        messages::{DataRow, Field, Protocol, RowDescription},
        ToDataRowColumn,
    },
};

use super::prelude::*;

pub struct ShowReplication;

#[async_trait]
impl Command for ShowReplication {
    fn name(&self) -> String {
        "SHOW REPLICATION".into()
    }

    fn parse(_sql: &str) -> Result<Self, Error> {
        Ok(ShowReplication {})
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
            Field::text("pg_replica_lag"),
            Field::text("pg_lsn"),
            Field::text("lsn_age"),
            Field::text("pg_is_in_recovery"),
        ]);
        let mut messages = vec![rd.message()?];
        let now = Instant::now();
        for (user, cluster) in databases().all() {
            for (shard_num, shard) in cluster.shards().iter().enumerate() {
                for (role, _ban, pool) in shard.pools_with_roles_and_bans() {
                    let mut row = DataRow::new();
                    let state = pool.state();

                    let lsn_read = state.lsn_stats.lsn.lsn > 0;
                    let lsn_age = now.duration_since(state.lsn_stats.fetched);

                    row.add(pool.id() as i64)
                        .add(user.database.as_str())
                        .add(user.user.as_str())
                        .add(pool.addr().host.as_str())
                        .add(pool.addr().port as i64)
                        .add(shard_num as i64)
                        .add(role.to_string())
                        .add(if lsn_read {
                            state
                                .replica_lag
                                .as_millis()
                                .to_string()
                                .to_data_row_column()
                        } else {
                            Data::null()
                        })
                        .add(if lsn_read {
                            state.lsn_stats.lsn.to_string().to_data_row_column()
                        } else {
                            Data::null()
                        })
                        .add(if lsn_read {
                            lsn_age.as_millis().to_string().to_data_row_column()
                        } else {
                            Data::null()
                        })
                        .add(if lsn_read {
                            state.lsn_stats.replica.to_data_row_column()
                        } else {
                            Data::null()
                        });

                    messages.push(row.message()?);
                }
            }
        }
        Ok(messages)
    }
}
