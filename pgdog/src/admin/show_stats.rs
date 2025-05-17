//! SHOW STATS.
use crate::backend::databases::databases;

use super::prelude::*;

pub struct ShowStats;

#[async_trait]
impl Command for ShowStats {
    fn name(&self) -> String {
        "SHOW STATS".into()
    }

    fn parse(_: &str) -> Result<Self, Error> {
        Ok(Self)
    }

    async fn execute(&self) -> Result<Vec<Message>, Error> {
        let mut fields = vec![
            Field::text("database"),
            Field::text("user"),
            Field::text("addr"),
            Field::numeric("port"),
            Field::numeric("shard"),
            Field::text("role"),
        ];
        fields.extend(
            ["total", "avg"]
                .into_iter()
                .flat_map(|prefix| {
                    [
                        Field::numeric(&format!("{}_xact_count", prefix)),
                        Field::numeric(&format!("{}_query_count", prefix)),
                        Field::numeric(&format!("{}_server_assignment_count", prefix)),
                        Field::numeric(&format!("{}_received", prefix)),
                        Field::numeric(&format!("{}_sent", prefix)),
                        Field::numeric(&format!("{}_xact_time", prefix)),
                        Field::numeric(&format!("{}_query_time", prefix)),
                        Field::numeric(&format!("{}_wait_time", prefix)),
                        // Field::numeric(&format!("{}_client_parse_count", prefix)),
                        Field::numeric(&format!("{}_server_parse_count", prefix)),
                        Field::numeric(&format!("{}_bind_count", prefix)),
                    ]
                })
                .collect::<Vec<Field>>(),
        );

        let mut messages = vec![RowDescription::new(&fields).message()?];

        let clusters = databases().all().clone();

        for (user, cluster) in clusters {
            let shards = cluster.shards();

            for (shard_num, shard) in shards.iter().enumerate() {
                let pools = shard.pools_with_roles();
                for (role, pool) in pools {
                    let stats = pool.state().stats;
                    let totals = stats.counts;
                    let averages = stats.averages;

                    let mut dr = DataRow::new();

                    dr.add(user.database.as_str())
                        .add(user.user.as_str())
                        .add(&pool.addr().host)
                        .add(pool.addr().port as i64)
                        .add(shard_num)
                        .add(role.to_string());

                    for stat in [totals, averages] {
                        dr.add(stat.xact_count)
                            .add(stat.query_count)
                            .add(stat.server_assignment_count)
                            .add(stat.received)
                            .add(stat.sent)
                            .add(stat.xact_time.as_millis() as u64)
                            .add(stat.query_time.as_millis() as u64)
                            .add(stat.wait_time.as_millis() as u64)
                            // .add(0_i64)
                            .add(stat.parse_count)
                            .add(stat.bind_count);
                    }

                    messages.push(dr.message()?);
                }
            }
        }
        Ok(messages)
    }
}
