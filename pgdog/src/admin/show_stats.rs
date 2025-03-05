//! SHOW STATS.
use crate::backend::{
    databases::databases,
    pool::{stats::Counts, Stats},
};

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
            Field::numeric("shard"),
        ];
        fields.extend(
            ["total", "avg"]
                .into_iter()
                .flat_map(|prefix| {
                    [
                        Field::numeric(&format!("{}_xact_count", prefix)),
                        Field::numeric(&format!("{}_server_assignment_count", prefix)),
                        Field::numeric(&format!("{}_received", prefix)),
                        Field::numeric(&format!("{}_sent", prefix)),
                        Field::numeric(&format!("{}_xact_time", prefix)),
                        Field::numeric(&format!("{}_query_time", prefix)),
                        Field::numeric(&format!("{}_wait_time", prefix)),
                        Field::numeric(&format!("{}_client_parse_count", prefix)),
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

            for (shard_num, shard) in shards.into_iter().enumerate() {
                let pools = shard.pools();
                let stats: Vec<Stats> = pools.into_iter().map(|pool| pool.state().stats).collect();
                let totals = stats.iter().map(|stats| stats.counts).sum::<Counts>();
                let averages = stats.iter().map(|stats| stats.averages).sum::<Counts>();

                let mut dr = DataRow::new();

                dr.add(user.database.as_str())
                    .add(user.user.as_str())
                    .add(shard_num);

                for stat in [totals, averages] {
                    dr.add(stat.xact_count)
                        .add(stat.server_assignment_count)
                        .add(stat.received)
                        .add(stat.sent)
                        .add(stat.xact_time)
                        .add(stat.query_time)
                        .add(stat.wait_time)
                        .add(0 as i64)
                        .add(0 as i64)
                        .add(0 as i64);
                }

                messages.push(dr.message()?);
            }
        }
        Ok(messages)
    }
}
