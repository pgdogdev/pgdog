use crate::backend::databases::databases;

use super::prelude::*;

pub struct ShowFdw;

#[async_trait]
impl Command for ShowFdw {
    fn name(&self) -> String {
        "SHOW FDW".into()
    }

    fn parse(_sql: &str) -> Result<Self, Error> {
        Ok(ShowFdw)
    }

    async fn execute(&self) -> Result<Vec<Message>, Error> {
        let rd = RowDescription::new(&[
            Field::bigint("id"),
            Field::text("database"),
            Field::text("user"),
            Field::text("database_name"),
            Field::text("addr"),
            Field::numeric("port"),
            Field::text("role"),
            Field::numeric("sv_idle"),
            Field::numeric("sv_active"),
            Field::numeric("sv_total"),
            Field::numeric("total_xact_count"),
            Field::numeric("total_query_count"),
            Field::numeric("avg_xact_count"),
            Field::numeric("avg_query_count"),
        ]);
        let mut messages = vec![rd.message()?];

        for (user, cluster) in databases().all() {
            if let Some(pools) = cluster.fdw_pools() {
                for (role, _ban, pool) in pools {
                    let state = pool.state();
                    let stats = state.stats;
                    let mut row = DataRow::new();
                    row.add(pool.id() as i64)
                        .add(user.database.as_str())
                        .add(user.user.as_str())
                        .add(pool.addr().database_name.as_str())
                        .add(pool.addr().host.as_str())
                        .add(pool.addr().port as i64)
                        .add(role.to_string())
                        .add(state.idle)
                        .add(state.checked_out)
                        .add(state.total)
                        .add(stats.counts.xact_count)
                        .add(stats.counts.query_count)
                        .add(stats.averages.xact_count)
                        .add(stats.averages.query_count);
                    messages.push(row.message()?);
                }
            }
        }

        Ok(messages)
    }
}
