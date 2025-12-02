//! SHOW REWRITE - per-cluster rewrite statistics

use crate::backend::databases::databases;

use super::prelude::*;

pub struct ShowRewrite;

#[async_trait]
impl Command for ShowRewrite {
    fn name(&self) -> String {
        "SHOW REWRITE".into()
    }

    fn parse(_: &str) -> Result<Self, Error> {
        Ok(Self)
    }

    async fn execute(&self) -> Result<Vec<Message>, Error> {
        let fields = vec![
            Field::text("database"),
            Field::text("user"),
            Field::numeric("parse"),
            Field::numeric("bind"),
            Field::numeric("simple"),
        ];

        let mut messages = vec![RowDescription::new(&fields).message()?];

        for (user, cluster) in databases().all() {
            let rewrite = {
                let stats = cluster.stats();
                let stats = stats.lock();
                stats.rewrite
            };

            let mut dr = DataRow::new();
            dr.add(user.database.as_str())
                .add(user.user.as_str())
                .add(rewrite.parse as i64)
                .add(rewrite.bind as i64)
                .add(rewrite.simple as i64);

            messages.push(dr.message()?);
        }

        Ok(messages)
    }
}
