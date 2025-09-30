use super::prelude::*;
use crate::backend::{databases::databases, pool::monitor::Monitor};

#[derive(Default)]
pub struct Healthcheck {
    id: Option<u64>,
}

#[async_trait]
impl Command for Healthcheck {
    fn name(&self) -> String {
        "HEALTHCHECK".into()
    }

    fn parse(sql: &str) -> Result<Self, Error> {
        let parts = sql.split(" ").collect::<Vec<_>>();

        match parts[..] {
            ["healthcheck"] => Ok(Self::default()),
            ["healthcheck", id] => Ok(Self {
                id: Some(id.parse()?),
            }),
            _ => Err(Error::Syntax),
        }
    }

    async fn execute(&self) -> Result<Vec<Message>, Error> {
        for database in databases().all().values() {
            for shard in database.shards() {
                for pool in shard.pools() {
                    if let Some(id) = self.id {
                        if id != pool.id() {
                            continue;
                        }
                    }

                    let _ = Monitor::healthcheck(&pool).await;
                }
            }
        }
        Ok(vec![])
    }
}
