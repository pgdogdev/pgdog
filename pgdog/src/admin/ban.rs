use super::prelude::*;
use crate::backend::{databases::databases, pool};

#[derive(Default)]
pub struct Ban {
    id: Option<u64>,
    unban: bool,
}

#[async_trait]
impl Command for Ban {
    fn name(&self) -> String {
        if self.unban {
            "UNBAN".into()
        } else {
            "BAN".into()
        }
    }

    fn parse(sql: &str) -> Result<Self, Error> {
        let parts = sql.split(" ").collect::<Vec<_>>();

        match parts[..] {
            ["ban"] => Ok(Self::default()),
            ["unban"] => Ok(Self {
                unban: true,
                ..Default::default()
            }),
            ["ban", id] => Ok(Self {
                id: Some(id.parse()?),
                ..Default::default()
            }),

            ["unban", id] => Ok(Self {
                id: Some(id.parse()?),
                unban: true,
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

                    if self.unban {
                        pool.unban();
                    } else {
                        pool.ban(pool::Error::ManualBan);
                    }
                }
            }
        }
        Ok(vec![])
    }
}
