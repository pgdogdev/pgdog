//! Pause pool(s), closing backend connections and making clients
//! wait indefinitely.

use crate::backend::databases::databases;

use super::prelude::*;

/// Pause pool(s).
#[derive(Default)]
pub struct Pause {
    user: Option<String>,
    database: Option<String>,
    resume: bool,
}

#[async_trait]
impl Command for Pause {
    fn parse(sql: &str) -> Result<Self, Error> {
        let parts = sql.split(" ").collect::<Vec<_>>();

        match parts[..] {
            ["pause"] => Ok(Self::default()),
            ["resume"] => Ok(Self {
                user: None,
                database: None,
                resume: true,
            }),

            [cmd, database] => Ok(Self {
                user: None,
                database: Some(database.to_owned()),
                resume: cmd == "resume",
            }),

            [cmd, user, database] => Ok(Self {
                user: Some(user.to_owned()),
                database: Some(database.to_owned()),
                resume: cmd == "resume",
            }),

            _ => Err(Error::Syntax),
        }
    }

    async fn execute(&self) -> Result<Vec<Message>, Error> {
        for (name, cluster) in databases().all() {
            if let Some(ref user) = self.user {
                if &name.user != user {
                    continue;
                }
            }
            if let Some(ref database) = self.database {
                if &name.database != database {
                    continue;
                }
            }
            for shard in cluster.shards() {
                for pool in shard.pools() {
                    if self.resume {
                        pool.resume();
                    } else {
                        pool.pause();
                    }
                }
            }
        }

        Ok(vec![])
    }

    fn name(&self) -> String {
        if self.resume {
            "RESUME".into()
        } else {
            "PAUSE".into()
        }
    }
}
