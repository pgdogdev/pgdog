use std::collections::HashMap;

use crate::backend::{databases::User, pool::Address};
use crate::config::{ConfigAndUsers, Role};

use super::{Database, Shard};

pub struct Databases {
    databases: HashMap<User, Database>,
}

impl Databases {
    pub fn from_config(config: &ConfigAndUsers) -> Self {
        let mut result = HashMap::new();
        let databases = config.config.databases();
        // Get only the superusers, if any.
        let mut users = config.users.users();
        users.iter_mut().for_each(|(k, v)| {
            v.retain(|v| v.superuser);
        });
        users.retain(|_, v| !v.is_empty());

        for users in users.values() {
            // We only retained non-empty vectors above.
            let superuser = users.first().unwrap().clone();
            let user = User {
                user: superuser.name.clone(),
                database: superuser.database.clone(),
            };

            if let Some(shards) = databases.get(&user.database) {
                let shards = shards
                    .into_iter()
                    .map(|shard| {
                        let primary = shard
                            .iter()
                            .find(|db| db.role == Role::Primary)
                            .map(|primary| Address::new(primary, &superuser));
                        let replicas = shard
                            .iter()
                            .filter(|db| db.role == Role::Replica)
                            .map(|replica| Address::new(replica, &superuser))
                            .collect();

                        Shard { primary, replicas }
                    })
                    .collect();
                result.insert(user, Database { shards });
            }
        }

        Databases { databases: result }
    }
}
