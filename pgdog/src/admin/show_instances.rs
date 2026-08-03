//! SHOW INSTANCES.
//!
//! The pgdog fleet, as recorded in the `pgdog.instances` registry on
//! shard 0 of every database with a `schema_admin` user: which
//! instances are heartbeating, and which rows belong to dead ones.

use crate::backend::databases::databases;
use crate::backend::fleet::registry;

use super::prelude::*;

pub struct ShowInstances;

#[async_trait]
impl Command for ShowInstances {
    fn name(&self) -> String {
        "SHOW INSTANCES".into()
    }

    fn parse(_: &str) -> Result<Self, Error> {
        Ok(Self)
    }

    async fn execute(&self) -> Result<Vec<Message>, Error> {
        let mut messages = vec![
            RowDescription::new(&[
                Field::text("database"),
                Field::numeric("node_id"),
                Field::text("hostname"),
                Field::text("version"),
                Field::text("started_at"),
                Field::text("heartbeat_at"),
                Field::bool("live"),
            ])
            .message()?,
        ];

        for database in registry::schema_admin_databases() {
            let Ok(cluster) = databases().schema_owner(&database) else {
                continue;
            };
            let Ok(instances) = registry::list(&cluster, 0).await else {
                continue;
            };
            for instance in instances {
                let mut data_row = DataRow::new();
                data_row
                    .add(database.as_str())
                    .add(instance.node_id)
                    .add(instance.hostname.as_str())
                    .add(instance.version.as_str())
                    .add(instance.started_at.as_str())
                    .add(instance.heartbeat_at.as_str())
                    .add(instance.live);
                messages.push(data_row.message()?);
            }
        }

        Ok(messages)
    }
}
