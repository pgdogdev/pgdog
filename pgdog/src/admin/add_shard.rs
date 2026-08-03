//! Add a shard to a database.
//!
//! `ADD SHARD <database> <shard> [AUTO]` launches the
//! [`AddShardTask`]: provision the named shard, declared with
//! `provisioning = true` in the config — schema, omnisharded data,
//! logical-replication catch-up — and activate it, on
//! `CUTOVER <database> <shard>` or automatically with `AUTO`.
//! Progress in `SHOW TASKS`, abort with `STOP_TASK`.

use tracing::info;

use crate::api::add_shard::AddShardTask;
use crate::api::run_task;
use crate::backend::databases::databases;

use super::prelude::*;

/// Add a shard to a database.
pub struct AddShard {
    database: String,
    shard: usize,
    auto_cutover: bool,
}

#[async_trait]
impl Command for AddShard {
    fn parse(sql: &str) -> Result<Self, Error> {
        let parts = sql.split(" ").collect::<Vec<_>>();

        let (database, shard, auto_cutover) = match parts[..] {
            ["add", "shard", database, shard] => (database, shard, false),
            ["add", "shard", database, shard, "auto"] => (database, shard, true),
            _ => return Err(Error::Syntax),
        };

        Ok(Self {
            database: database.to_string(),
            shard: shard.parse().map_err(|_| Error::Syntax)?,
            auto_cutover,
        })
    }

    async fn execute(&self) -> Result<Vec<Message>, Error> {
        info!(r#"adding shard {} to "{}""#, self.shard, self.database);

        // Cheap validation now for an immediate error on a missing
        // database, schema_admin user, or provisioning entry; the
        // deeper guards run inside the task and surface in SHOW TASKS.
        databases().schema_owner(&self.database)?;
        crate::backend::databases::provisioning_cluster(&self.database, self.shard)?.shutdown();

        let task_id = run_task(
            AddShardTask::builder()
                .database(self.database.clone())
                .shard(self.shard)
                .maybe_publication(None)
                .auto_cutover(self.auto_cutover)
                .build(),
        )
        .id();

        let mut dr = DataRow::new();
        dr.add(task_id.to_string());

        Ok(vec![
            RowDescription::new(&[Field::text("task_id")]).message()?,
            dr.message()?,
        ])
    }

    fn name(&self) -> String {
        "ADD SHARD".into()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_parse_add_shard() {
        let cmd = AddShard::parse("add shard prod 2").unwrap();
        assert_eq!(cmd.database, "prod");
        assert_eq!(cmd.shard, 2);
        assert!(!cmd.auto_cutover);

        let cmd = AddShard::parse("add shard prod 2 auto").unwrap();
        assert_eq!(cmd.shard, 2);
        assert!(cmd.auto_cutover);

        // The shard is required and must be a number.
        assert!(AddShard::parse("add shard prod").is_err());
        assert!(AddShard::parse("add shard prod auto").is_err());
        assert!(AddShard::parse("add shard").is_err());
        assert!(AddShard::parse("add").is_err());
        assert!(AddShard::parse("add shard prod 2 extra auto").is_err());
    }
}
