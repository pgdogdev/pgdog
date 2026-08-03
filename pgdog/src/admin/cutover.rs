use crate::api::cutover_registry::CutoverTarget;
use crate::backend::replication::logical::Error as ReplicationError;

use super::prelude::*;

pub struct Cutover {
    target: CutoverTarget,
}

#[async_trait]
impl Command for Cutover {
    fn name(&self) -> String {
        "CUTOVER".into()
    }

    fn parse(sql: &str) -> Result<Self, Error> {
        let parts: Vec<&str> = sql.split_whitespace().collect();

        let target = match parts[..] {
            ["cutover"] => CutoverTarget::First,
            ["cutover", "shard", database, shard] => CutoverTarget::Shard {
                database: database.to_string(),
                shard: shard.parse().map_err(|_| Error::Syntax)?,
            },
            ["cutover", id] => CutoverTarget::Id(id.parse().map_err(|_| Error::Syntax)?),
            _ => return Err(Error::Syntax),
        };

        Ok(Cutover { target })
    }

    async fn execute(&self) -> Result<Vec<Message>, Error> {
        // Cut over the targeted task: by id, by database and shard, or
        // the first parked one.
        if !crate::api::cutover_registry::trigger_cutover(self.target.clone()) {
            return Err(ReplicationError::NotReplication.into());
        }

        let mut dr = DataRow::new();
        dr.add("OK");

        Ok(vec![
            RowDescription::new(&[Field::text("cutover")]).message()?,
            dr.message()?,
        ])
    }
}
