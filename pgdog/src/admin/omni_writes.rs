//! Pause/resume writes to omnisharded tables for a database.
//!
//! Used while the database's topology changes underneath them, e.g.
//! during the `ADD SHARD` cutover: omni writes must reach every shard,
//! including the one being swapped in. Sharded traffic and reads are
//! unaffected. Like maintenance mode, this holds across config reloads.
//!

use crate::backend::fleet::barrier as omni_write_barrier;

use super::prelude::*;

/// Pause or resume omnisharded writes for a database.
pub struct OmniWrites {
    enable: bool,
    database: String,
}

#[async_trait]
impl Command for OmniWrites {
    fn parse(sql: &str) -> Result<Self, Error> {
        let parts = sql.split(" ").collect::<Vec<_>>();

        let (enable, database) = match parts[..] {
            ["omni_writes", "on", database] => (true, database.to_string()),
            ["omni_writes", "off", database] => (false, database.to_string()),
            _ => return Err(Error::Syntax),
        };

        Ok(Self { enable, database })
    }

    async fn execute(&self) -> Result<Vec<Message>, Error> {
        if self.enable {
            omni_write_barrier::stop(&self.database);
        } else {
            omni_write_barrier::start(&self.database);
        }

        Ok(vec![])
    }

    fn name(&self) -> String {
        let state = if self.enable { "ON" } else { "OFF" };
        format!("OMNI_WRITES {} {}", state, self.database)
    }
}
