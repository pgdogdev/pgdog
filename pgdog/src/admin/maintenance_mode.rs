//! Turn maintenance mode on/off.
//!
//! Maintenance mode is special: it's completely independent from the config
//! and will hold true during config changes, e.g. when some databases disappear
//! and new ones are added.
//!
//! This is useful when changing the sharding config online, for example.
//!

use crate::backend::maintenance_mode;

use super::prelude::*;

/// Turn maintenance mode on/off, optionally for a single database.
#[derive(Default)]
pub struct MaintenanceMode {
    enable: bool,
    database: Option<String>,
}

#[async_trait]
impl Command for MaintenanceMode {
    fn parse(sql: &str) -> Result<Self, Error> {
        let parts = sql.split(" ").collect::<Vec<_>>();

        let (enable, database) = match parts[..] {
            ["maintenance", "on"] => (true, None),
            ["maintenance", "off"] => (false, None),
            ["maintenance", "on", database] => (true, Some(database.to_string())),
            ["maintenance", "off", database] => (false, Some(database.to_string())),
            _ => return Err(Error::Syntax),
        };

        Ok(Self { enable, database })
    }

    async fn execute(&self) -> Result<Vec<Message>, Error> {
        let database = self.database.as_deref();
        if self.enable {
            maintenance_mode::start(database);
        } else {
            maintenance_mode::stop(database);
        }

        Ok(vec![])
    }

    fn name(&self) -> String {
        let state = if self.enable { "ON" } else { "OFF" };
        match &self.database {
            Some(database) => format!("MAINTENANCE {} {}", state, database),
            None => format!("MAINTENANCE {}", state),
        }
    }
}
