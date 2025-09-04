//! Turn maintenance mode on/off.

use crate::backend::maintenance_mode;

use super::prelude::*;

/// Turn maintenance mode on/off.
#[derive(Default)]
pub(crate) struct MaintenanceMode {
    enable: bool,
}

#[async_trait]
impl Command for MaintenanceMode {
    fn parse(sql: &str) -> Result<Self, Error> {
        let parts = sql.split(" ").collect::<Vec<_>>();

        match parts[..] {
            ["maintenance", "on"] => Ok(Self { enable: true }),
            ["maintenance", "off"] => Ok(Self { enable: false }),
            _ => Err(Error::Syntax),
        }
    }

    async fn execute(&self) -> Result<Vec<Message>, Error> {
        if self.enable {
            maintenance_mode::start();
        } else {
            maintenance_mode::stop();
        }

        Ok(vec![])
    }

    fn name(&self) -> String {
        format!("MAINTENANCE {}", if self.enable { "ON" } else { "OFF" })
    }
}
