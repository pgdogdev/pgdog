//! Pause the traffic, ignore all traffic coming from the clients, and proccess it only when resumed.

use tracing::info;

use crate::frontend::comms::comms;

use super::prelude::*;

/// Pause traffic.
#[derive(Default)]
pub struct PauseTraffic {
    resume: bool,
}

#[async_trait]
impl Command for PauseTraffic {
    fn parse(sql: &str) -> Result<Self, Error> {
        let parts = sql.split(" ").collect::<Vec<_>>();

        match parts[..] {
            ["pause_traffic"] => Ok(Self::default()),
            ["resume_traffic"] => Ok(Self { resume: true }),
            _ => Err(Error::Syntax),
        }
    }

    async fn execute(&self) -> Result<Vec<Message>, Error> {
        match self.resume {
            true => {
                comms().unpause_traffic();
                info!("Traffic resumed");
            }
            false => {
                comms().pause_traffic();
                info!("Traffic paused");
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
