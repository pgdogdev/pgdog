use crate::frontend::comms::comms;

use super::prelude::*;

pub(crate) struct Shutdown;

#[async_trait]
impl Command for Shutdown {
    fn name(&self) -> String {
        "SHUTDOWN".into()
    }

    fn parse(_: &str) -> Result<Self, Error> {
        Ok(Shutdown {})
    }

    async fn execute(&self) -> Result<Vec<Message>, Error> {
        tracing::warn!("🐕 PgDog is shutting down (admin SHUTDOWN command)");
        comms().shutdown();

        Ok(vec![])
    }
}
