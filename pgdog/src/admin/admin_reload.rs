//! FORCE RELOAD command.

use super::prelude::*;
use crate::backend::databases::{reload, terminate_active_connections};

pub(crate) struct ForceReload;

#[async_trait]
impl Command for ForceReload {
    fn name(&self) -> String {
        "FORCE_RELOAD".into()
    }

    fn parse(_sql: &str) -> Result<Self, Error> {
        Ok(ForceReload)
    }

    async fn execute(&self) -> Result<Vec<Message>, Error> {
        terminate_active_connections().await?;
        reload()?;

        Ok(vec![])
    }
}
