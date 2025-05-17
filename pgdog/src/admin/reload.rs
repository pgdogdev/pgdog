//! RELOAD command.

use super::prelude::*;
use crate::backend::databases::reload;

pub struct Reload;

#[async_trait]
impl Command for Reload {
    fn name(&self) -> String {
        "RELOAD".into()
    }

    fn parse(_sql: &str) -> Result<Self, Error> {
        Ok(Reload)
    }

    async fn execute(&self) -> Result<Vec<Message>, Error> {
        let _ = reload(); // TODO: error check.
        Ok(vec![])
    }
}
