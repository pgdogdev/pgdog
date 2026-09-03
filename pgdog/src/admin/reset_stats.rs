//! RESET STATS.
use crate::backend::databases::databases;

use super::prelude::*;

pub struct ResetStats;

#[async_trait]
impl Command for ResetStats {
    fn name(&self) -> String {
        "RESET STATS".into()
    }

    fn parse(_: &str) -> Result<Self, Error> {
        Ok(Self)
    }

    async fn execute(&self) -> Result<Vec<Message>, Error> {
        databases().reset_stats();
        Ok(vec![])
    }
}
