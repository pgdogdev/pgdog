//! RESET STATS.

use crate::backend::databases::reset_stats;

use super::prelude::*;

pub(crate) struct ResetStats;

#[async_trait]
impl Command for ResetStats {
    fn name(&self) -> String {
        "RESET STATS".into()
    }

    fn parse(_: &str) -> Result<Self, Error> {
        Ok(Self)
    }

    async fn execute(&self) -> Result<Vec<Message>, Error> {
        reset_stats();
        Ok(vec![])
    }
}
