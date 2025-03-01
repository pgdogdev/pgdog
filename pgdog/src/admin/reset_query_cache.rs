//! RESET QUERY_CACHE.
use crate::frontend::router::parser::Cache;

use super::prelude::*;

pub struct ResetQueryCache;

#[async_trait]
impl Command for ResetQueryCache {
    fn name(&self) -> String {
        "RESET QUERY CACHE".into()
    }

    fn parse(_: &str) -> Result<Self, Error> {
        Ok(Self)
    }

    async fn execute(&self) -> Result<Vec<Message>, Error> {
        Cache::reset();
        Ok(vec![])
    }
}
