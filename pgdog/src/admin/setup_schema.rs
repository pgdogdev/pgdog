//! SETUP SHARDS
use crate::backend::{databases::databases, Schema};

use super::prelude::*;

pub struct SetupSchema;

#[async_trait]
impl Command for SetupSchema {
    fn name(&self) -> String {
        "SETUP SCHEMA".into()
    }

    fn parse(_: &str) -> Result<Self, Error> {
        Ok(Self)
    }

    async fn execute(&self) -> Result<Vec<Message>, Error> {
        let databases = databases();
        for cluster in databases.all().values() {
            Schema::install(cluster)
                .await
                .map_err(|e| Error::Backend(Box::new(e)))?;
        }

        Ok(vec![])
    }
}
