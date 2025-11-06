use pgdog_config::sharding::ShardedSchema;
use std::{collections::HashMap, ops::Deref, sync::Arc};

#[derive(Debug, Clone)]
pub struct ShardedSchemas {
    schemas: Arc<HashMap<String, ShardedSchema>>,
}

impl Deref for ShardedSchemas {
    type Target = HashMap<String, ShardedSchema>;

    fn deref(&self) -> &Self::Target {
        &self.schemas
    }
}

impl ShardedSchemas {
    pub fn new(schemas: Vec<ShardedSchema>) -> Self {
        Self {
            schemas: Arc::new(
                schemas
                    .into_iter()
                    .map(|schema| (schema.name.clone(), schema))
                    .collect(),
            ),
        }
    }
}

impl Default for ShardedSchemas {
    fn default() -> Self {
        Self {
            schemas: Arc::new(HashMap::new()),
        }
    }
}
