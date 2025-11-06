use pgdog_config::sharding::ShardedSchema;
use std::{collections::HashMap, ops::Deref, sync::Arc};

#[derive(Debug, Clone)]
pub struct ShardedSchemas {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    schemas: HashMap<String, ShardedSchema>,
    default_mapping: Option<ShardedSchema>,
}

impl Inner {
    fn new(schemas: Vec<ShardedSchema>) -> Self {
        let without_default = schemas
            .iter()
            .filter(|schema| !schema.is_default())
            .cloned();
        let default_mapping = schemas.iter().find(|schema| schema.is_default()).cloned();

        Self {
            schemas: without_default
                .into_iter()
                .map(|schema| (schema.name().to_string(), schema))
                .collect(),
            default_mapping,
        }
    }
}

impl Deref for ShardedSchemas {
    type Target = HashMap<String, ShardedSchema>;

    fn deref(&self) -> &Self::Target {
        &self.inner.schemas
    }
}

impl ShardedSchemas {
    pub fn get(&self, name: &str) -> Option<&ShardedSchema> {
        if let Some(schema) = self.inner.schemas.get(name) {
            Some(schema)
        } else {
            self.inner.default_mapping.as_ref()
        }
    }

    pub fn new(schemas: Vec<ShardedSchema>) -> Self {
        Self {
            inner: Arc::new(Inner::new(schemas)),
        }
    }
}

impl Default for ShardedSchemas {
    fn default() -> Self {
        Self::new(vec![])
    }
}
