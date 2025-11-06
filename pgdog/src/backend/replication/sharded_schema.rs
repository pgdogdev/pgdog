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
    fn new(schemas: Vec<ShardedSchema>, default_shard: usize) -> Self {
        let database = schemas.first().map(|schema| schema.database.clone());
        Self {
            schemas: schemas
                .into_iter()
                .map(|schema| (schema.name.clone(), schema))
                .collect(),
            default_mapping: database.map(|database| ShardedSchema {
                database,
                name: "*".into(),
                shard: default_shard,
            }),
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
            inner: Arc::new(Inner::new(schemas, 0)),
        }
    }
}

impl Default for ShardedSchemas {
    fn default() -> Self {
        Self::new(vec![])
    }
}
