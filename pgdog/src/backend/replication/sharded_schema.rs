use pgdog_config::sharding::ShardedSchema;
use std::{collections::HashMap, ops::Deref, sync::Arc};

use crate::frontend::router::parser::Schema;

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
    /// Resolve multiple schemas to one schema and shard.
    pub fn resolve<'a>(&self, schemas: &[Option<Schema<'a>>]) -> Option<&ShardedSchema> {
        match &schemas {
            &[one] => {
                if let Some(schema) = one {
                    if let Some(schema) = self.inner.schemas.get(schema.name) {
                        return Some(schema);
                    }
                }

                self.inner.default_mapping.as_ref()
            }

            &[] => None,

            multiple => {
                for schema in multiple.iter() {
                    if let Some(schema) = schema {
                        if let Some(schema) = self.inner.schemas.get(schema.name) {
                            return Some(schema);
                        }
                    }
                }

                self.inner.default_mapping.as_ref()
            }
        }
    }

    pub fn get<'a>(&self, schema: Option<Schema<'a>>) -> Option<&ShardedSchema> {
        if let Some(schema) = schema {
            if let Some(schema) = self.inner.schemas.get(schema.name) {
                return Some(schema);
            }
        }

        self.inner.default_mapping.as_ref()
    }

    pub fn get_catch_all<'a>(&self, schema: Option<Schema<'a>>) -> Option<(&ShardedSchema, bool)> {
        if let Some(schema) = schema {
            if let Some(schema) = self.inner.schemas.get(schema.name) {
                return Some((schema, false));
            }
        }

        self.inner
            .default_mapping
            .as_ref()
            .map(|catch_all| (catch_all, true))
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
