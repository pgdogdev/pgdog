use crate::{
    backend::replication::ShardedSchemas,
    frontend::router::parser::{Schema, Shard},
    net::parameter::ParameterValue,
};

#[derive(Debug, Default, Clone)]
pub struct SchemaSharder {
    catch_all: bool,
    current: Option<Shard>,
    schema: Option<String>,
}

impl SchemaSharder {
    /// Resolve current schema.
    pub fn resolve(&mut self, schema: Option<Schema<'_>>, schemas: &ShardedSchemas) {
        if schemas.is_empty() {
            return;
        }

        let check = schemas.get(schema);
        if let Some(schema) = check {
            let catch_all = schema.is_default();
            let set =
                catch_all && self.current.is_none() || self.catch_all || self.current.is_none();
            if set {
                self.current = Some(schema.shard().into());
                self.catch_all = catch_all;
                self.schema = Some(schema.name().to_owned());
            }
        }
    }

    /// Resolve current schema from connection parameter.
    pub fn resolve_parameter(&mut self, parameter: &ParameterValue, schemas: &ShardedSchemas) {
        if schemas.is_empty() {
            return;
        }

        match parameter {
            ParameterValue::String(search_path) => {
                let schema = Schema::from(search_path.as_str());
                self.resolve(Some(schema), schemas)
            }

            ParameterValue::Tuple(search_paths) => {
                for schema in search_paths {
                    let schema = Schema::from(schema.as_str());
                    self.resolve(Some(schema), schemas);
                }
            }

            _ => (),
        }
    }

    pub fn get(&self) -> Option<(Shard, &str)> {
        if let Some(current) = self.current.as_ref() {
            if let Some(schema) = self.schema.as_ref() {
                return Some((current.clone(), schema.as_str()));
            }
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pgdog_config::sharding::ShardedSchema;

    #[test]
    fn test_catch_all_chosen_only_when_no_specific_match() {
        // Create a catch-all schema (no name) that routes to shard 0
        let catch_all = ShardedSchema {
            database: "test".to_string(),
            name: None, // This makes it a catch-all/default
            shard: 0,
            all: false,
        };

        // Create a specific schema "sales" that routes to shard 1
        let sales_schema = ShardedSchema {
            database: "test".to_string(),
            name: Some("sales".to_string()),
            shard: 1,
            all: false,
        };

        let schemas = ShardedSchemas::new(vec![catch_all, sales_schema]);

        // Test 1: When we resolve "sales", we should get shard 1 (specific match, not catch-all)
        let mut sharder = SchemaSharder::default();
        sharder.resolve(Some(Schema { name: "sales" }), &schemas);
        let result = sharder.get();
        assert_eq!(
            result.as_ref().map(|(s, _)| s.clone()),
            Some(Shard::Direct(1))
        );
        assert_eq!(result.as_ref().map(|(_, name)| *name), Some("sales"));
        assert!(!sharder.catch_all);

        // Test 2: When we resolve "unknown", we should get shard 0 (catch-all)
        let mut sharder = SchemaSharder::default();
        sharder.resolve(Some(Schema { name: "unknown" }), &schemas);
        let result = sharder.get();
        assert_eq!(
            result.as_ref().map(|(s, _)| s.clone()),
            Some(Shard::Direct(0))
        );
        assert_eq!(result.as_ref().map(|(_, name)| *name), Some("*"));
        assert!(sharder.catch_all);

        // Test 3: When we resolve None (no schema specified), we should get shard 0 (catch-all)
        let mut sharder = SchemaSharder::default();
        sharder.resolve(None, &schemas);
        let result = sharder.get();
        assert_eq!(
            result.as_ref().map(|(s, _)| s.clone()),
            Some(Shard::Direct(0))
        );
        assert_eq!(result.as_ref().map(|(_, name)| *name), Some("*"));
        assert!(sharder.catch_all);

        // Test 4: When catch-all is resolved first, then specific match,
        // the specific match should override the catch-all
        let mut sharder = SchemaSharder::default();
        sharder.resolve(Some(Schema { name: "unknown" }), &schemas); // catch-all first
        sharder.resolve(Some(Schema { name: "sales" }), &schemas); // specific second
        let result = sharder.get();
        assert_eq!(
            result.as_ref().map(|(s, _)| s.clone()),
            Some(Shard::Direct(1))
        );
        assert_eq!(result.as_ref().map(|(_, name)| *name), Some("sales"));
        assert!(!sharder.catch_all);

        // Test 5: When specific match is resolved first, catch-all should NOT override
        let mut sharder = SchemaSharder::default();
        sharder.resolve(Some(Schema { name: "sales" }), &schemas); // specific first
        sharder.resolve(Some(Schema { name: "unknown" }), &schemas); // catch-all second
        let result = sharder.get();
        assert_eq!(
            result.as_ref().map(|(s, _)| s.clone()),
            Some(Shard::Direct(1))
        );
        assert_eq!(result.as_ref().map(|(_, name)| *name), Some("sales"));
        assert!(!sharder.catch_all);
    }
}
