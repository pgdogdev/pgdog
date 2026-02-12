//! Schema operations.
pub mod columns;
pub mod relation;
pub mod sync;

pub use pgdog_stats::{
    Relation as StatsRelation, Relations as StatsRelations, Schema as StatsSchema, SchemaInner,
};
use serde::{Deserialize, Serialize};
use std::ops::DerefMut;
use std::{collections::HashMap, ops::Deref};
use tracing::debug;

pub use relation::Relation;

use super::{pool::Request, Cluster, Error, Server};
use crate::frontend::router::parser::Table;
use crate::net::parameter::ParameterValue;

static SETUP: &str = include_str!("setup.sql");

/// Load schema from database.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Schema {
    inner: StatsSchema,
}

impl Deref for Schema {
    type Target = StatsSchema;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for Schema {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl Schema {
    /// Load schema from a server connection.
    pub async fn load(server: &mut Server) -> Result<Self, Error> {
        let mut relations: StatsRelations = HashMap::new();
        for relation in Relation::load(server).await? {
            relations
                .entry(relation.schema().to_owned())
                .or_default()
                .insert(relation.name.clone(), relation.into());
        }

        let search_path = server
            .fetch_all::<String>("SHOW search_path")
            .await?
            .pop()
            .unwrap_or(String::from("$user, public"))
            .split(",")
            .map(|p| p.trim().replace("\"", ""))
            .collect();

        let inner = SchemaInner {
            search_path,
            relations,
        };

        Ok(Self {
            inner: StatsSchema::new(inner),
        })
    }

    /// The schema has been loaded from the database.
    pub(crate) fn is_loaded(&self) -> bool {
        !self.inner.relations.is_empty()
    }

    #[cfg(test)]
    pub(crate) fn from_parts(
        search_path: Vec<String>,
        relations: HashMap<(String, String), Relation>,
    ) -> Self {
        let mut nested: StatsRelations = HashMap::new();
        for ((schema, name), relation) in relations {
            nested
                .entry(schema)
                .or_default()
                .insert(name, relation.into());
        }
        Self {
            inner: StatsSchema::new(SchemaInner {
                search_path,
                relations: nested,
            }),
        }
    }

    /// Install PgDog functions and schema.
    pub async fn setup(server: &mut Server) -> Result<(), Error> {
        server.execute_checked(SETUP).await?;
        Ok(())
    }

    /// Install PgDog-specific functions and triggers.
    pub async fn install(cluster: &Cluster) -> Result<(), Error> {
        let shards = cluster.shards();
        let sharded_tables = cluster.sharded_tables();

        if shards.len() < 2 || sharded_tables.is_empty() {
            return Ok(());
        }

        for (shard_number, shard) in shards.iter().enumerate() {
            let mut server = shard.primary(&Request::default()).await?;
            Self::setup(&mut server).await?;
            let schema = Self::load(&mut server).await?;

            debug!("[{}] {:#?}", server.addr(), schema);

            for table in sharded_tables {
                for schema_table in schema
                    .tables()
                    .iter()
                    .filter(|table| table.schema() != "pgdog")
                {
                    let column_match = schema_table.columns().values().find(|column| {
                        column.column_name == table.column && column.data_type == "bigint"
                    });
                    if let Some(column_match) = column_match {
                        if table.name.is_none()
                            || table.name == Some(column_match.table_name.clone())
                        {
                            if table.primary {
                                let query = format!(
                                    "SELECT pgdog.install_next_id('{}', '{}', '{}', {}, {})",
                                    schema_table.schema(),
                                    schema_table.name,
                                    column_match.column_name,
                                    shards.len(),
                                    shard_number
                                );

                                server.execute(&query).await?;
                            }

                            let query = format!(
                                "SELECT pgdog.install_trigger('{}', '{}', '{}', {}, {})",
                                schema_table.schema(),
                                schema_table.name,
                                column_match.column_name,
                                shards.len(),
                                shard_number
                            );

                            server.execute(&query).await?;
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Get table by name.
    ///
    /// If the table has an explicit schema, looks up in that schema directly.
    /// Otherwise, iterates through the search_path to find the first match.
    pub fn table(
        &self,
        table: Table<'_>,
        user: &str,
        search_path: Option<&ParameterValue>,
    ) -> Option<&StatsRelation> {
        if let Some(schema) = table.schema {
            return self.inner.get(schema, table.name);
        }

        for schema in self.resolve_search_path(user, search_path) {
            if let Some(relation) = self.inner.get(schema, table.name) {
                return Some(relation.into());
            }
        }

        None
    }

    fn resolve_search_path<'a>(
        &'a self,
        user: &'a str,
        search_path: Option<&'a ParameterValue>,
    ) -> Vec<&'a str> {
        let path: &[String] = match search_path {
            Some(ParameterValue::Tuple(overriden)) => overriden.as_slice(),
            _ => &self.inner.search_path,
        };

        path.iter()
            .map(|p| if p == "$user" { user } else { p.as_str() })
            .collect()
    }

    /// Get all tables.
    pub fn tables(&self) -> Vec<&StatsRelation> {
        self.inner
            .relations
            .values()
            .flat_map(|tables| tables.values())
            .filter(|relation| relation.is_table())
            .collect()
    }

    /// Get all sequences.
    pub fn sequences(&self) -> Vec<&StatsRelation> {
        self.inner
            .relations
            .values()
            .flat_map(|tables| tables.values())
            .filter(|relation| relation.is_sequence())
            .collect()
    }

    /// Get search path components.
    pub fn search_path(&self) -> &[String] {
        &self.inner.search_path
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use indexmap::IndexMap;

    use crate::backend::pool::Request;
    use crate::backend::schema::relation::Relation;
    use crate::frontend::router::parser::Table;
    use crate::net::parameter::ParameterValue;

    use super::super::pool::test::pool;
    use super::Schema;

    #[tokio::test]
    async fn test_schema() {
        let pool = pool();
        let mut conn = pool.get(&Request::default()).await.unwrap();
        conn.execute("DROP SCHEMA IF EXISTS pgdog CASCADE")
            .await
            .unwrap();
        let _schema = Schema::load(&mut conn).await.unwrap();
        Schema::setup(&mut conn).await.unwrap();
        let schema = Schema::load(&mut conn).await.unwrap();
        let seq = schema
            .sequences()
            .into_iter()
            .find(|seq| seq.schema() == "pgdog")
            .cloned()
            .unwrap();
        assert!(
            matches!(
                seq.name.as_str(),
                "unique_id_seq" | "validator_bigint_id_seq"
            ),
            "{}",
            seq.name
        );

        let server_ok = conn.fetch_all::<i32>("SELECT 1 AS one").await.unwrap();
        assert_eq!(server_ok.first().unwrap().clone(), 1);

        let debug = conn
            .fetch_all::<String>("SELECT pgdog.debug()")
            .await
            .unwrap();
        assert!(debug.first().unwrap().contains("PgDog Debug"));
    }

    #[test]
    fn test_resolve_search_path_default() {
        let schema = Schema::from_parts(vec!["$user".into(), "public".into()], HashMap::new());

        let resolved = schema.resolve_search_path("alice", None);
        assert_eq!(resolved, vec!["alice", "public"]);
    }

    #[test]
    fn test_resolve_search_path_override() {
        let schema = Schema::from_parts(vec!["$user".into(), "public".into()], HashMap::new());

        let override_path = ParameterValue::Tuple(vec!["custom".into(), "other".into()]);
        let resolved = schema.resolve_search_path("alice", Some(&override_path));
        assert_eq!(resolved, vec!["custom", "other"]);
    }

    #[test]
    fn test_resolve_search_path_override_with_user() {
        let schema = Schema::from_parts(vec!["public".into()], HashMap::new());

        let override_path = ParameterValue::Tuple(vec!["$user".into(), "app".into()]);
        let resolved = schema.resolve_search_path("bob", Some(&override_path));
        assert_eq!(resolved, vec!["bob", "app"]);
    }

    #[test]
    fn test_table_with_explicit_schema() {
        let relations: HashMap<(String, String), Relation> = HashMap::from([
            (
                ("myschema".into(), "users".into()),
                Relation::test_table("myschema", "users", IndexMap::new()),
            ),
            (
                ("public".into(), "users".into()),
                Relation::test_table("public", "users", IndexMap::new()),
            ),
        ]);
        let schema = Schema::from_parts(vec!["$user".into(), "public".into()], relations);

        let table = Table {
            name: "users",
            schema: Some("myschema"),
            alias: None,
        };

        let result = schema.table(table, "alice", None);
        assert!(result.is_some());
        assert_eq!(result.unwrap().schema(), "myschema");
    }

    #[test]
    fn test_table_search_path_lookup() {
        let relations: HashMap<(String, String), Relation> = HashMap::from([(
            ("public".into(), "orders".into()),
            Relation::test_table("public", "orders", IndexMap::new()),
        )]);
        let schema = Schema::from_parts(vec!["$user".into(), "public".into()], relations);

        let table = Table {
            name: "orders",
            schema: None,
            alias: None,
        };

        // User schema "alice" doesn't have "orders", but "public" does
        let result = schema.table(table, "alice", None);
        assert!(result.is_some());
        assert_eq!(result.unwrap().schema(), "public");
    }

    #[test]
    fn test_table_found_in_user_schema() {
        let relations: HashMap<(String, String), Relation> = HashMap::from([
            (
                ("alice".into(), "settings".into()),
                Relation::test_table("alice", "settings", IndexMap::new()),
            ),
            (
                ("public".into(), "settings".into()),
                Relation::test_table("public", "settings", IndexMap::new()),
            ),
        ]);
        let schema = Schema::from_parts(vec!["$user".into(), "public".into()], relations);

        let table = Table {
            name: "settings",
            schema: None,
            alias: None,
        };

        // Should find in "alice" schema first (due to $user)
        let result = schema.table(table, "alice", None);
        assert!(result.is_some());
        assert_eq!(result.unwrap().schema(), "alice");
    }

    #[test]
    fn test_table_not_found() {
        let relations: HashMap<(String, String), Relation> = HashMap::from([(
            ("public".into(), "users".into()),
            Relation::test_table("public", "users", IndexMap::new()),
        )]);
        let schema = Schema::from_parts(vec!["$user".into(), "public".into()], relations);

        let table = Table {
            name: "nonexistent",
            schema: None,
            alias: None,
        };

        let result = schema.table(table, "alice", None);
        assert!(result.is_none());
    }

    #[test]
    fn test_table_with_overridden_search_path() {
        let relations: HashMap<(String, String), Relation> = HashMap::from([
            (
                ("custom".into(), "data".into()),
                Relation::test_table("custom", "data", IndexMap::new()),
            ),
            (
                ("public".into(), "data".into()),
                Relation::test_table("public", "data", IndexMap::new()),
            ),
        ]);
        let schema = Schema::from_parts(vec!["$user".into(), "public".into()], relations);

        let table = Table {
            name: "data",
            schema: None,
            alias: None,
        };

        // Override search_path to look in "custom" first
        let override_path = ParameterValue::Tuple(vec!["custom".into(), "public".into()]);
        let result = schema.table(table, "alice", Some(&override_path));
        assert!(result.is_some());
        assert_eq!(result.unwrap().schema(), "custom");
    }
}
