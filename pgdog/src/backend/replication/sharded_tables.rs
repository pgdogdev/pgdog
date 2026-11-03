//! Tables sharded in the database.
use pgdog_config::{LookupResult, OmnishardedTable, SystemCatalogsBehavior};

use crate::{
    backend::Schema,
    config::DataType,
    frontend::router::{
        parser::{Column, Table},
        sharding::{LookupCache, LookupTable, Mapping, ShardedTable},
    },
    net::parameter::ParameterValue,
};
use std::{collections::HashMap, sync::Arc};

#[derive(Default, Debug)]
struct Inner {
    tables: Vec<ShardedTable>,
    omnisharded: HashMap<String, bool>, // Name <-> sticky routing
    /// This is set only if we have the same sharding scheme
    /// across all tables, i.e., 3 tables with the same data type
    /// and list/range/hash function.
    common_mapping: Option<CommonMapping>,
    omnisharded_sticky: bool,
    system_catalogs: SystemCatalogsBehavior,
    /// Cached sharding key lookup query results. Lives on the
    /// cluster: `ShardedTables` is rebuilt from config for every
    /// cluster `databases::from_config` creates, so the cache is
    /// recreated, i.e. emptied, on config reload.
    lookup_cache: LookupCache,
}

#[derive(Debug)]
pub(crate) struct CommonMapping {
    /// The column data type.
    pub(crate) data_type: DataType,
    /// The list/range mapping, if any.
    /// If none, the column is using hash sharding.
    pub(crate) mapping: Option<Mapping>,
    /// The sharding key lookup shared by every table, if configured.
    /// Bare values routed through the common mapping translate through
    /// it, same as values extracted from statements.
    pub(crate) lookup: Option<CommonLookup>,
}

/// Lookup translation applied to bare sharding key values
/// (`pgdog_sharding_key` comments and `SET pgdog.sharding_key`).
#[derive(Debug, Clone)]
pub(crate) struct CommonLookup {
    /// The configured lookup query.
    pub(crate) query: String,
    /// Cache key for translations.
    pub(crate) table: LookupTable,
    /// How the query result is interpreted: hashed, or the shard
    /// number itself.
    pub(crate) result: LookupResult,
}

/// Sharded tables.
#[derive(Debug, Clone)]
pub(crate) struct ShardedTables {
    inner: Arc<Inner>,
}

impl Default for ShardedTables {
    fn default() -> Self {
        Self {
            inner: Arc::new(Inner::default()),
        }
    }
}

impl From<&[ShardedTable]> for ShardedTables {
    fn from(value: &[ShardedTable]) -> Self {
        Self::new(
            value.to_vec(),
            vec![],
            false,
            SystemCatalogsBehavior::default(),
        )
    }
}

impl ShardedTables {
    pub(crate) fn new(
        tables: Vec<ShardedTable>,
        omnisharded_tables: Vec<OmnishardedTable>,
        omnisharded_sticky: bool,
        system_catalogs: SystemCatalogsBehavior,
    ) -> Self {
        Self::with_lookup_cache(
            tables,
            omnisharded_tables,
            omnisharded_sticky,
            system_catalogs,
            LookupCache::default(),
        )
    }

    /// Like [`Self::new`], with a configured sharding key lookup cache.
    pub(crate) fn with_lookup_cache(
        tables: Vec<ShardedTable>,
        omnisharded_tables: Vec<OmnishardedTable>,
        omnisharded_sticky: bool,
        system_catalogs: SystemCatalogsBehavior,
        lookup_cache: LookupCache,
    ) -> Self {
        // common_mapping is used by comment-directive and parameter-hint routing
        // (pgdog_sharding_key), which receive a bare value with no table name.
        // It is only valid when every table agrees on the same sharding scheme,
        // so that any table's function produces the same shard for the same key.
        //
        // Only data_type, mapping and lookup_query are compared because those
        // are the only fields the bare-value routing path reads. The lookup
        // cache key comes from the first table, so a bare value and a
        // statement-extracted value share cached translations when there's
        // one table (rule) configured.
        let common_mapping = match tables.split_first() {
            Some((first, rest))
                if rest.iter().all(|t| {
                    t.data_type == first.data_type
                        && t.mapping == first.mapping
                        && t.lookup_query == first.lookup_query
                        && t.lookup_result == first.lookup_result
                }) =>
            {
                Some(CommonMapping {
                    data_type: first.data_type,
                    mapping: first.mapping.clone(),
                    lookup: first.lookup_query.clone().map(|query| CommonLookup {
                        query,
                        table: LookupTable::from(first),
                        result: first.lookup_result,
                    }),
                })
            }
            _ => None,
        };

        Self {
            inner: Arc::new(Inner {
                tables,
                omnisharded: omnisharded_tables
                    .into_iter()
                    .map(|table| (table.name, table.sticky_routing))
                    .collect(),
                common_mapping,
                omnisharded_sticky,
                system_catalogs,
                lookup_cache,
            }),
        }
    }

    pub(crate) fn tables(&self) -> &[ShardedTable] {
        &self.inner.tables
    }

    /// Cached sharding key lookup query results.
    pub(crate) fn lookup_cache(&self) -> &LookupCache {
        &self.inner.lookup_cache
    }

    pub(crate) fn omnishards(&self) -> &HashMap<String, bool> {
        &self.inner.omnisharded
    }

    pub(crate) fn is_omnisharded_sticky(&self, name: &str) -> Option<bool> {
        self.omnishards().get(name).cloned()
    }

    pub(crate) fn is_omnisharded_sticky_default(&self) -> bool {
        self.inner.omnisharded_sticky
    }

    /// System catalogs are to be joined across shards.
    pub(crate) fn is_system_catalog_sharded(&self) -> bool {
        self.inner.system_catalogs == SystemCatalogsBehavior::Sharded
    }

    pub(crate) fn is_system_catalog_sticky(&self) -> bool {
        self.inner.system_catalogs == SystemCatalogsBehavior::OmnishardedSticky
    }

    pub(crate) fn is_table_sharded(
        &self,
        table: Table<'_>,
        db_schema: &Schema,
        user: &str,
        search_path: Option<&ParameterValue>,
    ) -> bool {
        if self.omnishards().contains_key(table.name) {
            return false;
        }

        let named = self.tables().iter().any(|config| {
            config.name.is_some()
                && config.name_matches(table.name)
                && config
                    .schema
                    .as_deref()
                    .is_none_or(|schema| table.schema == Some(schema))
        });

        if named {
            return true;
        }

        let nameless = || self.tables().iter().filter(|config| config.name.is_none());
        nameless().next().is_some()
            && db_schema
                .table(table, user, search_path)
                .is_some_and(|relation| {
                    nameless().any(|config| relation.has_column(&config.column))
                })
    }

    /// The deployment has only one sharded table.
    pub(crate) fn common_mapping(&self) -> &Option<CommonMapping> {
        &self.inner.common_mapping
    }

    /// Determine if the column is sharded and return its data type,
    /// as declared in the schema.
    pub(crate) fn get_table(&self, column: Column<'_>) -> Option<&ShardedTable> {
        // Only fully-qualified columns can be matched.
        let table = column.table()?;

        for candidate in &self.inner.tables {
            if let Some(table_name) = candidate.name.as_ref()
                && !table.name_match(table_name)
                && !candidate.name_matches(table.name)
            {
                continue;
            }

            if let Some(schema_name) = candidate.schema.as_ref()
                && let Some(schema) = table.schema()
                && schema.name != schema_name
            {
                continue;
            }

            if column.name == candidate.column {
                return Some(candidate);
            }
        }

        None
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::frontend::router::sharding::LookupTable;
    use pgdog_config::SystemCatalogsBehavior;

    #[test]
    fn test_lookup_cache_is_per_instance() {
        let build = || ShardedTables::new(vec![], vec![], false, SystemCatalogsBehavior::default());

        // Each `ShardedTables` gets its own cache: clusters rebuilt on
        // config reload start with an empty one.
        let first = build();
        let second = build();

        let table = LookupTable {
            schema: None,
            name: Some("tenants".into()),
            column: "tenant_id".into(),
        };
        first
            .lookup_cache()
            .insert(table.clone(), "child".into(), "root".into());

        assert!(first.lookup_cache().get(&table, "child").is_some());
        assert!(second.lookup_cache().get(&table, "child").is_none());

        // Clones of the same instance share it.
        assert!(first.clone().lookup_cache().get(&table, "child").is_some());
    }
}

#[cfg(test)]
mod lookup_result_test {
    use super::*;
    use crate::frontend::router::sharding::ShardedTable;

    fn table(lookup_result: LookupResult) -> ShardedTable {
        ShardedTable {
            database: "pgdog".into(),
            column: "org_id".into(),
            lookup_query: Some("SELECT shard_id FROM orgs WHERE id = $1".into()),
            lookup_result,
            ..Default::default()
        }
    }

    #[test]
    fn test_common_mapping_requires_lookup_result_agreement() {
        // Agreement: common mapping exists and carries the result.
        let tables = ShardedTables::new(
            vec![table(LookupResult::Shard), table(LookupResult::Shard)],
            vec![],
            false,
            SystemCatalogsBehavior::default(),
        );
        let mapping = tables.common_mapping().as_ref().unwrap();
        assert_eq!(mapping.lookup.as_ref().unwrap().result, LookupResult::Shard);

        // Disagreement: same query, different interpretation. No
        // common mapping, so bare keys can't be routed ambiguously.
        let tables = ShardedTables::new(
            vec![table(LookupResult::Shard), table(LookupResult::Value)],
            vec![],
            false,
            SystemCatalogsBehavior::default(),
        );
        assert!(tables.common_mapping().is_none());
    }
}

#[cfg(test)]
mod is_table_sharded_test {
    use super::*;
    use crate::backend::schema::{Relation, columns::StatsColumn};
    use indexmap::IndexMap;

    fn config(name: Option<&str>, schema: Option<&str>, column: &str) -> ShardedTable {
        ShardedTable {
            database: "pgdog".into(),
            name: name.map(Into::into),
            schema: schema.map(Into::into),
            column: column.into(),
            ..Default::default()
        }
    }

    fn sharded_tables(tables: Vec<ShardedTable>, omnisharded: &[&str]) -> ShardedTables {
        ShardedTables::new(
            tables,
            omnisharded
                .iter()
                .map(|name| OmnishardedTable {
                    name: name.to_string(),
                    sticky_routing: false,
                })
                .collect(),
            false,
            SystemCatalogsBehavior::default(),
        )
    }

    fn relation(schema: &str, name: &str, columns: &[&str]) -> ((String, String), Relation) {
        let columns: IndexMap<_, _> = columns
            .iter()
            .map(|column| {
                let stats = StatsColumn {
                    table_schema: schema.into(),
                    table_name: name.into(),
                    column_name: column.to_string(),
                    ..Default::default()
                };
                (column.to_string(), stats.into())
            })
            .collect();
        (
            (schema.into(), name.into()),
            Relation::test_table(schema, name, columns),
        )
    }

    fn db_schema() -> Schema {
        Schema::from_parts(
            vec!["public".into()],
            HashMap::from([
                relation("public", "orders", &["tenant_id", "id"]),
                relation("public", "countries", &["id", "code"]),
                relation("tenants", "invoices", &["tenant_id", "id"]),
            ]),
        )
    }

    fn table<'a>(schema: Option<&'a str>, name: &'a str) -> Table<'a> {
        Table {
            name,
            schema,
            alias: None,
        }
    }

    fn is_sharded(tables: &ShardedTables, table: Table<'_>) -> bool {
        tables.is_table_sharded(table, &db_schema(), "pgdog", None)
    }

    #[test]
    fn test_named_config_without_schema_matches_any_schema() {
        let tables = sharded_tables(vec![config(Some("orders"), None, "tenant_id")], &[]);

        assert!(is_sharded(&tables, table(None, "orders")));
        assert!(is_sharded(&tables, table(Some("public"), "orders")));
        assert!(is_sharded(&tables, table(Some("other"), "orders")));
        assert!(!is_sharded(&tables, table(None, "countries")));
    }

    #[test]
    fn test_named_config_with_schema_requires_matching_schema() {
        let tables = sharded_tables(
            vec![config(Some("orders"), Some("public"), "tenant_id")],
            &[],
        );

        assert!(is_sharded(&tables, table(Some("public"), "orders")));
        assert!(!is_sharded(&tables, table(Some("other"), "orders")));
    }

    #[test]
    fn test_nameless_config_uses_loaded_schema_columns() {
        let tables = sharded_tables(vec![config(None, None, "tenant_id")], &[]);

        assert!(is_sharded(&tables, table(None, "orders")));
        assert!(is_sharded(&tables, table(Some("tenants"), "invoices")));
        assert!(!is_sharded(&tables, table(None, "countries")));
        assert!(!is_sharded(&tables, table(None, "invoices")));
        assert!(!is_sharded(&tables, table(None, "unknown")));
    }

    #[test]
    fn test_omnisharded_config_overrides_sharded_config() {
        let tables = sharded_tables(
            vec![
                config(Some("orders"), None, "tenant_id"),
                config(None, None, "tenant_id"),
            ],
            &["orders"],
        );

        assert!(!is_sharded(&tables, table(None, "orders")));
        assert!(!is_sharded(&tables, table(Some("public"), "orders")));
        assert!(is_sharded(&tables, table(Some("tenants"), "invoices")));
    }
}
