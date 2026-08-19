//! Tables sharded in the database.
use pgdog_config::{LookupResult, OmnishardedTable, SystemCatalogsBehavior};

use crate::{
    config::DataType,
    frontend::router::{
        parser::Column,
        sharding::{LookupCache, LookupTable, Mapping, ShardedTable},
    },
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
pub struct CommonMapping {
    /// The column data type.
    pub data_type: DataType,
    /// The list/range mapping, if any.
    /// If none, the column is using hash sharding.
    pub mapping: Option<Mapping>,
    /// The sharding key lookup shared by every table, if configured.
    /// Bare values routed through the common mapping translate through
    /// it, same as values extracted from statements.
    pub lookup: Option<CommonLookup>,
}

/// Lookup translation applied to bare sharding key values
/// (`pgdog_sharding_key` comments and `SET pgdog.sharding_key`).
#[derive(Debug, Clone)]
pub struct CommonLookup {
    /// The configured lookup query.
    pub query: String,
    /// Cache key for translations.
    pub table: LookupTable,
    /// How the query result is interpreted: hashed, or the shard
    /// number itself.
    pub result: LookupResult,
}

/// Sharded tables.
#[derive(Debug, Clone)]
pub struct ShardedTables {
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
    pub fn new(
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
    pub fn with_lookup_cache(
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

    pub fn tables(&self) -> &[ShardedTable] {
        &self.inner.tables
    }

    /// Cached sharding key lookup query results.
    pub fn lookup_cache(&self) -> &LookupCache {
        &self.inner.lookup_cache
    }

    pub fn omnishards(&self) -> &HashMap<String, bool> {
        &self.inner.omnisharded
    }

    pub fn is_omnisharded_sticky(&self, name: &str) -> Option<bool> {
        self.omnishards().get(name).cloned()
    }

    pub fn is_omnisharded_sticky_default(&self) -> bool {
        self.inner.omnisharded_sticky
    }

    /// System catalogs are to be joined across shards.
    pub fn is_system_catalog_sharded(&self) -> bool {
        self.inner.system_catalogs == SystemCatalogsBehavior::Sharded
    }

    /// The deployment has only one sharded table.
    pub fn common_mapping(&self) -> &Option<CommonMapping> {
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
