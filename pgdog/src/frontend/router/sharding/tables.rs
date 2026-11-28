use pg_raw_parse::{Node, nodes};
use pgdog_config::{DataType, Hasher, LookupResult};
use pgdog_vector::Vector;

use crate::{
    backend::ShardingSchema,
    frontend::router::parser::{Column, Table},
};

use super::Mapping;

/// Runtime representation of a sharded table, derived from [`pgdog_config::ShardedTableConfig`].
#[derive(Debug, Default, Clone, PartialEq)]
pub(crate) struct ShardedTable {
    pub(crate) database: String,
    pub(crate) name: Option<String>,
    pub(crate) prefix: bool,
    pub(crate) schema: Option<String>,
    pub(crate) column: String,
    pub(crate) primary: bool,
    pub(crate) centroids: Vec<Vector>,
    pub(crate) data_type: DataType,
    pub(crate) centroid_probes: usize,
    pub(crate) hasher: Hasher,
    pub(crate) mapping: Option<Mapping>,
    pub(crate) lookup_query: Option<String>,
    pub(crate) lookup_result: LookupResult,
}

impl ShardedTable {
    // Returns true if the INSERT sends the sharding key column (as opposed to relying on DEFAULT)
    pub(crate) fn insert_sends_key_column(&self, insert: &nodes::InsertStmt) -> bool {
        insert.cols().is_empty()
            || insert.cols().into_iter().any(|column| {
                matches!(column, Node::ResTarget(target) if target.name() == Some(self.column.as_str()))
            })
    }

    /// Whether `table` matches this table's configured name (or prefix).
    pub(crate) fn name_matches(&self, table: &str) -> bool {
        self.name.as_deref().is_some_and(|name| {
            if self.prefix {
                table.starts_with(name)
            } else {
                table == name
            }
        })
    }
}

/// The settings in the sharding config that decide which shards the rows of sharded tables are on
/// - the data type of the sharding key
/// - the hash function (`hasher`)
/// - the mapping from values to shards (e.g., lists or ranges)
///
/// E.g., if users and orders have equal `ShardingFunction`s, the users with id 5 and the orders with user_id 5 are on the same shard
/// So for JOINs ON users.id = orders.user_id, the rows that the JOINs join fundamentally align to the same shard
#[derive(Debug, PartialEq)]
pub(crate) struct ShardingFunction<'a> {
    data_type: &'a DataType,
    hasher: &'a Hasher,
    mapping: Option<&'a Mapping>,
}

impl<'a> ShardingFunction<'a> {
    /// Returns the sharding function of `table`
    /// E.g., for orders sharded by the hash of the bigint column user_id, we return bigint and the hasher and mapping that the sharding config sets for orders
    ///
    /// Returns None for tables sharded by centroids (vectors) or by lookup queries (queries that we run to find the shards corresponding to values)
    /// For those tables, the data type, hasher and mapping don't decide the shards
    pub(crate) fn of(table: &'a ShardedTable) -> Option<Self> {
        if !table.centroids.is_empty() || table.lookup_query.is_some() {
            return None;
        }

        Some(Self {
            data_type: &table.data_type,
            hasher: &table.hasher,
            mapping: table.mapping.as_ref(),
        })
    }
}

#[derive(Debug)]
pub(crate) struct Key<'a> {
    pub(crate) table: &'a ShardedTable,
    pub(crate) position: usize,
}

pub(crate) struct Tables<'a> {
    schema: &'a ShardingSchema,
}

impl<'a> Tables<'a> {
    pub(crate) fn new(schema: &'a ShardingSchema) -> Self {
        Tables { schema }
    }

    pub(crate) fn sharded(&'a self, table: Table) -> Option<&'a ShardedTable> {
        let tables = self.schema.tables().tables();

        tables.iter().find(|t| t.name_matches(table.name))
    }

    pub(crate) fn key(&'a self, table: Table, columns: &'a [Column]) -> Option<Key<'a>> {
        let tables = self.schema.tables().tables();

        // Check tables with name first.
        let sharded = tables.iter().find(|t| t.name_matches(table.name));

        if let Some(sharded) = sharded
            && let Some(position) = columns.iter().position(|col| col.name == sharded.column)
        {
            return Some(Key {
                table: sharded,
                position,
            });
        }

        // Check tables without name.
        let key: Option<(&'a ShardedTable, Option<usize>)> = tables
            .iter()
            .filter(|table| table.name.is_none())
            .map(|t| (t, columns.iter().position(|col| col.name == t.column)))
            .find(|t| t.1.is_some());
        if let Some(key) = key
            && let Some(position) = key.1
        {
            return Some(Key {
                table: key.0,
                position,
            });
        }

        None
    }
}
