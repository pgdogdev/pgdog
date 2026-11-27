//! Cross-shard `DISTINCT ON`.
//!
//! PgDog keeps the first row of every key once the shards' rows are sorted,
//! so each row has to carry its key. A key the client didn't select, or
//! selected under a name that isn't unique in the result, is added to the
//! shard query as a hidden column.

use pg_raw_parse::{ConstValue, Node, make, nodes};

use crate::backend::schema::Schema;

use super::Error;
use super::order_by::{contains_aggregate, unique_output_name};
use super::projection::HiddenAliases;

const ALIAS_PREFIX: &str = "__pgdog_distinct_on_col";

/// A result column that is part of the `DISTINCT ON` key.
#[derive(Debug, PartialEq, Clone)]
pub(crate) enum DistinctColumn {
    /// SELECT list position, counted from 0.
    Index(usize),
    /// A column of the result, by its name.
    Name(String),
    /// A column added for the key, which the client doesn't get.
    Hidden(String),
}

/// Where the merged rows have their `DISTINCT ON` key.
#[derive(Debug, PartialEq)]
pub(crate) struct DistinctOnRewrite {
    columns: Vec<DistinctColumn>,
}

impl DistinctOnRewrite {
    /// Result columns that make up the key.
    pub(crate) fn columns(&self) -> &[DistinctColumn] {
        &self.columns
    }

    /// Hidden columns added for the key.
    pub(crate) fn aliases(&self) -> impl Iterator<Item = &str> {
        self.columns.iter().filter_map(|column| match column {
            DistinctColumn::Hidden(alias) => Some(alias.as_str()),
            _ => None,
        })
    }
}

/// Whether the SELECT has `DISTINCT ON (...)`, not a plain `DISTINCT`.
pub(super) fn is_distinct_on(select: &nodes::SelectStmt) -> bool {
    !matches!(select.distinct_clause().first(), None | Some(Node::None))
}

/// Make every `DISTINCT ON` key a column of the shard rows.
///
/// Runs after the `ORDER BY` rewrite: the two usually name the same
/// expressions, and a sort helper is reused as the key.
///
/// `merges_aggregates` is set when PgDog merges the aggregates of the query.
pub(super) fn rewrite_select<'a>(
    select: &mut nodes::SelectStmtMut<'a, '_>,
    mem: make::MemoryToken<'a>,
    schema: &Schema,
    merges_aggregates: bool,
) -> Result<Option<DistinctOnRewrite>, Error> {
    if !is_distinct_on(select) {
        return Ok(None);
    }

    let mut columns = Vec::with_capacity(select.distinct_clause().len());
    let mut helpers = Vec::new();
    // Only needed when a key has to be added.
    let mut hidden_aliases = None;

    for key in select.distinct_clause() {
        match key {
            // Postgres counts SELECT list positions from 1. It rejects
            // anything else itself, so there is no column to record.
            Node::A_Const(constant) => {
                if let Some(ConstValue::Integer(position)) = constant.val()
                    && let Some(position) = usize::try_from(position)
                        .ok()
                        .and_then(|position| position.checked_sub(1))
                {
                    columns.push(DistinctColumn::Index(position));
                }
                continue;
            }
            Node::ColumnRef(column) => {
                if let Some(name) = unique_output_name(select, column) {
                    columns.push(DistinctColumn::Name(name));
                    continue;
                }
            }
            // A shard's aggregate covers only its own rows, and only the
            // SELECT list's aggregates are merged.
            key if merges_aggregates && contains_aggregate(key, schema) => {
                return Err(Error::UnsupportedDistinctOn(
                    "aggregates in DISTINCT ON are unsupported",
                ));
            }
            _ => {}
        }

        let alias = hidden_aliases
            .get_or_insert_with(|| HiddenAliases::new(ALIAS_PREFIX, select.target_list()))
            .next_alias();
        helpers.push(mem.make_res_target(Some(&alias), mem.empty(), mem.make_unique(key).uncast()));
        columns.push(DistinctColumn::Hidden(alias));
    }

    if !helpers.is_empty() {
        select
            .target_list_mut()
            .extend(mem, mem.make_list(&helpers));
    }

    Ok(Some(DistinctOnRewrite { columns }))
}
