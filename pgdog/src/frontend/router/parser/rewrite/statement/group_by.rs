//! Route-dependent rewrite for cross-shard `GROUP BY`.

use pg_raw_parse::{Node, make, nodes};

use super::{
    Error,
    projection::{HiddenAliases, expands_star},
};
use crate::{
    backend::schema::Schema,
    frontend::router::parser::{Aggregate, GroupByAlias},
};

const ALIAS_PREFIX: &str = "__pgdog_group_";

pub(super) const UNRESOLVED_ALIAS: &str = "GROUP BY on a SELECT alias is only supported when FROM has just tables \
     PgDog has loaded the schema for";

/// GROUP BY keys added to the shard query as hidden columns.
#[derive(Debug, PartialEq)]
pub(crate) struct GroupByRewrite {
    aliases: Vec<String>,
    aggregate: Aggregate,
}

impl GroupByRewrite {
    pub(crate) fn new(aliases: Vec<String>, aggregate: Aggregate) -> Self {
        Self { aliases, aggregate }
    }

    pub(crate) fn aliases(&self) -> &[String] {
        &self.aliases
    }

    /// The aggregate with every GROUP BY key, including the hidden ones.
    pub(crate) fn aggregate(&self) -> &Aggregate {
        &self.aggregate
    }
}

/// Select every GROUP BY key that's missing from the SELECT list, so the
/// shard groups are merged on all of their keys.
///
/// ```sql
/// SELECT category FROM items GROUP BY category, region
/// ```
///
/// is sent to every shard as
///
/// ```sql
/// SELECT category, region AS __pgdog_group_0 FROM items GROUP BY category, region
/// ```
///
/// Returns `None` if every key is already selected.
pub(super) fn rewrite_select<'a>(
    select: &mut make::Unique<'a, &'a nodes::SelectStmt>,
    mem: make::MemoryToken<'a>,
    aggregate: &Aggregate,
    schema: &Schema,
) -> Result<Option<GroupByRewrite>, Error> {
    if aggregate.group_by().len() == select.group_clause().len() {
        return Ok(None);
    }
    // Merged rows are read by SELECT list position, which star expansion shifts.
    if expands_star(select) {
        return Err(Error::UnsupportedGroupBy(
            "SELECT * with GROUP BY columns that aren't selected is unsupported",
        ));
    }

    let mut hidden_aliases = HiddenAliases::new(ALIAS_PREFIX, select.target_list());
    let mut aliases = vec![];
    let mut hidden = vec![];
    let mut group_by = vec![];

    for node in select.group_clause().iter() {
        if let Some(column) = Aggregate::group_by_column(select, node, schema) {
            group_by.push(column);
            continue;
        }
        match node {
            // A merged row can't tell which grouping set it came from.
            Node::GroupingSet(_) => {
                return Err(Error::UnsupportedGroupBy(
                    "GROUPING SETS, ROLLUP and CUBE are unsupported",
                ));
            }
            // Shards can't resolve a SELECT alias inside the hidden column,
            // so the name has to be a known column of the FROM tables.
            Node::ColumnRef(column)
                if GroupByAlias::resolve(select, column, schema) == Some(GroupByAlias::Unknown) =>
            {
                return Err(Error::UnsupportedGroupBy(UNRESOLVED_ALIAS));
            }
            _ => {}
        }
        let alias = hidden_aliases.next_alias();
        group_by.push(select.target_list().len() + hidden.len());
        hidden.push(mem.make_res_target(Some(&alias), mem.empty(), mem.make_unique(node)));
        aliases.push(alias);
    }

    select
        .as_mut()
        .target_list_mut()
        .extend(mem, mem.make_list(&hidden));

    Ok(Some(GroupByRewrite::new(
        aliases,
        aggregate.clone().with_group_by(group_by),
    )))
}
