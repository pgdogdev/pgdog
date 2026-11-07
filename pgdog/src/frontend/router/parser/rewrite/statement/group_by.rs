//! Route-dependent rewrite for cross-shard `GROUP BY`.

use std::collections::HashSet;

use pg_raw_parse::{Node, make, nodes};

use super::{
    Error,
    projection::{column_name, expands_star, target_output_name},
};
use crate::frontend::router::parser::Aggregate;

const ALIAS_PREFIX: &str = "__pgdog_group_";

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

    let mut taken: HashSet<String> = select
        .target_list()
        .iter()
        .filter_map(|target| target_output_name(target).map(str::to_owned))
        .collect();
    let mut next_alias = 0;
    let mut aliases = vec![];
    let mut hidden = vec![];
    let mut group_by = vec![];

    for node in select.group_clause().iter() {
        if let Some(column) = Aggregate::group_by_column(select, node) {
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
            // Shards can't resolve a SELECT alias inside the hidden column.
            Node::ColumnRef(column) if groups_by_output_alias(select, column) => {
                return Err(Error::UnsupportedGroupBy(
                    "GROUP BY on a SELECT alias is unsupported",
                ));
            }
            _ => {}
        }
        let alias = loop {
            let alias = format!("{ALIAS_PREFIX}{next_alias}");
            next_alias += 1;
            if taken.insert(alias.clone()) {
                break alias;
            }
        };
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

fn groups_by_output_alias(select: &nodes::SelectStmt, column: &nodes::ColumnRef) -> bool {
    column.fields().len() == 1
        && select.target_list().iter().any(|target| {
            target
                .name()
                .is_some_and(|name| Some(name) == column_name(column))
        })
}
