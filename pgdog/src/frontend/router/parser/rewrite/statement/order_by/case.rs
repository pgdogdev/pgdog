use pg_raw_parse::{Node, make, nodes, walk};
use std::collections::HashSet;

use crate::backend::schema::Schema;

use super::{OrderBySource, ProjectionRewritePlan, push_helper};

/// Project row-level CASE sort keys. Aggregation and DISTINCT need their own
/// rewrite rules because adding a target can change the query's semantics.
pub(in crate::frontend::router::parser::rewrite::statement) fn rewrite_cases<'a>(
    select: &mut nodes::SelectStmtMut<'a, '_>,
    mem: make::MemoryToken<'a>,
    schema: &Schema,
    plan: &mut ProjectionRewritePlan,
) {
    if !select
        .sort_clause()
        .iter()
        .any(|sort| matches!(sort.node(), Node::CaseExpr(_)))
        || !select.distinct_clause().is_empty()
        || !select.group_clause().is_empty()
        || !matches!(select.having_clause(), Node::None)
        || select.op != nodes::SetOperation::SETOP_NONE
    {
        return;
    }

    // Walk expressions as well as targets: an aggregate nested in CASE or
    // present only in ORDER BY must also prevent this rewrite. Without a
    // function catalog, conservatively leave function calls alone.
    let mut aggregate = false;
    let mut names = HashSet::new();
    walk::walk(Node::SelectStmt(select), |node| match node {
        Node::FuncCall(func) => {
            let name = func.funcname().iter().next_back().and_then(Node::as_str);
            aggregate |= schema.aggregate_functions.is_empty()
                || name.is_some_and(|name| schema.aggregate_functions.contains(name))
                || func.agg_star
                || func.agg_distinct
                || !func.agg_order().is_empty()
                || !matches!(func.agg_filter(), Node::None)
                || func.over().is_some();
        }
        Node::JsonAggConstructor(_) => aggregate = true,
        Node::ResTarget(target) => {
            if let Some(name) = target.name() {
                names.insert(name.to_owned());
            }
        }
        Node::String(string) => {
            if let Some(name) = string.sval() {
                names.insert(name.to_owned());
            }
        }
        _ => {}
    });
    if aggregate {
        return;
    }

    // Include known star expansions, and keep names deterministic so a cached
    // prepared variant still matches if its AST is evicted and rebuilt.
    names.extend(
        schema
            .relations
            .values()
            .flat_map(|relations| relations.values())
            .flat_map(|relation| relation.columns.keys().cloned()),
    );

    let mut helpers = Vec::new();
    for (position, mut sort) in select.sort_clause_mut().into_iter().enumerate() {
        if !matches!(sort.node(), Node::CaseExpr(_)) {
            continue;
        }

        let mut suffix = position;
        let alias = loop {
            let alias = format!("__pgdog_order_case{suffix}");
            if names.insert(alias.clone()) {
                break alias;
            }
            suffix += 1;
        };
        helpers.push(mem.make_res_target(
            Some(&alias),
            mem.empty(),
            mem.make_unique(sort.node()).uncast(),
        ));
        sort.set_node(
            mem.make_column_ref(mem.make_list(&[mem.make_string(Some(&alias)).uncast()]))
                .uncast(),
        );
        push_helper(
            plan,
            position,
            OrderBySource::Column(alias.clone()),
            alias,
            true,
        );
    }
    select
        .target_list_mut()
        .extend(mem, mem.make_list(&helpers));
}
